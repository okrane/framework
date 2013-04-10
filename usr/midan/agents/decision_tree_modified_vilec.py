'''
Created on 15 nov. 2010

@author: midan
'''
from simep.core.subtactic import Tactic
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog
from random import *
import sqlite3
import re


class DecisionTree(Tactic):
    decision_tree = None
    def __init__(self, setup, context, parameters):
        Tactic.__init__(self, setup, context, parameters)
        # assign the value of threshold to be aggressive
        self.threshold_aggressive = 40
        self.trade_list= list()
        self.aggressive_opposite_quantity_ratio = 0.5
        
        if DecisionTree.decision_tree == None:
            DecisionTree.decision_tree = self.load_decision_tree()
        
                
    
    def get_current_vwap(self):
        price_vol = 0
        volume    = 0
        for t in self.trade_list :
            price_vol += t.size * t.price
            volume    += t.size 
        
        if volume == 0 : 
            vwap = 0
        else:
            vwap = price_vol/volume
        return volume,vwap
        
                
    
    '''######################################################################################################
    ######################################   PURE VIRTUAL FUNCTIONS   #######################################
    ######################################################################################################'''
    
    def _pvtl__update_child_plot_curves(self):
        pass
    
    def _pvtl__update_child_plot_figure(self):
        pass
    
    def _pvtl__update_last_plot(self):
        market_volume_str = str(int(self._bus['market_volume']))
        self._figure_title = self._figure_title.replace(']', ', market_volume=' + market_volume_str + ']')
        
    

    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
    def process(self, evt):        
        #print self['MultDt']
        #print self['MinMultAts']
        #print self['ParentRefPrice']
        if not self.marketManager.getFeedInfo('AE'):
            print "Waiting for AE Feed..."
            return
        
        trades = self.marketManager.getTrades(evt.getMarketId())
        for t in trades :
            self.trade_list.append(t)
            
        self.log ("[%d] ExecQty = %d. RemainingQty = %d. AvailableQty = %d. LeavesQty = %d. OptimalQty = %d" % 
            (self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'),
             self.moneyManager.getExecQty(), 
             self.moneyManager.getTotalQty() - self.moneyManager.getExecQty(),
             self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty(),
             self.moneyManager.getLeavesQty(), 
             self['OptimalQty']))
                
        ats = int(self.marketManager.getFeedInfo('AE', 'A_QTY_3') / self.marketManager.getFeedInfo('AE', 'A_NPLRS_3') * (1.0 + random() * 0.2 - 0.1))        
                
        
        
        '''####################################################################################################################
        ######################################  Call Decision Tree  ########################################################
        ####################################################################################################################'''      
        
        need_to_be_aggressive = False
        
        if self._bus['imb_p_s_600'] == None :
            predicted_value = None
        else:
            divider = (100-self._bus['imb_p_s_600'])
            if divider > 0: 
                predicted_value = self.get_predicted_value([float(100*self._bus['imb_p_s_600'])/divider, # v@buy/v@sell 10' in % 
                    self._bus['volume_less_arrival_p_s_600'], # volume less than arrival price 10' in % 
                    self._bus['garman_klass_bp_s_900'],       # volGK 15' in bps
                    self._bus['vwavg_spread_bp_s_900'],       # vwas  15' in bps
                    self._bus['imb_p_s_900'],                 # imbalance bid-ask 15' in %
                    self._bus['ret_p_s_900']])                 # return 15' in %)
            else:
                predicted_value = None
                 
        if predicted_value is not None:
            need_to_be_aggressive  = (predicted_value < self.threshold_aggressive) 
                
        
        '''####################################################################################################################
        ######################################  Aggressive Tracking  #######################################################
        ####################################################################################################################'''
        reserved_quantity = 0
        callbacks = self.bus.get('%s_NoCallbacks' % self['ParentAutomatonOrderID'])
        if not callbacks:
            callbacks = 1        
        
        
        # Check if in the money
        if self.side * self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1') < self.side * self['ParentRefPrice']: 
        
            qty_aggressive = self.aggressive_opposite_quantity_ratio * self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE_SIZE1')
            qty_aggressive = min(max(1 * ats, min(qty_aggressive, 8 * ats)), self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - self.moneyManager.getLeavesQty())            

            if need_to_be_aggressive:
                self.createLimitOrder(evt.getMarketId(), 
                                      self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1'), 
                                      qty_aggressive, 
                                      execType = 'Fak', 
                                      prefix = 'AggressiveTracking')
                reserved_quantity = qty_aggressive
                self.log('Aggressive Order Fak: %d@%f. BidAsk: [%d|%f - %d|%f]. PredictedValue: %f.' % (qty_aggressive, self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST_OPPOSITE1'), self.marketManager.getBid(0).size, self.marketManager.getBid(0).price, self.marketManager.getAsk(0).size, self.marketManager.getAsk(0).price, predicted_value))
            
        self.bus.put('%s_NoCallbacks' % self['ParentAutomatonOrderID'], callbacks + 1)
        
        
        '''####################################################################################################################
        ######################################  Passive Tracking  ##########################################################
        ####################################################################################################################'''
        price = self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST1') - self.side * self.marketManager.getFeedInfo('AE', 'KO_DIST') * self.getReferenceData('PriceStep', venue_id = evt.getMarketId(), price = self.marketManager.getFeedInfo(evt.getMarketId(), 'BEST1'))
        if self['Price']:
            price = max(price, self['Price']) if self.side == 1 else min(price, self['Price'])            
        
        #Compute Quantity
        quantity = int(self['OptimalQty'] / 5 * (1 + random() * 0.2 - 0.1)) # TODO: splitting of orders
        quantity = min(max(1 * ats, min(quantity, 8 * ats)), self.moneyManager.getTotalQty() - self.moneyManager.getExecQty() - reserved_quantity)
        
        
        if not self.moneyManager.getPrivateBook():      
            self.createLimitOrder(evt.getMarketId(), price, quantity, prefix = 'PassiveTracking' )
            self.next_deal = int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) + self.marketManager.getFeedInfo('AE', 'PARCL_SIZE') * self.marketManager.getFeedInfo('AE', 'CONV_FAC')
            self.order_status = True
            self.log('Insert Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity, price, self.marketManager.getBid(0).size, self.marketManager.getBid(0).price, self.marketManager.getAsk(0).size, self.marketManager.getAsk(0).price, self.next_deal))
        
        elif self.next_deal <= int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) or not self.order_status:
            order_id = self.moneyManager.getPrivateBook().keys()[0] # always only 1 single order on the market            
            
            if (self.order_status and (self.moneyManager.getPrivateBook()[order_id]['Price'] != price or self.moneyManager.getPrivateBook()[order_id]['LeavesQty'] < quantity)) \
               or (not self.order_status and self.side * self.moneyManager.getPrivateBook()[order_id]['Price'] < self.side * price) :
                
                self.modifyOrder(evt.getMarketId(), order_id, price, quantity)
                self.next_deal = int(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM')) + self.marketManager.getFeedInfo('AE', 'PARCL_SIZE') * self.marketManager.getFeedInfo('AE', 'CONV_FAC')
                self.order_status = False
                
                self.log('Modify Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity, price, self.marketManager.getBid(0).size, self.marketManager.getBid(0).price, self.marketManager.getAsk(0).size, self.marketManager.getAsk(0).price, self.next_deal))            
            
            elif not self.order_status and self.side * self.moneyManager.getPrivateBook()[order_id]['Price'] >= self.side * price:
                self.order_status = True
            
    
    def processReport(self, evt):
        total_qty_so_far = self.bus.get('%s_TotalQuantity' % self['ParentAutomatonOrderID'])
        if not total_qty_so_far:
            total_qty_so_far = 0
        for x in self.moneyManager.getLastExecutions():
            self.log("Order Filled: Qty=%d Price=%f" % (x['Quantity'], x['Price']))
            total_qty_so_far += x['Quantity']
        
        self.log('Total Exec Quantity by Simep Agent: %d' % total_qty_so_far)
        self.bus.put('%s_TotalQuantity' % self['ParentAutomatonOrderID'], total_qty_so_far)
    
    
    def acceptSubscription(self, sub_id, sub):
        return sub_id == self.primary_venue_id

    def acceptVenue(self, venue_id, venue):
        if venue.info["type"] != "Lit":
            return False        
        if venue.info["primary"] == '1':
            self.primary_venue_id = venue_id
            return True
        return False

    def extraSubscriptions(self):
        ''' Returns any extra subscriptions needed. 
            @return
                Dictionary: {name: (Source, Ric, ReqdProcess)}
        '''
        return {'AE':('VWAP_P', 'S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc' % (self['InstrumentID'], self.primary_venue_id, self['EffectiveTime'][9:], self['ExpireTime'][9:]) )}
                
    def list2Dict(self, list, prefix):
        d = dict()
        for i in range(len(list)):
            d['%s%d' % (prefix, i)] = [list[i]] 
        return d
    
    def print_orderbook(self, lob):
        lob_str  = '%s BID1: %05d @ %06.03f' %(self._timestamp + ',', lob.getBid(0).size, lob.getBid(0).price)
        lob_str += '    '
        lob_str += 'ASK1: %05d @ %06.03f' %(lob.getAsk(0).size, lob.getAsk(0).price)
        if self['print_orderbook']:
            pyLog('')
            pyLog(lob_str)
        for i in range(1,5):
            lob_str  = '%s BID%d: %05d @ %06.03f' %('             ', i+1, lob.getBid(i).size, lob.getBid(i).price)
            lob_str += '    '
            lob_str += 'ASK%d: %05d @ %06.03f' %(i+1, lob.getAsk(i).size, lob.getAsk(i).price)
            if self['print_orderbook']:
                pyLog(lob_str)
        if self['print_orderbook']:
            pyLog('')
            raw_input("Press 'Enter' to continue...")
    
    # this function requires 4 vectors scripting a regression tree
    # which is computed from MATLAB
    # - splitVar     : variable used to split a node
    # - cutoffVal    : threshold value to split a node
    # - childrenNode : children nodes of one node
    # - nodeMean     : mean value, used as predicted value 
    def get_predicted_value(self, vec_indicator):
        if sum([vec_indicator[i] is not None for i in range(len(vec_indicator))]) == 0: 
            next_node = 0
            while self.decision_tree['splitVar'][next_node] > 0:            
                split_var  = self.decision_tree['splitVar'][next_node]
                if vec_indicator[split_var] < self.decision_tree['cutoffVal'][next_node]:
                    next_node = self.decision_tree['leftNode'][next_node]
                else:
                    next_node = self.decision_tree['rightNode'][next_node]            
                     
            return self.decision_tree['nodeMean'][next_node]
        else:
            return None
        
    def load_decision_tree(self):
        conn = sqlite3.connect('C:/st_sim/simep/databases/decision_tree')
        conn.row_factory = sqlite3.Row
        c = conn.cursor()       
        splitter = re.compile('[\\[\\]\;]')
        c.execute("select split_var, cutoff_val, left_node, right_node, node_mean from decision_tree where trading_destination_id = %d and security_id = %d" \
                  % (self['trading_destination_id'], self['security_id']))
        decision_tree = {}
        for data in c:
            a_vec = splitter.split(str(data['split_var']))
            decision_tree['splitVar'] = [int(a_vec[i]) - 1 for i in range(1,len(a_vec)-1)]
            a_vec = splitter.split(str(data['cutoff_val']))        
            decision_tree['cutoffVal'] = [float(a_vec[i]) for i in range(1,len(a_vec)-1)]
            a_vec = splitter.split(str(data['left_node']))        
            decision_tree['leftNode'] = [int(a_vec[i]) -1 for i in range(1,len(a_vec)-1)]
            a_vec = splitter.split(str(data['right_node']))        
            decision_tree['rightNode'] = [int(a_vec[i]) -1 for i in range(1,len(a_vec)-1)]
            a_vec = splitter.split(str(data['node_mean']))        
            decision_tree['nodeMean'] = [float(a_vec[i]) for i in range(1,len(a_vec)-1)]
        return decision_tree