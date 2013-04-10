'''
Created on 11 mai 2011
@author: wehua
'''
'''Smart Slippage Tactic 

  CACVersion $File: SmartSlippage.py$ Version 1.1.0 $Branch: QuantitativeResearch$ $Date: 03/05/2012 06:07:35 PM$

'''
surround_branch = '$Branch: QuantitativeResearch$'
surround_header = '$Header: SmartSlippage.py: Revision: 9: Author: wehua: Date: 03/05/2012 06:07:35 PM$'
tok_head        =  surround_header.split(': ') 
tok_branch      =  surround_branch.split(': ') 


__version__ = "1.1.1"
__author__  = 'wehuang@cheuvreux.com'


from random import *
from simep.tactic import Tactic
from sm.Inter_S import *

class SmartSlippage(Tactic):
    
    def __init__(self, setup, context, parameters):
        Tactic.__init__(self, setup, context, parameters)
        self.next_deal = 0  
        self.cumulatedvolume = 0;

    def reset(self, client_params, reject_msg):               
        
        TotalQty = self.moneyManager.getTotalQty()
        ExecQty = self.moneyManager.getExecQty()
        LeaveQty = self.moneyManager.getLeavesQty()
        self.log('Reset Slippage Order, totalqty, %f execqty, %f leaveqty %f' % (TotalQty,ExecQty,LeaveQty))
        if client_params['OrderQty'] != TotalQty:
            self.log('target_qty %f' % (self.target_qty))
            self.log('Le')
            if client_params['OrderQty'] < self.target_qty:
                self.target_qty = client_params['OrderQty']
                if client_params['OrderQty'] < ExecQty:
                    self.log('Cannot reset, new total qty < execqty, cancel all existing orders')

                    pb = self.moneyManager.getPrivateBook('limit_order')
                    if pb:    
                        order_id = pb.keys()[0]
                        order = pb[order_id]
                        self.cancelOccurrence(order['VenueId'], order_id)
                        
                    pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
                    if pb:    
                        order_id = pb.keys()[0]
                        order = pb[order_id]
                        self.cancelOccurrence(order['VenueId'], order_id)
                        self.moneyManager.killSM('Inter_S')
                        
                    reject_msg.message = 'Cannot reset, new order quantity is smaller than executed quantity'
                    return Tactic.Modify.REJECT
                elif client_params['OrderQty'] < ExecQty + LeaveQty:
                    self.log('modify')
                    pb = self.moneyManager.getPrivateBook('limit_order')
                    if pb:
                        order_id = pb.keys()[0]
                        order = pb[order_id]
                        quantity = client_params['OrderQty'] - ExecQty
                        self.modifyOccurrence('MarketOrder',
                                              order['VenueId'], 
                                              order_id,
                                              tactic_params = {'OrderQty': quantity,
                                                               'Price': order['Price'],
                                                               'OrderType': 'limit'})
                        
                    pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
                    if pb:
                        order_id = pb.keys()[0]
                        order = pb[order_id]
                        quantity = client_params['OrderQty'] - ExecQty
                        self.modifyOccurrence('MarketOrder',
                                              order['VenueId'], 
                                              order_id,
                                              tactic_params = {'OrderQty': quantity,
                                                               'Price': order['Price'],
                                                               'OrderType': 'limit'})
                        
                    return Tactic.Modify.REINITIALIZE
        
        if client_params.has_key('Price'):
            self.log('new limit price %f' % (client_params['Price']))
            pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
            if pb:    
                order_id = pb.keys()[0]
                order = pb[order_id]
                if order['Price'] * self.side - self.side * client_params['Price'] > 0.00001:
                    self.log('new price constraint %f, Cancel passive order as it violates this new constraint' % (client_params['Price']))
                    self.cancelOccurrence(order['VenueId'], order_id)
                    self.moneyManager.killSM('Inter_S')
                    return Tactic.Modify.REINITIALIZE
                
            pb = self.moneyManager.getPrivateBook('limit_order')
            if pb:    
                order_id = pb.keys()[0]
                order = pb[order_id]
                if order['Price'] * self.side - self.side * client_params['Price'] > 0.00001:
                    self.log('new price constraint %f, Cancel passive order as it violates this new constraint' % (client_params['Price']))
                    self.cancelOccurrence(order['VenueId'], order_id)                    
                    return Tactic.Modify.REINITIALIZE
                
        return Tactic.Modify.ACCEPT

    def checkParameters(self, parameters, error):
        
        self._parameters = dict(parameters)
        
        # Buy = 1, Sell = 2
        if self['Side'] == '1' or self['Side'] == 1:
            self.side = 1
        else:
            self.side = -1
                
        if not isinstance(self['WaitingTime'],int):
            self['WaitingTime'] = 10000
            
        return True
    
    def slippage_order(self,ae,venue_id):
        ats = int(self.marketManager.getVenue(venue_id)['ats'] * (0.9 + random() * 0.2))
        
        if ((isinstance(self.marketManager.getFeedInfo('AE', 'PARCL_SIZE'), int) or isinstance(self.marketManager.getFeedInfo('AE', 'PARCL_SIZE'), float)) and (isinstance(self.marketManager.getFeedInfo('AE', 'CONV_FAC'), int) or isinstance(self.marketManager.getFeedInfo('AE', 'CONV_FAC'), float))):               
            cycle_size = min(10,int(ae['PARCL_SIZE'] * ae['CONV_FAC']))                
        else:                
            cycle_size = min(10,int(self['AggregatedDealsDefault']))
        
        TotalQty = self.moneyManager.getTotalQty()
        ExecQty = self.moneyManager.getExecQty()
        self.log('Execqty %f totalqty %f' % (ExecQty,TotalQty))
        market = self.marketManager.getFeedInfo(venue_id)
        price_bestbid = market['BEST_BID1']
        price_bestask = market['BEST_ASK1']
        tick_size = self.getReferenceData('PriceStep', venue_id = venue_id, price = price_bestbid)
        spread_size = int((price_bestask-price_bestbid)/tick_size+0.01)
        
        lts_index = ae['ORDER_SIDE']
        if not isinstance(ae['ORDER_SIDE'], int):
            lts_index = 0
        else:
            lts_index = ae['ORDER_SIDE']
            
        if lts_index == 1:
            lts_index = -1
        if lts_index == 2:
            lts_index = 1
            
        self.log('Last trade side %d' % (lts_index))
        volume_imbalance = float(market['BEST_ASIZ1']+market['BEST_ASIZ2']-market['BEST_BSIZ1']-market['BEST_BSIZ2'])/(market['BEST_ASIZ1']+market['BEST_ASIZ2']+market['BEST_BSIZ1']+market['BEST_BSIZ2']) 
        if self.side == -1:
            volume_imbalance = -volume_imbalance
            
        self.log('Imbalance %f' % (volume_imbalance))
        self.initialbid = price_bestbid
        self.initialask = price_bestask
        
        pb = self.moneyManager.getPrivateBook('limit_order')
        if pb:    
            for ord_id in pb.keys():
                quantity     = pb[ord_id]['OrderQty']
                self.log('Cancel Order %d' % (quantity))
                self.cancelOccurrence(venue_id, ord_id)
        
        pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
        if pb:    
            for ord_id in pb.keys():
                quantity     = pb[ord_id]['OrderQty']
                self.log('Cancel Order %d' % (quantity))
                self.cancelOccurrence(venue_id, ord_id)
                self.moneyManager.killSM('Inter_S')
                
                
        if volume_imbalance <-0.3:
            index_imb = 0
        elif volume_imbalance <0.3:
            index_imb = 1
        else:
            index_imb = 2
            
        self.log('self side equals %d' % (self.side))
        
        if lts_index == self.side:
            if spread_size == 1:
                ratio = 0;
            elif spread_size == 2:
                ratio = float(self.ltb_2[index_imb])
            elif spread_size == 3:
                ratio = float(self.ltb_3[index_imb])
            elif spread_size == 4:
                ratio = float(self.ltb_4[index_imb])
            else:
                ratio = float(self.ltb[index_imb])
        else:
            if spread_size == 1:
                ratio = 0;
            elif spread_size == 2:
                ratio = float(self.lto_2[index_imb])
            elif spread_size == 3:
                ratio = float(self.lto_3[index_imb])
            elif spread_size == 4:
                ratio = float(self.lto_4[index_imb])
            else:
                ratio = float(self.lto[index_imb])
        
        ratio = max(0,ratio)
        
        quantity = min(self['MaxMultATS'] * ats,TotalQty - ExecQty)
        if ratio == 0:
            if self['Price'] == None:
                self.log('Spread = %f ratio = %f, sending aggressive order' % (spread_size,ratio))
                if market['BEST_OPPOSITE_SIZE1'] >= quantity:
                    oc = self.createOccurrence('MarketOrder',
                                               venue_id = self.primary_venue_id, 
                                               prefix = 'AggressiveOrder',
                                               tactic_params = {'OrderQty': quantity, 
                                                                'Price': market['BEST_OPPOSITE1'], 
                                                                'OrderType': 'limit',
                                                                'MarketTimeInForce': 'Fak'} 
                                               )
                    self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                    self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                    self.target_qty = ExecQty + quantity
                    self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                else:
                    oc = self.createOccurrence('MarketOrder',
                                           venue_id = self.primary_venue_id, 
                                           prefix = 'AggressiveOrder',
                                           tactic_params = {'OrderQty': quantity, 
                                                            'Price': market['BEST_OPPOSITE2'], 
                                                            'OrderType': 'limit',
                                                            'MarketTimeInForce': 'Fak'} 
                                           )
                    self.log('Sending Aggressive Order to Opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                    self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                    self.target_qty = ExecQty + quantity
                    self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
            else:
                self.log('Spread = %f ratio = %f, sending aggressive order' % (spread_size,ratio))
                if market['BEST_OPPOSITE_SIZE1'] >= quantity:
                    if self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side - 0.00001:
                        oc = self.createOccurrence('MarketOrder',
                                                   venue_id = self.primary_venue_id, 
                                                   prefix = 'AggressiveOrder',
                                                   tactic_params = {'OrderQty': quantity, 
                                                                    'Price': market['BEST_OPPOSITE1'], 
                                                                    'OrderType': 'limit',
                                                                    'MarketTimeInForce': 'Fak'} 
                                                   )
                        self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                        self.target_qty = ExecQty + quantity
                        self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                    else:
                        oc = self.createOccurrence('MarketOrder',
                                        venue_id = venue_id, 
                                        prefix = 'limit_order',
                                        tactic_params = {'OrderQty': quantity, 
                                                        'Price': self['Price'], 
                                                        'OrderType': 'limit'} 
                                        )
                        
                        self.target_qty = ExecQty + quantity
                        self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                        self.log('Cannot send aggressive order as limit Price %f reached, send instead limit passive order' % (self['Price']))
                else:
                    if self['Price'] * self.side > market['BEST_OPPOSITE2'] * self.side - 0.00001:
                        oc = self.createOccurrence('MarketOrder',
                                               venue_id = self.primary_venue_id, 
                                               prefix = 'AggressiveOrder',
                                               tactic_params = {'OrderQty': quantity, 
                                                                'Price': market['BEST_OPPOSITE2'], 
                                                                'OrderType': 'limit',
                                                                'MarketTimeInForce': 'Fak'} 
                                               )
                        self.log('Sending Aggressive Order to Opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                        self.target_qty = ExecQty + quantity
                        self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                    elif self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side - 0.00001:
                        oc = self.createOccurrence('MarketOrder',
                                                   venue_id = self.primary_venue_id, 
                                                   prefix = 'AggressiveOrder',
                                                   tactic_params = {'OrderQty': quantity, 
                                                                    'Price': market['BEST_OPPOSITE1'], 
                                                                    'OrderType': 'limit',
                                                                    'MarketTimeInForce': 'Fak'} 
                                                   )
                        self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.AGGRESSIVE))
                        self.target_qty = ExecQty + quantity
                        self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                    else:
                        oc = self.createOccurrence('MarketOrder',
                                        venue_id = venue_id, 
                                        prefix = 'limit_order',
                                        tactic_params = {'OrderQty': quantity, 
                                                        'Price': self['Price'], 
                                                        'OrderType': 'limit'} 
                                        )
                        
                        self.target_qty = ExecQty + quantity
                        self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                        self.log('Cannot send aggressive order as limit Price %f reached, send instead limit passive order' % (self['Price']))
        else:
            tick_change = max(min(spread_size-1,int(ratio*spread_size)),1)
            if self.side == 1:
                price_limit = price_bestbid + tick_change * tick_size
            else:
                price_limit = price_bestask - tick_change * tick_size
                
            self.price_slip = price_limit
            if self['Price'] == None:
                oc = self.createOccurrence('MarketOrder',
                                venue_id = venue_id, 
                                prefix = 'SlippageOrder_Strategy',
                                tactic_params = {'OrderQty': quantity, 
                                                'Price': price_limit, 
                                                'OrderType': 'limit'} 
                                )
                self.log('spread_size = %f ratio = %f Sending limit Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (spread_size,ratio,quantity,price_limit,market['BEST_BSIZ1'],price_bestbid,market['BEST_ASIZ1'],price_bestask,self.next_deal))
                self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.INIT))
                self.target_qty = ExecQty + quantity
                self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
            else:
                if self['Price'] * self.side > price_limit * self.side - 0.00001:
                    oc = self.createOccurrence('MarketOrder',
                                    venue_id = venue_id, 
                                    prefix = 'SlippageOrder_Strategy',
                                    tactic_params = {'OrderQty': quantity, 
                                                    'Price': price_limit, 
                                                    'OrderType': 'limit'} 
                                    )
                    self.log('spread_size = %f ratio = %f Sending limit Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (spread_size,ratio,quantity,price_limit,market['BEST_BSIZ1'],price_bestbid,market['BEST_ASIZ1'],price_bestask,self.next_deal))
                    self.moneyManager.registerSM('Inter_S',Inter_S(oc.id, 'Inter_S', self['WaitingTime'],Inter_S.States.INIT))
                    self.target_qty = ExecQty + quantity
                    self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                else:
                    oc = self.createOccurrence('MarketOrder',
                                    venue_id = venue_id, 
                                    prefix = 'limit_order',
                                    tactic_params = {'OrderQty': quantity, 
                                                    'Price': self['Price'], 
                                                    'OrderType': 'limit'} 
                                    )
                        
                    self.target_qty = ExecQty + quantity
                    self.next_deal = int(ae['C1_CONVNUM']) + cycle_size
                    self.log('Cannot send passive order as limit Price %f reached, send instead limit passive order' % (self['Price']))
                    
    def process(self, evt):
        
        if not self.marketManager.getFeedInfo('AE'):
            self.log('No Analitical Engine image found. Skipping this event...')          
            return

        if not isinstance(self.marketManager.getFeedInfo('AE', 'C1_CONVNUM'), int):
            self.error('Unable to retrieve Bussiness Time data <C1_CONVNUM> from the AE.')

        if not isinstance(self.marketManager.getVenue(evt.getVenueId())['ats'],int) or isinstance(self.marketManager.getVenue(evt.getVenueId())['ats'],float):
            self.error('Unable to compute ats from the market manager.')

        venue_id = evt.getVenueId()
        ae = self.marketManager.getFeedInfo('AE')
        ats = int(self.marketManager.getVenue(venue_id)['ats'] * (0.9 + random() * 0.2))

        ExecQty = self.moneyManager.getExecQty()
        if ExecQty == 0:
            self.cumulatedvolume = ae['QTE_CNT1']
            
        if self.next_deal <= ae['C1_CONVNUM']:
            self.slippage_order(ae,venue_id)
        else:
            inter_s = self.moneyManager.getSM('Inter_S')
            if inter_s:
                if inter_s.state == Inter_S.States.AT_MID:
                    market = self.marketManager.getFeedInfo(venue_id)
                    self.log('at mid, waiting for further decision')
                    price_bestbid = market['BEST_BID1']
                    price_bestask = market['BEST_ASK1']
                    size_bestask = market['BEST_ASIZ1']
                    size_bestask2 = market['BEST_ASIZ2']
                    size_bestbid = market['BEST_BSIZ1']
                    size_bestbid2 = market['BEST_BSIZ2']

                    random_res = random()
                    if self.side == 1:
                        if price_bestbid - self.price_slip > 0.00001 and price_bestask - self.initialask <= 0.00001:
                            self.log('price_bestbid %f, price_slip %f' % (price_bestbid,self.price_slip))
                            if size_bestbid > ats and random_res > float(size_bestask-size_bestbid-size_bestbid2)/(size_bestask+size_bestbid+size_bestbid2):
                                self.log('Imbalance Level %f' % (float(size_bestask-size_bestbid-size_bestbid2)/(size_bestask+size_bestbid+size_bestbid2)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state price_bestbid > self.price_slip: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        elif price_bestask - self.initialask > 0.00001 and price_bestbid - self.price_slip <= 0.00001:
                            self.log('price_bestask %f, price_initialask %f' % (price_bestask,self.initialask))
                            if random_res > float(size_bestask-size_bestbid)/(size_bestask+size_bestbid):
                                self.log('Imbalance Level %f' % (float(size_bestask-size_bestbid)/(size_bestask+size_bestbid)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state price_bestask > self.initialask: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        elif price_bestask - self.initialask > 0.00001 and price_bestbid - self.price_slip > 0.00001:
                            self.log('price_bestask %f, price_initialask %f' % (price_bestask,self.initialask))
                            self.log('price_bestbid %f, price_slip %f' % (price_bestbid,self.price_slip))
                            if random_res > float(size_bestask-size_bestbid -size_bestbid2 )/(size_bestask+size_bestbid + size_bestbid2):
                                self.log('Imbalance Level %f' % (float(size_bestask-size_bestbid -size_bestbid2)/(size_bestask+size_bestbid + size_bestbid2)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state price_bestask > self.initialask: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                    else:
                        if price_bestask - self.price_slip < -0.00001 and price_bestbid - self.initialbid >= -0.00001:
                            self.log('price_bestask %f, price_slip %f' % (price_bestask,self.price_slip))
                            if size_bestask > ats and random_res > float(size_bestbid-size_bestask-size_bestask2)/(size_bestask+size_bestbid+size_bestask2):
                                self.log('Imbalance Level %f' % (float(size_bestbid-size_bestask-size_bestask2)/(size_bestask+size_bestbid+size_bestask2)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state price_bestask < self.price_slip: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        elif price_bestbid - self.initialbid < -0.00001 and price_bestask - self.price_slip >= -0.00001:
                            self.log('price_bestbid %f, price_initialbid %f' % (price_bestbid,self.initialbid))
                            if random_res > float(size_bestbid-size_bestask)/(size_bestask+size_bestbid):
                                self.log('Imbalance Level %f' % (float(size_bestbid-size_bestask)/(size_bestask+size_bestbid)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state price_bestbid < self.initialbid: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        elif price_bestbid - self.initialbid < -0.00001 and price_bestask - self.price_slip < -0.00001:
                            self.log('price_bestask %f, price_slip %f' % (price_bestask,self.price_slip))
                            self.log('price_bestbid %f, price_initialbid %f' % (price_bestbid,self.initialbid))
                            if size_bestask > ats and random_res > float(size_bestbid-size_bestask-size_bestask2)/(size_bestask+size_bestbid+size_bestask2):
                                self.log('Imbalance Level %f' % (float(size_bestbid-size_bestask-size_bestask2)/(size_bestask+size_bestbid+size_bestask2)))
                                inter_s.state = Inter_S.States.AGGRESSIVE
                                self.log('change to aggressive state price_bestask < self.price_slip: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                self.log('Anti Gaming: %f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))

                    ExecQty = self.moneyManager.getExecQty()
                    if ae['QTE_CNT1'] - self.cumulatedvolume - ExecQty > self.target_qty:
                         inter_s.state = Inter_S.States.AGGRESSIVE
                         self.log('change to aggressive state, the slippage tactic needs to catch up the undone quantities')


                    if self['Price'] != None:
                        if self['Price'] * self.side - market['BEST_OPPOSITE1'] * self.side < -0.00001 and inter_s.state == Inter_S.States.AGGRESSIVE:
                            pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
                            ExecQty = self.moneyManager.getExecQty()
                            if pb:    
                                order_id = pb.keys()[0]
                                order = pb[order_id]
                                self.cancelOccurrence(order['VenueId'], order_id)
                                self.moneyManager.killSM('Inter_S')
                                
                            quantity = self.target_qty - ExecQty
                            if quantity > 0:        
                                oc = self.createOccurrence('MarketOrder',
                                                venue_id = venue_id, 
                                                prefix = 'limit_order',
                                                tactic_params = {'OrderQty': quantity, 
                                                                'Price': self['Price'], 
                                                                'OrderType': 'limit'} 
                                                )
                                
                            self.log('Cannot pass from passive to aggressive as limit Price %f reached, send instead limit passive order' % (self['Price']))
                            
                            return

                if inter_s.state == Inter_S.States.AGGRESSIVE:
                    market = self.marketManager.getFeedInfo(venue_id)
                    TotalQty = self.moneyManager.getTotalQty()
                    ExecQty = self.moneyManager.getExecQty()
                    pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
                    if pb:    
                        for ord_id in pb.keys():
                            quantity     = pb[ord_id]['OrderQty']
                            self.log('Cancel Order %d' % (quantity))
                            self.cancelOccurrence(venue_id, ord_id)

                    if ExecQty < self.target_qty:
                        quantity = self.target_qty - ExecQty
                        if self['Price'] == None:
                            if market['BEST_OPPOSITE_SIZE1'] >= quantity: 
                                oc = self.createOccurrence('MarketOrder',
                                                           venue_id = venue_id, 
                                                           prefix = 'AggressiveOrder',
                                                           tactic_params = {'OrderQty': quantity, 
                                                                            'Price': market['BEST_OPPOSITE1'], 
                                                                            'OrderType': 'limit',
                                                                            'MarketTimeInForce': 'Fak'} 
                                                           )
                            
                                inter_s.setOrderName(oc)
                                self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                            else:
                                oc = self.createOccurrence('MarketOrder',
                                                           venue_id = venue_id, 
                                                           prefix = 'AggressiveOrder',
                                                           tactic_params = {'OrderQty': quantity, 
                                                                            'Price': market['BEST_OPPOSITE2'], 
                                                                            'OrderType': 'limit',
                                                                            'MarketTimeInForce': 'Fak'} 
                                                           )
                            
                                inter_s.setOrderName(oc)
                                self.log('Sending Aggressive Order to best opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                        else:
                            if market['BEST_OPPOSITE_SIZE1'] >= quantity: 
                                if self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side - 0.00001:
                                    oc = self.createOccurrence('MarketOrder',
                                                               venue_id = venue_id, 
                                                               prefix = 'AggressiveOrder',
                                                               tactic_params = {'OrderQty': quantity, 
                                                                                'Price': market['BEST_OPPOSITE1'], 
                                                                                'OrderType': 'limit',
                                                                                'MarketTimeInForce': 'Fak'} 
                                                               )
                                    
                                    inter_s.setOrderName(oc)
                                    self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                else:
                                    oc = self.createOccurrence('MarketOrder',
                                        venue_id = venue_id, 
                                        prefix = 'limit_order',
                                        tactic_params = {'OrderQty': quantity, 
                                                        'Price': self['Price'], 
                                                        'OrderType': 'limit'} 
                                        )
                                    self.moneyManager.killSM('Inter_S')
                                    self.log('Cannot send aggressive order as limit Price %f reached, send instead limit passive order' % (self['Price']))
                                    return
                            else:
                                if self['Price'] * self.side > market['BEST_OPPOSITE2'] * self.side - 0.00001:
                                    oc = self.createOccurrence('MarketOrder',
                                                               venue_id = venue_id, 
                                                               prefix = 'AggressiveOrder',
                                                               tactic_params = {'OrderQty': quantity, 
                                                                                'Price': market['BEST_OPPOSITE2'], 
                                                                                'OrderType': 'limit',
                                                                                'MarketTimeInForce': 'Fak'} 
                                                               )
                                    
                                    inter_s.setOrderName(oc)
                                    self.log('Sending Aggressive Order to best opposite 2: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE2'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                elif self['Price'] * self.side > market['BEST_OPPOSITE1'] * self.side - 0.00001:
                                    oc = self.createOccurrence('MarketOrder',
                                                               venue_id = venue_id, 
                                                               prefix = 'AggressiveOrder',
                                                               tactic_params = {'OrderQty': quantity, 
                                                                                'Price': market['BEST_OPPOSITE1'], 
                                                                                'OrderType': 'limit',
                                                                                'MarketTimeInForce': 'Fak'} 
                                                               )
                                    
                                    inter_s.setOrderName(oc)
                                    self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
                                else:
                                    oc = self.createOccurrence('MarketOrder',
                                        venue_id = venue_id, 
                                        prefix = 'limit_order',
                                        tactic_params = {'OrderQty': quantity, 
                                                        'Price': self['Price'], 
                                                        'OrderType': 'limit'} 
                                        )
                                    self.moneyManager.killSM('Inter_S')
                                    self.log('Cannot send aggressive order as limit Price %f reached, send instead limit passive order' % (self['Price']))
                                    return
                                
                if inter_s.state == Inter_S.States.NOT_SENT:
                    self.log('passive order not sent, reassess the best slippage strategy')
                    self.slippage_order(ae,venue_id)

    def processTimer(self,evt):
        self.log('Timer %s is on')
        venue_id = self.primary_venue_id
        market = self.marketManager.getFeedInfo(venue_id)
        best_opposite = market['BEST_OPPOSITE1']
        current_pb = self.moneyManager.getPrivateBook('SlippageOrder_Strategy')
        if current_pb:    
            for ord_id in current_pb.keys():
                quantity     = current_pb[ord_id]['OrderQty']
                self.log('Cancel Order %d' % (quantity))
                self.cancelOccurrence(venue_id, ord_id)
        else:
            self.log('no existing slippage order')
            
        ExecQty = self.moneyManager.getExecQty()
        price = best_opposite
        if self['Price'] != None:
            if price * self.side - self['Price'] * self.side  > 0.00001:
                price = self['Price']
                
        if ExecQty < self.target_qty:
            quantity = self.target_qty - ExecQty
            oc = self.createOccurrence('MarketOrder',
                                        venue_id = venue_id, 
                                        prefix = 'limit_order',
                                        tactic_params = {'OrderQty': quantity, 
                                                        'Price': price, 
                                                        'OrderType': 'limit'} 
                                        )
            self.log('Sending Aggressive Order: %d@%f. BidAsk: [%d|%f - %d|%f]. Next Update: %d.' % (quantity,market['BEST_OPPOSITE1'],market['BEST_BSIZ1'],market['BEST_BID1'],market['BEST_ASIZ1'],market['BEST_ASK1'],self.next_deal))
        self.moneyManager.killSM('Inter_S')
            
    def processReport(self, evt):
        pass
    
    
    def acceptSubscription(self, sub_id, sub):        
        return sub_id == self.primary_venue_id

    def acceptVenue(self, venue_id, venue):
                
        if venue.info["type"] != "Lit":
            return False        
        if str(venue.info["destinationId"]) == str(self['destinationId']):
            self.primary_venue_id = venue_id
            self.trading_destination  = venue.info['destinationId']
            slippage_ratio = self.getReferenceData('SlippageEstimator',venue_id = venue_id)
            
            self.ltb = slippage_ratio('lastTradeBest').split(';')
            self.ltb_2 = slippage_ratio('lastTradeBest_2').split(';')
            self.ltb_3 = slippage_ratio('lastTradeBest_3').split(';')
            self.ltb_4 = slippage_ratio('lastTradeBest_4').split(';')
            
            self.lto = slippage_ratio('lastTradeOpposite').split(';')
            self.lto_2 = slippage_ratio('lastTradeOpposite_2').split(';')
            self.lto_3 = slippage_ratio('lastTradeOpposite_3').split(';')
            self.lto_4 = slippage_ratio('lastTradeOpposite_4').split(';')
            return True
        
        return False

    def extraSubscriptions(self):
        
        ''' Returns any extra subscriptions needed. 
            @return
                Dictionary: {name: (Source, Ric, ReqdProcess)}
        '''
        return {'AE':(self['vwapSource'], 'S=%d@%s;P=bEHRC;Z=UTC;H=4;F=%s;T=%s;EX=top;EX=cross;EX=auc;PL=BI' % (self['InstrumentID'], self.trading_destination, self['EffectiveTime'][9:], self['ExpireTime'][9:]) )}
