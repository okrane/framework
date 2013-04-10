'''
Created on 25 oct. 2010

@author: midan
'''

from simep.core.baseobserver import BaseObserver
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog
from simep.funcs.dbtools.securities_tools import SecuritiesTools


class VolumeLimitNTicks(BaseObserver):
    
    
    
    @staticmethod
    def public_parameters():
        
        setup      = {'name'                   : {'label' : 'Name'                 , 'value' : 'StockObserver001'},
                      'indicators'             : {'label' : 'Indicators'           , 'value' : 'market_volume;market_vwap;avg_exec_time;avg_spread_15;avg_trade_size_15;garman_klass_15'}}
        parameters = {'dist_tick'              : {'label' : 'Distant in ticks'     , 'value' : 1},
                      'plot_mode'              : {'label' : 'Plot Mode (0, 1, 2)'  , 'value' : 2},
                      'print_orderbook'        : {'label' : 'Print the Orderbook'  , 'value' : 'False'}}
        return {'setup': setup, 'parameters': parameters}
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, parameters, trace):
        BaseObserver.__init__(self, setup, context, parameters, trace)
        self._figure_title      = '[date=' + context['date'] + ', ric=' + str(context['ric']) + ', trading_destination_id=' + str(context['trading_destination_id']) + ']'
        
        self.tick_size = None        
    
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
        
    def process(self, event):
        code = self.update(event)
        if code == 1:
            lob = event.getLob()
            # compute volume at limit -/+ 1 tick
            if self.tick_size is None:           
                self.tick_size = self.getReferenceData('PriceStep', 
                                               venue_id = self['trading_destination_id'],
                                               price = lob.getBid(0).price)
            limit_bid_size = [];
            limit_ask_size = [];
            for nb_ticks in range(0,4):
                price = lob.getBid(0).price - nb_ticks * self.tick_size
                idx_price = sum([price <=  lob.getBid(i).price for i in range(0,4)])
                limit_bid_size.append(sum([lob.getBid(i).size for i in range(0,idx_price)]))
                
                price = lob.getAsk(0).price + nb_ticks * self.tick_size
                idx_price = sum([price >=  lob.getAsk(i).price for i in range(0,4)])
                limit_ask_size.append(sum([lob.getAsk(i).size for i in range(0,idx_price)]))
            
            all_new_trades = event.getTrades()
            trade_size = 0
            trade_over_ask = None
            for new_trade in all_new_trades:
                trade_size += new_trade.size
                trade_over_ask = new_trade.overAsk
                
            dictValue = {'market_volume': [self._bus['market_volume']], 
                         'market_vwap'  : [self._bus['market_vwap']], 
                         'garman_klass' : [self._bus['garman_klass']],
                         'avg_exec_time': [self._bus['avg_exec_time']],
                         'avg_spread'   : [self._bus['avg_spread_15']],
                         'ats'          : [self._bus['avg_trade_size_15']],
                         'trade_size'   : [trade_size],
                         'trade_over_ask': [trade_over_ask]}
            dictValue.update(self.list2Dict([lob.getBid(i).price for i in range(4)], 'bid_price_'))
            dictValue.update(self.list2Dict([lob.getAsk(i).price for i in range(4)], 'ask_price_'))
            dictValue.update(self.list2Dict([lob.getBid(i).size for i in range(4)], 'bid_size_'))
            dictValue.update(self.list2Dict([lob.getAsk(i).size for i in range(4)], 'ask_size_'))
            dictValue.update(self.list2Dict(limit_bid_size, 'limit_bid_size_'))
            dictValue.update(self.list2Dict(limit_ask_size, 'limit_ask_size_'))
                    
            self.appendIndicator(pyData('init', date = [self._timestamp], value = dictValue))
            
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
