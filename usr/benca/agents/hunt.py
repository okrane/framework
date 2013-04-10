'''
Created on 28 juil. 2010

@author: benca
'''


from simep.core.basetrader import BaseTrader
from simep.bin.simepcore import Order
from simep.tools import date2num
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog


class Hunt(BaseTrader):
    
    
    @staticmethod
    def public_parameters():
        
        setup      = {'name'                   : {'label' : 'Name'               , 'value' : 'Hunt001'}}
        parameters = {'asked_qty'              : {'label' : 'Asked Quantity'     , 'value' : 100000}, 
                      'cycle_time'             : {'label' : 'Cycle Time'         , 'value' : 5},
                      'limit_price'            : {'label' : 'Limit Price'        , 'value' : 40.5}, 
                      'minimum_size'           : {'label' : 'Minimum Size'       , 'value' : 500}, 
                      'use_business_time'      : {'label' : 'Uses Business Time' , 'value' : False},
                      'plot_mode'              : {'label' : 'Plot Mode (0, 1, 2)', 'value' : 2}, 
                      'max_price_index'        : {'label' : 'Maximum Price Index in the Orderbook', 'value' : 3}}
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return ['avg_exec_time_s_300']
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, params, trace):
        self._figure_title             = 'Hunt [date=' + context['date'] + ', ric=' + str(context['ric']) +  ', side=%s, asked_qty=' + str(params['asked_qty']) + ', limit_price=' + str(params['limit_price']) + ', cycle_time=' + str(params['cycle_time']) + ']'
        self._figure_title             = self._figure_title % ('BUY' if params['side']==Order.Buy else 'SELL')
        BaseTrader.__init__(self, setup, context, params, trace)
        self.securityIdEventsOnly      = True
        self._remaining_qty            = params['asked_qty']
        self._next_order_business_time = self['cycle_time']
        self._order                    = None
        self._order_venue_id           = None
    
    
    
    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
    
    def process(self, event):
        code     = self.update(event)
        venue_id = event.getVenueId()
        if code == 0:
            self._cancel_order()
        if code == 1:
            self._perform_hunt(venue_id)
    
    def processReport(self, event):
        ba   = self.ba
        code = self.update_report(event)
        venue_id = event.getVenueId()
        if code > 0:
            self.append_indicator(pyData('init', date= [ba['event']['TIME_NUM']], value= {'executed_quantity'  : [event.size],
                                                                               'execution_price'    : [event.price],
                                                                               'VWAP'        : [ba['feed'][venue_id]['VWAP']],
                                                                               'VOLUME'      : [ba['feed'][venue_id]['VOLUME']],
                                                                               'algo_exec_volume'   : [self._exec_qty],
                                                                               'business_time'      : [ba['feed'][venue_id]['NUM_MOVES']],
                                                                               'ask_price_1'        : [ba['feed'][venue_id]['ask_price_1']],
                                                                               'ask_size_1'         : [ba['feed'][venue_id]['ask_size_1']],
                                                                               'bid_price_1'        : [ba['feed'][venue_id]['bid_price_1']],
                                                                               'bid_size_1'         : [ba['feed'][venue_id]['bid_size_1']]}))
            self._remaining_qty -= event.size
        return None
    
    
    
    '''######################################################################################################
    #########################################   PRIVATE FUNCTIONS   #########################################
    ######################################################################################################'''
    
    def _check_time(self, venue_id):
        ba = self.ba
        if self._order == None:
            return True
        elif self['use_business_time']:
            if ba['feed'][venue_id]['NUM_MOVES'] < self._next_order_business_time:
                return False
            else:
                self._next_order_business_time = ba['feed'][venue_id]['NUM_MOVES'] + self['cycle_time']
                return True
        else:
            if ba['event']['TIME_NUM'] < (self._order_time+ba['feed'][venue_id]['avg_exec_time']*self['cycle_time']):
                return False
            else:
                return True
        
    def _perform_hunt(self, venue_id):
        if not self._check_time(venue_id):
            return
        # aggregate order book to get the venue_id of the best opposite price
        lob = self.mkt_mgr.getLob()
        if self['side'] == Order.Buy:
            venue_id = lob.getAsk(0).market_id
        else:
            venue_id = lob.getBid(0).market_id
        # compute available volume on the best venue
        lob = self.mkt_mgr.getLob(venue_id)
        if venue_id == '':
            self.print_orderbooks()
            #raw_input("Press 'Enter' to continue...")
        if self['side'] == Order.Buy:
            available_volume = 0
            for i in range(self['max_price_index']):
                if lob.getAsk(i).price <= self['limit_price']:
                    available_volume += lob.getAsk(i).size
        else :
            available_volume = 0
            for i in range(self['max_price_index']):
                if lob.getBid(i).price >= self['limit_price']:
                    available_volume += lob.getBid(i).size
        # send order on that best venue
        if available_volume >= self['minimum_size'] or available_volume > self._remaining_qty:
            qty = min(available_volume, self._remaining_qty)
            self._cancel_order()
            self._order_time = ba['event']['TIME_NUM']
            self._order = self.send_limit_fok_order(venue_id, qty, self['limit_price'], self['name'])
            self._order_venue_id = venue_id
    
    def _cancel_order(self):
        self.cancel_order(self._order_venue_id, self._order)
        self._order = None
