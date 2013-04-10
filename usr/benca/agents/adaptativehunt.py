'''
Created on 28 juil. 2010

@author: benca
'''


from simep.core.basetrader import BaseTrader
from simep.bin.simepcore import Order
from simep.tools import date2num
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog


class AdaptativeHunt(BaseTrader):
    
    
    @staticmethod
    def public_parameters():
        setup      = {'name'                   : {'label' : 'Name'               , 'value' : 'AdaptativeHunt001'}}
        parameters = {'asked_qty'              : {'label' : 'Asked Quantity'     , 'value' : 100000}, 
                      'cycle_time'             : {'label' : 'Cycle Time'         , 'value' : 5},
                      'delta_time'             : {'label' : 'Delta Time'         , 'value' : '03:00:00:000'}, 
                      'price_coefficient'      : {'label' : 'Price Coefficient'  , 'value' : 0.4}, 
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
        self._figure_title              = 'AdaptativeHunt [date=' + context['date'] + ', ric=' + str(context['ric']) +  ', side=%s, asked_qty=' + str(params['asked_qty']) + ', price_coefficient=' + str(params['price_coefficient']) + ', cycle_time=' + str(params['cycle_time']) + ']'
        self._figure_title              = self._figure_title % ('BUY' if params['side']==Order.Buy else 'SELL')
        params['delta_time']            = date2num(params['delta_time'])
        BaseTrader.__init__(self, setup, context, params, trace)
        self.securityIdEventsOnly       = True
        self.params['hunt_start_time']  = 0.0
        self._do_hunt                   = False
        self._remaining_qty             = params['asked_qty']
        self._next_order_business_time  = self['cycle_time']
        self._order                     = None
        self._order_venue_id            = None
        for venue_id in self.ba['venue_ids']:
            self.params['hunt_start_time'] = ba['feed'][venue_id]['start_time'] + self['delta_time']
    
    
    
    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
    
    def process(self, event):
        code = self.update(event)
        venue_id = event.getVenueId()
        if code == 0:
            self._cancel_order()
        if code == 1:
            self._process_market_trades(event)
            lob = self.mkt_mgr.getLob()
            self._perform_hunt(lob, venue_id)
    
    def processReport(self, event):
        code = self.update_report(event)
        venue_id = event.getVenueId()
        ba = self.ba
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
    
    def _process_market_trades(self, event):
        venue_id = event.getVenueId()
        ba = self.ba
        if ba['event']['TIME_NUM'] < self['hunt_start_time'] or self._do_hunt:
            return
        # organize trades list
        trades_list = []
        for i in range(len(ba['feed'][venue_id]['DEALS_PRICES'])):
            found_price = False
            price       = ba['feed'][venue_id]['DEALS_PRICES'][i]
            qty         = ba['feed'][venue_id]['DEALS_QTYS'][i]
            for trade in trades_list:
                if trade[0] == price:
                    trade[1] += qty
                    break
            if not found_price:
                trades_list.append([price,qty])
        # sort market list
        if self['side'] == Order.Buy:
            trades_list.sort(key = lambda trade:trade[0], reverse = True)
        elif self['side'] == Order.Sell:
            trades_list.sort(key = lambda trade:trade[0])
        total_volume = 0
        for volume in trades_list:
            total_volume += volume[1]
        target_volume = round(self['price_coefficient']*total_volume)
        current_volume = 0
        for i in range(len(trades_list)-1):
            if current_volume < target_volume:
                current_volume += trades_list[i][1]
                if target_volume <= current_volume:
                    self._hunt_price = trades_list[i][0]
                    self.params['limit_price'] = trades_list[i][0]
        self._do_hunt = True
    
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
            if ba['event']['TIME_NUM'] < (self._order_time+self._dt*self['cycle_time']):
                return False
            else:
                return True
        
    def _perform_hunt(self, lob, venue_id):
        ba = self.ba
        if ba['event']['TIME_NUM'] < self['hunt_start_time'] or not self._do_hunt:
            return
        if not self._check_time(venue_id):
            return
        if self['side'] == Order.Buy:
            available_volume = 0
            for i in range(self['max_price_index']):
                if lob.getAsk(i).price <= self['limit_price']:
                    available_volume += lob.getAsk(i).size
        else :
            available_volume = 0
            for i in range(self['max_price_index']):
                if lob.getBid(i).price <= self['limit_price']:
                    available_volume += lob.getBid(i).size
        if available_volume >= self['minimum_size'] or available_volume > self._remaining_qty:
            qty = min(available_volume, self._remaining_qty)
            self._cancel_order()
            self._order_time = ba['event']['TIME_NUM']
            self._order = self.send_limit_fok_order(venue_id, qty, self['limit_price'], self['name'])
            self._order_venue_id = venue_id
    
    def _cancel_order(self):
        self.cancel_order(self._order_venue_id, self._order)
        self._order = None

        