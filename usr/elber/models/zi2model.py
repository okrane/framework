'''
Created on 13 sept. 2010

@author: elber
'''

from simep import __release__
from simep.sched import Order
from simep.core.baseagent import BaseAgent
from simep.tools import date2num
from random import gauss, random
from numpy.random import poisson
import math

class ZI2Model(BaseAgent):
    
    @staticmethod
    def public_parameters():
        parameters = {'mu'                      : {'label' : 'Market order rate'             , 'value' : 5.0e-05},
                      'sigma'                   : {'label' : 'order size'                    , 'value' : 100},
                      'alpha'                   : {'label' : 'Limit Order Fill Rate'         , 'value' : 0.00012},
                      'price_range'             : {'label' : 'Price Range in Number of Ticks', 'value' : 8},
                      'start_price'             : {'label' : 'Start Price'                   , 'value' : 20.0},
                      'tick_size'               : {'label' : 'Tick Size'                     , 'value' : 0.01},
                      'timestep'                : {'label' : 'Timestep'                      , 'value' : '00:00:10:000000'},
                      'execution_start_time'    : {'label' : 'Execution Start Time'          , 'value' : '+00:10:00:000000'},
                      'delta_cancel'            : {'label' : 'Delta cancellation of orders'  , 'value' : 1.1e-07}}
        return {'setup': {}, 'parameters': parameters}
    
    def __init__(self, setup, context, params, trace):
        trad_id = context['trading_destination_ids'][0]
        for k,v in context.iteritems():
            if isinstance(v, dict):
                context[k] = v[trad_id]
        context['opening']              = date2num(context['opening'])
        context['closing']              = date2num(context['closing'])
        context['timestep']             = date2num(context['timestep'])
        context['execution_start_time'] = date2num(context['execution_start_time'][1:])+context['opening']
        super(ZI2Model,self).__init__(setup, context, params, trace)
        ba                     = self.ba
        ba['venue_id']         = ba['venue_ids'][0]
        self.needAllEvts       = False
        self.needExecReportEvt = True
        self.setIsSimulator(True)
        self.time2wakeup       = self['opening']
        self.actions           = []
        self.start_time_interval = self['opening']
        self.order_size_std    = 0#math.sqrt(math.pi/2)*self['sigma']
        self.generate_actions()
        self.order_ids_list    = []
        self.order_sizes_list  = []
        self.total_size        = 0
    
    def generate_actions(self):
        # get feed
        ba   = self.ba
        feed = ba['feed'][ba['venue_id']][0]
        best_ask = feed['BEST_ASK1'] if feed['BEST_ASK1'] != None else self['start_price']
        best_bid = feed['BEST_BID1'] if feed['BEST_BID1'] != None else self['start_price']
        # generate actions
        while len(self.actions) == 0:
            # initialize an empty list for actions
            actions    = []
            actions_LO = []
            actions_MO = []
            # loop on the side
            for side in [Order.Sell, Order.Buy]:
                # draw a poisson sample
                n = poisson(self['alpha']*self['timestep']/(2*self['sigma']))
                # build actions and generate timestamp
                for i in range(n):
                    if side == Order.Buy:
                        price       = best_ask - self['tick_size']*int(self['price_range']*random()+1)
                    else:                        
                        price       = best_bid + self['tick_size']*int(self['price_range']*random()+1)
                    #draw a half-normal sample
                    qty = gauss(self['sigma'], self.order_size_std)
                    qty = abs(int(qty))
                    actions_LO.append({'timestamp'   : self.start_time_interval+long(self['timestep']*random()),
                                       'type'        : Order.Limit,
                                       'quantity'    : qty,
                                       'side'        : side,
                                       'price'       : self['tick_size']*round(price/self['tick_size'])})
            if feed['TIME_NUM'] >= self['execution_start_time']:
                m = poisson(self['mu']*self['timestep']/(2*self['sigma']))
                side = int(2*random())
                for i in range(m):
                    #draw a half-normal sample
                    qty = gauss(self['sigma'], self.order_size_std)
                    qty = abs(int(qty))
                    actions_MO.append({'timestamp'   : self.start_time_interval+long(self['timestep']*random()),
                                       'type'        : Order.Market,
                                       'quantity'    : qty,
                                       'side'        : side,
                                       'price'       : 0.0})
            # concatenation des deux listes d'actions market orders et limit orders
            actions.extend(actions_MO)
            actions.extend(actions_LO)   
            # if there is no action, just skip this part
            if len(actions) > 0:
                # we sort the actions list
                actions.sort(key = lambda x: x['timestamp'])
                # make sure timestamps are different !
                for i in range(len(actions)-1):
                    if actions[i]['timestamp'] == actions[i+1]['timestamp']:
                        actions[i+1]['timestamp'] += 1L
            # save actions
            self.actions = actions
            # increment the start_time_interval
            self.start_time_interval += self['timestep']
            
    def get_order_to_cancel(self):
        ba   = self.ba
        feed = ba['feed'][ba['venue_id']][0]
        if feed['TIME_NUM'] >= self['execution_start_time']:
            venue_id = self.ba['venue_id']
            p = poisson(self['delta_cancel']*self['timestep'])
            for i in range(p):
                n = len(self.order_ids_list)
                if n < 100:
                    return
                idx = int(n*random())
                order_id = self.order_ids_list.pop(idx)
                self.total_size -= self.order_sizes_list.pop(idx)
                self.cancelOrder(venue_id, order_id)
        
            
    
    def process(self, event):
        # update
        self.update(event)
        # check closing
        if(self.currentTime >= self['closing']):            
            return True
        # get feed
        ba   = self.ba
        feed = ba['feed'][ba['venue_id']][0]
        # check trades
        if feed['TRADE_EVENT']:
            self.total_size -= feed['LAST_TRDS_GROUP']['VOLUME']
        # get action
        if len(self.actions) >= 1:
            current_action = self.actions.pop(0)
        # generate lists of actions until a non-empty list comes out
        self.generate_actions()
        # increase time2wakeup : next_action timestamp
        self.time2wakeup = self.actions[0]['timestamp']
        # print orderbook
        #if random() <= 0.003:
         #   self.print_orderbooks()
        # cancel some orders
        self.get_order_to_cancel()
        # send order
        o = self.create_order(self.ba['venue_id'], 
                              self['name'], 
                              current_action['side'], 
                              current_action['price'], 
                              current_action['quantity'], 
                              current_action['type'],
                              Order.DAY)
        # save o
        self.order_ids_list.append(o.orderId)
        self.order_sizes_list.append(current_action['quantity'])
        self.total_size += current_action['quantity']

        
        

