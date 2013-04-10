'''
Created on 13 sept. 2010

@author: elber
'''

from simep import __release__
from simep.sched import Order
from simep.core.baseagent import BaseAgent
from simep.tools import date2num
from random import gauss, random
from numpy.random import exponential, randint,gamma,poisson
import math

class ZI3Model(BaseAgent):
    
    @staticmethod
    def public_parameters():
        parameters = {'mu'                      : {'label' : 'number of Market order rate'             , 'value' : 0.0001},
                      'sigma'                   : {'label' : 'order size'                    , 'value' : 100},
                      'alpha'                   : {'label' : 'number of Limit Order Fill Rate'         , 'value' : 0.0001},
                     'order_size_std'           : {'label' :  'Order size std'               ,  'value': 1.00  },
                      'start_price'             : {'label' : 'Start Price'                   , 'value' : 20.0},
                      'tick_size'               : {'label' : 'Tick Size'                     , 'value' : 0.01},                      
                      'execution_start_time'    : {'label' : 'Execution Start Time'          , 'value' : '+00:10:00:000000'},
                      'delta_cancel'            : {'label' : 'Delta cancellation of orders'  , 'value' : 0.000005},
                      'price_std'               : {'label' : 'price std'                      , 'value': 0.01},
                      'price_mean'              : {'label' : ' price mean'                    , 'value' : 0.01},
                      'price_law'               : {'label' : 'price law'                       ,'value' : 'log_normal'},
                      'size_law'                : {'label' : 'size law'                        ,'value' : 'half_normal'}}
        return {'setup': {}, 'parameters': parameters}
    
    def __init__(self, setup, context, params, trace):
        trad_id = context['trading_destination_ids'][0]
        for k,v in context.iteritems():
            if isinstance(v, dict):
                context[k] = v[trad_id]
        context['opening']              = date2num(context['opening'])
        context['closing']              = date2num(context['closing'])
        context['execution_start_time'] = date2num(context['execution_start_time'][1:])+context['opening']
        if (float(context['alpha']) - float(context['mu'])) > 0 :
            context['delta_cancel']         = (float(context['alpha']) - float(context['mu']))
        else :
            context['delta_cancel'] =0.000000000001
        super(ZI3Model,self).__init__(setup, context, params, trace)
        ba                     = self.ba
        ba['venue_id']         = ba['venue_ids'][0]
        self.needAllEvts       = False
        self.needExecReportEvt = True
        self.setIsSimulator(True)
        self.time2wakeup       = self['opening']
        self.actions           = []
        self.start_time        = self['opening']
        self.order_size_std    = 0#math.sqrt(math.pi/2)*self['sigma']
        self.venue_id = self.ba['venue_ids'][0]
        self.order_ids_list    = []
        self.order_sizes_list  = []
        self.total_size        = 0
        self.MO_size           = 0
        self.LO_size           = 0
        self.cancelled_size    = 0
        self['mu']                  = float(self['mu'])
        self['sigma']               = float (self['sigma'])
        self['alpha']               = float( self['alpha'])
        self.t  = {}
        self.t1 = {}   
        self.t2 = {}
        self.t3 = {}       
        self.t1['time']      = long(exponential(1.0/(self['alpha']))) + self.start_time        
        self.t1['operation'] = "limit_order"
        self.t2['time']      = long(exponential(1.0/(self['mu']))) + self.start_time
        self.t2['operation'] = "market_order"
        self.t3['time']      = long(exponential(1/self['delta_cancel'])) + self.start_time 
        self.t3['operation'] = "cancel_order"
        self.generate_actions()
        
    # we generate one action 
    def generate_actions(self):
        # get feed
        ba   = self.ba
        feed = ba['feed'][ba['venue_id']][0]
        best_bid = feed['BEST_BID1'] if feed['BEST_BID1'] != None else self['start_price']
        best_ask = feed['BEST_ASK1'] if feed['BEST_ASK1'] != None else best_bid
        
        
        # select the lowest time
        times = [self.t1['time'], self.t2['time'], self.t3['time']]
        d   = min(times)
        idx = times.index(d)
        t   = [self.t1, self.t2, self.t3][idx]
        #print self.start_time
        #print self.t1['time']
        #print self.t2['time']
        #print self.t3['time']
        
        if self['size_law']== 'half_normal' :
        #draw a half-normal sample
            qty = gauss(self['sigma'], self['order_size_std'])
            qty = abs(int(qty)) 
        elif self['size_law']== 'log_normal' :
            qty = gauss(math.log(self['sigma']), self['order_size_std'])
            qty = math.exp((qty)) 
            qty = round(qty)
            qty = int(qty)
        else :
            qty = 100
            
        side = Order.Buy if randint(2) else Order.Sell 
        #spread_log    = math.log(best_ask) - math.log(best_bid)
        #spread        = best_ask - best_bid
            #nb_tick_spread = int(round(spread/self['tick_size']))
        if t['operation'] == "limit_order"  : 
            moyenne    = math.log(self['price_mean'])
            if side == Order.Buy: 
                if self['price_law']=='log_normal':
                    a           = math.exp(gauss(moyenne,self['price_std'])) 
                    price       = best_ask - a
                    price       = round(price/self['tick_size'])* self['tick_size']
                    while price < 0 :
                        a        = math.exp(gauss(moyenne,self['price_std'])) 
                        price    = best_ask - a
                        price    = round(price/self['tick_size'])* self['tick_size']
                elif self['price_law']=='gamma' :
                    a           = gamma(self['price_std'],self['price_mean'])
                    price       = best_ask - a
                    price       = round(price/self['tick_size'])* self['tick_size']
                    while price < 0 :
                        a        = math.exp(gauss(moyenne,self['price_std'])) 
                        price    = best_ask - a
                        price    = round(price/self['tick_size'])* self['tick_size']
                elif self['price_law']=='poisson' :
                    a           = poisson(self['price_mean']/self['tick_size' ])
                    a           = a*self['tick_size']
                    price       = best_ask - a
                    #price       = round(price/self['tick_size'])* self['tick_size']
                    while price < 0 :
                        a           = poisson(self['price_mean']/self['tick_size' ])
                        a           = a*self['tick_size']
                        price       = best_ask - a
                        #price    = round(price/self['tick_size'])* self['tick_size']
                elif self['price_law']== 'uniforme':
                    #on se limit a 100 ticks
                    a           = randint(self['price_mean']*10000)
                    a           = a*self['tick_size']
                    price       = best_ask - a
                    price       = round(price/self['tick_size'])* self['tick_size']
                    while price < 0 :
                        a           = randint(self['price_mean']*10000)
                        a           = a*self['tick_size']
                        price       = best_ask - a
                        price       = round(price/self['tick_size'])* self['tick_size']
                    
                    
            else: 
                if self['price_law']=='log_normal': 
                    #moyenne   = math.log(spread)                      
                    a       = math.exp(gauss(moyenne,self['price_std']))
                    price   = best_bid + a
                    price   = round(price/self['tick_size'])* self['tick_size'] 
                elif self['price_law']=='gamma' :
                    a           = gamma(self['price_std'],self['price_mean'])
                    price   = best_bid + a
                    price   = round(price/self['tick_size'])* self['tick_size'] 
                elif self['price_law']=='poisson':
                    a           = poisson(self['price_mean']/self['tick_size' ])
                    a           = a*self['tick_size']
                    price   = best_bid + a
                elif self['price_law']== 'uniforme':
                    a           = randint(self['price_mean']*10000)
                    a           = a*self['tick_size']
                    price   = best_bid + a
                        
                    
                    
            self.actions.append({'timestamp'          : t['time'] ,
                                    'type'        : Order.Limit,
                                    'quantity'    : qty        ,
                                    'side'        : side       ,
                                    'type_action' : "fillin"      ,
                                    'price'       : price})
            self.t1['time']  += long(exponential(1.0/self['alpha']))
            
             
        elif  t['operation'] == "market_order" :
            if feed['TIME_NUM'] >= self['execution_start_time']:
                self.actions.append({'timestamp'     : t['time']   ,
                                       'type'        : Order.Market,
                                       'quantity'    : qty         ,
                                       'side'        : side        ,
                                       'type_action' : "fillin"      ,
                                       'price'       : 0.0})
            self.t2['time']  += long(exponential(1.0/self['mu']))
           
            
        
                
        elif t['operation'] == "cancel_order" :
            if feed['TIME_NUM'] >= self['execution_start_time'] :    
                self.actions.append({'timestamp'     : t['time']   ,
                                       'type'        : Order.Market,
                                       'quantity'    : qty         ,
                                       'side'        : side        ,
                                       'type_action' : "cancel"      ,
                                       'price'       : 0.0})                
            self.t3['time']  += exponential(1.0/self['delta_cancel'])
            
    
            
    
    def process(self, event):        
        # update
        self.update(event)
        # check closing
        if(self.currentTime >= self['closing']):            
            return True
        # get feed
        ba   = self.ba
        #feed = ba['feed'][ba['venue_id']][0]
        #=======================================================================
        # # check trades
        # if feed['TRADE_EVENT']:
        #    self.total_size -= feed['LAST_TRDS_GROUP']['VOLUME']
        #=======================================================================
        # get action
        current_action = None
        if len(self.actions) >= 1:
            current_action = self.actions.pop(0)
        # generate an action
        self.generate_actions()
        # increase time2wakeup : next_action timestamp
        if len(self.actions) != 0 :
            self.time2wakeup = long(self.actions[0]['timestamp'])
        
        # send or delete an order
        if current_action != None :
            #add an order to the order_book
            if current_action['type_action'] == "fillin" :
                o = self.create_order(self.ba['venue_id'], 
                              self['name'], 
                              current_action['side'], 
                              current_action['price'], 
                              current_action['quantity'], 
                              current_action['type'],
                              Order.DAY)
                # save o
                if current_action['type'] == Order.Limit:
                    self.order_ids_list.append(o.orderId)
                    self.order_sizes_list.append(current_action['quantity'])
                    #self.total_size += current_action['quantity']
            else :
                #cancel an order 
                n               = len(self.order_ids_list)
                if n >= self['min_limit_orders_num']:
                    idx             = randint(n)
                    #self.cancel_order(self.venue_id)
                    order_id        = self.order_ids_list.pop(idx)
                    self.cancelOrder(self.venue_id, order_id)
                    self.total_size -= self.order_sizes_list.pop(idx)
                    #self.actions.pop(order_id)
            # print orderbook
        if random() <= self['print_frequency']:
            self.print_orderbooks()
    
    def processReport(self, event):
        order_id = event.orderSnapshot.orderId
        try:
            self.order_ids_list.remove(order_id)
        except ValueError:
            pass 
    