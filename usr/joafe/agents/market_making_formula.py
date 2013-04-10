'''
Created on 2 febr. 2011

@author: oligu-joafe-benca
'''

from simep.core.basetrader import BaseTrader
from simep.sched import Order
from simep.tools import date2num
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog
import matplotlib.pyplot
import math




class MyMarketMakingStrategy(BaseTrader):
    
    @staticmethod
    def public_parameters():
        setup      = {'name'                   : {'label' : 'Name'               , 'value' : 'MyMarketMakingStrategy001'}}
        parameters = {'slice_timestep'         : {'label' : 'Slicing Timestep'   , 'value' : '00:05:00:000000'}, 
                      'plot_mode'              : {'label' : 'Plot Mode (0, 1, 2)', 'value' : 1}}
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return ['garman_klass_m_s_300', 'avg_spread_m_s_300', 'avg_trade_size_t_300']
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, params, trace):
        self._figure_title               = '[date=%s, ric=%s]' %(context['date'], context['ric'])
        BaseTrader.__init__(self, setup, context, params, trace)
        self._use_volume_proportion_plot = False
        self._disable_volume_curves      = True
        ''' SLICES PARAMETERS'''
        self._algo_start_time            = self._bus['start_time'] + date2num('00:05:00:000000')
        self._timestep                   = date2num(self['slice_timestep'])
        self._next_update                = self._algo_start_time + self._timestep
        self.slices                      = [(self._algo_start_time, 0)]
        ''' SPREAD VARIABLES '''
        self._spread_sum                 = 0.0
        self._number_of_spread_measures  = 0
        self._garman_klass_sum           = 0.0
        self._number_of_gk_measures      = 0
        self._avg_trade_size             = 0.0
        self._number_of_ats_measures     = 0
        ''' STRATEGY VARIABLES '''
        self.bought_shares               = 0
        self.sold_shares                 = 0
        self.pnl                         = 0.0
        self.tick_size                   = 0.01
        self.q                           = 0.0
        self._buy_order                  = None
        self._sell_order                 = None
        self.delta_b                     = 0
        self.delta_a                     = 0
        self.delta_b_list                = []
        self.delta_a_list                = []
        self.pnls_list                   = []
        self.q_list                      = []
        self.mid_price                   = 0.0
        self.last_bid_price_1            = None
        self.last_ask_price_1            = None
        self.bid_price_1                 = None
        self.ask_price_1                 = None
    
    def display_gamma_curve(self):
        sigma = self['sigma']
        k     = self['k']
        A     = self['A']
        x     = [0.001+0.001*i for i in range(1000)]
        y     = [1.0/e*math.log(1+e/k)+0.5*math.sqrt(sigma**2*e/(2*k*A)*(1+e/k)**(1+k/e)) for e in x]
        matplotlib.pyplot.plot(x,y)
        matplotlib.pyplot.show()
    
    def rnd_bid(self, x):
        return int(x/self.tick_size)*self.tick_size
    
    def rnd_ask(self, x):
        return int(x/self.tick_size+1)*self.tick_size
    
    def print_orderbook(self, lob):
        lob_str  = '%s BID1: %05d @ %06.03f' %(self._timestamp + ',', lob.getBid(0).size, lob.getBid(0).price)
        lob_str += '    '
        lob_str += 'ASK1: %05d @ %06.03f' %(lob.getAsk(0).size, lob.getAsk(0).price)
        pyLog('')
        pyLog(lob_str)
        for i in range(1,5):
            lob_str  = '%s BID%d: %05d @ %06.03f' %('                ', i+1, lob.getBid(i).size, lob.getBid(i).price)
            lob_str += '    '
            lob_str += 'ASK%d: %05d @ %06.03f' %(i+1, lob.getAsk(i).size, lob.getAsk(i).price)
            pyLog(lob_str)
        pyLog('')
        raw_input("Press 'Enter' to continue...")
        pyLog('')
        
    
    
    
    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
    
    def process(self, event):
        self.slices_calculator(event)
        code = self.update(event)
        if code == 1 and self._time_t >= self._algo_start_time:
            if not self['perform_market_making']:
                if self._bus['avg_spread_m_s_300'] != None:
                    self._spread_sum += self._bus['avg_spread_m_s_300']
                    self._number_of_spread_measures += 1
                if self._bus['garman_klass_m_s_300'] != None:
                    self._garman_klass_sum += self._bus['garman_klass_m_s_300']
                    self._number_of_gk_measures += 1
                if self._bus['avg_trade_size_t_300'] != None:
                    self._avg_trade_size += self._bus['avg_trade_size_t_300']
                    self._number_of_ats_measures += 1
                return False
            # check that there are two limits
            if self._bus['bid_price_2'] == None or self._bus['ask_price_2'] == None:
                self.cancel_buy_order()
                self.cancel_sell_order()
                return False
            self.mid_price = self.compute_mid_price()
            # check that we have at least 2 orderbooks
            if self.last_ask_price_1 == None or self.last_bid_price_1 == None:
                self.last_bid_price_1 = self.bid_price_1
                self.last_ask_price_1 = self.ask_price_1
                return False
            # debug
            if self['debug']:
                self.print_orderbook(event.getLob())
            # create local variables
            dq = self['dq']
            e  = self['gamma']
            k  = self['k']
            s  = self['sigma']
            A  = self['A']
            # Q
            self.q = float(self.bought_shares - self.sold_shares)/dq
            # buy case
            self.delta_b = 1.0/e*math.log(1+e/k)+0.5*(2.0*self.q+1.0)*math.sqrt(s**2*e/(2.0*k*A)*(1.0+e/k)**(1.0+k/e))
            price_bid = self.rnd_bid(self.mid_price - self.delta_b)
            # sell case
            self.delta_a = 1.0/e*math.log(1+e/k)-0.5*(2.0*self.q-1.0)*math.sqrt(s**2*e/(2.0*k*A)*(1.0+e/k)**(1.0+k/e))
            price_ask = self.rnd_ask(self.mid_price + self.delta_a)
            # check prices have moved
            if self.last_bid_price_1 != self.bid_price_1 or self.last_ask_price_1 != self.ask_price_1:
                self.cancel_buy_order()
                self.cancel_sell_order()
            else:
                return False
            # check prices are different
            if price_bid >= price_ask:
                if self.delta_a > self.delta_b:
                    price_ask += self.tick_size
                else:
                    price_bid -= self.tick_size
            if price_bid >= price_ask:
                raise 'bid_price and ask_price are crossing !'
            # improve prices if spread is large
            if self.bid_price_1+self.tick_size < price_bid:
                price_bid = self.bid_price_1 + self.tick_size
            if self.ask_price_1-self.tick_size > price_ask:
                price_ask = self.ask_price_1 - self.tick_size
            if self['benchmark']:
                price_bid = self.bid_price_1
                price_ask = self.ask_price_1
            # send orders
            if self._buy_order == None:
                self.send_limit_buy_order(dq , price_bid)
            if self._sell_order == None:
                self.send_limit_sell_order(dq, price_ask)
            self.last_bid_price_1 = self.bid_price_1
            self.last_ask_price_1 = self.ask_price_1
    
    def processReport(self, event):
        code     = self.update_report(event)
        if code == 1:
            fields = event.orderSnapshot.orderId.split('$')
            if fields[1] == 'BUY':
                self.bought_shares += event.size
                self.pnl -= event.size*event.price
                if self['debug']:
                    if self._buy_order.orderId != event.orderId:
                        raise 'bad execution : expected order_id was %s, and received %s' %(self._buy_order.orderId, event.orderId)
                self.cancel_buy_order()
                if self['debug']:
                    print 'executed buy order : %d@%f' %(event.size,event.price)
            elif fields[1] == 'SELL':
                self.sold_shares += event.size
                self.pnl += event.size*event.price
                if self['debug']:
                    if self._sell_order.orderId != event.orderId:
                        raise 'bad execution : expected order_id was %s, and received %s' %(self._sell_order.orderId, event.orderId)
                self.cancel_sell_order()
                if self['debug']:
                    print 'executed sell order : %d@%f' %(event.size,event.price)
        return None
    
    
    
    '''######################################################################################################
    ######################################   PURE VIRTUAL FUNCTIONS   #######################################
    ######################################################################################################'''
    
    def _pvtl__update_child_plot_curves(self):
        if self['show_curve'] == 'pnl':
            price = self._bus['market_vwap'] if self._bus['market_vwap'] != None else self.mid_price
            self.pnls_list.append(self.pnl + (self.bought_shares-self.sold_shares)*price)
        elif self['show_curve'] == 'delta':
            self.delta_b_list.append(-self.delta_b)
            self.delta_a_list.append(self.delta_a)
        elif self['show_curve'] == 'q':
            self.q_list.append(self['dq']*self.q)
    
    def _pvtl__update_child_plot_figure(self):
        matplotlib.pyplot.subplot(211)
        if self['show_curve'] == 'pnl':
            matplotlib.pyplot.plot_date(self._bus['curves']['datetime'],  self.pnls_list, xdate=True, ydate=False, color='r', linestyle='-', linewidth=1, marker='None')
        elif self['show_curve'] == 'delta':
            matplotlib.pyplot.plot_date(self._bus['curves']['datetime'],  self.delta_a_list, xdate=True, ydate=False, color='r', linestyle='-', linewidth=1, marker='None')
            matplotlib.pyplot.plot_date(self._bus['curves']['datetime'],  self.delta_b_list, xdate=True, ydate=False, color='g', linestyle='-', linewidth=1, marker='None')
        elif self['show_curve'] == 'q':
            matplotlib.pyplot.plot_date(self._bus['curves']['datetime'],  self.q_list   , xdate=True, ydate=False, color='g', linestyle='-', linewidth=1, marker='None')
    
    def _pvtl__update_last_plot(self):
        pass
    
    def _pvtl__last_update(self):
        if self['perform_market_making']:
            print self._bus['market_volume']
            print self._bus['business_time_t']
            print self.bought_shares
            print self.sold_shares
        else:
            print self._spread_sum / self._number_of_spread_measures
            print self._garman_klass_sum / self._number_of_gk_measures
            print self._avg_trade_size / self._number_of_ats_measures
        
    
    
    '''######################################################################################################
    #########################################   PRIVATE FUNCTIONS   #########################################
    ######################################################################################################'''
    
    def slices_calculator(self, event):
        if event.eventType != event.IdleEvent:
            time_t = event.getEvtTime()
            if time_t < self._algo_start_time:
                return
            while time_t > self._next_update:
                self.slices.append( (self._next_update, self._bus['market_volume']) )
                self._next_update += self._timestep
    
    def send_limit_buy_order(self, qty, price):
        price = round(price*1000000)/1000000.0
        order = self.create_order(self['name']+'$BUY', Order.Buy, price, int(qty), Order.Limit, Order.DAY)
        order.needExecReport = True
        if self['debug']:
            print 'sending buy order : %d@%f' %(qty,price)
        self._orderbook.processCreateOrder(order)
        self._buy_order = order
        return order
    
    def send_limit_sell_order(self, qty, price):
        self.cancel_sell_order()
        price = round(price*1000000)/1000000.0
        order = self.create_order(self['name']+'$SELL', Order.Sell, price, int(qty), Order.Limit, Order.DAY)
        order.needExecReport = True
        if self['debug']:
            print 'sending sell order : %d@%f' %(qty,price)
        self._orderbook.processCreateOrder(order)
        self._sell_order = order
        return order
    
    def cancel_sell_order(self):
        if self._sell_order != None:
            self._orderbook.processCancelOrder(self._sell_order.orderId)
            self._sell_order = None    
    
    def cancel_buy_order(self):
        if self._buy_order != None:
            self._orderbook.processCancelOrder(self._buy_order.orderId)
            self._buy_order = None
    
    def compute_mid_price(self):
        # bid
        bidp1 = self._bus['bid_price_1']
        bids1 = self._bus['bid_size_1']
        if self._buy_order != None:
            buy_price = self._buy_order.price
            buy_size  = self._buy_order.remain
            if buy_price == bidp1 and bids1 == buy_size:
                considered_best_bid = self._bus['bid_price_2']
            else:
                considered_best_bid = bidp1
        else:
            considered_best_bid = bidp1
        self.bid_price_1 = considered_best_bid
        # ask
        askp1 = self._bus['ask_price_1']
        asks1 = self._bus['ask_size_1']
        if self._sell_order != None:
            ask_price = self._sell_order.price
            ask_size  = self._sell_order.remain
            if ask_price == askp1 and asks1 == ask_size:
                considered_best_ask = self._bus['ask_price_2']
            else:
                considered_best_ask = askp1
        else:
            considered_best_ask = askp1
        self.ask_price_1 = considered_best_ask
        return (considered_best_ask + considered_best_bid)/2.0
    
    
    
    
    
    
    
    