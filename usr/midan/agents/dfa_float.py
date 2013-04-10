from simep.core.tactic import Tactic
from simep.sched import Order, Trade
from simep.funcs.data.pyData import pyData
from simep.tools import date2num
from random import uniform
import math
from simep.core.busmanager import BusManager
from simep.funcs.dbtools.securities_tools import SecuritiesTools





class dfa_float(Tactic): 
    # trading rules
    # R1: order size  = 20% agg size
    # R2: order price = agg limit price
    # R3:
    
    __tbt2_dir__ = 'Q:/tick_ged'
    __database__ = 'sqlite'
    
    @staticmethod
    def public_parameters():
        
        setup      = {'name'              : {'label' : 'Name'             , 'value' : 'dfaFloat001'}}
        
        # client parameters
        parameters = {'ref_idx'           : {'label' : 'Reference Index', 'value' : 'FTE.PA'},
                      'ref_idx_sec_id'    : {'label' : 'Ref Idx Sec Id' , 'value' : 110},
                      'ref_idx_td_id'     : {'label' : 'Ref Idx TD Id'  , 'value' : 4},
                      'price_limit'       : {'label' : 'Price Limit'      , 'value' : 10}, 
                      'soft_limit'        : {'label' : 'Soft Limit'       , 'value' : 1}, 
                      'buy'               : {'label' : 'Buy'              , 'value' : Order.Buy},
                      'rel_del'           : {'label' : 'Reload Delay'     , 'value' : 0},
                      'child_size'        : {'label' : 'Child Order Size (%)' , 'value' : 20},
                      'child_fin'         : {'label' : 'Child Order Finished (%)', 'value' : 100},
                      'min_size_2_join'   : {'label' : 'Min Size to Join (%)' , 'value' : 20},
                      'hist_avg_t_size'   : {'label' : 'Historic Average Touch Size', 'value': 100},
                      'hist_trade_volume' : {'label' : 'Historic ADV', 'value': 10000},
                      'idx_rel_off'       : {'label' : 'Index Relative Offset (%)', 'value' : None},
                      'o_rel_trig'        : {'label' : 'Order Reload Trigger Volume (%)', 'value' : 0},                      
                      'tag_vol_src'       : {'label' : 'Target Volume Source = all', 'value' : True},
                      'rnd_fact'          : {'label' : 'Random Factor (%)', 'value' : 20},
                      'const_max_size'    : {'label' : 'Max Size override Rule Const (%)', 'value' : 50},
                      'one_tick'          : {'label' : 'One Tick Size', 'value' : 0.01},
                      'one_tick'          : {'label' : 'One Tick Size', 'value' : 0.01},                                                                  
                      'ats_width'         : {'label' : 'ATS Window Width (business time)', 'value' : 60}}
        
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def check_parameters(parameters):
        pass
    
    def __init__(self, setup, context, parameters, trace):
              
        super(dfa_float, self).__init__(setup, context, parameters, trace)
        self.__instantiate_ref_index()
        self.needExecReportEvt = True
        self.needAllEvts = True
        self.time_stamp = 0
        # volume timer
        self.vol_timer_join   = 0
        self.vol_timer_insert = 0
        # calendar timer 
        self.cal_timer_join   = 0
        self.cal_timer_insert = 0
        
        self.thres_vol_rnd_insert = 0
        self.thres_cal_rnd_insert = 0
        
        self.thres_vol_rnd_join = 0
        self.thres_cal_rnd_join = 0
        
        self.thres_vol_rnd_insert = 0
        self.thres_cal_rnd_insert = 0
        # volume of child orders that have been sent 
        self.sent_vol_child_order = 0
        # volume of child orders that have been executed
        self.exec_vol_child_order = 0
        # pct of finished child order
        self.pct_chld_order_fin = 0
        # if true then save all indicators
        self.exploration_mode = True
        # define a const 1 if Buy, -1 if Sell
        if (self['buy'] == Order.Buy):
            self.buy_const = +1
        else:
            self.buy_const = -1
        # to generate warnings
        self.last_keys = []
        # the first value for index
        self.first_idx_value = None
        
        # the first value for stock
        self.first_stock_value = None
                
        # vector of all market trades
        self.market_trades = []
        # initialize return for index 
        self.idx_ret = 0.0
        # initialize avg_price 
        self.avg_price = self['price_limit']
        # initialize exec qty
        self.exec_qty = 0.0
        # executions
        self.info_orders = {'insert': {'price': 0.0, 'size': 0.0, 'nb': 0.0}, \
                            'modify': {'price': 0.0, 'size': 0.0, 'nb': 0.0}, \
                            'cancel': {'price': 0.0, 'size': 0.0, 'nb': 0.0}, \
                            'exec'  : {'price': 0.0, 'size': 0.0, 'nb': 0.0} }
        self.info_indicators = { 'pct chld fin': 0.0, 'exec qty': 0.0, 'vwap strat': 0.0 }
    
    def __instantiate_ref_index(self):
        from simep.models.robmodel import ROBModel
        
        trace = BusManager.get_trace()
        sched = BusManager.get_sched()
        trading_hours = SecuritiesTools.get_trading_hours(dfa_float.__database__, 
                                                          self['ref_idx_sec_id'], 
                                                          self['ref_idx_td_id'], 
                                                          self['date'])
        
        setup   = {'name'                   :'ROB_REF_INDEX',
                   'login'                  : 'LOOGIN'}
        context = {'ric'                    : self['ref_idx'],
                   'security_id'            : self['ref_idx_sec_id'],
                   'trading_destination_id' : self['ref_idx_td_id'],
                   'date'                   : self['date'],
                   'input_file_name'        : dfa_float.__tbt2_dir__,
                   'data_type'              : 'TBT2',
                   'opening'                : trading_hours[0],
                   'closing'                : trading_hours[1]}
        params  = {'number_of_bt'           : 10,
                   'full'                   : False}
        sched.addOrderBook('light', self['ref_idx'])
        ref_idx_rob = ROBModel(setup, context, params, trace)
        sched.addAgent(ref_idx_rob)
        BusManager.new_bus({'trading_destination_id' : self['ref_idx_td_id'], 
                            'name'                   : 'stock_REF_IDX', 
                            'input_file_name'        : dfa_float.__tbt2_dir__, 
                            'date'                   : self['date'], 
                            'security_id'            : self['ref_idx_sec_id'], 
                            'ric'                    : self['ref_idx']},
                           [])
        
    def initialize(self, market, bus):
        Tactic.initialize(self, market, bus)
        # get index LOB
        self.idx_LOB = self.market.findOrderBook(self['ref_idx'])
        # from historic trades and price limit compute ADV
        # actually take the whole ADV no constraint on limit price
        self.thres_vol = self['o_rel_trig'] * self['hist_trade_volume'] /100        
        
        self.wait_2_join     = False
        self.wait_2_insert   = False
        
        self.no_delay = (self['rel_del'] == 0) and (self['o_rel_trig'] == 0)
        self.activateTimer('insert')
       
    def enabled(self, context = None):
        return True
    
    def process(self, evt):
        # depending on which event (from index LOB or stock LOB) 
        if evt.getOrderBookRIC() == self['ref_idx']:
            if self['idx_rel_off'] is not None:
                all_new_trades = evt.getTrades()
                if len(all_new_trades) > 0:
                    last_trade = all_new_trades[-1]
                # uniquely for the first observation of index
                if (self.first_idx_value is None) and (len(all_new_trades) > 0):
                    self.first_idx_value = all_new_trades[0].price
                
                # compute the index return iif there is new trade
                if len(all_new_trades) > 0:
                    self.idx_ret = (last_trade.price - self.first_idx_value)/self.first_idx_value 
                               
        elif evt.getOrderBookRIC() == self['ric']:
            
            # uniquely for the first observation of index
            if (self.first_stock_value is None):
                all_new_trades = evt.getTrades()
                if (len(all_new_trades) > 0):
                    self.first_stock_value = all_new_trades[0].price
            
            self.updateTimer(evt)
            [ord_prc, ord_size, min_quantity_ok] = self.computeOrderPriceVolume(evt)            
            
            ok_2_place_order = self.checkCond('insert', min_quantity_ok)
                       
            if ok_2_place_order:           
                self.info_orders['insert'] = {'price': ord_prc, 'size': ord_size, 'nb': 1}                 
                self.doAction('insert', ord_size, ord_prc)
            else:
                ok_2_modify_order = self.checkCond('join', min_quantity_ok)            
                if ok_2_modify_order:
                    self.info_orders['modify'] = {'price': ord_prc, 'size': ord_size, 'nb': 1}
                    # self.print_orderbook(evt)
                    self.doAction('join', ord_size, ord_prc)
            self.flushInfo()                       
            
        return None
    
    def processReport(self, evtReport):
        # update exec
        
        self.info_orders['exec']['price'] = (self.info_orders['exec']['price']*self.info_orders['exec']['size'] + evtReport.size * evtReport.price) / \
                                (self.info_orders['exec']['size'] + evtReport.size)
        self.info_orders['exec']['size'] += evtReport.size
        self.info_orders['exec']['nb'] +=1
        
        # update average execution price
        self.avg_price = 1.0*(self.exec_qty * self.avg_price + evtReport.size * evtReport.price)/(self.exec_qty + evtReport.size) 
        # update executed volume of child orders
        self.exec_vol_child_order += evtReport.size
        
        self.pct_chld_order_fin = 100.0 * self.exec_vol_child_order / self.sent_vol_child_order
        
        self.exec_qty += evtReport.size
        
        self.info_indicators['pct chld fin'] = self.pct_chld_order_fin
        self.info_indicators['exec qty']     = self.exec_qty
        self.info_indicators['vwap strat']   = self.avg_price 
        
        # trigger 2 timers iif 100% child orders finished, only once
        if (self.pct_chld_order_fin >= self['child_fin']):
            # activate the timer to insert            
            self.activateTimer('insert')
        
        return None
    
    def _createLimitOrder(self, qty, price, prefix):
        order = self.create_order(self['name'], self['buy'], price, qty, Order.Limit, Order.DAY)
        order.needExecReport = True
        self._orderbook.processCreateOrder(order)        
        return order
    
    def _createMarketOrder(self, qty, prefix):
        order = self.create_order(self['name'], self['buy'], 0.0, qty, Order.Market, Order.DAY)
        order.needExecReport = True
        self._orderbook.processCreateOrder(order)
        return order
    
    def _modifyOrder(self, qty, price):
        order        = self.create_order(self['name'], self['buy'], price, qty, Order.Limit, Order.DAY)
        order.remain = qty
        order.price  = price
        order_id = '%s$%03d' %(self['name'], self.active_order_number)
        order.orderId = order_id
        order.needExecReport = True
        self._orderbook.processReplaceOrder(order_id, order)
        return order
      
            
    def update(self, param):
        pass
    
    @staticmethod
    def mean(x):
        return sum(x)/len(x)
    
    def activateTimer(self, curState):
    
        # randomize the next reload timing +/- 20%    
        rnd_vol_fact  = 100 + uniform(-self['rnd_fact'],self['rnd_fact'])
        rnd_time_fact = 100 + uniform(-self['rnd_fact'],self['rnd_fact'])
        
        if curState == 'join':            
            # compute threshold for times to join            
            self.thres_vol_rnd_join  = self.thres_vol * rnd_vol_fact /100
            if self.no_delay:
                self.thres_time_rnd_join = uniform(1,120)
                #self.thres_time_rnd_join = 300
            else:
                self.thres_time_rnd_join = self['rel_del'] * rnd_time_fact /100
            # reset timers
            self.vol_timer_join = 0
            self.cal_timer_join = 0                
            # and trigger the timer to join
            self.wait_2_join = True            
            # stop the timer to insert
            self.wait_2_insert = False
            
        elif curState == 'insert':
            # compute threshold for times to insert            
            self.thres_vol_rnd_insert  = self.thres_vol * rnd_vol_fact /100
            if self.no_delay:
                self.thres_time_rnd_insert = uniform(1,120)
                # self.thres_time_rnd_insert = 600
            else:
                self.thres_time_rnd_insert = self['rel_del'] * rnd_time_fact /100
            # reset timers
            self.vol_timer_insert = 0
            self.cal_timer_insert = 0
            # and trigger the timer to insert
            self.wait_2_insert = True
            # de-active the timer to join
            self.wait_2_join = False                    
            
        return None
    
    def updateTimer(self, evt):
        
        # memorize the market trades
        cum_mk_vol = 0
        for trade in evt.getTrades():                
            if len(self.market_trades)>=self['ats_width']:
                self.market_trades.pop(0)                     
            self.market_trades.append((self.time_stamp, trade.price, trade.size))
            cum_mk_vol += trade.size
            
        if self.wait_2_insert:
            self.cal_timer_insert += 0.001*(evt.getEvtTime() - self.time_stamp)
            self.vol_timer_insert += cum_mk_vol
        
        if self.wait_2_join:
            self.cal_timer_join += 0.001*(evt.getEvtTime() - self.time_stamp)
            self.vol_timer_join += cum_mk_vol
                            
        self.time_stamp = evt.getEvtTime()
        
        # update indicators
        self.info_indicators['cal timer join']   =  self.cal_timer_join 
        self.info_indicators['vol timer join']   =  self.vol_timer_join 
        self.info_indicators['cal timer insert'] =  self.cal_timer_insert 
        self.info_indicators['vol timer insert'] =  self.vol_timer_insert 
        
        return None
    
    def computeOrderPriceVolume(self, evt):
        
        # compute the index relative limit price
        if self['idx_rel_off'] is not None:
            idx_rel_lim_price = (1 + (self.idx_ret - self['idx_rel_off']*self.buy_const/100))*self.first_stock_value
        else:
            idx_rel_lim_price = self['price_limit']
        # get the order book
        lob = evt.getLob()
        bid_price = lob.getBid(0).price
        ask_price = lob.getAsk(0).price        
        bid_size  = lob.getBid(0).size            
        ask_size  = lob.getAsk(0).size
            
        # compute ATS(60)
        # if no new trade, just take the historical value
        if (len(self.market_trades) > 0):
            last_60_trade = map(lambda x: x[2], self.market_trades)
            ats = dfa_float.mean(last_60_trade)
        else:
            ats = self['hist_avg_t_size']
        
        # Live Touch Minus 1 tick, false if not exist
        if (self['buy'] == Order.Buy): 
            live_touch_minus_1 = (lob.getBid(1).price < bid_price - self['one_tick'])
            live_touch_minus_1_tick_size = lob.getBid(1).size if live_touch_minus_1 else 0
        else:
            live_touch_minus_1 = (lob.getAsk(1).price > ask_price + self['one_tick'])
            live_touch_minus_1_tick_size = lob.getAsk(1).size if live_touch_minus_1 else 0

                            
        # compute all secondary parameters
        # update aggregate average size
        agg_size = (self['hist_avg_t_size'] + bid_size + ask_size + live_touch_minus_1_tick_size)/4
                    
        # compute the child order limit size
        # remark: need to convert to integer 
        ord_size = int(math.floor(self['child_size'] * agg_size / 100))
        
        # compute the price of child order
        ord_prc = (min(self['price_limit'], idx_rel_lim_price, bid_price) if (self['buy'] == Order.Buy) \
                   else max(self['price_limit'], idx_rel_lim_price, ask_price))
                     

        # R7 Max Size Override Price Rule
        far_touch = (lob.getBid(0) if (self['buy'] == Order.Buy) else lob.getAsk(0))            
        if ord_size > self['const_max_size']*far_touch.size/100:
            ord_prc -= self['one_tick']*self.buy_const
             
        # R8 Random Price Offset Rule
        # generate a random number
        a_rnd_value = uniform(0,101)
        ord_prc -= (max(self['one_tick'] , ord_prc * uniform(5,150)/10000.0) if a_rnd_value > 99 else 0)*self.buy_const
        
        # and also verify some condition        
        # min size to join BID or OFFER
        # modify the exist order
        if (self['buy'] == Order.Buy):
            idx_prc = sum([lob.getBid(i).price>=ord_prc for i in range(0,4)])-1
            if idx_prc > 3:
                quantity_at_prc = agg_size
            else:
                quantity_at_prc = lob.getBid(idx_prc).size
        else:
            idx_prc = sum([lob.getAsk(i).price<=ord_prc for i in range(0,4)])-1
            if idx_prc > 3:
                quantity_at_prc = agg_size
            else:
                quantity_at_prc = lob.getAsk(idx_prc).size
            
        min_size_2_join_ok = (quantity_at_prc > self['min_size_2_join']*agg_size/100.0)  
        
        #if min_size_2_join_ok:
        #    ord_prc = bid_price if (self['buy'] == Order.Buy) else ask_price
        
        # save to indicators
        self.info_indicators['ats']          =  ats
        self.info_indicators['bid size']     =  bid_size  
        self.info_indicators['ask size']     =  ask_size 

        self.info_indicators['agg size']     =  agg_size 
        self.info_indicators['order size']   =  ord_size 
        self.info_indicators['order price']  =  ord_prc 
        self.info_indicators['ask price']    =  ask_price 
        self.info_indicators['bid price']    =  bid_price 
        
        self.info_indicators['min size join ok'] =  min_size_2_join_ok
        self.info_indicators['idx rel price']    =  idx_rel_lim_price          
        self.info_indicators['wait to insert']   =  self.wait_2_insert 
        self.info_indicators['wait to join']     =  self.wait_2_join
        
        
        return [ord_prc, ord_size, min_size_2_join_ok]
    
    def checkCond(self, curState, min_quantity_ok):
        if curState == 'insert':
            cond_ok = ((((self.vol_timer_insert >= self.thres_vol_rnd_insert) or (self.cal_timer_insert >= self.thres_time_rnd_insert)) and not (self.no_delay)) \
                       or ((self.cal_timer_insert >= self.thres_time_rnd_insert) and self.no_delay)) and ((self.avg_price - self['soft_limit'])*self.buy_const > 0) \
                       and self.wait_2_insert
                       
        elif curState == 'join':
            cond_ok = ((((2*self.vol_timer_join >= self.thres_vol_rnd_join) or (2*self.cal_timer_join >= self.thres_time_rnd_join)) and not (self.no_delay)) \
                       or ((self.cal_timer_join >= self.thres_time_rnd_join) and self.no_delay)) \
                       and min_quantity_ok \
                       and self.wait_2_join
        return cond_ok   
        
    
    def doAction(self, curState, ord_size, ord_prc):
        if curState != 'none':
            # update sent volume of child orders 
            self.sent_vol_child_order = ord_size
            # and reset the executed volume of child orders
            self.exec_vol_child_order = 0
            
            if curState == 'insert':
                # trading rule R1 + R2
                self._activeOrder = self._createLimitOrder(ord_size, ord_prc, 'limitPrice')
                self.active_order_number = self._order_number
            elif curState == 'join':                
                self._modifyOrder(ord_size, ord_prc)               
            
            # activate the timer to join 
            self.activateTimer('join')
            
        return None            
        
    def flushInfo(self):
        
        vals = {}
        
        # universal values
        for k in self.info_orders.keys():
            vals['%s price' % k] = [self.info_orders[k]['price']]
            vals['%s size'  % k] = [self.info_orders[k]['size']]
            vals['%s nb'    % k] = [self.info_orders[k]['nb']]
            self.info_orders[k] = {'price': 0.0, 'size': 0.0, 'nb': 0.0}
        
        # specific values
        if self.exploration_mode:
            for k in self.info_indicators.keys():
                vals[k] = [self.info_indicators[k]]
            
            
            if len(self.last_keys)>0:
                for nk in list( set(self.info_indicators.keys()) ^ set(self.last_keys)):
                    print ">>> you should initialize <%s> before the first call to flushIndicators..." % nk
            self.last_keys = self.info_indicators.keys()
        # save
        self.appendIndicator( pyData('init', date = [self.time_stamp], value = vals ))
         
    def print_orderbook(self, evt):
        lob = evt.getLob()
        lob_str  = '%s, BID1: %04d @ %06.03f' %(evt.getEvtTimeStr(), lob.getBid(0).size, lob.getBid(0).price)
        lob_str += '    '
        lob_str += 'ASK1: %04d @ %06.03f' %(lob.getAsk(0).size, lob.getAsk(0).price)
        print lob_str
        for i in range(1,5):
            lob_str  = '%s, BID%d: %04d @ %06.03f' %('            ', i+1, lob.getBid(i).size, lob.getBid(i).price)
            lob_str += '    '
            lob_str += 'ASK%d: %04d @ %06.03f' %(i+1, lob.getAsk(i).size, lob.getAsk(i).price)
            print lob_str
        print ''
        
        