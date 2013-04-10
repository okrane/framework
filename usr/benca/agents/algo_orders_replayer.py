'''
Created on 8 sept. 2010

@author: benca
'''

import sys
import os
from simep import __stsim_directory__, MAXIMUM_VALID_PRICE, SCREEN_RESOLUTION_WIDTH, SCREEN_RESOLUTION_HEIGHT, __release__
sys.path.append(os.path.dirname(__stsim_directory__)+'st_algo')
from AlgHoster.lib.pydelegate.scripts.simep.moneymanager import MoneyManager


from simep.sched import Order
from simep.core.baseagent import BaseAgent
from simep.funcs.stdio.utils import pyLog
from simep.tools import date2num
from usr.dev.benca.funcs.stdio.orders_loader import OrdersLoader
if __release__:
    from simep.core.analyticsmanager import AnalyticsManager
    from datetime import datetime
    import matplotlib.pyplot




class EventReport:

    def __init__(self, price, size, timestamp, security_id, venue_id):
        self.evtTimeMs     = timestamp
        self.price         = price
        self.size          = size
        self.eventType     = 1 & 16
        self.obVenueId     = venue_id
        self.obSecuritytId = security_id
        self.orderId       = ""


class AlgoOrdersReplayer(BaseAgent):
    
    MAX_DESYNCHRONIZATION_TIME = 1000000L
    
    @staticmethod
    def public_parameters():
        setup      = {'name'            : {'label' : 'Name'                    , 'value' : 'OrdersReplayer001'}}
        parameters = {'plot_mode'       : {'label' : 'Plot Mode (0, 1, 2)'     , 'value' : 0},
                      'delta_t'         : {'label' : 'Delta Time'              , 'value' : '00:00:02:000000'},
                      'cmd_filename'    : {'label' : 'Orders Commands Filename', 'value' : 'C:/st_sim/usr/dev/benca/data/detail_occ_3_Jn}0026.txt'},
                      'observer_name'   : {'label' : 'Name Of The Observer'    , 'value' : 'SimpleAlgoOrdersObserver'},
                      'observer_module' : {'label' : 'Module Of The Observer'  , 'value' : 'usr.dev.benca.agents.simple_algo_orders_observer'}}
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return [] #['avg_spread_bp_t_60', 'avg_trade_size_t_60', 'vwavg_price_m_t_60']
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, params, trace):
        self._figure_title = '[date=' + context['date'] + ', ric=' + str(context['ric']) + ']'
        params['delta_t']                = date2num(params['delta_t'])
        super(AlgoOrdersReplayer, self).__init__(setup, context, params, trace)
        ba                               = self.ba
        if len(ba['venue_ids'])         == 1:
            ba['venue_id']               = ba['venue_ids'][0]
        else:
            ba['venue_id']               = 'all'
        venue_id                         = ba['venue_id']
        tmp                              = OrdersLoader.load_orders(self['cmd_filename'])
        ba['cmd_pydata']                 = tmp[0]
        ba['header']                     = tmp[1]
        ba['side']                       = Order.Buy if ba['header']['Side']=='B' else Order.Sell 
        ba['unbufferize_time']           = ba['feed'][venue_id][0]['start_time']
        ba['market_manager']             = ba['feed'][venue_id][0]
        ba['market_actions']             = []
        ba['cmd_index']                  = 0
        ba['algo_buffered_actions']      = []
        ba['algo_buffered_actions_time'] = ba['cmd_pydata'].date[ba['cmd_index']]
        while ba['cmd_index'] < len(ba['cmd_pydata'].date) and ba['algo_buffered_actions_time'] == ba['cmd_pydata'].date[ba['cmd_index']]:
            ba['algo_buffered_actions'].append(dict([(key,val[ba['cmd_index']]) for (key,val) in ba['cmd_pydata'].value.iteritems()]))
            ba['cmd_index']             += 1
        ba['money_manager']              = MoneyManager()
        ba['money_manager'].setTotalQty(ba['header']['OrderQty'])
        ba['event_report']               = EventReport(-1.0, 0, 0L, context['security_id'], venue_id)
        ba['observing_start_time']       = ba['cmd_pydata'].date[0]  - self['delta_t']
        ba['observing_end_time']         = ba['cmd_pydata'].date[-1] + self['delta_t']
        ba['algo_action_time']           = 0L
        ba['prev_prev_feed']             = {}
        prev_prev_feed                   = ba['prev_prev_feed']
        prev_prev_feed['BEST_ASK1']      = None
        prev_prev_feed['BEST_BID1']      = None
        prev_prev_feed['BEST_ASIZ1']     = 0
        prev_prev_feed['BEST_BSIZ1']     = 0
        prev_prev_feed['TIME_NUM']       = 0L
        setup_observers                  = setup.copy()
        setup_observers['name']          = params['observer_name']+'000'
        cmd                              = "from %s import %s\nba['observer'] = %s(setup_observers, context, {'print' : True, 'log_filename' : 'C:/obs1.log'}, trace)"%(params['observer_module'],params['observer_name'],params['observer_name'])
        exec cmd in locals()
        """
        Definition of curves and variables relative to curves
        """
        ba['curves']                     = {}
        ba['curves'][venue_id]           = {}
        curves                           = ba['curves'][venue_id]
        curves['BEST_ASK']               = []
        curves['BEST_BID']               = []
        curves['DATETIMES']              = []
        curves['MKT_TRADES']             = ([], [])
        curves['ALGO_TRADES']            = ([], [])
        curves['ALGO_INSERTS']           = ([], [])
        curves['ALGO_CANCELS']           = ([], [])
        curves['ALGO_EXECS']             = ([], [])
        curves['year']                   = int(context['date'][:4])
        curves['month']                  = int(context['date'][4:6])
        curves['day']                    = int(context['date'][6:8])
        curves['figure_id']              = AnalyticsManager.get_figure_number()
        ba['curves']['start_datetime']   = datetime(curves['year'], 
                                            curves['month'], 
                                            curves['day'], 
                                            (ba['observing_start_time']/3600000000L),
                                            (ba['observing_start_time']%3600000000L)/60000000L,
                                            (ba['observing_start_time']%60000000L)/1000000L,
                                            (ba['observing_start_time']%1000000L))
        ba['curves']['end_datetime']     = datetime(curves['year'], 
                                            curves['month'], 
                                            curves['day'], 
                                            (ba['observing_end_time']/3600000000L),
                                            (ba['observing_end_time']%3600000000L)/60000000L,
                                            (ba['observing_end_time']%60000000L)/1000000L,
                                            (ba['observing_end_time']%1000000L))
        
    
    
    
    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
    
    def initialize(self):
        super(AlgoOrdersReplayer, self).initialize()
        ba                                  = self.ba
        self.marketManager.feed             = ba['market_manager']
        self.marketManager.ae_venue_id      = ba['venue_id']
        self.marketManager.default_venue_id = ba['venue_id']
        ba['observer'].marketManager        = self.marketManager
        ba['observer'].moneyManager         = ba['money_manager']
        
        
    def process(self, event):
        code     = self.update(event)
        ba       = self.ba
        time_num = ba['event']['TIME_NUM']
        if code == 1 and time_num >= ba['observing_start_time'] and time_num <= ba['observing_end_time']:
            """
            Get the previous and current feed, and then destrip them
            """
            venue_id  = ba['venue_id']
            curr_feed = ba['feed'][venue_id][0]
            prev_feed = ba['feed'][venue_id][1]
            self.__destrip(ba, time_num, curr_feed, prev_feed)
            if self['plot_mode'] != 0:
                self.__update_market_curves(ba, venue_id, time_num, curr_feed)
            """
            Temporary verifications
            """
            print ba['market_actions']
            self.print_orderbooks()
            algo_did_actions  = False
            cmd_pydata        = ba['cmd_pydata']
            number_of_actions = len(cmd_pydata.date)
            cmd_index         = ba['cmd_index']
            """
            Get the current algo action timestamp
            """
            if len(ba['algo_buffered_actions']) == 0 and cmd_index < number_of_actions:
                ba['algo_buffered_actions_time'] = cmd_pydata.date[cmd_index]
                while cmd_index < len(cmd_pydata.date) and ba['algo_buffered_actions_time'] == cmd_pydata.date[cmd_index]:
                    ba['algo_buffered_actions'].append(dict([(key,val[cmd_index]) for (key,val) in cmd_pydata.value.iteritems()]))
                    cmd_index += 1
                ba['cmd_index'] = cmd_index
            """
            Check that the current action is not out of the time window
            """
            if time_num > ba['algo_buffered_actions_time']+self['delta_t']:
                raise RuntimeError("AlgoOrdersReplayer::process -> algo action wasn't been found in the market inside the time window")
            """
            If the current time is located in the time window [current_algo_time-1s, current_algo_time+1s], 
            then we can perform the search.
            """
            if time_num >= ba['algo_buffered_actions_time']-self['delta_t']:
                for market_action in ba['market_actions']:
                    j = 0
                    for i in range(len(ba['algo_buffered_actions'])):
                        i          -= j
                        algo_action = ba['algo_buffered_actions'][i]
                        """
                        Match algo_action with market action
                        """
                        condition  = (market_action['Price'] == algo_action['Price'] or (algo_action['Type'] in ('I','U') and market_action['Aggressive']))
                        condition &= ((algo_action['Type'] in ('I','Z') and market_action['OrderQty'] == algo_action['OrderQty']) or (algo_action['Type'] in ('C','U') and market_action['OrderQty'] == algo_action['LeavesQty']))
                        condition &= (market_action['Type'] == algo_action['Type'] or (algo_action['Type'] == 'U' and market_action['Type'] == 'I'))
                        if condition:
                            """
                            Create the order and execution dictionnaries
                            """
                            order = {}
                            order['AmountValidation'] = True
                            if algo_action['Type'] == 'I':
                                order['EffectiveTime'] = algo_action['Timestamp']
                            else:
                                order['EffectiveTime'] = self.__find_INSERT_cmd_key('Timestamp', ba)
                            order['Side']              = '1' if algo_action['Side']=='B' else '2'
                            order['Price']             = str(algo_action['Price'])
                            order['LeavesQty']         = str(algo_action['LeavesQty'])
                            order['OrderQty']          = str(algo_action['OrderQty'])
                            order['OrderType']         = str(algo_action['OrderType'])
                            order['VenueId']           = str(self['trading_destination_id'])
                            order['ExpireTime']        = str(algo_action['ExpireTime'])
                            order['TimeInForce']       = str(algo_action['TimeInForce'])
                            # create execution
                            execution = {}
                            execution['execId']        = str(algo_action['MarketOrderId'])
                            execution['execRefId']     = str(algo_action['OrderMarketRef'])
                            execution['timestamp']     = str(algo_action['Timestamp'])
                            execution['quantity']      = str(algo_action['OrderQty'])
                            execution['price']         = str(algo_action['Price'])
                            """
                            Update the money manager
                            """
                            if   algo_action['Type'] == 'I':
                                ba['money_manager'].updateBook(MoneyManager.NEW      , self['ric'], algo_action['OrderInternalRef'], order, execution)
                                ba['observer'].process(event)
                                self.__add_algo_action('I', ba, venue_id, time_num, algo_action['Price'], algo_action['OrderQty'])
                            elif algo_action['Type'] == 'Z':
                                event.size  = algo_action['OrderQty']
                                event.price = algo_action['Price']
                                ba['money_manager'].updateBook(MoneyManager.EXECUTION, self['ric'], algo_action['OrderInternalRef'], order, execution)
                                ba['observer'].processReport(event)
                                self.__add_algo_action('Z', ba, venue_id, market_action['Time'], algo_action['Price'], algo_action['OrderQty'])
                            elif algo_action['Type'] == 'C':
                                ba['money_manager'].updateBook(MoneyManager.CANCEL   , self['ric'], algo_action['OrderInternalRef'], order, execution)
                                ba['observer'].process(event)
                                self.__add_algo_action('C', ba, venue_id, time_num, algo_action['Price'], algo_action['LeavesQty'])
                            elif algo_action['Type'] == 'U':
                                ba['money_manager'].updateBook(MoneyManager.MODIFY   , self['ric'], algo_action['OrderInternalRef'], order, execution)
                                ba['observer'].process(event)
                                self.__add_algo_action('U', ba, venue_id, time_num, algo_action['Price'], algo_action['LeavesQty'])
                            """
                            Clear some stuff
                            """
                            ba['algo_buffered_actions'].pop(i)
                            j += 1
                            ba['money_manager'].clearLastExecutions()
                            algo_did_actions = True
                            break
            """
            Call the observer process function, even if there is no algo action detected
            """
            if not algo_did_actions:
                ba['observer'].process(event)
            return False
    
    def results(self):
        ba                      = self.ba
        if self['plot_mode'] != 0:
            self.__draw(ba, ba['venue_id'])
        self.indicators         = ba['observer'].results()
        self.indicators.info['class_name'] = 'AlgoOrdersReplayer'
        return self.indicators
    
    
    
    '''######################################################################################################
    #########################################   PRIVATE FUNCTIONS   #########################################
    ######################################################################################################'''
    
    def __find_INSERT_cmd_key(self, key, ba):
        cmd   = None
        index = ba['cmd_index']-1
        while index >= 0:
            if ba['cmd_pydata'].value['OrderInternalRef'][index] == ba['cmd_pydata'].value['OrderInternalRef'][index]:
                cmd = ba['cmd_pydata'].value[key][index]
                break
            index -= 1
        if cmd == None:
            raise RuntimeError("FATAL ERROR : insert order couldn't be found")
        return cmd
    
    def __destrip(self, ba, time_num, curr_feed, prev_feed):
        ba['market_actions'] = []
        """
        Previous LOB will be modified, so we need to copy it. Current LOB will not be modified.
        """
        prev_bid  = [e for e in prev_feed['BEST_BID']]
        prev_bsiz = [e for e in prev_feed['BEST_BSIZ']]
        curr_bid  = curr_feed['BEST_BID']
        curr_bsiz = curr_feed['BEST_BSIZ']
        prev_ask  = [e for e in prev_feed['BEST_ASK']]
        prev_asiz = [e for e in prev_feed['BEST_ASIZ']]
        curr_ask  = curr_feed['BEST_ASK']
        curr_asiz = curr_feed['BEST_ASIZ']
        prev_prev_feed = ba['prev_prev_feed']
        """
        We have to update the "prev" and "prev_prev" parameters, which mainly work as a state machine. This
        is just a fix in order to deal with high frequency updates
        """
        if prev_prev_feed['BEST_ASK1'] == None:
            prev_prev_feed['BEST_ASK1']  = prev_ask[0]
            prev_prev_feed['BEST_ASIZ1'] = prev_asiz[0]
            prev_prev_feed['TIME_NUM']   = prev_feed['TIME_NUM']
        if prev_prev_feed['BEST_BID1'] == None:
            prev_prev_feed['BEST_BID1']  = prev_bid[0]
            prev_prev_feed['BEST_BSIZ1'] = prev_bsiz[0]
            prev_prev_feed['TIME_NUM']   = prev_feed['TIME_NUM']
        """
        First, we have to aggregate trades according to their prices
        """
        trades = {}
        for i in curr_feed['LAST_DEALS_IDXS']:
            if curr_feed['DEALS_TYPES'][i] in (0,18):
                trades.setdefault(curr_feed['DEALS_PRICES'][i], 0)
                trades[curr_feed['DEALS_PRICES'][i]] += curr_feed['DEALS_QTYS'][i]
        """
        Second, we have to remove the current trades from the old lob, in order to detect executions
        """
        min_price        = curr_feed['LAST_TRDS_GROUP']['MIN_PRICE']
        max_price        = curr_feed['LAST_TRDS_GROUP']['MAX_PRICE']
        executed_bid_qty = 0
        executed_ask_qty = 0
        aggressive_sell  = False
        aggressive_buy   = False
        for deal_price,deal_qty in trades.iteritems():
            if deal_price <= prev_bid[0]:
                aggressive_sell = True
                for j in range(5):
                    bid_price = prev_bid[j]
                    if deal_price == bid_price:
                        size = deal_qty - prev_bsiz[j]
                        if size > 0:
                            ba['market_actions'].append({'Type':'I', 'OrderQty':size, 'Price':bid_price, 'Aggressive': False})
                            prev_bsiz[j]  = 0
                        else:
                            prev_bsiz[j] -= deal_qty
                            if min_price < bid_price:
                                ba['market_actions'].append({'Type':'C', 'OrderQty':prev_bsiz[j], 'Price':bid_price, 'Aggressive': False})
                                prev_bsiz[j] = 0
                        executed_bid_qty += deal_qty
            elif deal_price >= prev_ask[0]:
                aggressive_buy = True
                for j in range(5):
                    ask_price = prev_ask[j]
                    if deal_price == ask_price:
                        size = deal_qty - prev_asiz[j]
                        if size > 0:
                            ba['market_actions'].append({'Type':'I', 'OrderQty':size, 'Price':ask_price, 'Aggressive': False})
                            prev_asiz[j]  = 0
                        else:
                            prev_asiz[j] -= deal_qty
                            if max_price > ask_price:
                                ba['market_actions'].append({'Type':'C', 'OrderQty':prev_asiz[j], 'Price':ask_price, 'Aggressive': False})
                                prev_asiz[j] = 0
                        executed_ask_qty += deal_qty
            elif prev_prev_feed['TIME_NUM'] >= time_num-AlgoOrdersReplayer.MAX_DESYNCHRONIZATION_TIME:
                if deal_price <= prev_prev_feed['BEST_BID1']:
                    aggressive_sell = True
                    ba['market_actions'].append({'Type':'I', 'OrderQty':deal_qty, 'Price':deal_price, 'Aggressive': False})
                    executed_bid_qty += deal_qty
                elif deal_price >= prev_prev_feed['BEST_ASK1']:
                    aggressive_buy = True
                    ba['market_actions'].append({'Type':'I', 'OrderQty':deal_qty, 'Price':deal_price, 'Aggressive': False})
                    executed_ask_qty += deal_qty
        """
        Add a generic market order just in order to know when this one took place in the sequence of ba['market_actions']
        """
        if aggressive_sell:
            ba['market_actions'].append({'Type':'I', 'OrderQty':executed_bid_qty , 'Price':min_price, 'Aggressive': True})
        if aggressive_buy:
            ba['market_actions'].append({'Type':'I', 'OrderQty':executed_ask_qty, 'Price':max_price, 'Aggressive': True})
        """
        Add all the trades AFTER the insertion of the aggressive order(s) !
        """
        for i in curr_feed['LAST_DEALS_IDXS']:
            if curr_feed['DEALS_TYPES'][i] in (0,18):
                ba['market_actions'].append({'Type':'Z', 'OrderQty':curr_feed['DEALS_QTYS'][i], 'Price':curr_feed['DEALS_PRICES'][i], 'Time':curr_feed['DEALS_TIMES'][i],'Aggressive': False})
        """
        BID : now, we can substract lobs in order to find the insertion/cancels
        """
        curr_i = 0
        prev_i = 0
        while curr_i<5 and prev_i<5:
            curr_price = curr_bid[curr_i]
            prev_price = prev_bid[prev_i]
            if prev_price == curr_price:
                size = curr_bsiz[curr_i] - prev_bsiz[prev_i]
                if size > 0:
                    ba['market_actions'].append({'Type':'I', 'OrderQty':size, 'Price':curr_price, 'Aggressive': False})
                elif size < 0:
                    ba['market_actions'].append({'Type':'C', 'OrderQty':-size, 'Price':curr_price, 'Aggressive': False})
                curr_i += 1
                prev_i += 1
            elif curr_price > prev_price:
                ba['market_actions'].append({'Type':'I', 'OrderQty':curr_bsiz[curr_i], 'Price':curr_price, 'Aggressive': False})
                curr_i += 1
            elif prev_bsiz[prev_i] != 0:
                ba['market_actions'].append({'Type':'C', 'OrderQty':prev_bsiz[prev_i], 'Price':prev_price, 'Aggressive': False})
                prev_i += 1
            else:
                prev_i += 1
        """
        ASK : now, we can substract lobs in order to find the insertion/cancels
        """
        curr_i = 0
        prev_i = 0
        while curr_i<5 and prev_i<5:
            curr_price = curr_ask[curr_i]
            prev_price = prev_ask[prev_i]
            if prev_price == curr_price:
                size = curr_asiz[curr_i] - prev_asiz[prev_i]
                if size > 0:
                    ba['market_actions'].append({'Type':'I', 'OrderQty':size, 'Price':curr_price, 'Aggressive': False})
                elif size < 0:
                    ba['market_actions'].append({'Type':'C', 'OrderQty':-size, 'Price':curr_price, 'Aggressive': False})
                curr_i += 1
                prev_i += 1
            elif curr_price < prev_price:
                ba['market_actions'].append({'Type':'I', 'OrderQty':curr_asiz[curr_i], 'Price':curr_price, 'Aggressive': False})
                curr_i += 1
            elif prev_asiz[prev_i] != 0:
                ba['market_actions'].append({'Type':'C', 'OrderQty':prev_asiz[prev_i], 'Price':prev_price, 'Aggressive': False})
                prev_i += 1
            else:
                prev_i += 1
        """
        Update the prev_prev params
        """
        if prev_ask[0] != curr_ask[0]:
            prev_prev_feed['BEST_ASK1']  = prev_ask[0]
            prev_prev_feed['BEST_ASIZ1'] = prev_asiz[0]
            prev_prev_feed['TIME_NUM']   = prev_feed['TIME_NUM']
        if prev_bid[0] != curr_bid[0]:
            prev_prev_feed['BEST_BID1']  = prev_bid[0]
            prev_prev_feed['BEST_BSIZ1'] = prev_bsiz[0]
            prev_prev_feed['TIME_NUM']   = prev_feed['TIME_NUM']
        
    
    def __update_market_curves(self, ba, venue_id, time_num, curr_feed):
        curves        = ba['curves'][venue_id]
        curves['BEST_ASK'].append(curr_feed['BEST_ASK1'])
        curves['BEST_BID'].append(curr_feed['BEST_BID1'])
        curves['DATETIMES'].append(datetime(curves['year'], 
                                            curves['month'], 
                                            curves['day'], 
                                            (time_num/3600000000L),
                                            (time_num%3600000000L)/60000000L,
                                            (time_num%60000000L)/1000000L,
                                            (time_num%1000000L)))
        if curr_feed['TRADE_EVENT']:
            for i in curr_feed['LAST_DEALS_IDXS']:
                t = curr_feed['DEALS_TIMES'][i]
                v = curr_feed['DEALS_QTYS'][i]
                curves['MKT_TRADES'][0].append(datetime(curves['year'], 
                                                        curves['month'], 
                                                        curves['day'], 
                                                        (t/3600000000L),
                                                        (t%3600000000L)/60000000L,
                                                        (t%60000000L)/1000000L,
                                                        (t%1000000L)))
                curves['MKT_TRADES'][1].append(v)
    
    def __add_algo_action(self, type, ba, venue_id, time, price, qty):
        curves = ba['curves'][venue_id]
        time   = datetime(curves['year'], 
                 curves['month'], 
                 curves['day'], 
                 (time/3600000000L),
                 (time%3600000000L)/60000000L,
                 (time%60000000L)/1000000L,
                 (time%1000000L))
        if type == 'Z':
            curves['ALGO_TRADES'][0].append(time)
            curves['ALGO_TRADES'][1].append(qty)
            for i in range(len(curves['MKT_TRADES'][0])-1, 0, -1):
                if curves['MKT_TRADES'][0][i] == time and curves['MKT_TRADES'][1][i] == qty:
                    curves['MKT_TRADES'][0].pop(i)
                    curves['MKT_TRADES'][1].pop(i)
                    break
            curves['ALGO_EXECS'][0].append(time)
            curves['ALGO_EXECS'][1].append(price)
        elif type in ('I', 'U'):
            curves['ALGO_INSERTS'][0].append(time)
            curves['ALGO_INSERTS'][1].append(price)
        elif type == 'C':
            curves['ALGO_CANCELS'][0].append(time)
            curves['ALGO_CANCELS'][1].append(price)
    
    def __draw(self, ba, venue_id):
        curves = ba['curves'][venue_id]
        matplotlib.pyplot.figure(curves['figure_id'], figsize=(SCREEN_RESOLUTION_WIDTH/80.0,SCREEN_RESOLUTION_HEIGHT/80.0))
        matplotlib.pyplot.get_current_fig_manager().window.wm_geometry('+-16+-16')
        """
        Price curves
        """
        matplotlib.pyplot.subplot(211)
        matplotlib.pyplot.plot_date(curves['DATETIMES'], curves['BEST_ASK'], xdate=True, ydate=False, color='g', linestyle='-', linewidth=1, marker='None')
        matplotlib.pyplot.plot_date(curves['DATETIMES'], curves['BEST_BID'], xdate=True, ydate=False, color='c', linestyle='-', linewidth=1, marker='None')
        matplotlib.pyplot.plot_date(curves['ALGO_INSERTS'][0], curves['ALGO_INSERTS'][1], xdate=True, ydate=False, color='b', linestyle='', marker='^', markersize=8.0)
        matplotlib.pyplot.plot_date(curves['ALGO_CANCELS'][0], curves['ALGO_CANCELS'][1], xdate=True, ydate=False, color='r', linestyle='', marker='v', markersize=8.0)
        matplotlib.pyplot.plot_date(curves['ALGO_EXECS'][0]  , curves['ALGO_EXECS'][1]  , xdate=True, ydate=False, color='r', linestyle='', marker='x', markersize=8.0)
        """
        Volume curves
        """
        matplotlib.pyplot.subplot(212)
        matplotlib.pyplot.bar(curves['MKT_TRADES'][0] , curves['MKT_TRADES'][1] , color='g', width=0.00001, linewidth=0, orientation='vertical')
        matplotlib.pyplot.bar(curves['ALGO_TRADES'][0], curves['ALGO_TRADES'][1], color='r', width=0.00001, linewidth=0, orientation='vertical')
        matplotlib.pyplot.plot_date(curves['MKT_TRADES'][0] , curves['MKT_TRADES'][1] , xdate=True, ydate=False, linestyle='', marker='o', markerfacecolor='g', markersize=8.0)
        matplotlib.pyplot.plot_date(curves['ALGO_TRADES'][0], curves['ALGO_TRADES'][1], xdate=True, ydate=False, linestyle='', marker='o', markerfacecolor='r', markersize=8.0)
        """
        Labels, grid, limits,... for subplot(211)
        """
        sub = matplotlib.pyplot.subplot(211)
        matplotlib.pyplot.xlim(ba['curves']['start_datetime'],ba['curves']['end_datetime'])
        matplotlib.pyplot.ylabel('price')
        matplotlib.pyplot.grid(True)
        sub.xaxis.set_major_formatter( (matplotlib.dates.DateFormatter('%H:%M:%S')))
        for label in sub.xaxis.get_ticklabels():
            label.set_rotation(-30)
        """
        Labels, grid, limits,... for subplot(212)
        """
        sub = matplotlib.pyplot.subplot(212)
        matplotlib.pyplot.xlim(ba['curves']['start_datetime'],ba['curves']['end_datetime'])
        matplotlib.pyplot.ylabel('volume')
        matplotlib.pyplot.grid(True)
        sub.xaxis.set_major_formatter( (matplotlib.dates.DateFormatter('%H:%M:%S')))
        for label in sub.xaxis.get_ticklabels():
            label.set_rotation(-30)
        
