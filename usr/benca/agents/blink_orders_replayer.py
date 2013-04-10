'''
Created on 8 sept. 2010

@author: benca
'''

import sys
import os
from simep import __stsim_directory__, MAXIMUM_VALID_PRICE, SCREEN_RESOLUTION_WIDTH, SCREEN_RESOLUTION_HEIGHT, __release__
sys.path.append(os.path.dirname(__stsim_directory__)+'st_algo')


from simep.sched import Order
from simep.core.baseagent import BaseAgent
from simep.funcs.stdio.utils import pyLog
from simep.tools import date2num
from usr.dev.benca.funcs.stdio.orders_loader import OrdersLoader
if __release__:
    from simep.core.analyticsmanager import AnalyticsManager
    from datetime import datetime
    import matplotlib.pyplot




class BlinkOrdersReplayer(BaseAgent):
    
    MAX_DESYNCHRONIZATION_TIME = 1000000L
    
    @staticmethod
    def public_parameters():
        parameters = {'plot_mode'       : {'label' : 'Plot Mode (0, 1, 2)'     , 'value' : 0},
                      'cmd_filename'    : {'label' : 'Orders Commands Filename', 'value' : 'C:/st_sim/usr/dev/benca/data/detail_occ_3_Jn}0026.txt'},
                      'observer_name'   : {'label' : 'Name Of The Observer'    , 'value' : 'BlinkAntigamingIndicators'},
                      'observer_module' : {'label' : 'Module Of The Observer'  , 'value' : 'usr.dev.benca.agents.blink_antigaming_indicators'}}
        return {'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return [] #['avg_spread_bp_t_60', 'avg_trade_size_t_60', 'vwavg_price_m_t_60']
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, params, trace):
        super(BlinkOrdersReplayer, self).__init__(setup, context, params, trace)
        ba                               = self.ba
        if len(ba['venue_ids'])         == 1:
            ba['venue_id']               = ba['venue_ids'][0]
        else:
            raise ValueError('There should be only the primary trading_destination_id of the given stock for BlinkOrdersReplayer')
        venue_id                         = ba['venue_id']
        tmp                              = OrdersLoader.load_orders(self['cmd_filename'])
        ba['cmd_pydata']                 = tmp[0]
        ba['header']                     = tmp[1]
        cmd_pydata                       = ba['cmd_pydata']
        ba['cmd_index']                  = 0
        ba['algo_buffered_actions']      = []
        ba['algo_buffered_actions_time'] = cmd_pydata.date[ba['cmd_index']]
        while ba['cmd_index'] < len(cmd_pydata.date) and ba['algo_buffered_actions_time'] == cmd_pydata.date[ba['cmd_index']]:
            ba['algo_buffered_actions'].append(dict([(key,val[ba['cmd_index']]) for (key,val) in cmd_pydata.value.iteritems()]))
            ba['cmd_index']                += 1
        setup_observers                     = setup.copy()
        setup_observers['name']             = params['observer_name']+'000'
        cmd                                 = "from %s import %s\nba['observer']  =  %s(setup_observers, context, {'print' : True, 'log_filename' : 'C:/obs1.log'}, trace)"%(params['observer_module'],params['observer_name'],params['observer_name'])
        exec cmd in locals()
        ba['observer'].indicators           = self.indicators
        """
        Get the first trade timestamp and last trade timestamp for each client order id
        """
        ba['observing_start_time']          = cmd_pydata.date[0]
        ba['observing_end_time']            = cmd_pydata.date[-1] + 1000000L
        """
        Definition of curves and variables relative to curves
        """
        ba['curves']                        = {}
        curves                              = ba['curves']
        curves['BEST_ASK']                  = []
        curves['BEST_BID']                  = []
        curves['DATETIMES']                 = []
        curves['MKT_TRADES']                = ([], [])
        curves['year']                      = int(context['date'][:4])
        curves['month']                     = int(context['date'][4:6])
        curves['day']                       = int(context['date'][6:8])
        curves['CLIENTS_TRADES']            = {}
        curves['CLIENTS_INSERTS']           = {}
        curves['CLIENTS_CANCELS']           = {}
        curves['CLIENTS_EXECS']             = {}
        curves['figure_ids']                = {}
        ba['curves']['start_datetime']      = None
        
    
    
    
    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
    
    def initialize(self):
        super(BlinkOrdersReplayer, self).initialize()
        ba                                  = self.ba
        self.marketManager.ae_venue_id      = ba['venue_id']
        self.marketManager.default_venue_id = ba['venue_id']
        ba['observer'].marketManager        = self.marketManager
        ba['observer'].initialize()
        
    def process(self, event):
        code       = self.update(event)
        ba         = self.ba
        time_num   = ba['event']['TIME_NUM']
        cmd_pydata = ba['cmd_pydata']
        if code == 1 and time_num >= cmd_pydata.date[0] and time_num <= cmd_pydata.date[-1]+1000000L:
            """
            Get the previous and current feed, and then destrip them
            """
            venue_id  = ba['venue_id']
            curr_feed = ba['feed'][venue_id][0]
            if self['plot_mode'] != 0:
                self.__update_market_curves(ba, time_num, curr_feed)
            """
            Temporary verifications
            """
            
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
            If the current time is located in the time window [current_algo_time-1s, current_algo_time+1s], 
            then we can perform the search.
            """
            if time_num >= ba['algo_buffered_actions_time']:
                for i in range(len(ba['algo_buffered_actions'])):
                    algo_action         = ba['algo_buffered_actions'][i]
                    ba['observer'].processAction(algo_action)
                    self.__add_algo_action(ba, algo_action)
                ba['algo_buffered_actions'] = []
            """
            Call the observer process function, even if there is no algo action detected
            """
            ba['observer'].process(event)
            return False
    
    def results(self):
        ba = self.ba
        if self['plot_mode'] != 0:
            self.__draw(ba)
        ba['observer'].results()
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
        
    
    def __update_market_curves(self, ba, time_num, curr_feed):
        curves        = ba['curves']
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
    
    def __add_algo_action(self, ba, algo_action):
        curves          = ba['curves']
        client_order_id = algo_action['ClientOrderId']
        time            = algo_action['Timestamp']
        if  not curves['figure_ids'].has_key(client_order_id):
            curves['figure_ids'][client_order_id]      = AnalyticsManager.get_figure_number()
            curves['CLIENTS_TRADES'][client_order_id]  = ([], [])
            curves['CLIENTS_INSERTS'][client_order_id] = ([], [])
            curves['CLIENTS_CANCELS'][client_order_id] = ([], [])
            curves['CLIENTS_EXECS'][client_order_id]   = ([], [])
        # get datetime
        time   = datetime(curves['year'], 
                          curves['month'], 
                          curves['day'], 
                          (time/3600000000L),
                          (time%3600000000L)/60000000L,
                          (time%60000000L)/1000000L,
                          (time%1000000L))
        # update start time and end time
        if ba['curves']['start_datetime'] == None:
            ba['curves']['start_datetime'] = time
        ba['curves']['end_datetime']       = time
        # update curves
        if algo_action['Type'] == 'Z':
            curves['CLIENTS_TRADES'][client_order_id][0].append(time)
            curves['CLIENTS_TRADES'][client_order_id][1].append(algo_action['OrderQty'])
            curves['CLIENTS_EXECS'][client_order_id][0].append(time)
            curves['CLIENTS_EXECS'][client_order_id][1].append(algo_action['Price'])
        elif algo_action['Type'] in ('I', 'U'):
            curves['CLIENTS_INSERTS'][client_order_id][0].append(time)
            curves['CLIENTS_INSERTS'][client_order_id][1].append(algo_action['Price'])
        elif algo_action['Type'] == 'C':
            curves['CLIENTS_CANCELS'][client_order_id][0].append(time)
            curves['CLIENTS_CANCELS'][client_order_id][1].append(algo_action['Price'])
    
    def __draw(self, ba):
        curves = ba['curves']
        count  = 0
        for client_order_id, figure_id in curves['figure_ids'].iteritems():
            if len(curves['figure_ids']) == 1:
                matplotlib.pyplot.figure(figure_id, figsize=(SCREEN_RESOLUTION_WIDTH/80.0,SCREEN_RESOLUTION_HEIGHT/80.0))
                matplotlib.pyplot.get_current_fig_manager().window.wm_geometry('+-16+-16')
            elif len(curves['figure_ids']) == 2:
                matplotlib.pyplot.figure(figure_id, figsize=(SCREEN_RESOLUTION_WIDTH/80.0,SCREEN_RESOLUTION_HEIGHT/170.0))
                y = SCREEN_RESOLUTION_HEIGHT/2*(count)
                y -= 20 if y <= 10 else 8
                matplotlib.pyplot.get_current_fig_manager().window.wm_geometry('+-16+%d' %y)
            else:
                matplotlib.pyplot.figure(figure_id, figsize=(SCREEN_RESOLUTION_WIDTH/150.0,SCREEN_RESOLUTION_HEIGHT/170.0))
                y = SCREEN_RESOLUTION_HEIGHT/2*(((count)/2)%2)
                x = SCREEN_RESOLUTION_WIDTH /2*((count)%2)-16
                y -= 20 if y <= 10 else 8
                matplotlib.pyplot.get_current_fig_manager().window.wm_geometry('+%d+%d' %(x,y))  
            count += 1
            matplotlib.pyplot.title("[ric=%s, date=%s, client_order_id=%s]" %(self['ric'], self['date'], client_order_id))
            """
            Price curves
            """
            matplotlib.pyplot.subplot(211)
            matplotlib.pyplot.plot_date(curves['DATETIMES'], curves['BEST_ASK'], xdate=True, ydate=False, color='g', linestyle='-', linewidth=1, marker='None')
            matplotlib.pyplot.plot_date(curves['DATETIMES'], curves['BEST_BID'], xdate=True, ydate=False, color='c', linestyle='-', linewidth=1, marker='None')
            matplotlib.pyplot.plot_date(curves['CLIENTS_INSERTS'][client_order_id][0], curves['CLIENTS_INSERTS'][client_order_id][1], xdate=True, ydate=False, color='b', linestyle='', marker='^', markersize=8.0)
            matplotlib.pyplot.plot_date(curves['CLIENTS_CANCELS'][client_order_id][0], curves['CLIENTS_CANCELS'][client_order_id][1], xdate=True, ydate=False, color='r', linestyle='', marker='v', markersize=8.0)
            matplotlib.pyplot.plot_date(curves['CLIENTS_EXECS'][client_order_id][0]  , curves['CLIENTS_EXECS'][client_order_id][1]  , xdate=True, ydate=False, color='r', linestyle='', marker='x', markersize=8.0)
            """
            Volume curves
            """
            matplotlib.pyplot.subplot(212)
            matplotlib.pyplot.bar(curves['MKT_TRADES'][0] , curves['MKT_TRADES'][1] , color='g', width=0.00001, linewidth=0, orientation='vertical')
            matplotlib.pyplot.bar(curves['CLIENTS_TRADES'][client_order_id][0], curves['CLIENTS_TRADES'][client_order_id][1], color='r', width=0.00001, linewidth=0, orientation='vertical')
            matplotlib.pyplot.plot_date(curves['MKT_TRADES'][0] , curves['MKT_TRADES'][1] , xdate=True, ydate=False, linestyle='', marker='o', markerfacecolor='g', markersize=8.0)
            matplotlib.pyplot.plot_date(curves['CLIENTS_TRADES'][client_order_id][0], curves['CLIENTS_TRADES'][client_order_id][1], xdate=True, ydate=False, linestyle='', marker='o', markerfacecolor='r', markersize=8.0)
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
        
