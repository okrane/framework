'''
Created on 2 febr. 2011

@author: oligu-joafe-benca
'''
from simep import MAXIMUM_VALID_PRICE, __release__
from simep.core.baseplotagent import BasePlotAgent
from simep.sched import Order
from simep.tools import date2num
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog
from math import exp, sqrt, log, ceil, floor
from random import random,seed
import numpy
import scipy.linalg as sl
from usr.dev.joafe.scenarii.TrdCrv import TrdCrv
if __release__:
    import matplotlib.pyplot
seed(5)


class BestExec(BasePlotAgent):
    
    
    '''######################################################################################################
    ##########################################   STATIC METHODS   ###########################################
    ######################################################################################################'''
    
    @staticmethod
    def public_parameters():
        setup      = {'name'                   : {'label' : 'Name'                                              , 'value' : 'BestExec001'}}
        parameters = {'calibration_timestep'   : {'label' : 'Calibration Timestep'                              , 'value' : '00:05:00:000000'}, 
                      'slice_timestep'         : {'label' : 'Slice Timestep'                                    , 'value' : '00:05:00:000000'}, 
                      'slice_sub_timestep'     : {'label' : 'Slice Sub Timestep'                                , 'value' : '00:00:01:000000'},
                      'asked_qty'              : {'label' : 'Algo Asked Quantity'                               , 'value' : 10000},
                      'side'                   : {'label' : 'Side'                                              , 'value' : 'Order.Sell'},
                      'max_exec_time'          : {'label' : 'Maximum Time For an Order to be (partially) filled', 'value' : '00:00:20:000000'},
                      'ref_order_size'         : {'label' : 'Reference Order Size'                              , 'value' : 0.8},
                      'initial_ats'            : {'label' : 'Average Trading Size aka ATS'                      , 'value' : 200},
                      'ewma_theta_ats'         : {'label' : 'EWMA Theta for ATS'                                , 'value' : 1.0/6.0},
                      'ewma_theta'             : {'label' : 'EWMA Theta'                                        , 'value' : 1.0/24.0},
                      'max_spread'             : {'label' : 'Maximum Spread For Conditional Calibration'        , 'value' : 3},
                      'max_order_price'        : {'label' : 'Maximum Number of Ticks above BEST_ASK1'           , 'value' : 3},
                      'algo_start_time'        : {'label' : 'Algo Start Time'                                   , 'value' : '+01:00:00:000000'},
                      'algo_end_time'          : {'label' : 'Algo End Time'                                     , 'value' : '+03:00:00:000000'},
                      'initial_A'              : {'label' : 'Initial Value of A'                                , 'value' : '0.2,0.2,0.2'},
                      'initial_k'              : {'label' : 'Initial Value of k'                                , 'value' : '0.8,0.8,0.8'},
                      'initial_sigma'          : {'label' : 'Initial Value of Sigma'                            , 'value' : 0.001},
                      'initial_gamma'          : {'label' : 'Initial Value of Gamma'                            , 'value' : 0.01},
                      'matrix_size'            : {'label' : 'Size of the Matrix'                                , 'value' : 12},
                      'tolerance'              : {'label' : 'Tolerance for Newton Function'                     , 'value' : 0.00001},
                      'plot_mode'              : {'label' : 'Plot Mode (0, 1, 2)'                               , 'value' : 1},
                      'total_volume'           : {'label' : 'Total Volume expected in the day'                  , 'value' : 5000000},
                      'delta_target'           : {'label' : 'First Expected Quote (0, 1, 2)'                    , 'value' : 1},
                      'mean_A'                 : {'label' : 'Mean A'                                            , 'value' : 0.2},
                      'mean_k'                 : {'label' : 'Mean k'                                            , 'value' : 0.8},
                      'mean_spread'            : {'label' : 'Mean Spread'                                       , 'value' : 2}}
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return []
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, params, trace):
        params['initial_A']            = map(float, params['initial_A'].split(','))
        params['initial_k']            = map(float, params['initial_k'].split(','))
        self.ba                        = {}
        params['calibration_timestep'] = date2num(params['calibration_timestep'])
        params['slice_timestep']       = date2num(params['slice_timestep'])
        params['slice_sub_timestep']   = date2num(params['slice_sub_timestep'])
        params['max_exec_time']        = date2num(params['max_exec_time'])
        params['algo_start_time']      = date2num(params['algo_start_time'][1:])
        params['algo_end_time']        = date2num(params['algo_end_time'][1:])
        BasePlotAgent.__init__(self, setup, context, params, trace)
        self.set_modes(trading=True, proportion_plot=False)
        ba             = self.ba
        ba['mvars']    = {}
        # problems here to be solved in order wor the agent to work on the algobox
        #----------------------------------------------
        venue_id       = ba['venue_ids'][0]
        ba['venue_id'] = venue_id
        feed           = ba['feed'][venue_id][0]
        #----------------------------------------------
        self['algo_start_time'] += feed['start_time']
        self['algo_end_time']   += feed['start_time']
        mv                       = ba['mvars']
        # initialize trading curves
        list_length = int(round((self['algo_end_time']-self['algo_start_time']) / self['slice_timestep'])) + 1
        flag_DB                  = True
        mv['asked_qty']          = int(self['asked_qty_multiple'] * self['initial_ats'])
        mv['trading_curve']      = TrdCrv(self['security_id'], self['date'], self['trading_destination_ids'][0], list_length-1, mv['asked_qty'], self['total_volume'], flag_DB)
        mv['trading_curve_to_plot'] = []
        # initialize matrices
        mv['tick_size']          = None
        mv['order_ref_price']    = None
        mv['volume_5_mns']       = 0
        mv['nb_trds_5_mns']      = 0
        mv['slice_index']        = 0
        mv['exec_qty']           = 0
        mv['theoretical_qty']    = mv['asked_qty']
        mv['remain_qty']         = mv['asked_qty']
        mv['exec_qty_slice']     = 0
        mv['remain_qty_slice']   = mv['trading_curve'][0]-mv['trading_curve'][1]
        mv['start_time_slice']   = ba['parameters']['algo_start_time']
        mv['end_time_slice']     = mv['start_time_slice']+self['slice_timestep'] # This allows to start the algorithm at the right time
        # flags about slices
        mv['flag_exec']          = False
        mv['flag_slice_success'] = False
        mv['last_order_time']    = 0L
        mv['remain_qty_lob']     = 0  
        mv['calibration_points'] = []
        self['matrix_size']      = self['max_spread']+self['max_order_price'] 
        mv['delta_times_matrix'] = [[0.0 for j in range(self['matrix_size'])] for i in range(self['max_spread'])]
        mv['counter_matrix']     = [[0   for j in range(self['matrix_size'])] for i in range(self['max_spread'])]
        mv['exec_times_matrix']  = [[0.0 for j in range(self['matrix_size'])] for i in range(self['max_spread'])]
        mv['lambda_matrix']      = [[0.0 for j in range(self['matrix_size'])] for i in range(self['max_spread'])]
        mv['trading_curve_to_plot_missing_points'] = 0
        mv['market_vwap_curve']  = []
        mv['market_vwap']        = None
        mv['aggressive_volume']  = 0.0
        mv['percentage_aggressive'] = 0.0
        mv['garman_klass_period'] = 0.0
        mv['number_of_calibrations'] = int(round((self['algo_end_time']-self['algo_start_time']) / self['calibration_timestep']))
        mv['spread_turnover']    = 0.0
        mv['volume_over_period'] = 0
        mv['turnover_over_period'] = 0.0
        mv['volume_over_slice'] = 0
        mv['turnover_over_slice'] = 0.0
        mv['curve_start_time']   = self['algo_start_time']
        mv['vwap_curve']         = 0.0
        mv['theoretical_slice_index'] = 0
        mv['just_after'] = True
        mv['executed_volume_since_last_stat'] = 0
        mv['pnl'] = 0.0
        
        # initialize the private orderbook
        mv['private_orderbook']  = []
        # define calibration parameters
        mv['calibration_params'] = {'start_time'         : feed['start_time'],
                                    'sigma'              : self['initial_sigma'],
                                    'A'                  : self['initial_A'],
                                    'k'                  : self['initial_k'],
                                    'gamma'              : self['initial_gamma'],
                                    'ats'                : float(self['initial_ats']),
                                    'gk_estimator'       : garman_klass_m_s(self['calibration_timestep'])}
        cp = mv['calibration_params']
        # fill exec_times_matrix
        A = self['initial_A']
        k = self['initial_k']
        for i in range(self['max_spread']):
            for j in range(self['matrix_size']):
                lmbda = A[i]*exp(-k[i]*(j+1))
                mv['lambda_matrix'][i][j] = lmbda
                mv['exec_times_matrix'][i][j] = ( (1-exp(-lmbda*self['max_exec_time']/1000000.0))/lmbda )
        # define the numpy array (delta)
        ord_size = cp['ats']*self['ref_order_size']
        max_slice = mv['trading_curve'][0]-mv['trading_curve'][1]
        for i in range(len(mv['trading_curve'])-1):
            max_slice = max(max_slice, mv['trading_curve'][i]-mv['trading_curve'][i+1])    
        mv['q_slice_max'] = int(ceil(max_slice)/ord_size) # cp['ats'] is a float number
        
        mv['q_slice_max_calib_gamma'] = mv['q_slice_max']
        mv['q_slice_max'] = 2*mv['q_slice_max']  # 2 : factor to allow moves in ATS and not enough execution
        T  = int(self['slice_timestep']/1000000.0+0.1)
        dt = int(self['slice_sub_timestep']/1000000.0+0.1)
        mv['nb_sub_timesteps'] = int(1 + floor(T/dt))
        cp['gamma'] = self.find_gamma(mv, cp)
        mv['delta_matrix'] = numpy.zeros((mv['nb_sub_timesteps']-1, mv['q_slice_max']+1, self['max_spread']))
        for i in range(self['max_spread']):
            mv['delta_matrix'][:,:,i] = calcdelta(k[i], cp['gamma'], cp['sigma']/0.01, A[i], mv['q_slice_max'], T, dt, mv['nb_sub_timesteps'])
        
        # instantiate timeragent -- this is weird, I now, but necessary to have a timer wakeup
        #---------------------------------------
        #mv['timer_agent'] = TimerAgentForBestExec({'name' : 'TimerAgentForBestExec%d' %setup['counter'], 'counter' : setup['counter']}, context, {}, trace)
        #mv['timer_agent'].set_wakeup_time(self['algo_start_time'])
        #mv['time_is_up'] = False
        #mv['timer_agent'].isTvfoAgent(False)
        #AnalyticsManager.sched.addAgent(mv['timer_agent'])
        #---------------------------------------
        
 
    
    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
    
    def process(self, event):
        code = self.update(event)
        if code == 1:
            # get dictionaries
            ba           = self.ba
            venue_id     = ba['venue_id']
            fd           = self.marketManager.getFeedInfo(venue_id)
            mv           = ba['mvars']
            cp           = mv['calibration_params']
            time_num     = event.getTimeStamp()
            private_book = dict(self.moneyManager.getPrivateBook())
            after        = time_num >= self['algo_end_time']
            before       = time_num < self['algo_start_time']
            
            if before:
                mv['volume_at_start'] = fd['VOLUME']
                mv['price_start'] = fd['BEST_BID1']
            
                
            # indicators computed over the period
            self.update_indicators(mv, fd, before, after)
            # stop condition
            if after:
                for order_id in private_book.keys():
                    self.cancel_order(venue_id, order_id)
                mv['remain_qty_lob']  = 0
                mv['order_ref_price'] = None
                mv['percentage_aggressive'] = mv['aggressive_volume'] / mv['exec_qty']
                if mv['just_after']:
                    qty = mv['remain_qty']
                    if qty > 0:
                        self.create_order(venue_id, self['name'], self['side'], 0.0, qty, Order.Market, Order.DAY)
                    self.calc_vwap_curve(fd,mv)
                    mv['just_after'] = False
                return False
            
            while time_num >= mv['curve_start_time']+self['slice_timestep']:
                self.calc_vwap_curve(fd,mv)
                mv['curve_start_time']  += self['slice_timestep']
                
            if mv['remain_qty'] <= 0:
                for order_id in private_book.keys():
                    self.cancel_order(venue_id, order_id)
                mv['remain_qty_lob']  = 0
                mv['order_ref_price'] = None
                mv['percentage_aggressive'] = mv['aggressive_volume'] / mv['exec_qty']
                return False
            
            self.get_and_check_tick_size(fd, mv)
            # update executions triplets
            self.update_exec_probabilities(fd, mv, time_num)
            # update calibration params
            while time_num >= cp['start_time']+self['calibration_timestep']:
                if time_num > self['algo_start_time']+self['calibration_timestep']:
                    self.update_participation(fd,mv,time_num)
                self.update_calibration_params(ba, fd, mv, cp, time_num)
                cp['start_time']  += self['calibration_timestep']
            # condition to start trading
            if before:
                return False
            
            
            #self.print_orderbooks(pyLog)
            
            # flag checking
            flag_new_slice = False
            if mv['flag_slice_success']:
                mv['slice_index']       += 1
                mv['start_time_slice']   = time_num
                mv['end_time_slice']    += self['slice_timestep'] # Be careful, the slice is longer than slice_timestep
                mv['exec_qty_slice']     = 0 
                mv['remain_qty_slice']   = mv['remain_qty'] - mv['trading_curve'][mv['slice_index']+1]
                #print time_num
                self.update_delta(mv, cp)
                #print mv['delta_matrix'][:,2,1]
                flag_new_slice           = True
            else:
                while time_num >= mv['end_time_slice']: # New slice because of time
                    mv['slice_index']        += 1
                    mv['start_time_slice']   = mv['end_time_slice']
                    mv['end_time_slice']    += self['slice_timestep']
                    mv['exec_qty_slice']     = 0
                    mv['remain_qty_slice']   = mv['remain_qty'] - mv['trading_curve'][mv['slice_index']+1]
                    #print time_num
                    self.update_delta(mv, cp)
                    #print mv['delta_matrix'][:,2,1]
                    flag_new_slice           = True
            
            # sending order
            if flag_new_slice or mv['flag_exec'] or (time_num > mv['last_order_time'] + self['max_exec_time']):
                # order
                mv['flag_slice_success'] = False
                mv['flag_exec']          = False
                # compute price
                time_index = int((time_num - mv['start_time_slice'])/self['slice_sub_timestep'])
                time_index = min(time_index, mv['nb_sub_timesteps']-2)
                #print "time index"
                #print time_index
                q_slice    = int(ceil((mv['remain_qty_slice'])/(cp['ats']*self['ref_order_size'])))
                q_slice    = min(q_slice, mv['q_slice_max']) 
                spread     = min(int(fd['SPREAD']/mv['tick_size']+0.1), self['max_spread'])
                delta_t_q  = mv['delta_matrix'][time_index, q_slice, spread-1]
                floored_delta = int(delta_t_q);
                if random() >= delta_t_q-floored_delta:
                    price = fd['BEST_BID1']+min(floored_delta,spread+self['max_order_price'])*mv['tick_size']
                    
                else:
                    price = fd['BEST_BID1']+min(floored_delta+1,spread+self['max_order_price'])*mv['tick_size']
                # leave quantity to substract
                total_qty  = 0
                if mv['order_ref_price'] == None or abs(mv['order_ref_price']-price) > 1e-8:
                    for order_id in private_book.keys():
                        self.cancel_order(venue_id, order_id)
                    private_book = dict(self.moneyManager.getPrivateBook())
                    mv['remain_qty_lob']  = 0
                    mv['order_ref_price'] = None
                else:
                    total_qty = mv['remain_qty_lob']
                # send order
                qty  = min(int(cp['ats']*self['ref_order_size']), mv['remain_qty_slice'])
                qty -= total_qty
                if qty > 0:
                    prefix = 'aggressive' if fd['BEST_BID1']>=price else 'passive'
                    self.create_order(venue_id, prefix, self['side'], price, qty, Order.Limit, Order.DAY)
                    #print "spread:"
                    #print fd['BEST_ASK1'] - fd['BEST_BID1']
                    #print "quote:"
                    #print price - fd['BEST_ASK1'] 
                    mv['order_ref_price'] = price
                    mv['last_order_time'] = time_num
                    mv['remain_qty_lob'] += qty
    
    def processReport(self, event):
        code     = self.update_report(event)
        if code == 1:
            # get dictionaries
            ba   = self.ba
            size = event.size
            print event.getTimeStamp()
            print size
            
            #self.print_orderbooks(pyLog)
            mv   = ba['mvars']
            mv['exec_qty']                        += size
            mv['remain_qty']                      -= size
            mv['exec_qty_slice']                  += size
            mv['remain_qty_slice']                -= size
            mv['remain_qty_lob']                  -= size
            mv['executed_volume_since_last_stat'] += size
            mv['pnl']                             += size * event.price
            if 'aggressive' in event.orderSnapshot.orderId:
                print 'agg'
                mv['aggressive_volume'] += size
            if mv['remain_qty_lob'] <= 0:
                mv['order_ref_price'] = None
            # update timer
            #mv['timer_agent'].time2wakeup = time_num + self['max_exec_time']
            #mv['profit_n_loss']     += size*event.price 
        if mv['remain_qty_slice']   == 0:
            mv['flag_slice_success'] = True
        mv['flag_exec']              = True
    
    def last_process(self):
        ba            = self.ba
        mv            = ba['mvars']
        cp            = mv['calibration_params']
        venue_id      = ba['venue_id']
        title         = ba[venue_id]['figure_titles']
        garman_klass  = mv['garman_klass_period']/mv['number_of_calibrations']
        vwas          = mv['spread_turnover'] / mv['volume_over_period']
        vwap_strat    = ba['turnover_algo'][venue_id]/ba['exec_qty'][venue_id]
        vwap_curve    = mv['vwap_curve'] / mv['theoretical_qty']
        performance   = (vwap_strat - vwap_curve)/vwas
        imp_shortfall =  mv['pnl'] / float(mv['exec_qty']) - mv['price_start']
        title         = title.replace(']', ', IS=%f, vwas_period=%f, per_agg=%f, perf=%f ]' %(imp_shortfall,vwas,mv['percentage_aggressive'],performance))
        ba[venue_id]['figure_titles'] = title
        # save data whn the algo dies
        self.append_indicator({'gamma' : cp['gamma'],
                               'IS' : imp_shortfall,
                               'perf' : performance,
                               'aggr': mv['percentage_aggressive']}, self['closing'])
        f = open('C:/output_test.txt', 'a')
        f.write(str(self['security_id'])+';'+str(self['date'])+';'+str(self['delta_target'])+';'+str(self['ref_order_size'])+';'+str(self['asked_qty_multiple'])+';'+str(self['ewma_theta'])+';'+str(cp['gamma'])+';'+str(imp_shortfall)+';'+str(performance)+';'+str(mv['percentage_aggressive'])+'\n')
        f.close()
    
    
    
    
    '''######################################################################################################
    ######################################   PURE VIRTUAL FUNCTIONS   #######################################
    ######################################################################################################'''
    
    def _pvtl__update_child_plot_curves(self):
        ba = self.ba
        time_num = ba['event']['TIME_NUM']
        mv = ba['mvars']
        if time_num >= self['algo_start_time'] and time_num < self['algo_end_time']:
            slice_time = float(time_num-self['algo_start_time'])/self['slice_timestep']
            indice     = int(slice_time)
            theta      = slice_time-indice
            interpolated_qty = mv['trading_curve'][0]-(theta*mv['trading_curve'][indice+1]+(1-theta)*mv['trading_curve'][indice])
            mv['trading_curve_to_plot'].append(interpolated_qty)
        elif time_num >= self['algo_end_time']:
            mv['trading_curve_to_plot_missing_points'] += 1
    
    def _pvtl__update_child_plot_figure(self, venue_id):
        ba = self.ba
        mv = ba['mvars']
        datetimes = ba['curves']['DATETIME']
        n  = len(datetimes)
        ns = len(mv['trading_curve_to_plot'])
        matplotlib.pyplot.subplot(211)
        matplotlib.pyplot.plot(datetimes[(n-ns-mv['trading_curve_to_plot_missing_points']):(n-mv['trading_curve_to_plot_missing_points'])],  mv['trading_curve_to_plot'], color='y', linestyle='-', linewidth=1, marker='None')

        
    
    
    
    
    '''######################################################################################################
    ##########################################   UPDATE FUNCTIONS   #########################################
    ######################################################################################################'''


    def update_participation(self,fd,mv,time_num):
        volume = float(fd['VOLUME'] - mv['volume_at_start'])
        if volume > 0.0:
            participation = float(mv['executed_volume_since_last_stat'])/ (volume)
        else:
            participation = 0.0    
        mv['executed_volume_since_last_stat'] = 0
        mv['volume_at_start'] = fd['VOLUME']
        print participation

            
    def update_calibration_params(self, ba, fd, mv, cp, time_num):
        exec_times_matrix = mv['exec_times_matrix']
        lambda_matrix     = mv['lambda_matrix']
        new_sigma         = cp['gk_estimator'].update(fd, time_num)
        if new_sigma == None:
            return
        cp['sigma']       = self['ewma_theta']*new_sigma + (1-self['ewma_theta'])*cp['sigma']
        mv['garman_klass_period'] += new_sigma
        #print "new_sigma"
        #print new_sigma
        # Average the tables and compute lambda matrix
        delta_times_matrix = mv['delta_times_matrix']
        counter_matrix     = mv['counter_matrix']
        for i in range(self['max_spread']):
            for j in range(self['matrix_size']):
                counter = counter_matrix[i][j]
                if counter != 0:
                    new_exec_time = (delta_times_matrix[i][j] / counter) / 1000000.0
                    exec_times_matrix[i][j] = self['ewma_theta']*new_exec_time + (1-self['ewma_theta'])*exec_times_matrix[i][j]
                lambda_matrix[i][j] = newtonlambdan(exec_times_matrix[i][j], self['max_exec_time']/1000000.0, self['tolerance'])
        # Compute A,k
        threshold = 1.0/300.0 # CAREFUL : threshold is in seconds-1
        for i in range(self['max_spread']):
            lambda_vector = []
            delta_vector  = []
            for j in range(self['matrix_size']):
                if ((lambda_matrix[i][j] > threshold) or (j<=2)):
                    lambda_vector.append(lambda_matrix[i][j])
                    delta_vector.append(j+1)
            delta_max = len(lambda_vector)
            a11       = delta_max
            a12       = -sum(delta_vector)
            a22       = sum([e*e for e in delta_vector])
            tmp       = map(log, lambda_vector)
            b1        = sum(tmp)
            b2        = -sum([e*f for (e,f) in zip(delta_vector, tmp)])
            det       = a11*a22 - a12*a12
            cp['A'][i]= exp((a22*b1 - a12*b2) / det)
            cp['k'][i]= (a11*b2 - a12*b1) / det
        # Reset the information in tables
        del mv['delta_times_matrix']
        del mv['counter_matrix']
        mv['delta_times_matrix'] = [[0.0 for j in range(self['matrix_size'])] for i in range(self['max_spread'])]
        mv['counter_matrix']     = [[0   for j in range(self['matrix_size'])] for i in range(self['max_spread'])]
        nb_trds_5_mns = mv['nb_trds_5_mns']
        volume_5_mns = mv['volume_5_mns']
        if nb_trds_5_mns != 0:
            new_ats = float(volume_5_mns) / nb_trds_5_mns
            mv['calibration_params']['ats'] = self['ewma_theta_ats']*new_ats + (1-self['ewma_theta_ats'])*mv['calibration_params']['ats']  
        mv['nb_trds_5_mns'] = 0
        mv['volume_5_mns'] = 0
    
    def update_delta(self, mv, cp):
        q_slice = int(ceil((mv['remain_qty_slice'])/(cp['ats']*self['ref_order_size']))) # cp['ats'] is a float number
        mv['q_slice_max'] = 2*q_slice # 2 : factor to allow moves in ATS
        T  = int((mv['end_time_slice']-mv['start_time_slice'])/1000000.0+0.1) # in second
        #print "T"
        #print T
        dt = int(self['slice_sub_timestep']/1000000.0+0.1)
        mv['nb_sub_timesteps'] = int(1 + floor(T/dt))
        mv['delta_matrix']     = numpy.zeros((mv['nb_sub_timesteps']-1, mv['q_slice_max']+1, self['max_spread']))
        #print "sigma"
        #print cp['sigma']
        #print "A"
        #print cp['A']
        #print "k"
        #print cp['k']
        #print "T"
        #print T
        #print "nb_de_points_en_temps"
        #print mv['nb_sub_timesteps']
        #cp['sigma'] = 0 # Pour tester a vol = 0
        for i in range(self['max_spread']):
            mv['delta_matrix'][:,:,i] = calcdelta(cp['k'][i], cp['gamma'], cp['sigma']/mv['tick_size'], cp['A'][i], mv['q_slice_max'], T, dt, mv['nb_sub_timesteps'])
        a=0
    
    def update_exec_probabilities(self, fd, mv, current_time):
        # take max price if SIDE == SELL
        trade_max_price = fd['LAST_TRDS_GROUP']['MAX_PRICE']
        # delta_times_matrix
        calibration_points = mv['calibration_points']
        max_exec_time      = self['max_exec_time']
        delta_times_matrix = mv['delta_times_matrix']
        counter_matrix     = mv['counter_matrix']
        tick_size          = mv['tick_size']
        j = 0
        for i in range(len(calibration_points)):
            i            -= j
            triplets_list = calibration_points[i]
            t             = triplets_list[0]
            if current_time - t > self['max_exec_time']:
                for triplet in triplets_list[1]:
                    # triplet[0] = order_price, triplet[1] = spread, triplet[2] = delta_ticks
                    i1                          = triplet[1]-1
                    i2                          = triplet[2]-1
                    delta_times_matrix[i1][i2] += max_exec_time
                    counter_matrix[i1][i2]     += 1
                calibration_points.pop(i)
                j += 1
            else:
                break
        # perform 
        if fd['TRADE_EVENT']:
            mv['nb_trds_5_mns'] += 1
            mv['volume_5_mns']  += fd['LAST_TRDS_GROUP']['VOLUME']
            for (t, triplets_list) in calibration_points:
                delta_time = current_time - t
                # triplet[0] = order_price, triplet[1] = spread, triplet[2] = delta
                j = 0
                for i in range(len(triplets_list)):
                    i -= j
                    triplet = triplets_list[i]
                    if triplet[0] <= trade_max_price:
                        i1                          = triplet[1]-1
                        i2                          = triplet[2]-1
                        delta_times_matrix[i1][i2] += delta_time
                        counter_matrix[i1][i2]     += 1
                        triplets_list.pop(i)
                        j += 1
                
        # extend triplets_list
        spread          = int(round(fd['SPREAD']/mv['tick_size']))
        number_of_ticks = self['matrix_size']
        spread          = min(spread,          self['max_spread'])
        calibration_points.append((current_time, []))
        append_function = calibration_points[-1][1].append
        for i in range(1, number_of_ticks+1):
            append_function((fd['BEST_BID1']+i*tick_size, spread, i))
    
    def update_indicators(self, mv, fd, before, after):
        # VOLUME INDICATOR
        if fd['TRADE_EVENT'] and (not before) and (not after):
            mv['volume_over_period']   += fd['LAST_TRDS_GROUP']['VOLUME']
            mv['turnover_over_period'] += fd['LAST_TRDS_GROUP']['TURNOVER']
            mv['volume_over_slice']   += fd['LAST_TRDS_GROUP']['VOLUME']
            mv['turnover_over_slice'] += fd['LAST_TRDS_GROUP']['TURNOVER']
        # VWAP INDICATOR
            if mv['volume_over_period'] != 0:
                mv['market_vwap'] = mv['turnover_over_period'] / mv['volume_over_period']
        # VWAS INDICATOR
            mv['spread_turnover'] += fd['LAST_TRDS_GROUP']['VOLUME']*fd['SPREAD']
    
    
    def calc_vwap_curve(self,fd,mv):
        
        vwap_slice = fd['LAST_TRDS_GROUP']['VWAP']
        if mv['volume_over_slice'] != 0:
                vwap_slice = mv['turnover_over_slice'] / mv['volume_over_slice']
        mv['vwap_curve'] += vwap_slice *  (mv['trading_curve'][mv['theoretical_slice_index']] - mv['trading_curve'][mv['theoretical_slice_index']+1])  
        mv['theoretical_slice_index'] +=1
        mv['volume_over_slice']   = 0
        mv['turnover_over_slice'] = 0.0
        
    
    
    def get_and_check_tick_size(self, fd, mv):
        if mv['tick_size'] == None:
            ref_price = fd['BEST_ASK1'] if fd['BEST_ASK1'] != None else fd['BEST_BID1']
            tick_sizes = [e for e in fd['tick_sizes']]
            tick_sizes.reverse()
            i = 0
            while tick_sizes[i][0] > ref_price:
                i += 1
            mv['tick_size_inf_price'] = tick_sizes[i][0]
            mv['tick_size_sup_price'] = tick_sizes[i-1][0] if i!= 0 else MAXIMUM_VALID_PRICE
            mv['tick_size'] = tick_sizes[i][1]
        else:
            i = 0
            while i < 4 and fd['BEST_ASK'][i+1] != None:
                i += 1
            ref_price = fd['BEST_ASK'][i]
            if ref_price >= mv['tick_size_sup_price']:
                raise ValueError('There is a tick_size change during the day ! Choose another day')
            i = 0
            while i < 4 and fd['BEST_BID'][i+1] != None:
                i += 1
            ref_price = fd['BEST_BID'][i]
            if ref_price <= mv['tick_size_inf_price']:
                raise ValueError('There is a tick_size change during the day ! Choose another day')
    
    def find_gamma(self, mv, cp):
        T  = int((mv['end_time_slice']-mv['start_time_slice'])/1000000.0+0.1) # in second
        dt = int(self['slice_sub_timestep']/1000000.0+0.1)
        nb_t = mv['nb_sub_timesteps']
        target = self['mean_spread'] + self['delta_target']
        sigma = self['initial_sigma']/self['tick_size'] # good unit
        deltas0 = calcdelta(self['mean_k'], 0.000001, sigma, self['mean_A'], mv['q_slice_max_calib_gamma']+2,T,dt,nb_t)
        deltamax = deltas0[0,mv['q_slice_max_calib_gamma']]
        if target > deltamax:
            gamma0 = 0.000001
            print gamma0
            return gamma0
        gamma0 = 1.0
        deltas0 = calcdelta(self['mean_k'],gamma0, sigma, self['mean_A'],mv['q_slice_max_calib_gamma']+2,T,dt,nb_t)
        delta0 = deltas0[0,mv['q_slice_max_calib_gamma']]
        if delta0 < target:
            while delta0 < target:
                gamma0 = gamma0/2
                deltas0 = calcdelta(self['mean_k'],gamma0, sigma, self['mean_A'],mv['q_slice_max_calib_gamma']+2,T,dt,nb_t)
                delta0 = deltas0[0,mv['q_slice_max_calib_gamma']]
            gammainf = gamma0
            gammasup = gamma0*2
        else:
            while delta0 > target:
                gamma0 = gamma0*2
                deltas0 = calcdelta(self['mean_k'],gamma0,sigma, self['mean_A'],mv['q_slice_max_calib_gamma']+2,T,dt,nb_t)
                delta0 = deltas0[0,mv['q_slice_max_calib_gamma']]
            gammainf = gamma0/2
            gammasup = gamma0
        gamma0 = (gammainf+gammasup)/2
        deltas0 = calcdelta(self['mean_k'],gamma0,sigma, self['mean_A'],mv['q_slice_max_calib_gamma']+2,T,dt,nb_t)
        delta0 = deltas0[0,mv['q_slice_max_calib_gamma']]
        while abs(delta0 - target) > 0.01:
            if delta0 > target:
                gammainf = gamma0
                gamma0 = (gammainf+gammasup)/2
                deltas0 = calcdelta(self['mean_k'],gamma0,sigma, self['mean_A'],mv['q_slice_max_calib_gamma']+2,T,dt,nb_t)
                delta0 = deltas0[0,mv['q_slice_max_calib_gamma']]
            else:
                gammasup = gamma0
                gamma0 = (gammainf+gammasup)/2
                deltas0 = calcdelta(self['mean_k'],gamma0,sigma, self['mean_A'],mv['q_slice_max_calib_gamma']+2,T,dt,nb_t)
                delta0 = deltas0[0,mv['q_slice_max_calib_gamma']]
        calcdelta(self['mean_k'],gamma0,sigma, self['mean_A'],mv['q_slice_max_calib_gamma']+2,T,dt,nb_t)
        delta0 = deltas0[0,mv['q_slice_max_calib_gamma']]
        print gamma0
        return gamma0
        
    
    
    
    
    
    
    
    
    
    
    
'''######################################################################################################
###########################################   OTHER FUNCTIONS   #########################################
######################################################################################################'''
    
def newtonlambdan(z, dt, tol):
    if (z >= dt):
        lambdaX = 0.0
    else:
        lambdaX = 1.0/z
        iter    = 0
        y = z - (1.0-exp(-lambdaX*dt))/lambdaX
        while ((abs(y)>tol) and (iter < 20)):
            lambdaX = lambdaX*(z*lambdaX - 2 + (2+lambdaX*dt)*exp(-lambdaX*dt))/(-1+(1+lambdaX*dt)*exp(-lambdaX*dt))
            y = z - (1.0-exp(-lambdaX*dt))/lambdaX
            iter += 1
    return lambdaX
    
    
class garman_klass_m_s:
    
    """ DEPENDENCIES : 
        -> time_t
        -> start_time
        -> market_trades
        -> gkXX
    """
    
    def __init__(self, window_size):
        self._window_size        = window_size
        self._start_index        = 0
        self._activated          = False
    
    def update(self, fd, time_num):
        w     = self._window_size
        n     = fd['LAST_DEAL_IDX']+1
        dprcs = fd['DEALS_PRICES']
        if self._activated:
            b     = self._start_index
            dtims = fd['DEALS_TIMES']
            if b != n:
                if b < n:
                    while (b != n) and (dtims[b]+w < time_num):
                        b += 1
                    if b == n:
                        self._start_index = b
                        return 0.0
                    trade_prices = dprcs[b:n]
                else:
                    m    = fd['MAX_TRDS_NUMBER']
                    while (b != n) and (dtims[b]+w < time_num):
                        b += 1
                        b  = b % m
                    if b == n:
                        self._start_index = b
                        return 0.0
                    trade_prices = dprcs[b:]+dprcs[:n]
                open         = dprcs[b]
                high         = max(trade_prices)
                low          = min(trade_prices)
                u = high - low
                d = fd['TRDPRC_1'] - open
                gk2 = 0.5*u*u - 0.3863*d*d
                if gk2 >= 0.0:
                    result = sqrt(gk2*(1000000.0/self._window_size))
                else:
                    raise ValueError('Garman Klass Estimator is Wrong !')
                    result = 0.0
            else:
                result = 0.0
            self._start_index = b
            return result
        else:
            if fd['TIME_NUM'] >= fd['start_time'] + w:
                self._activated = True
                self.update(fd, time_num)


def calcmat(alpha,eta,q):
    AA = numpy.zeros((q+1,q+1))
    for i in range(1,q+1):
        AA[i,i-1] = -eta
        AA[i,i] = alpha * i**2
    return AA

def calcv(alpha,eta,q,T,nb_t):
    dt = T / (nb_t-1)
    v = numpy.zeros((nb_t,q+1))
    v[nb_t-1,0] = 1.0
    AA = calcmat(alpha,eta,q)
    B = sl.expm(-AA*dt)
    for j in range(nb_t-2,-1,-1):
        v[j,:] = numpy.dot(v[j+1,:],B.transpose())
    return v

def calcdelta(k,Gamma,sigma,A,q,T,dt,nb_t):
    alpha = (k/2.0)*Gamma*sigma**2.0
    eta = A*(1.0+Gamma/k)**(-(1.0+k/Gamma))
    C = (1.0/Gamma)*numpy.log(1.0+Gamma/k)
    delta = numpy.zeros((nb_t-1,q+1))
    v = calcv(alpha,eta,q,T,nb_t)
    for j in range(0,nb_t-1):
        for n in range(1,q+1):
            delta[j,n] = (1.0/k)*numpy.log(v[j,n]/v[j,n-1]) + C
    return delta
