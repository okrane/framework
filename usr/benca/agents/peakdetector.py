'''
Created on 12 aout 2010

@author: benca
'''

from simep.core.baseobserver import BaseObserver
from simep.core.analyticsmanager import AnalyticsManager
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog
from simep.tools import *
import usr.dev.sivla.funcs.DBTools.SE
import math
import matplotlib.pyplot
import datetime



class PeakDetector(BaseObserver):
    
    
    @staticmethod
    def public_parameters():
        setup      = {'name'                   : {'label' : 'Name'               , 'value' : 'PeakDetector001'}}
        parameters = {'plot_mode'              : {'label' : 'Plot Mode (0, 1, 2)', 'value' : 2}, 
                      'data_type'              : {'label' : 'Type of the Data'   , 'value' : 'normalized_proportion_slices'}, 
                      'smoothing_type'         : {'label' : 'Type of the Smoothing', 'value' : 'gaussian'}, 
                      'left_time_window_size'  : {'label' : 'Left Time Window Size'   , 'value' : '00:15:00:000'}, 
                      'standart_deviation'     : {'label' : 'Standart Deviation' , 'value' : '00:05:00:000'},
                      'peak_alert_percent'     : {'label' : 'Peak Alert Percentage' , 'value' : 0.05},
                      'peak_height_threshold'  : {'label' : 'Peak Height Threshold' , 'value' : 3},
                      'date_window_for_median_data': {'label' : 'Size of the Date Window to get Median Data' , 'value' : 20},
                      'time_to_start_estimating_daily_volume': {'label' : 'Time to Start Estimating the Daily Volume And Using It' , 'value' : '09:00:00'}}
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return ['slices_vector_300', 'curves']
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, parameters, trace):
        BaseObserver.__init__(self, setup, context, parameters, trace)
        self.needExecReportEvt     = False
        self._parameters['left_time_window_size'] = date2num(parameters['left_time_window_size'])
        self._parameters['half_time_detection_window_size'] = self._parameters['left_time_window_size']
        self._parameters['standart_deviation'] = date2num(parameters['standart_deviation'])
        self._parameters['time_to_start_estimating_daily_volume'] = date2num(parameters['time_to_start_estimating_daily_volume'])
        self._full_convolution     = 1
        self._win_start_data_index = 0
        self._mean_start_data_index= 0
        self._mean_end_data_index  = 0
        self._mean_end_data_index2 = 0
        self._smoothed_data_index  = 0
        self._sigma_data_index     = 0
        self._percent_of_discarded_peaks = 0.05
        self._detection_win_mean_data_index = 0
        self._detection_win_start_data_index= 0
        self._previous_slice_index = 0
        self._cumulated_prop_slices= 0.0
        self._data                 = []
        self._smoothed_data        = []
        self._sigma_data           = []
        self._peaks                = []
        self._peaks_datetime       = []
        self._data_curves_difference = []
        self._hisorical_data_matrix  = []
        self._figure_title         = '[date=' + context['date'] + ', ric=' + str(context['ric']) + ', trading_destination_id=' + str(context['trading_destination_id']) + ']'
        self._prvt__GetHisoricalData()
        self._normalizing_volume = 0
        self._fig_count          = 0
    
    
    
    '''######################################################################################################
    ##########################################   HISTORICAL DATA   ##########################################
    ######################################################################################################'''
    
    def _prvt__GetHisoricalData(self):
        self._prop_slices = dev.usr.sivla.funcs.DBTools.SE.get_se('volume-curve', self['security_id'], self['trading_destination_id'], self['date']).value['Usual day'][8:]
        prop_slices_norm = 0.0
        for v in self._prop_slices:
            prop_slices_norm += v
        prop_slices_norm = 1.0/prop_slices_norm
        for i in range(len(self._prop_slices)):
            self._prop_slices[i] *= prop_slices_norm
        self._daily_market_volume = 2.4*dev.usr.sivla.funcs.DBTools.SE.get_se('aggressivity-levels', self['security_id'], self['trading_destination_id'], self['date']).value['DEFAULT'][1]
    
    
    
    '''######################################################################################################
    ######################################   PURE VIRTUAL FUNCTIONS   #######################################
    ######################################################################################################'''
    
    def _pvtl__update_child_plot_curves(self):
        pass
    
    def _pvtl__update_child_plot_figure(self):
        total_volume = float(self._bus['VOLUME'])
        market_volume_str = str(int(total_volume))
        self._figure_title = self._figure_title.replace(']', ', market_volume=' + market_volume_str + ']')
    
    def _pvtl__update_last_plot(self):
        pass
    
    
    
    '''######################################################################################################
    ########################################   UPDATING FUNCTIONS   #########################################
    ######################################################################################################'''
    
    def _prvt__AppendIndicator(self, lob):
        ae = ba['feed']
        if ae['BEST_ASK1']!=None and ae['BEST_BID1']!=None:
            self.appendIndicator(pyData('init', 
                                date = [ba['event']['TIME_STR']], 
                                value = {'BestBidPrice': [lob.getBid(0).price], 
                                         'BestAskPrice': [lob.getAsk(0).price], 
                                         'BestBidQty'  : [lob.getBid(0).size], 
                                         'BestAskQty'  : [lob.getAsk(0).size]}))
    
    def _prvt__EstimateDailyVolume(self):
        if ba['event']['TIME_NUM'] < self['time_to_start_estimating_daily_volume']:
            normalizing_volume = self._daily_market_volume
        else:
            if self._previous_slice_index != len(self._bus['slices_vector_300']['time'])-1:
                while self._previous_slice_index != len(self._bus['slices_vector_300']['time'])-1:
                    self._cumulated_prop_slices += self._prop_slices[self._previous_slice_index]
                    self._normalizing_volume    += self._bus['slices_vector_300']['VOLUME'][self._previous_slice_index]
                    self._previous_slice_index  += 1
            normalizing_volume = self._normalizing_volume / self._cumulated_prop_slices
        return normalizing_volume
    
    def _prvt__AssignDataVector(self):
        normalizing_volume = self._prvt__EstimateDailyVolume()
        self._data = []
        if self['data_type'] == 'proportion_slices':
            for i in range(len(self._bus['slices_vector_300']['VOLUME'])):
                self._data.append(self._bus['slices_vector_300']['VOLUME'][i]/(1.0*normalizing_volume))
        elif self['data_type'] == 'normalized_proportion_slices':
            for i in range(len(self._bus['slices_vector_300']['VOLUME'])):
                self._data.append(self._bus['slices_vector_300']['VOLUME'][i]/(1.0*normalizing_volume*self._prop_slices[i]))
        self._numerical_time = self._bus['slices_vector_300']['time']
    
    def _prvt__PerformNoSmoothing(self):
        self._smoothed_data = self._prop_slices[:len(self._data)]
    
    def _prvt__PerformFullGaussianSmoothing(self):
        ''' UPDATE INDEX '''
        if self._smoothed_data_index == 0:
            while(self._numerical_time[self._smoothed_data_index]+self['left_time_window_size']) < ba['event']['TIME_NUM']:
                self._smoothed_data_index += 1
        ''' UPDATE INDEXES '''
        if self._mean_start_data_index == 0:
            while(self._numerical_time[self._mean_start_data_index] + self['left_time_window_size'] < ba['event']['TIME_NUM']):
                self._mean_start_data_index += 1
        while(self._numerical_time[self._mean_end_data_index] + self['left_time_window_size'] < ba['event']['TIME_NUM']):
            self._mean_end_data_index += 1
        ''' STANDART DEVIATION '''
        denom           = 1.0/(2.0*self['standart_deviation']**2)
        win_start_index = 0
        win_end_index   = 0
        ''' CONVOLUTION '''
        for index in range(self._mean_start_data_index, len(self._numerical_time)):
            ''' GET STARTING INDEX '''
            self._smoothing_window = []
            mean = self._numerical_time[index]
            while self._numerical_time[win_start_index] + self['left_time_window_size'] < mean:
                win_start_index += 1
            if index < self._mean_end_data_index:
                while self._numerical_time[win_end_index] - self['left_time_window_size'] < mean:
                    win_end_index += 1
            else:
                win_end_index = len(self._numerical_time)
            ''' COMPUTE SMOOTHING WINDOW '''
            for t in self._numerical_time[win_start_index:win_end_index]:
                self._smoothing_window.append(math.exp(-((t-mean)**2)*denom))
            window_norm = 0.0
            for t in self._smoothing_window:
                window_norm += t
            new_point = 0.0
            for i in range(win_end_index-win_start_index):
                new_point += self._smoothing_window[i]*self._data[win_start_index+i]
            i = index-self._smoothed_data_index
            self._smoothed_data[i:] = [new_point/window_norm]
    
    
    def _prvt__PerformPeakDetectionWithBL(self):
        if ba['event']['TIME_NUM'] > self._bus['start_time'] + 2*self._parameters['half_time_detection_window_size']:
            ''' UPDATE INDEX '''
            if self._sigma_data_index == 0:
                while(self._numerical_time[self._sigma_data_index]+self['half_time_detection_window_size']) < ba['event']['TIME_NUM']:
                    self._sigma_data_index += 1
            ''' UPDATE INDEXES '''
            while self._numerical_time[self._detection_win_start_data_index] + 2*self['half_time_detection_window_size'] < ba['event']['TIME_NUM']:
                self._detection_win_start_data_index += 1
            while self._numerical_time[self._detection_win_mean_data_index] + self['half_time_detection_window_size'] < ba['event']['TIME_NUM']:
                self._detection_win_mean_data_index += 1
            ''' COMPUTE SIGMA '''
            if len(self._smoothed_data) == 0:
                return
            data_curves_diff = [i for i in self._smoothed_data[self._detection_win_start_data_index:]]
            sorted_data_curves_diff = [(i, abs(self._smoothed_data[i]-self._data[self._smoothed_data_index+i])) for i in range(len(self._smoothed_data[self._detection_win_start_data_index:]))]
            sorted_data_curves_diff.sort(key = lambda m:m[1], reverse = True)
            n = len(sorted_data_curves_diff)
            m = int(round(self._percent_of_discarded_peaks*n))+1
            for i in range(min(n,m)):
                data_curves_diff[sorted_data_curves_diff[i][0]] = 0
            new_point = 0.0
            for i in range(n):
                new_point += data_curves_diff[i]
            new_point /= n
            new_point /= (1-self._percent_of_discarded_peaks)
            diff = (self._data[self._sigma_data_index+self._detection_win_start_data_index] 
                    - self._smoothed_data[self._sigma_data_index-self._smoothed_data_index+self._detection_win_start_data_index])
            new_point = diff / new_point
            self._sigma_data[self._detection_win_start_data_index:] = [new_point]
            ''' COMPUTE SIGMA '''
            tmp = [(i,self._sigma_data[i]) for i in range(len(self._sigma_data))]
            tmp.sort(key = lambda m:m[1], reverse = True)
            self._peaks          = []
            self._peaks_datetime = []
            for i in range(int(self['peak_alert_percent']*len(tmp)+1)):
                if self._data[self._sigma_data_index+tmp[i][0]] - self._smoothed_data[self._sigma_data_index-self._smoothed_data_index+tmp[i][0]]  > self['peak_height_threshold']:
                    self._peaks.append(self._data[self._sigma_data_index+tmp[i][0]])
                    peak_datetime = datetime.datetime.strptime(num2date(self._numerical_time[self._sigma_data_index+tmp[i][0]])[0:8], '%H:%M:%S')
                    self._peaks_datetime.append(peak_datetime)
    
    def _prvt__PerformPeakDetectionWithoutBL(self):
        self._peaks          = []
        self._peaks_datetime = []
        for i in range(len(self._data)):
            if self._data[i] - self._smoothed_data[i] > self['peak_height_threshold']:
                self._peaks.append(self._data[i])
                peak_datetime = datetime.datetime.strptime(num2date(self._numerical_time[i])[0:8], '%H:%M:%S')
                self._peaks_datetime.append(peak_datetime)
            

    def _prvt__PlotFigure(self):
        if self['plot_mode'] == 2 and not self._this_is_last_plot:
            if ba['event']['TIME_NUM'] > self._real_time_update_time:
                self._real_time_update_time += self._real_time_update_timestep
            else:
                return
        if self._this_is_first_plot:
            if self['plot_mode'] == 2:
                matplotlib.pyplot.ion()
            self._figure_number = AnalyticsManager.get_figure_number()
            matplotlib.pyplot.figure(self._figure_number, figsize=(16,12))
            self._this_is_first_plot = False
        elif self._this_is_last_plot:
            matplotlib.pyplot.close(self._figure_number)
            matplotlib.pyplot.figure(self._figure_number, figsize=(16,12))
        else:
            if self['plot_mode'] == 2:
                matplotlib.pyplot.ion()
                matplotlib.pyplot.clf()
        if self._this_is_last_plot:
            self._PUREVIRTUAL_UpdateLastPlot()
        px = matplotlib.pyplot.subplot(111)
        if 'proportion_slices' in self['data_type']:
            n1 = len(self._smoothed_data)
            matplotlib.pyplot.plot_date(self._bus['slices_vector_300']['DATETIME'],  self._data, xdate=True, ydate=False, color='g', linestyle='-', linewidth=1, marker='None')
            matplotlib.pyplot.plot_date(self._bus['slices_vector_300']['DATETIME'][self._smoothed_data_index:n1+self._smoothed_data_index],  self._smoothed_data, xdate=True, ydate=False, color='r', linestyle='-', linewidth=1, marker='None')
            matplotlib.pyplot.plot_date(self._peaks_datetime,  self._peaks, xdate=True, ydate=False, color='b', linestyle=' ', linewidth=0, marker='s')
            matplotlib.pyplot.ylabel('Volume Proportion')
        px.xaxis.set_major_formatter( (matplotlib.dates.DateFormatter('%H:%M')))
        for label in px.xaxis.get_ticklabels():
            label.set_rotation(-30)
        matplotlib.pyplot.xlim(self._bus['start_datetime'],self._bus['end_datetime'])
        matplotlib.pyplot.grid(True)
        if self['plot_mode'] == 2 and not self._this_is_last_plot:
            matplotlib.pyplot.draw()
            #matplotlib.pyplot.savefig('C:\\P13rr34l41n\\normalized_prop'+str(self._fig_count)+'.png', format='png', dpi=150)
            self._fig_count += 1
            matplotlib.pyplot.ioff()
        
    

    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
        
    def process(self, event):
        self.update(event)
        if ba['event']['TIME_NUM'] > self._bus['start_time'] + 2*self['left_time_window_size']:
            ''' BESTBID OR BESTASK OR VOLUME '''
            self._prvt__AssignDataVector()
            ''' PERFORM SMOOTHING '''
            if self['smoothing_type'] == 'gaussian':
                self._prvt__PerformFullGaussianSmoothing()
                self._prvt__PerformPeakDetectionWithBL()
            elif self['smoothing_type'] == 'none':
                self._prvt__PerformNoSmoothing()
                self._prvt__PerformPeakDetectionWithoutBL()
            if self['plot_mode'] == 2:
                self._prvt__PlotFigure()
    
    def results(self):
        AnalyticsManager.final_update_bus(self['ric'], self['trading_destination_id'])
        self._this_is_last_plot = True
        if self['plot_mode'] != 0:
            self._prvt__PlotFigure()
            if self['plot_mode'] == 2:
                matplotlib.pyplot.ioff()
        return self.indicators

