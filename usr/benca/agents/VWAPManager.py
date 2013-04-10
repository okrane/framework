'''
Created on 26 juil. 2010

@author: benca
'''

from simep.bin.simepcore import Order, Trade
from simep.core.basetrader import BaseTrader
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog
from simep.tools import *
import usr.dev.sivla.funcs.DBTools.SE
import usr.dev.sivla.funcs.TradingCurves.QRLib
import datetime
import matplotlib.pyplot
import math

''' Tactics Imports '''
import VWAPTacticPursuit
import VWAPTacticAggressive
import VWAPTacticWouldLevel
import VWAPTacticVwapComp



class VWAPManager(BaseTrader):
    
    @staticmethod
    def indicators_list():
        return ['avg_exec_time_s_300', 'avg_trade_size_s_300']
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, params, trace):
        self._side = 1 if params['side']== Order.Buy else -1
        params['start_time']            = date2num(params['start_time'])
        params['end_time']              = date2num(params['end_time'])
        # algo modes
        self._use_vwap_comp             = False
        # constants
        self._compute_min_max_qty       = True if (params['min_per'] > 0.0) and (params['min_per'] < params['max_per']) else False
        self._minmaxpercent_coeff       = 1.1
        self._slice_timestep            = 300 * 1000
        self._number_of_slices          = int(date2num('08:30:00:000')/self._slice_timestep)
        # variables
        self._PI_t                      = 0.0
        self._PI_t_H                    = 0.0
        self._PI_t_L                    = 0.0
        self._min_exec_qty              = 0
        self._max_exec_qty              = 0
        self._all_reserved_qty          = 0
        self._prev_slice_index          = 0
        self._next_slice_index          = 1
        # display title
        self._figure_title              = '[date=' + context['date'] + ', ric=' + str(context['ric']) +  ', side=%s, asked_qty=' + str(params['asked_qty']) + ', min_per=' + str(params['min_per']) + ', max_per=' + str(params['max_per']) + ', exec_style=' + str(params['execution_style']) + ']'
        self._figure_title              = self._figure_title % ('BUY' if self._side==1 else 'SELL')
        # data curves
        self._prvt__GetDataCurves(context, params)
        BaseTrader.__init__(self, setup, context, params, trace)
        self._prvt__UpdateVolumeCurve()
        # initialize base class and update
        # slices params
        self._prev_slice_time           = params['start_time']
        self._next_slice_time           = self._prev_slice_time + self._slice_timestep
        self._se_curves_timestep        = [datetime.datetime.strptime(num2date(i*(self['end_time']-self['start_time'])/self._number_of_slices+self['start_time'])[0:8],'%H:%M:%S') for i in range(self._number_of_slices+1)]
        # instantiate tactics
        if params['use_pursuit']:
            self._PURSUIT    = VWAPTacticPursuit.PursuitTacticVWAP(self, params['pursuit_cycle_time'], params['pursuit_distance'], params['pursuit_use_constant_size'], params['pursuit_use_business_time'])
        if params['use_aggressive']:
            self._AGGRESSIVE = VWAPTacticAggressive.AggressiveTacticVWAP(self, params['pursuit_use_business_time'])
        if params['use_wouldlevel']:
            self._WOULDLEVEL = VWAPTacticWouldLevel.WouldLevelTacticVWAP(self)
        if self._use_vwap_comp:
            self._VWAPCOMP = VWAPTacticVwapComp.VwapCompTacticVWAP(self)
    
    
    
    '''######################################################################################################
    ########################################   STATISTICAL ENGINE   #########################################
    ######################################################################################################'''
    
    def _prvt__GetDataCurves(self, context, params):
        # compute the SE volume cumulated curve
        self._SE_volume           = usr.dev.sivla.funcs.DBTools.SE.get_se('volume-curve',        context['security_id'], context['trading_destination_id'], context['date']).value['Usual day'][8:]
        self._SE_volatility       = usr.dev.sivla.funcs.DBTools.SE.get_se('volatility-curve',    context['security_id'], context['trading_destination_id'], context['date']).value['USUAL DAY'][3:]
        self._SE_market_impact    = usr.dev.sivla.funcs.DBTools.SE.get_se('market-impact',       context['security_id'], context['trading_destination_id'], context['date']).value['Overall']
        self._SE_aggressivity     = usr.dev.sivla.funcs.DBTools.SE.get_se('aggressivity-levels', context['security_id'], context['trading_destination_id'], context['date']).value['DEFAULT']
        self._SE_gamma            = self._SE_market_impact[0]
        self._SE_kappa            = self._SE_market_impact[1]
        params['daily_market_volume'] = self._SE_aggressivity[1]
        self._SE_lambda           = 1e-6
        if   params['execution_style'] == 'Passive':
            self._SE_aggress_up = self._SE_aggressivity[11]*params['asked_qty']
            self._SE_aggress_dn = self._SE_aggressivity[10]*params['asked_qty']
        elif params['execution_style'] == 'Neutral':
            self._SE_aggress_up = self._SE_aggressivity[9]*params['asked_qty']
            self._SE_aggress_dn = self._SE_aggressivity[8]*params['asked_qty']
        elif params['execution_style'] == 'Aggressive':
            self._SE_aggress_up = self._SE_aggressivity[3]*params['asked_qty']
            self._SE_aggress_dn = self._SE_aggressivity[2]*params['asked_qty']
        elif params['execution_style'] == 'Highly Aggressive' or params['execution_style'] == 'HighlyAggressive' or params['execution_style'] == 'Highly_Aggressive':
            self._SE_aggress_up = self._SE_aggressivity[7]*params['asked_qty']
            self._SE_aggress_dn = self._SE_aggressivity[6]*params['asked_qty']
        elif params['execution_style'] == 'Get Me Done' or params['execution_style'] == 'GetMeDone' or params['execution_style'] == 'Get_Me_Done':
            self._SE_aggress_up = self._SE_aggressivity[5]*params['asked_qty']
            self._SE_aggress_dn = self._SE_aggressivity[4]*params['asked_qty']
        else:
            pyLog('ERROR : unknown aggressivity level')
            raise ValueError('ERROR : unknown aggressivity level')
    
    def _prvt__UpdateVolumeCurve(self):
        t = self._prev_slice_index
        n = len(self._SE_volume)
        A = usr.dev.sivla.funcs.TradingCurves.QRLib.VwapTradingCurves(n - t, 
           int(round(self['asked_qty']*(1-self._PIs[0][t]))) if t!=0 else self['asked_qty'], 
           self._SE_aggress_up, 
           self._SE_aggress_dn, 
           self._SE_lambda, 
           self._SE_kappa, 
           self._SE_gamma, 
           self._SE_volume[t:], 
           self._SE_volatility[t:], 
           self['daily_market_volume'])
        if t == 0:
            self._PIs = A
            for i in range(3):
                for j in range(t,n+1):
                    self._PIs[i][j] = 1 - self._PIs[i][j]
        else:
            CurrentPIs = [self._PIs[0][t],self._PIs[1][t],self._PIs[2][t]]
            # re-organize trading curves
            for i in range(3):
                for j in range(t,n+1):
                    self._PIs[i][j] = (CurrentPIs[i]-1)*A[i][j-t] + 1
    
    
    
    '''######################################################################################################
    #########################################   PRIVATE FUNCTIONS   #########################################
    ######################################################################################################'''
    
    def update_reserved_qty(self):
        self._all_reserved_qty = 0
        if self['use_aggressive']:
            if self._AGGRESSIVE._order != None:
                self._all_reserved_qty += self._AGGRESSIVE._reserved_qty
        if self['use_pursuit']:
            if self._PURSUIT._order != None:
                self._all_reserved_qty += self._PURSUIT._reserved_qty
        if self['use_wouldlevel']:
            if self._WOULDLEVEL._order != None:
                self._all_reserved_qty += self._WOULDLEVEL._reserved_qty
        if self._use_vwap_comp:
            if self._VWAPCOMP._order != None:
                self._all_reserved_qty += self._VWAPCOMP._reserved_qty
    
    def _prvt__update_volume_slice(self):
        if ba['event']['TIME_NUM'] >= self._next_slice_time:
            # determine whether it is the next slice, or the one after, or the one after after that one... you know !
            delta_slice = int( math.floor( (ba['event']['TIME_NUM']-self._next_slice_time)/self._slice_timestep ) + 1 )
            # perform updating
            self._prev_slice_index += delta_slice
            self._next_slice_index += delta_slice
            self._prev_slice_time  += delta_slice * self._slice_timestep
            self._next_slice_time  += delta_slice * self._slice_timestep
            
    def _prvt__interpolate_pis(self):
        self._PI_t      =  (ba['event']['TIME_NUM'] - self._prev_slice_time) * self._PIs[0][self._next_slice_index] + \
                           (self._next_slice_time - ba['event']['TIME_NUM']) * self._PIs[0][self._prev_slice_index]
        self._PI_t_H    =  (ba['event']['TIME_NUM'] - self._prev_slice_time) * self._PIs[2][self._next_slice_index] + \
                           (self._next_slice_time - ba['event']['TIME_NUM']) * self._PIs[2][self._prev_slice_index]
        self._PI_t_L    =  (ba['event']['TIME_NUM'] - self._prev_slice_time) * self._PIs[1][self._next_slice_index] + \
                           (self._next_slice_time - ba['event']['TIME_NUM']) * self._PIs[1][self._prev_slice_index]
        self._PI_t     /=  (self._slice_timestep)
        self._PI_t_H   /=  (self._slice_timestep)
        self._PI_t_L   /=  (self._slice_timestep)
        self._PI_t_L = min(self._PI_t_L, self._PI_t)
        self._PI_t_H = max(self._PI_t_H, self._PI_t)
        ''' UPDATE MINEXECQTY AND MAXEXECQTY '''
        if self._compute_min_max_qty:
            self._min_exec_qty = int(min( max(self['min_per']*self._bus['VOLUME'], self._PI_t_L*self['asked_qty']) , self['max_per']*self._bus['VOLUME'] ))
            self._max_exec_qty = int(max( min(self['max_per']*self._bus['VOLUME'], self._PI_t_H*self['asked_qty']) , self['min_per']*self._bus['VOLUME'] ))
            if self._min_exec_qty > self._PI_t_H*self['asked_qty']:
                self._max_exec_qty = min( self['max_per'] , self['min_per']*self._minmaxpercent_coeff ) * self._bus['VOLUME']
            if self._max_exec_qty < self._PI_t_L*self['asked_qty']:
                self._min_exec_qty = max( self['min_per'] , self['max_per']/self._minmaxpercent_coeff ) * self._bus['VOLUME']
            if self._min_exec_qty > self._max_exec_qty:
                pyLog('FATAL ERROR : self._min_exec_qty > self._max_exec_qty')
            ''' UPDATE self._PI_t_L AND self._PI_t_H '''
            self._PI_t_L = float(self._min_exec_qty)/self['asked_qty']
            self._PI_t_H = float(self._max_exec_qty)/self['asked_qty']
            ''' MAKE SURE THAT PI-(t) < PI(t) < PI+(t) '''
            self._PI_t   = max(self._PI_t, self._PI_t_L)
            self._PI_t   = min(self._PI_t, self._PI_t_H)
    
    def _prvt__cancel_all_orders(self):
        if self['use_pursuit']:
            self._PURSUIT.cancel_order()
        if self['use_aggressive']:
            self._AGGRESSIVE.cancel_order()
        if self['use_wouldlevel']:
            self._WOULDLEVEL.cancel_order()
        if self._use_vwap_comp:
            self._VWAPCOMP.cancel_order()
    
    def _prvt__high_level_manager(self, lob):
        # place order
        if   self._exec_qty <= int(self._PI_t_L*self['asked_qty'])-1:
            if self['use_pursuit']:
                self._PURSUIT.send_order(lob)
            if self['use_aggressive']:
                self._AGGRESSIVE.send_order(lob)
            if self['use_wouldlevel']:
                self._WOULDLEVEL.send_order(lob)
            if self._use_vwap_comp:
                self._VWAPCOMP.send_order(lob)
        elif self._exec_qty <= int(self._PI_t*self['asked_qty'])-1:
            if self['use_pursuit']:
                self._PURSUIT.send_order(lob)
            if self['use_aggressive']:
                self._AGGRESSIVE.cancel_order()
            if self['use_wouldlevel']:
                self._WOULDLEVEL.send_order(lob)
            if self._use_vwap_comp:
                self._VWAPCOMP.send_order(lob)
        elif self._exec_qty <= int(self._PI_t_H*self['asked_qty']):
            if self['use_aggressive']:
                self._AGGRESSIVE.cancel_order()
            if self['use_wouldlevel']:
                self._WOULDLEVEL.send_order(lob)
            if self._use_vwap_comp:
                self._VWAPCOMP.send_order(lob)
        elif self._exec_qty > int(round(self._PI_t_H*self['asked_qty'])):
            if self['use_pursuit']:
                self._PURSUIT.cancel_order()
            if self['use_aggressive']:
                self._AGGRESSIVE.cancel_order()
    
    
    
    '''######################################################################################################
    ######################################   PURE VIRTUAL FUNCTIONS   #######################################
    ######################################################################################################'''
    
    def _pvtl__update_child_plot_curves(self):
        pass
    
    def _pvtl__update_child_plot_figure(self):
        matplotlib.pyplot.subplot(211)
        matplotlib.pyplot.plot_date(self._se_curves_timestep,  self._PIs[0], xdate=True, ydate=False, color='r', linestyle='-', linewidth=2, marker='None')
        matplotlib.pyplot.plot_date(self._se_curves_timestep,  self._PIs[1], xdate=True, ydate=False, color='g', linestyle='-', linewidth=1, marker='None')
        matplotlib.pyplot.plot_date(self._se_curves_timestep,  self._PIs[2], xdate=True, ydate=False, color='g', linestyle='-', linewidth=1, marker='None')
    
    def _pvtl__update_last_plot(self):
        current_string = ', plr=%.04f]' %(float(self['asked_qty'])/self._bus['VOLUME'])
        self._figure_title = self._figure_title.replace(']',current_string)

    
    
    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
        
    def process(self, event):
        code = self.update(event)
        lob = event.getLob()
        self._prvt__update_volume_slice()
        self._prvt__interpolate_pis()
        if code == 0:
            self._prvt__cancel_all_orders()
        if code == 1:
            self._prvt__high_level_manager(lob)
 
    def processReport(self, evtReport):
        OrderId = evtReport.orderId
        for type in ['_PURSUIT','_AGGRESSIVE','_WOULDLEVEL','_VWAPCOMP']:
            if type in OrderId:
                OrderId = OrderId.split(type)[0]
                if OrderId != self['name']:
                    return None
                if evtReport.price > 100000.0 or evtReport.price < 0.1:
                    pyLog("FATAL ERROR : evtReport.price > 100000.0 or evtReport.price < 0.1")
                    return None
                self._exec_qty            += evtReport.size
                self._weighted_price_algo += evtReport.size * evtReport.price
                if   type == '_PURSUIT':
                    self._PURSUIT.update_reserved_qty(evtReport.orderSnapshot.remain)
                elif type == '_AGGRESSIVE':
                    self._AGGRESSIVE.update_reserved_qty(evtReport.orderSnapshot.remain)
                elif type == '_WOULDLEVEL':
                    self._WOULDLEVEL.update_reserved_qty(evtReport.orderSnapshot.remain)
                elif type == '_VWAPCOMP':
                    self._VWAPCOMP.update_reserved_qty(evtReport.orderSnapshot.remain)
                tactic_type = type[1]
                self._algo_trades_list['price'].append(evtReport.price)
                self._algo_trades_list['size'].append(evtReport.size)
                self._algo_trades_list['time'].append(ba['event']['TIME_NUM'])
                self._algo_trades_list['type'].append(tactic_type)
                self.appendIndicator(pyData('init', 
                                            date = [ba['event']['TIME_STR']], 
                                            value = {'OrderType': [tactic_type], 
                                                     'ExecPrice': [evtReport.price], 
                                                     'ExecQty': [evtReport.size], 
                                                     'PI(t)': [self._PI_t], 
                                                     'PI+(t)': [self._PI_t_H], 
                                                     'PI-(t)': [self._PI_t_L], 
                                                     'VwapPI(t)': [float(self._exec_qty)/self['asked_qty']], 
                                                     'MarketVolume': [self._bus['VOLUME']], 
                                                     'MinPerMarketVolume': [int(self['min_per']*self._bus['VOLUME'])], 
                                                     'MaxPerMarketVolume': [int(self['max_per']*self._bus['VOLUME'])]}))
                break
        return None

