'''
Created on 3 dec. 2010

@author: benca
'''


from simep.core.baseplotagent import BasePlotAgent
from simep.funcs.data.pyData import pyData
from simep.funcs.stdio.utils import pyLog
from simep.tools import date2num
from simep.core.analyticsmanager import AnalyticsManager
import math



class StocksComparator(BasePlotAgent):
    
    
    '''######################################################################################################
    ##########################################   STATIC METHODS   ###########################################
    ######################################################################################################'''
    
    @staticmethod
    def public_parameters():
        
        setup      = {'name'                   : {'label' : 'Name'                                     , 'value' : 'StocksComparator001'}}
        parameters = {'plot_mode'              : {'label' : 'Plot Mode (0, 1, 2)'                      , 'value' : 0}, 
                      'ric2'                   : {'label' : 'Ric of the other stock'                   , 'value' : 'STM.MI'}, 
                      'trading_destination_id2': {'label' : 'Trading Desintation ID of the other stock', 'value' : 6},
                      'security_id2'           : {'label' : 'Securityid ID of the other stock'         , 'value' : 7153},
                      'paris_spread_threshold' : {'label' : 'Threshold on the Spread on Paris'         , 'value' : 0.0055},
                      'milan_spread_threshold' : {'label' : 'Threshold on the Spread on Milan'         , 'value' : 0.007},
                      'results_filename'       : {'label' : 'Results Text Filename'                    , 'value' : 'C:/st_work/usr/dev/benca/stmicroelectronics/stm'}}
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return []
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, params, trace):
        BasePlotAgent.__init__(self, setup, context, params, trace)
        self.needAllEvts = True
        self.add_another_stock(self['security_id2'], ['%d%s'%(self['trading_destination_id2'],self['ric2'])])
        ba = self.ba
        self.indicators.info['r1_tick_sizes'] = ba['feed'][self.ba['venue_ids'][0]][0]['tick_sizes']
        self.indicators.info['r2_tick_sizes'] = ba['feed'][self.ba['venue_ids'][1]][0]['tick_sizes']
        ba['mvars'] = {'all_events'            : {},
                       'all_bigspreads_event'  : {},
                       'paris_event'           : {},
                       'milan_event'           : {},
                       'trade_paris_event'     : {},
                       'trade_milan_event'     : {},
                       'lob_paris_event'       : {},
                       'lob_milan_event'       : {},
                       'any_trade_event'       : {},
                       'any_lob_event'         : {},
                       'no_trade_event'        : {},
                       'bigspread_paris_event' : {},
                       'bigspread_milan_event' : {},
                       'any_bigspread_event'   : {},
                       'no_bigspread_event'    : {},
                       'bigspread_paris_&_paris_event' : {},
                       'bigspread_milan_&_milan_event' : {},
                       'any_bigspread_&_paris_event'   : {},
                       'any_bigspread_&_milan_event'   : {},
                       'all_bigspreads_&_paris_event'  : {},
                       'all_bigspreads_&_milan_event'  : {},
                       'no_bigspread_&_paris_event'    : {},
                       'no_bigspread_&_milan_event'    : {}}
        for val in ba['mvars'].values():
            val['avg_spread']       = 0.0
            val['avg_ratio_spread'] = 0.0
            val['avg_bsiz1']        = 0.0
            val['avg_asiz1']        = 0.0
            val['avg_ratio_bsiz1']  = 0.0
            val['avg_ratio_asiz1']  = 0.0
            val['std_spread']       = 0.0
            val['std_ratio_spread'] = 0.0
            val['std_bsiz1']        = 0.0
            val['std_asiz1']        = 0.0
            val['std_ratio_bsiz1']  = 0.0
            val['std_ratio_asiz1']  = 0.0
            val['number_of_evts']   = 0
            val['volume']           = 0
        
    

    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
        
    def process(self, event):
        ba = self.ba
        mv = ba['mvars']
        ae = ba['feed']
        if self.update(event) <= 0:
            return False
        # get venue id
        active_venue_id = ba['event']['VENUE_ID']
        paris_venue_id  = ba['venue_ids'][0]
        milan_venue_id  = ba['venue_ids'][1]
        if active_venue_id == paris_venue_id:
            passiv_vid = milan_venue_id
        else:
            passiv_vid = paris_venue_id
        ae[passiv_vid][0]['TRADE_EVENT'] = False
        # get feeds
        la = ae[active_venue_id][0]
        lp = ae[paris_venue_id][0]
        lm = ae[milan_venue_id][0]
        # increments indicators
        if lp['MID_PRICE'] != None and lm['MID_PRICE'] != None:
            is_paris_event  = (active_venue_id == paris_venue_id)
            is_milan_event  = (active_venue_id == milan_venue_id)
            trade_paris     = (lp['TRADE_EVENT'])
            trade_milan     = (lm['TRADE_EVENT'])
            bigspread_paris = (lp['SPREAD'] >= self['paris_spread_threshold'])
            bigspread_milan = (lm['SPREAD'] >= self['milan_spread_threshold'])
            for (key,val) in mv.iteritems():
                # check condition
                if   key == 'all_bigspreads_event':
                    condition = bigspread_milan and bigspread_paris
                elif key == 'all_bigspreads_&_milan_event':
                    condition = (bigspread_milan and bigspread_paris) and is_milan_event
                elif key == 'all_bigspreads_&_paris_event':
                    condition = (bigspread_milan and bigspread_paris) and is_paris_event
                elif key == 'all_events':
                    condition = True
                elif key == 'any_bigspread_event':
                    condition = bigspread_milan or bigspread_paris
                elif key == 'any_bigspread_&_milan_event':
                    condition = (bigspread_milan or bigspread_paris) and is_milan_event
                elif key == 'any_bigspread_&_paris_event':
                    condition = (bigspread_milan or bigspread_paris) and is_paris_event
                elif key == 'any_lob_event':
                    condition = (not trade_paris) or (not trade_milan)
                elif key == 'any_trade_event':
                    condition = (trade_paris or trade_milan)
                elif key == 'bigspread_milan_event':
                    condition = bigspread_milan
                elif key == 'bigspread_milan_&_milan_event':
                    condition = bigspread_milan and is_milan_event
                elif key == 'bigspread_paris_event':
                    condition = bigspread_paris
                elif key == 'bigspread_paris_&_paris_event':
                    condition = bigspread_paris and is_paris_event
                elif key == 'lob_milan_event':
                    condition = (not trade_milan and is_milan_event)
                elif key == 'lob_paris_event':
                    condition = (not trade_paris and is_paris_event)
                elif key == 'milan_event':
                    condition = is_milan_event
                elif key == 'no_bigspread_event':
                    condition = not bigspread_milan and not bigspread_paris
                elif key == 'no_bigspread_&_milan_event':
                    condition = (not bigspread_milan) and (not bigspread_paris) and is_milan_event
                elif key == 'no_bigspread_&_paris_event':
                    condition = (not bigspread_milan) and (not bigspread_paris) and is_paris_event
                elif key == 'no_trade_event':
                    condition = not trade_paris and not trade_milan
                elif key == 'paris_event':
                    condition = is_paris_event
                elif key == 'trade_paris_event':
                    condition = trade_paris
                elif key == 'trade_milan_event':
                    condition = trade_milan
                else:
                    condition = False
                # if condition do update
                if condition:
                    val['avg_spread']       += la['SPREAD']
                    val['avg_ratio_spread'] += (lp['SPREAD'])/(lm['SPREAD'])
                    val['avg_bsiz1']        += la['BEST_BSIZ1']
                    val['avg_asiz1']        += la['BEST_ASIZ1']
                    val['avg_ratio_bsiz1']  += (lp['BEST_BSIZ1']/lm['BEST_BSIZ1'])
                    val['avg_ratio_asiz1']  += (lp['BEST_ASIZ1']/lm['BEST_ASIZ1'])
                    val['std_spread']       += la['SPREAD']**2
                    val['std_ratio_spread'] += (lp['SPREAD'])/(lm['SPREAD'])**2
                    val['std_bsiz1']        += la['BEST_BSIZ1']**2
                    val['std_asiz1']        += la['BEST_ASIZ1']**2
                    val['std_ratio_bsiz1']  += (lp['BEST_BSIZ1']/lm['BEST_BSIZ1'])**2
                    val['std_ratio_asiz1']  += (lp['BEST_ASIZ1']/lm['BEST_ASIZ1'])**2
                    val['number_of_evts']   += 1
                    val['volume']           += la['LAST_TRDS_GROUP']['VOLUME']
    
    
    
    '''######################################################################################################
    ######################################   PURE VIRTUAL FUNCTIONS   #######################################
    ######################################################################################################'''
    
    def last_process(self):
        ba  = self.ba
        mv  = ba['mvars']
        lp  = ba['feed'][ba['venue_ids'][0]][0]
        lm = ba['feed'][ba['venue_ids'][1]][0]
        if lp['VOLUME'] == 0 or lm['VOLUME'] == 0:
            return
        mkt_volumes_ratio = float(lm['VOLUME'])/lp['VOLUME']
        if self['results_filename'] != None:
            for (key,val) in mv.iteritems():
                if val['number_of_evts'] == 0:
                    continue
                for k in val.keys():
                    if k != 'number_of_evts' and k != 'volume':
                        val[k] /= val['number_of_evts']
                for k in val.keys():
                    if 'std_' in k:
                        val[k] -= val['avg_%s'%k[4:]]**2
                for k in val.keys():
                    if 'std_' in k:
                        if val[k] > 0.0:
                            val[k] = math.sqrt(val[k])
                        else:
                            for i in range(50):
                                pyLog('ERROR : %s = %f' %(k,val[k]))
                            val[k] = 0.0
                for k in val.keys():
                    if 'siz1' in k:
                        val[k[0:4]+'vrn_'+k[4:]] = val[k]*mkt_volumes_ratio
                val['volume_ratio'] = float(lp['VOLUME'])/lm['VOLUME']
                self.append_to_m_file('%s_%s.m' %(self['results_filename'],key), val)
    
    