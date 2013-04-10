'''
Created on 10 febr. 2011

@author: benca
'''


from simep.core.baseagent import BaseAgent
from simep.tools import date2num



class LimitOrdersFlowEstimator(BaseAgent):
    
    
    
    '''######################################################################################################
    ##########################################   STATIC METHODS   ###########################################
    ######################################################################################################'''
    
    @staticmethod
    def public_parameters():
        parameters = {'result_filename'  : {'label' : 'Result Filename' , 'value' : 'C:/limit_orders_flow.m'},
                      'range'            : {'label' : 'Range'           , 'value' : 100}}
        return {'setup': {}, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return []
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, parameters, trace):
        super(LimitOrdersFlowEstimator, self).__init__(setup, context, parameters, trace)
        self.needExecReportEvt    = False        
        self.needAllEvts          = True
        self.securityIdEventsOnly = True
        ba                        = self.ba
        ba['mvrs'] = {'VENUE_ID'    : self.ba['venue_ids'][0] if len(self.ba['venue_ids']) == 1 else 'all',
                      'tick_size'   : None,
                      'map_bsiz'    : [0]*self['range'],
                      'map_bcnt'    : [0]*self['range'],
                      'map_asiz'    : [0]*self['range'],
                      'map_acnt'    : [0]*self['range']}
        
    

    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
        
    def process(self, event):
        if self.update(event) <= 0:
            return False
        ba       = self.ba
        mp       = ba['mvrs']
        fd       = ba['feed'][mp['VENUE_ID']]
        c_fd     = fd[0]
        p_fd     = fd[1]
        # perform tick size detection
        if c_fd['TRDPRC_1'] != None:
            if mp['tick_size'] == None:
                price    = float(c_fd['TRDPRC_1'])
                ticks    = c_fd['tick_sizes']
                ticks.reverse()
                for t in ticks:
                    if t[0] <= price:
                        mp['tick_size'] = round(t[1]*1000000.0)/1000000.0
                        break
        else:
            return False
        tick     = mp['tick_size']
        best_ask = c_fd['BEST_ASK1']
        best_bid = c_fd['BEST_BID1']
        # measure on bid
        if p_fd['BEST_ASK1'] == c_fd['BEST_ASK1']:
            c   = 0
            p   = 0
            c_bid  = c_fd['BEST_BID']
            c_bsiz = c_fd['BEST_BSIZ']
            p_bid  = p_fd['BEST_BID']
            p_bsiz = p_fd['BEST_BSIZ']
            while c < 5 and p < 5:
                if c_bid[c] != p_bid[p]:
                    if c_bid[c] > p_bid[p]:
                        idx = int(abs(c_bid[c] - best_ask)/tick + 0.1)-1
                        if idx < len(mp['map_bsiz']):
                            mp['map_bsiz'][idx] += c_bsiz[c]
                            mp['map_bcnt'][idx] += 1
                        c += 1
                    else:
                        p += 1
                else:
                    if c_bsiz[c] > p_bsiz[p]:
                        idx = int(abs(c_bid[c] - best_ask)/tick + 0.1)-1
                        if idx < len(mp['map_bsiz']):
                            mp['map_bsiz'][idx] += c_bsiz[c]-p_bsiz[p]
                            mp['map_bcnt'][idx] += 1
                    c += 1
                    p += 1
        # measure on ask
        if p_fd['BEST_BID1'] == c_fd['BEST_BID1']:
            c   = 0
            p   = 0
            c_ask  = c_fd['BEST_ASK']
            c_asiz = c_fd['BEST_ASIZ']
            p_ask  = p_fd['BEST_ASK']
            p_asiz = p_fd['BEST_ASIZ']
            while c < 5 and p < 5:
                if c_ask[c] != p_ask[p]:
                    if c_ask[c] > p_ask[p]:
                        idx = int(abs(c_ask[c] - best_bid)/tick + 0.1)-1
                        if idx < len(mp['map_asiz']):
                            mp['map_asiz'][idx] += c_asiz[c]
                            mp['map_acnt'][idx] += 1
                        c += 1
                    else:
                        p += 1
                else:
                    if c_asiz[c] > p_asiz[p]:
                        idx = int(abs(c_ask[c] - best_bid)/tick + 0.1)-1
                        if idx < len(mp['map_asiz']):
                            mp['map_asiz'][idx] += c_asiz[c]-p_asiz[p]
                            mp['map_acnt'][idx] += 1
                    c += 1
                    p += 1
    
    
    def last_process(self):
        ba       = self.ba
        mp       = ba['mvrs']
        print mp['map_bsiz']
        print mp['map_bcnt']
        print mp['map_asiz']
        print mp['map_acnt']
        dict_data = {}
        for i in range(self['range']):
            dict_data['map_acnt%02d' %i] = mp['map_acnt'][i]
            dict_data['map_asiz%02d' %i] = mp['map_asiz'][i]
            dict_data['map_bcnt%02d' %i] = mp['map_bcnt'][i]
            dict_data['map_bsiz%02d' %i] = mp['map_bsiz'][i]
        self.append_to_m_file(self['result_filename'], dict_data)
        
