'''
Created on 17 mai 2011

@author: elber
'''

from simep.core.baseagent import BaseAgent
from simep.tools import date2num
import scipy.io.matlab.mio
import os
from simep.core.baseagent import BaseAgent
from collections import defaultdict
from simep.tools import date2num
from matplotlib.pylab import *
from matplotlib.pyplot import *
from numpy import *
from string import *
from scipy.io import *
import scipy.io.matlab.mio
from simep.funcs.data.pyData import *
from sched import Event


class LimitMarketCancelOrdersFlowsEstimators(BaseAgent):
    
    
    
    '''######################################################################################################
    ##########################################   STATIC METHODS   ###########################################
    ######################################################################################################'''
    
    @staticmethod
    def public_parameters():
        parameters = {'number_of_events' : {'label' : 'number of events', 'value'  : 100}}
        return {'setup': {}, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return []
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, parameters, trace):
        super(LimitMarketCancelOrdersFlowsEstimators, self).__init__(setup, context, parameters, trace)
        self.needExecReportEvt    = False        
        self.needAllEvts          = True
        self.securityIdEventsOnly = True
        ba                        = self.ba
        ba['mvrs'] = {'VENUE_ID'    : self.ba['venue_ids'][0] if len(self.ba['venue_ids']) == 1 else 'all',
                      'tick_size'   : 0.01}
         
        self.number_of_events  = 0
        self.bid_increase = 0
        self.ask_increase = 0
        self.bid_decrease = 0
        self.ask_decrease = 0
        self.best_bid_increase = 0
        self.best_ask_increase = 0
        self.best_ask_decrease = 0
        self.best_bid_decrease = 0
        self.mean_spread  = 0
        self.t_0          = 0
        self.t_final      = 0
        self.delta_t      = 0
        self.volume_trade = 0
        self.mean_size_best_bid = 0
        self.mean_size_best_ask = 0
        self.p_initial = 0
        self.p_final  = 0
        self.number_of_trades = 0
        self.delta_p_ask  = 0
        self.delta_p_bid  = 0
        self.p = []
        
        self.delta_siz_bid = 0
        self.delta_siz_ask = 0
        self.resultats  = []
        
        
        
        
    

    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
        
    def process(self, event):
            #self.print_orderbooks()
            #raw_input('wait')
            # on compte que les events avant 15h30
        #if event.getTimeStamp()<55800000010 :
        if self.update(event) <= 0:
            return False
        
        ba       = self.ba
        mp       = ba['mvrs']
        fd       = ba['feed'][mp['VENUE_ID']]
        c_fd     = fd[0]
        p_fd     = fd[1]
        self.mean_size_best_ask += c_fd['BEST_ASIZ1']
        self.mean_size_best_bid+= c_fd['BEST_BSIZ1']
        
        if  (c_fd['BEST_BSIZ1'] -p_fd['BEST_BSIZ1'] != 0 or c_fd['BEST_ASIZ1'] -p_fd['BEST_ASIZ1'] != 0) and c_fd['SPREAD']!=None:
            self.delta_siz_bid += c_fd['BEST_BSIZ1'] -p_fd['BEST_BSIZ1'] 
            self.delta_siz_ask += c_fd['BEST_ASIZ1'] -p_fd['BEST_ASIZ1'] 
            self.mean_spread += c_fd['SPREAD']
            self.number_of_events += 1
            
        if self.number_of_events == 1:
            self.delta_p_ask  = -c_fd['BEST_ASK1']
            self.delta_p_bid  = -c_fd['BEST_BID1']
            self.pmid_0       = (c_fd['BEST_ASK1'] + c_fd['BEST_BID1'])/2
            self.t_0          = event.getTimeStamp()
            
        # calucl des volumes des increases et des decreases du best ask et best bid
        
        #=======================================================================
        # # best bid decrease and increase
        # if c_fd['BEST_BID1'] == p_fd['BEST_BID1']:
        #    if c_fd['BEST_BSIZ1'] - p_fd['BEST_BSIZ1']>0:
        #        self.best_bid_increase = c_fd['BEST_BSIZ1'] - p_fd['BEST_BSIZ1']
        #    else:
        #        self.best_bid_decrease = -(c_fd['BEST_BSIZ1'] - p_fd['BEST_BSIZ1'])
        # elif c_fd['BEST_BID'] < p_fd['BEST_BID1']:
        #    self.best_bid_decrease += p_fd['BEST_BSIZ1']
        #    for k in range(1,5) :
        #        if c_fd['BEST_BID1'] < p_fd['BEST_BID'][k]:
        #            self.best_bid_decrease += p_fd['BEST_BSIZ'][k]
        #        elif c_fd['BEST_BID1'] == p_fd['BEST_BID'][k]:
        #            self.best_bid_decrease += p_fd['BEST_BSIZ'][k] - c_fd['BEST_BSIZ1']
        # elif c_fd['BEST_BID1'] > p_fd['BEST_BID1']:
        #    self.best_bid_increase += c_fd['BEST_BSIZ1']
        #    for k in range(1,5) :
        #        if c_fd['BEST_BID'][k] > p_fd['BEST_BID1']:
        #            self.best_bid_increase += c_fd['BEST_BSIZ'][k]
        #        elif c_fd['BEST_BID'][k] == p_fd['BEST_BID1']:
        #            self.best_bid_increase += c_fd['BEST_BSIZ'][k] - c_fd['BEST_BSIZ1']
        #            
        # # best ask decrease and increase
        # if c_fd['BEST_ASK1'] == p_fd['BEST_ASK1']:
        #    if c_fd['BEST_ASIZ1'] - p_fd['BEST_ASIZ1']>0:
        #        self.best_ask_increase = c_fd['BEST_ASIZ1'] - p_fd['BEST_ASIZ1']
        #    else:
        #        self.best_ask_decrease = -(c_fd['BEST_ASIZ1'] - p_fd['BEST_ASIZ1'])
        # elif c_fd['BEST_ASK1'] < p_fd['BEST_ASK1']:
        #    self.best_ask_increase += c_fd['BEST_ASIZ1']
        #    for k in range(1,5) :
        #        if c_fd['BEST_ASK'][k] < p_fd['BEST_ASK1']:
        #            self.best_ask_increase += c_fd['BEST_ASIZ'][k]
        #        elif c_fd['BEST_ASK'][k] == p_fd['BEST_ASK1']:
        #            self.best_ask_increase += c_fd['BEST_BSIZ'][k] - p_fd['BEST_BSIZ1']
        # elif c_fd['BEST_ASIZ1'] > p_fd['BEST_ASIZ1']:
        #    self.best_ask_decrease += p_fd['BEST_ASIZ1']
        #    for k in range(1,5) :
        #        if c_fd['BEST_ASK1'] > p_fd['BEST_ASK'][k]:
        #            self.best_ask_decrease += p_fd['BEST_ASIZ'][k]
        #        elif c_fd['BEST_ASK1'] == p_fd['BEST_ASK'][k]:
        #            self.best_ask_decrease += p_fd['BEST_ASIZ'][k] - c_fd['BEST_ASIZ1']
        #=======================================================================
                    
                
            
                
        
        event_type = 'cancel_order'
        
        
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
            pass
            #return False
        #tick     = mp['tick_size']
        #best_ask = c_fd['BEST_ASK1']
        #best_bid = c_fd['BEST_BID1']
        
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
                        event_type = 'limit_order'
                        self.bid_increase += c_bsiz[c]    
                        c += 1
                    else:
                        p += 1
                else:
                    if c_bsiz[c] > p_bsiz[p]:
                        self.bid_increase += c_bsiz[c] - p_bsiz[p]
                        event_type = 'limit_order'
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
                    if c_ask[c] < p_ask[p]:
                        event_type = 'limit_order'
                        
                        self.ask_increase += c_asiz[c]
                        c += 1
                    else:
                        p += 1
                else:
                    if c_asiz[c] > p_asiz[p]:
                        event_type = 'limit_order'
                        self.ask_increase += c_asiz[c] - p_asiz[p]
                    c += 1
                    p += 1
        # market orders
        if  c_fd['TRADE_EVENT']:
            venue_id = event.getVenueId()
            trades = self.marketManager.getTrades(venue_id)
            if len(trades) != 0:
                self.p.append(trades[0].price)
                self.p_final = trades[-1].price
            else :
                pass
                #raw_input('ce n est pas possible d avoir un evenement trade sans trades')
            
            # est ce que je compte un seul trade ou la dimension de la liste trades trade
            self.number_of_trades += 1
            self.volume_trade += c_fd['LAST_TRDS_GROUP']['VOLUME']
            event_type = 'market_order'
            if c_fd['BEST_ASIZ1'] < p_fd['BEST_ASIZ1']:
                self.ask_decrease += c_fd['LAST_TRDS_GROUP']['VOLUME']
                #self.delta_siz_ask -= c_fd['LAST_TRDS_GROUP']['VOLUME'] 
            else :
                self.bid_decrease += c_fd['LAST_TRDS_GROUP']['VOLUME']
                #self.delta_siz_bid -= c_fd['LAST_TRDS_GROUP']['VOLUME'] 
            
        if event_type == 'cancel_order' :#or event_type =='market_order':
            #===================================================================
            # if c_fd['BEST_ASK1']==p_fd['BEST_BID']:
            #    for k in range(5):
            #        self.bid_decrease += c_fd['BEST_BSIZ'][k] - p_fd['BEST_BSIZ'][k]
            #        self.ask_decrease += c_fd['BEST_ASIZ'][k] - p_fd['BEST_ASIZ'][k]
            #===================================================================
            #bid decrease
            if p_fd['BEST_ASK1'] == c_fd['BEST_ASK1']:
                c   = 0
                p   = 0
                c_bid  = c_fd['BEST_BID']
                c_bsiz = c_fd['BEST_BSIZ']
                p_bid  = p_fd['BEST_BID']
                p_bsiz = p_fd['BEST_BSIZ']
                while c < 5 and p < 5:
                    
                    if c_bid[c] != p_bid[p]:
                        #cette condition doit etre vraie toujours
                        if c_bid[c] < p_bid[p]: 
                            self.bid_decrease += p_bsiz[p]
                            p += 1
                        
                            
                        else:
                            #if event_type=='cancel_order':
                            print event_type   
                            raw_input('il y a une erreur : bid prix!!!')
                            c += 1
                    else:
                        # normalement cette condition doit etre verifier toujours
                        if c_bsiz[c] <= p_bsiz[p]:
                            self.bid_decrease +=p_bsiz[p] - c_bsiz[c]
                        # ce else est toujour faux
                        else :
                            print event_type
                            raw_input('il y a une erreur : bid size !!!')
                        c += 1
                        p += 1
            # ask decrease
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
                            self.ask_decrease += p_asiz[p]
                            p += 1
                        else:
                            if event_type=='cancel_order':
                                raw_input('pas possible ask')
                            c += 1
                    else:
                        if c_asiz[c] < p_asiz[p]:
                            self.ask_decrease +=p_asiz[p] - c_asiz[c]
                        c += 1
                        p += 1
            
        
        #=======================================================================
        # if event_type == 'cancel_order' or event_type == 'limit_order' :
        #    # best bid decrease and increase
        #    if c_fd['BEST_BID1'] == p_fd['BEST_BID1']:
        #        if c_fd['BEST_BSIZ1'] - p_fd['BEST_BSIZ1']>0:
        #            self.best_bid_increase = c_fd['BEST_BSIZ1'] - p_fd['BEST_BSIZ1']
        #        else:
        #            self.best_bid_decrease = -(c_fd['BEST_BSIZ1'] - p_fd['BEST_BSIZ1'])
        #    elif c_fd['BEST_BID'] < p_fd['BEST_BID1']:
        #        self.best_bid_decrease += p_fd['BEST_BSIZ1']
        #        for k in range(1,5) :
        #            if c_fd['BEST_BID1'] < p_fd['BEST_BID'][k]:
        #                self.best_bid_decrease += p_fd['BEST_BSIZ'][k]
        #            elif c_fd['BEST_BID1'] == p_fd['BEST_BID'][k]:
        #                self.best_bid_decrease += p_fd['BEST_BSIZ'][k] - c_fd['BEST_BSIZ1']
        #    elif c_fd['BEST_BID1'] > p_fd['BEST_BID1']:
        #        self.best_bid_increase += c_fd['BEST_BSIZ1']
        #        for k in range(1,5) :
        #            if c_fd['BEST_BID'][k] > p_fd['BEST_BID1']:
        #                self.best_bid_increase += c_fd['BEST_BSIZ'][k]
        #            elif c_fd['BEST_BID'][k] == p_fd['BEST_BID1']:
        #                self.best_bid_increase += c_fd['BEST_BSIZ'][k] - c_fd['BEST_BSIZ1']
        #               
        #    # best ask decrease and increase
        #    if c_fd['BEST_ASK1'] == p_fd['BEST_ASK1']:
        #        if c_fd['BEST_ASIZ1'] - p_fd['BEST_ASIZ1']>0:
        #            self.best_ask_increase = c_fd['BEST_ASIZ1'] - p_fd['BEST_ASIZ1']
        #        else:
        #            self.best_ask_decrease = -(c_fd['BEST_ASIZ1'] - p_fd['BEST_ASIZ1'])
        #    elif c_fd['BEST_ASK1'] < p_fd['BEST_ASK1']:
        #        self.best_ask_increase += c_fd['BEST_ASIZ1']
        #        for k in range(1,5) :
        #            if c_fd['BEST_ASK'][k] < p_fd['BEST_ASK1']:
        #                self.best_ask_increase += c_fd['BEST_ASIZ'][k]
        #            elif c_fd['BEST_ASK'][k] == p_fd['BEST_ASK1']:
        #                self.best_ask_increase += c_fd['BEST_BSIZ'][k] - p_fd['BEST_BSIZ1']
        #    elif c_fd['BEST_ASIZ1'] > p_fd['BEST_ASIZ1']:
        #        self.best_ask_decrease += p_fd['BEST_ASIZ1']
        #        for k in range(1,5) :
        #            if c_fd['BEST_ASK1'] > p_fd['BEST_ASK'][k]:
        #                self.best_ask_decrease += p_fd['BEST_ASIZ'][k]
        #            elif c_fd['BEST_ASK1'] == p_fd['BEST_ASK'][k]:
        #                self.best_ask_decrease += p_fd['BEST_ASIZ'][k] - c_fd['BEST_ASIZ1']
        #                
        # self.delta_siz_bid = self.best_bid_increase - self.best_bid_decrease
        # self.delta_siz_ask = self.best_ask_increase - self.best_ask_decrease
        # 
        #               
        #=======================================================================
            
        
        if (self.number_of_events == self['number_of_events']) :
            self.pmid_f       = (c_fd['BEST_ASK1'] + c_fd['BEST_BID1'])/2
            self.delta_p_ask  += c_fd['BEST_ASK1']
            self.delta_p_bid  += c_fd['BEST_BID1']
            self.t_final = event.getTimeStamp()
            self.delta_t = self.t_final - self.t_0
            self.mean_size_best_ask = self.mean_size_best_ask/self['number_of_events']
            self.mean_size_best_bid = self.mean_size_best_bid/self['number_of_events']
            self.mean_spread        = self.mean_spread/self['number_of_events']
            self.delta_p  = self.pmid_f - self.pmid_0
            if len(self.p)!= 0:
                self.p_initial = self.p[0] 
             
                
            #===================================================================
            # list_tmp[0] = self.p_initial
            # list_tmp[1] = self.p_final
            # list_tmp[2] = self.bid_increase
            # list_tmp[3] = self.ask_increase
            # list_tmp[4] = self.bid_decrease
            # list_tmp[5] = self.ask_decrease
            # list_tmp[6] = self.mean_spread
            # list_tmp[7] = self.mean_size_best_ask
            # list_tmp[8] = self.mean_size_best_bid
            # list_tmp[9] = self.number_of_trades
            # list_tmp[10] = self.volume_trade
            # list_tmp[11] = self.best_ask_decrease
            # list_tmp[12] = self.best_ask_increase
            # list_tmp[13] = self.best_bid_decrease
            # list_tmp[14] = self.best_bid_increase
            # list_tmp[15] = self.delta_t
            # self.resultats.append(list_tmp)
            #===================================================================
            D = {}
            D['p_initial'] = self.p_initial
            D['p_final'] = self.p_final
            D['bid_increase'] = self.bid_increase
            D['ask_increas'] = self.ask_increase
            D['bid_decrease']= self.bid_decrease
            D['ask_decrease'] = self.ask_decrease
            D['mean_spread']= self.mean_spread
            D['mean_size_best_ask']= self.mean_size_best_ask
            D['mean_size_best_bid'] = self.mean_size_best_bid
            D['number_of_trade'] = self.number_of_trades
            D['volume_trade'] = self.volume_trade
            D['best_ask_decrease'] = self.best_ask_decrease
            D['best_ask_increase']= self.best_ask_increase
            D['best_bid_decrease'] = self.best_bid_decrease
            D['best_bid_increase']= self.best_bid_increase
            D['delta_t']= self.delta_t
            D['delta_bid'] = self.delta_siz_bid
            D['delta_ask'] = self.delta_siz_ask
            D['delta_p']   = self.delta_p
            D['delta_p_bid']= self.delta_p_bid
            D['delta_p_ask'] = self.delta_p_ask
            self.append_indicator(D,self.t_0)
            
  
            #===================================================================
            # for i,e in enumerate(list_tmp):
            #    D['%02d'%(int(i+1))] = e
            # self.append_indicator(D,self.t_0)
            #===================================================================
            
            
            # reinitialisation de tout
            self.bid_increase = 0
            self.ask_increase = 0
            self.bid_decrease = 0
            self.ask_decrease = 0
            self.mean_spread  = 0
            self.t_0          = 0
            self.t_final      = 0
            self.delta_t      = 0
            self.volume_trade = 0
            self.mean_size_best_bid = 0
            self.mean_size_best_ask = 0
            self.p_initial = 0
            self.p_final  =  0
            self.first_trade=0
            self.number_of_events = 0
            self.number_of_trades = 0
            self.p = []
            self.best_ask_decrease = 0
            self.best_ask_increase = 0
            self.best_bid_decrease = 0
            self.best_bid_increase = 0
            
            self.delta_siz_bid = 0
            self.delta_siz_ask = 0
            self.delta_p =0
            self.pmid_0 = 0
            self.pmid_f = 0
            self.delta_p_ask  = 0
            self.delta_p_bid  = 0
    
    def last_process(self):
        print 'finish'
        raw_input('press enter')
        pass