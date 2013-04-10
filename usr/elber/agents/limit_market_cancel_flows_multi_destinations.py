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
from string import *
from scipy.io import *
import scipy.io.matlab.mio
import numpy as num




def destination(venue_id): 
    k = 0
    destination = []
    while venue_id[k].isdigit():
        destination.append(venue_id[k])
        k += 1
    if destination !=[]:
        destinationn = ''.join(destination)
    else :
        destinationn = '0'
    return int(destinationn)



class LimitMarketCancelOrdersFlowsMultiDestinations(BaseAgent):
    
    
    
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
        super(LimitMarketCancelOrdersFlowsMultiDestinations, self).__init__(setup, context, parameters, trace)
        self.needAllEvts = True
        ba                        = self.ba
        ba['mvrs'] = {'VENUE_IDS'    : self.ba['venue_ids'] ,
                      'tick_size'   : 0.01}
        self.siz_ask      = {}
        self.size_bid     = {}
        self.number_siz_bid = {}
        self.number_siz_ask = {}
        self.number_increase_ask = {}
        self.number_increase_bid = {}
        self.number_decrease_ask ={}
        self.number_decrease_bid = {}
        self.discover_ask = {}
        self.disapear_ask = {}
        self.disapear_bid = {}
        self.discover_bid = {}
        self.number_discover_ask = {}
        self.number_discover_bid = {}
        self.number_disapear_ask = {}
        self.number_disapear_bid = {}
        self.increase_ask = {}
        self.decrease_ask = {}
        self.increase_bid = {}
        self.decrease_bid = {}
        self.volume_trade = {}
        self.number_trade = {}
        self.spread = {}
        self.delta_p_mid = {}
        self.mean_p_mid = {}
        self.p_mid = {}
        self.number_events = {}
        self.ofi = {}
        resultat = {}
        
        for venue_id in ba['mvrs']['VENUE_IDS']:
            destination_id = destination(venue_id)
            dest = 'destination%d' %destination_id
            self.siz_ask[dest] = 0
            self.size_bid[dest] = 0
            self.number_siz_bid[dest] = 0
            self.number_siz_ask [dest] = 0
            self.number_increase_ask [dest] = 0
            self.number_increase_bid [dest] = 0
            self.number_decrease_ask[dest] = 0
            self.number_decrease_bid [dest] = 0
            self.discover_ask[dest] = 0
            self.discover_bid[dest] = 0
            self.disapear_ask[dest] = 0
            self.disapear_bid[dest] = 0
            self.number_discover_ask[dest] = 0
            self.number_discover_bid[dest] = 0
            self.number_disapear_ask[dest] = 0
            self.number_disapear_bid[dest] = 0
            self.increase_ask[dest] = 0
            self.increase_bid[dest] = 0
            self.decrease_ask[dest] = 0
            self.decrease_bid[dest] = 0
            
            self.volume_trade[dest] = 0
            self.number_trade[dest] = 0
            self.spread[dest] = 0
            self.delta_p_mid[dest] = 0
            self.mean_p_mid[dest] = 0
            self.p_mid[dest] = []
            self.number_events[dest]=0
            self.ofi[dest] = 0
        self.t = 0
        """ ----------------- pour mono ------------------------------------"""
        for venue_id in ba['mvrs']['VENUE_IDS']:
            destination_id = destination(venue_id)
            dest = 'destination%d' %destination_id+'_m'
            self.siz_ask[dest] = 0
            self.size_bid[dest] = 0
            self.number_siz_bid[dest] = 0
            self.number_siz_ask[dest] = 0
            self.number_increase_ask[dest] = 0
            self.number_increase_bid[dest] = 0
            self.number_decrease_ask[dest] = 0
            self.number_decrease_bid[dest] = 0
            self.discover_ask[dest] = 0
            self.discover_bid[dest] = 0
            self.disapear_ask[dest] = 0
            self.disapear_bid[dest] = 0
            self.number_discover_ask[dest] = 0
            self.number_discover_bid[dest] = 0
            self.number_disapear_ask[dest] = 0
            self.number_disapear_bid[dest] = 0
            self.increase_ask[dest] = 0
            self.increase_bid[dest] = 0
            self.decrease_ask[dest] = 0
            self.decrease_bid[dest] = 0
            self.volume_trade[dest] = 0
            self.number_trade[dest] = 0
            self.spread[dest] = 0
            self.delta_p_mid[dest] = 0
            self.mean_p_mid[dest] = 0
            self.p_mid[dest] = []
            self.number_events[dest]=0
            resultat[dest]   =[]
        
            
        
    
        
    

    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
    
          
    def process(self, event):
        #self.print_orderbooks()
        #raw_input('wait')
        if self.update(event) <= 0:
            return False 
        
        ba       = self.ba
        mp       = ba['mvrs']
        n        = len(mp['VENUE_IDS'])
        c_fd = {}
        p_fd = {}
        # la liste des feed de chaque destination, le dernier feed contient l agregation de toutes les destinations
        for i in range(n) :
            fd= ba['feed'][mp['VENUE_IDS'][i]]
            destination_id = destination( mp['VENUE_IDS'][i]) 
            c_fd['destination%d' %destination_id]= fd[0]
            p_fd['destination%d' %destination_id]=fd[1]
        #destination agrege       
        dest_a='destination0'    
        event_destination = event.getVenueId()
        destination_id = destination(event_destination)
        c_dest = 'destination%d' %destination_id
        k=0
        for i in range(1 if n==1 else n-1) :
            dest = 'destination%d'%destination(mp['VENUE_IDS'][i])
            if c_fd[dest]['BEST_ASK1']!= None and p_fd[dest]['BEST_ASK1']!=None and c_fd[dest]['BEST_BID1']!= None and p_fd[dest]['BEST_BID1']!=None :
                k = 0
                # pour l ask
                p_ask = p_fd[dest_a]['BEST_ASK1']
                c_ask = c_fd[dest_a]['BEST_ASK1']    
                
                # cas 1
                if c_fd[dest]['BEST_ASK1']== c_ask and p_fd[dest]['BEST_ASK1']== p_ask :
                    delta_p = c_fd[dest]['BEST_ASK1'] - p_fd[dest]['BEST_ASK1']
                    if  delta_p == 0 :
                        delta_size = c_fd[dest]['BEST_ASIZ1'] - p_fd[dest]['BEST_ASIZ1']
                        if delta_size < 0 :
                            self.decrease_ask[dest] += -delta_size
                            self.number_decrease_ask[dest]+=1
                            k+=1
                        elif delta_size> 0 :
                            self.increase_ask[dest] += delta_size
                            self.number_increase_ask[dest] += 1
                            k+=1
                    elif delta_p > 0 :
                        #self.discover_ask[dest] += c_fd[dest]['BEST_ASIZ1']
                        #self.number_discover_ask[dest] += 1
                        self.decrease_ask[dest] += p_fd[dest]['BEST_ASIZ1']
                        self.number_decrease_ask[dest]+=1
                        k+=1
                    elif delta_p < 0 :
                        self.increase_ask[dest] += c_fd[dest]['BEST_ASIZ1'] 
                        self.number_increase_ask[dest] += 1
                        k+=1
                
                #cas 2    
                elif c_fd[dest]['BEST_ASK1']== c_ask and p_fd[dest]['BEST_ASK1']!= p_ask:
                    if c_fd[dest]['BEST_ASK1'] >= p_fd[dest]['BEST_ASK1'] :
                        self.discover_ask[dest] += c_fd[dest]['BEST_ASIZ1']
                        #k+=1
                        self.number_discover_ask[dest] += 1
                    else :
                        self.increase_ask[dest] += c_fd[dest]['BEST_ASIZ1']
                        self.number_increase_ask[dest] += 1
                        k+=1
                    
                #cas 3   
                elif c_fd[dest]['BEST_ASK1']!= c_ask and p_fd[dest]['BEST_ASK1'] == p_ask:
                    if c_fd[dest]['BEST_ASK1']== p_fd[dest]['BEST_ASK1']:
                        self.disapear_ask[dest] += p_fd[dest]['BEST_ASIZ1']
                        self.number_disapear_ask[dest] += 1
                        #k+=1
                    elif c_fd[dest]['BEST_ASK1'] != p_fd[dest]['BEST_ASK1']:
                        self.decrease_ask[dest] += p_fd[dest]['BEST_ASK1']
                        self.number_decrease_ask[dest]+=1
                        k+=1
                
                
            
            
                #pour le bid
                p_bid = p_fd[dest_a]['BEST_BID1']
                c_bid = c_fd[dest_a]['BEST_BID1']  
                
                
                # cas 1
                if c_fd[dest]['BEST_BID1']== c_bid and p_fd[dest]['BEST_BID1']== p_bid :
                    delta_p = c_fd[dest]['BEST_BID1'] - p_fd[dest]['BEST_BID1']
                    if  delta_p == 0 :
                        delta_size = c_fd[dest]['BEST_BSIZ1'] - p_fd[dest]['BEST_BSIZ1']
                        if delta_size < 0 :
                            self.decrease_bid[dest] += -delta_size
                            self.number_decrease_bid[dest] += 1
                            k+=1
                        elif delta_size > 0 :
                            self.increase_bid[dest] += delta_size
                            self.number_increase_bid[dest] += 1
                            k+=1
                    elif delta_p > 0 :
                        self.increase_bid[dest] += c_fd[dest]['BEST_BSIZ1']
                        self.number_increase_bid[dest] += 1
                        k+=1
                    else :
                        
                        self.discover_bid[dest] += c_fd[dest]['BEST_BSIZ1']
                        self.number_discover_bid[dest] += 1
                        self.decrease_bid[dest] += p_fd[dest]['BEST_BSIZ1'] 
                        self.number_decrease_bid[dest] += 1
                        k+=1
              
                    
                
                #cas 2    
                elif c_fd[dest]['BEST_BID1']== c_bid and p_fd[dest]['BEST_BID1']!= p_bid:
                    if c_fd[dest]['BEST_BID1'] > p_fd[dest]['BEST_BID1'] :
                        self.increase_bid[dest] += c_fd[dest]['BEST_BSIZ1']
                        self.number_increase_bid[dest] += 1
                        k+=1
                    else :
                        self.discover_bid[dest] += c_fd[dest]['BEST_BSIZ1']
                        self.number_discover_bid[dest] +=1
                        #k+=1
                    
                #cas 3   
                elif c_fd[dest]['BEST_BID1']!= c_bid and p_fd[dest]['BEST_BID1'] == p_bid:
                    if c_fd[dest]['BEST_BID1']== p_fd[dest]['BEST_BID1']:
                        self.disapear_bid[dest] += p_fd[dest]['BEST_BSIZ1']
                        self.number_disapear_bid[dest] += 1
                    elif c_fd[dest]['BEST_BID1'] != p_fd[dest]['BEST_BID1']:
                        self.decrease_bid[dest] += p_fd[dest]['BEST_BID1']
                        self.number_decrease_bid[dest] += 1
                        k+=1
                if k != 0 :
                    if k>2 :
                        print 'k',k
                    # si k est different de zero cela veut dire qu'on est sur la destination courante
                    # ie : dest == c_dest
                    self.number_events[dest] += k
                    self.p_mid[dest].append((c_fd[dest]['BEST_ASK1'] + c_fd[dest]['BEST_BID1'])/2)
                    self.spread[dest]  +=  c_fd[dest]['BEST_ASK1'] - c_fd[dest]['BEST_BID1']
                    
                
          
                
        #pour la destination agrege
        
        pnb = c_fd[dest_a]['BEST_BID1']
        pnb0 = p_fd[dest_a]['BEST_BID1']
        pna = c_fd[dest_a]['BEST_ASK1']
        pna0 = p_fd[dest_a]['BEST_ASK1']
        qnb = c_fd[dest_a]['BEST_BSIZ1']
        qnb0 = p_fd[dest_a]['BEST_BSIZ1']
        qna  = c_fd[dest_a]['BEST_ASIZ1']
        qna0 = p_fd[dest_a]['BEST_ASIZ1']
        tmp = (pnb>=pnb0)*qnb - (pnb<=pnb0)*qnb0 - (pna<=pna0)*qna + (pna>=pna0)*qna0
        #cette condition veut dire qu'il y a eu une modif sur au moins un des best
        if  (pna!=pna0 or qna!=qna0) or (pnb!=pnb0 or qnb!=qnb0): #(pna>pna0 or qna<qna0) or (pnb>pnb0 or qnb>qnb0):
            self.ofi[dest_a] += tmp
            self.spread[dest_a] += c_fd['SPREAD']
            self.number_events[dest_a] +=1
            self.p_mid[dest_a] += (pnb +pna)/2  
        tmp_bid = (pnb>=pnb0)*qnb - (pnb<=pnb0)*qnb0
        if (pnb>pnb0)or (pnb == pnb0 and qnb>qnb0) :
            self.increase_bid[dest_a] += tmp_bid
            self.number_increase_bid[dest_a] +=1  
        if (pnb<pnb0) or (qnb<qnb0 and pnb==pnb0) :
            self.decrease_bid[dest_a] += tmp_bid 
            self.number_decrease_bid[dest_a] +=1
            if  (pnb<pnb0) :
                self.discover_bid[dest_a] += pnb
                self.number_discover_bid[dest_a] += 1
        
        tmp_ask =  (pna<=pna0)*qna - (pna>=pna0)*qna0   
        if (pna>pna0 or(pna==pna0 and qna<qna0)) :
            self.decrease_ask[dest_a] += tmp_ask   
            self.number_decrease_ask[dest_a] +=1
            if (pna>pna0) :
                self.discover_ask[dest_a] += qna
                self.number_discover_ask[dest_a] +=1
        if (pna<pna0 or(pna == pna0 and qna>qna0)) :
            self.increase_ask[dest_a] += tmp_ask  
            self.number_increase_ask[dest_a] +=1
        
        if  c_fd['TRADE_EVENT']:
            venue_id = event.getVenueId()
            trades = self.marketManager.getTrades(venue_id)
            if len(trades) != 0:
                self.volume_trade +=c_fd['LAST_TRDS_GROUP']['VOLUME']
                self.number_of_event_trade += 1
                self.number_of_trades += len(trades)
            else :
                raw_input('on a un evenement trade avec une liste vide de trades !!!!!')
        
        if c_fd[dest_a]['BEST_ASK1']!= None and p_fd[dest_a]['BEST_ASK1']!=None and c_fd[dest_a]['BEST_BID1']!= None and p_fd[dest_a]['BEST_BID1']!=None :
            m = 0
            delta_p = c_fd[dest_a]['BEST_ASK1'] - p_fd[dest_a]['BEST_ASK1']
            if  delta_p == 0 :
                delta_size = c_fd[dest_a]['BEST_ASIZ1'] - p_fd[dest_a]['BEST_ASIZ1']
                if delta_size < 0 :
                    self.decrease_ask[dest_a] += -delta_size
                    self.number_decrease_ask[dest_a]+=1
                    m+=1
                elif delta_size> 0 :
                    self.increase_ask[dest_a] += delta_size
                    self.number_increase_ask[dest_a] += 1
            elif delta_p > 0 :
                self.discover_ask[dest_a] += c_fd[dest_a]['BEST_ASIZ1']
                self.number_discover_ask[dest_a] += 1
                self.decrease_ask[dest_a] += p_fd[dest_a]['BEST_ASIZ1']
                self.number_decrease_ask[dest_a]+=1
                m+=1
            else :
                self.increase_ask[dest_a] += c_fd[dest_a]['BEST_ASIZ1'] 
                self.number_increase_ask[dest_a] += 1
                m+=1
                
                # pour le bid
            delta_p = c_fd[dest_a]['BEST_BID1'] - p_fd[dest_a]['BEST_BID1']
            if  delta_p == 0 :
                delta_size = c_fd[dest_a]['BEST_BSIZ1'] - p_fd[dest_a]['BEST_BSIZ1']
                if delta_size < 0 :
                    self.decrease_bid[dest_a] += -delta_size
                    self.number_decrease_bid[dest_a] += 1
                    m+=1
                elif delta_size > 0 :
                    self.increase_bid[dest_a] += delta_size
                    self.number_increase_bid[dest_a] += 1
            elif delta_p > 0 :
                self.increase_bid[dest_a] += c_fd[dest_a]['BEST_BSIZ1']
                self.number_increase_bid[dest_a] += 1
                m+=1
            else :
                self.decrease_bid[dest_a] += p_fd[dest_a]['BEST_BSIZ1'] 
                self.discover_bid[dest_a] += c_fd[dest_a]['BEST_BSIZ1']
                self.number_discover_bid[dest_a] += 1
                self.number_decrease_bid[dest_a] += 1
                m+=1
            if k>1 and m<2:
                print 'km',k,m
                #normalement je dois trouve m qui vaut toujours soit 1 soit zero, mais des fois je trouve m vaut 2!!!
            if m != 0:
                if m>=2 :
                    print 'm',m
                self.p_mid[dest_a].append((c_fd[dest_a]['BEST_ASK1'] + c_fd[dest_a]['BEST_BID1'])/2)
                self.spread[dest_a]  +=  c_fd[dest_a]['BEST_ASK1'] - c_fd[dest_a]['BEST_BID1']
                self.number_events[dest_a] += m #1
                
                
                
            
            #calcul de siz_bid et siz_ask
            if p_fd[dest_a]['BEST_ASIZ1'] != c_fd[dest_a]['BEST_ASIZ1'] :
                self.siz_ask[dest_a] += c_fd[dest_a]['BEST_ASIZ1'] 
                self.number_siz_ask[dest_a]+=1
            if p_fd[dest_a]['BEST_BSIZ1'] != c_fd[dest_a]['BEST_BSIZ1'] :   
                self.size_bid[dest_a] += c_fd[dest_a]['BEST_BSIZ1']
                self.number_siz_bid[dest_a]+=1
                
            if p_fd[c_dest]['BEST_ASIZ1'] != c_fd[c_dest]['BEST_ASIZ1'] :
                self.siz_ask[c_dest] += c_fd[c_dest]['BEST_ASIZ1'] 
                self.number_siz_ask[c_dest]+=1
            if p_fd[c_dest]['BEST_BSIZ1'] != c_fd[c_dest]['BEST_BSIZ1'] :
                self.size_bid[c_dest] += c_fd[c_dest]['BEST_BSIZ1']
                self.number_siz_ask[c_dest]+=1
            
            
            
            
            
        
        #pour les trades 
        if c_fd[dest_a]['TRADE_EVENT'] :
            venue_id = event.getVenueId()
            trades = self.marketManager.getTrades(venue_id)
            if len(trades) != 0:
                self.volume_trade[dest_a] += c_fd[dest_a]['LAST_TRDS_GROUP']['VOLUME']
                self.number_trade[dest_a] += 1
                
                self.volume_trade[c_dest] += c_fd[c_dest]['LAST_TRDS_GROUP']['VOLUME']
                self.volume_trade[c_dest+'_m'] +=c_fd[c_dest]['LAST_TRDS_GROUP']['VOLUME']
                self.number_trade[c_dest] += 1
                self.number_trade[c_dest+'_m'] += 1
                
                
        for i in range(1 if n==1 else n-1) : 
            dest = 'destination%d'%destination(mp['VENUE_IDS'][i])
            if c_fd[dest]['TRADE_EVENT'] :
                pass
                #self.volume_trade[dest] += c_fd[dest]['LAST_TRDS_GROUP']['VOLUME']
                #self.number_trade[dest] += 1
        """----------------------------------------------debut mono ---------------------------------------------""" 
        #pour les mono destinantions
        mm = 0
        list = []
        if c_fd[c_dest]['BEST_ASK1']!= None and p_fd[c_dest]['BEST_ASK1']!=None and c_fd[c_dest]['BEST_BID1']!= None and p_fd[c_dest]['BEST_BID1']!=None :
            delta_p = c_fd[c_dest]['BEST_ASK1'] - p_fd[c_dest]['BEST_ASK1']
            if  delta_p == 0 :
                delta_size = c_fd[c_dest]['BEST_ASIZ1'] - p_fd[c_dest]['BEST_ASIZ1']
                if delta_size < 0 :
                    self.decrease_ask[c_dest+'_m'] += -delta_size
                    self.number_decrease_ask[c_dest+'_m']+=1
                    mm+=1
                    list.append(1)
                elif delta_size> 0 :
                    self.increase_ask[c_dest+'_m'] += delta_size
                    self.number_increase_ask[c_dest+'_m'] += 1
            elif delta_p > 0 :
                self.discover_ask[c_dest+'_m'] += c_fd[c_dest]['BEST_ASIZ1']
                self.number_discover_ask[c_dest+'_m'] += 1
                self.decrease_ask[c_dest+'_m'] += p_fd[c_dest]['BEST_ASIZ1']
                self.number_decrease_ask[c_dest+'_m']+=1
                mm+=1
                list.append(2)
            else :
                self.increase_ask[c_dest+'_m'] += c_fd[c_dest]['BEST_ASIZ1'] 
                self.number_increase_ask[c_dest+'_m'] += 1
                mm+=1
                list.append(3)
                
                # pour le bid
            delta_p = c_fd[c_dest]['BEST_BID1'] - p_fd[c_dest]['BEST_BID1']
            if  delta_p == 0 :
                delta_size = c_fd[c_dest]['BEST_BSIZ1'] - p_fd[c_dest]['BEST_BSIZ1']
                if delta_size < 0 :
                    self.decrease_bid[c_dest+'_m'] += -delta_size
                    self.number_decrease_bid[c_dest+'_m'] += 1
                    mm+=1
                    list.append(4)
                elif delta_size > 0 :
                    self.increase_bid[c_dest+'_m'] += delta_size
                    self.number_increase_bid[c_dest+'_m'] += 1
            elif delta_p > 0 :
                self.increase_bid[c_dest+'_m'] += c_fd[c_dest]['BEST_BSIZ1']
                self.number_increase_bid[c_dest+'_m'] += 1
                mm+=1
                list.append(5)
            else :
                self.decrease_bid[c_dest+'_m'] += p_fd[c_dest]['BEST_BSIZ1'] 
                self.discover_bid[c_dest+'_m'] += c_fd[c_dest]['BEST_BSIZ1']
                self.number_discover_bid[c_dest+'_m'] += 1
                self.number_decrease_bid[c_dest+'_m'] += 1
                mm+=1
                list.append(6)
                #normalement je dois trouve m qui vaut toujours soit 1 soit zero, mais des fois je trouve m vaut 2!!!
            if mm != 0:
                if mm>=2 :
                    print 'mm',mm
                    print list
                self.p_mid[c_dest+'_m'].append((c_fd[c_dest]['BEST_ASK1'] + c_fd[c_dest]['BEST_BID1'])/2)
                self.spread[c_dest+'_m']  +=  c_fd[c_dest]['BEST_ASK1'] - c_fd[c_dest]['BEST_BID1']
                self.number_events[c_dest+'_m'] += mm
        """------------------------------------- fin mono-----------------------------------------------------"""
        
        
                
        j = 101
        for i in range(n) :
            
            destination_id = destination(mp['VENUE_IDS'][i])
            dest = 'destination%d' %destination_id
            j = min(j,self.number_events[dest])       
        #enregistrement des resultats
        if j == self['number_of_events'] :
            self.t = event.getTimeStamp()
            """---------------enregistrement pour multi destinantion------------------------------------------------"""
        
            D = {}
            for i in range(n) :
                destination_id = destination(mp['VENUE_IDS'][i])
                dest = 'destination%d' %destination_id
                self.mean_p_mid[dest] = num.mean(self.p_mid[dest])
                self.delta_p_mid[dest] = self.p_mid[dest][-1] - self.p_mid[dest][0]
                D['siz_ask_'+dest] = self.siz_ask[dest]
                D['number_siz_ask_'+dest] = self.number_siz_ask[dest]
                D['siz_bid_'+dest] = self.size_bid[dest]
                D['number_siz_bid_'+dest] = self.number_siz_bid[dest]
                D['discover_ask_'+dest]= self.discover_ask[dest]
              
                D['discover_bid_'+dest] = self.discover_bid[dest]
            
                D['disapear_ask_'+dest] = self.disapear_ask[dest]
              
                D['disapear_bid_'+dest] = self.disapear_bid[dest]
               
                D['number_discover_bid_'+dest]= self.number_discover_bid[dest]
                D['number_discover_ask_'+dest] = self.number_discover_ask[dest]
                D['number_disapear_ask_'+dest] = self.number_disapear_ask[dest]
                D['number_disapear_bid_'+dest] = self.number_disapear_bid[dest]
                D['icrease_ask_'+dest] = self.increase_ask[dest]
                D['descrease_ask_'+dest]= self.decrease_ask[dest]
                D['icrease_bid_'+dest] = self.increase_bid[dest]
                D['descrease_bid_'+dest]= self.decrease_bid[dest]
                D['volume_trade_'+dest] = self.volume_trade[dest]
                D['number_trade_'+dest] = self.number_trade[dest]
                D['spread_'+dest] = self.spread[dest]/self.number_events[dest]
                D['delta_p_mid_'+dest]= self.delta_p_mid[dest]
                D['mean_p_mid_'+dest] = self.mean_p_mid[dest]
                D['number_events_'+dest] = self.number_events[dest]
                D['number_increase_ask_'+dest] = self.number_increase_ask[dest]
                D['number_increase_bid_'+dest] = self.number_increase_bid[dest]
                D['number_decrease_ask_'+dest] = self.number_decrease_ask[dest]
                D['self.number_decrease_bid_'+dest] = self.number_decrease_bid[dest]
#                if self.number_decrease_ask[dest] == 0 :
#                    print self.decrease_ask[dest]
#                if self.number_increase_ask[dest] == 0 :
#                    print 'dec','inc',self.decrease_ask[dest],self.increase_ask[dest]

               
            
            #reinitialisation de toutes les varaibles
                self.siz_ask[dest] = 0
                self.size_bid[dest] = 0
                self.number_siz_bid[dest] = 0
                self.number_siz_ask[dest] = 0
                self.number_increase_ask[dest] = 0
                self.number_increase_bid[dest] = 0
                self.number_decrease_ask[dest] = 0
                self.number_decrease_bid[dest] = 0
                self.discover_ask[dest] = 0
                self.discover_bid[dest] = 0
                self.disapear_ask[dest] = 0
                self.disapear_bid[dest] = 0
                self.number_discover_ask[dest] = 0
                self.number_discover_bid[dest] = 0
                self.number_disapear_ask[dest] = 0
                self.number_disapear_bid[dest] = 0
                self.increase_ask[dest] = 0
                self.increase_bid[dest] = 0
                self.decrease_ask[dest] = 0
                self.decrease_bid[dest] = 0
                
                self.volume_trade[dest] = 0
                self.number_trade[dest] = 0
                self.spread[dest] = 0
                self.delta_p_mid[dest] = 0
                self.mean_p_mid[dest] = 0
                self.p_mid[dest] = []
                self.number_events[dest]=0
                self.ofi[dest]= 0
            
            self.append_indicator(D, self.t) 
            """----------------------------fin enregistrement pour multi destinantion-----------------------"""
        D_m = {}
        """---------------------enregistrement mono ------------------------------------------"""
        for i in range(n-1) :
            
            destination_id = destination(mp['VENUE_IDS'][i])
            dest = 'destination%d' %destination_id +'_m'
            if self.number_events[dest] == self['number_of_events']:
                self.mean_p_mid[dest] = num.mean(self.p_mid[dest])
                self.delta_p_mid[dest] = self.p_mid[dest][-1] - self.p_mid[dest][0]
                D_m['siz_ask_'+dest] = self.siz_ask[dest]
                D_m['number_siz_ask_'+dest] = self.number_siz_ask[dest]
                D_m['siz_bid_'+dest] = self.size_bid[dest]
                D_m['number_siz_bid_'+dest] = self.number_siz_bid[dest]
                D_m['discover_ask_'+dest]= self.discover_ask[dest]
              
                D_m['discover_bid_'+dest] = self.discover_bid[dest]
            
                D_m['disapear_ask_'+dest] = self.disapear_ask[dest]
              
                D_m['disapear_bid_'+dest] = self.disapear_bid[dest]
               
                D_m['number_discover_bid_'+dest]= self.number_discover_bid[dest]
                D_m['number_discover_ask_'+dest] = self.number_discover_ask[dest]
                D_m['number_disapear_ask_'+dest] = self.number_disapear_ask[dest]
                D_m['number_disapear_bid_'+dest] = self.number_disapear_bid[dest]
                D_m['icrease_ask_'+dest] = self.increase_ask[dest]
                D_m['descrease_ask_'+dest]= self.decrease_ask[dest]
                D_m['icrease_bid_'+dest] = self.increase_bid[dest]
                D_m['descrease_bid_'+dest]= self.decrease_bid[dest]
                D_m['volume_trade_'+dest] = self.volume_trade[dest]
                D_m['number_trade_'+dest] = self.number_trade[dest]
                D_m['spread_'+dest] = self.spread[dest]/self.number_events[dest]
                D_m['delta_p_mid_'+dest]= self.delta_p_mid[dest]
                D_m['mean_p_mid_'+dest] = self.mean_p_mid[dest]
                D_m['number_events_'+dest] = self.number_events[dest]
                D_m['number_increase_ask_'+dest] = self.number_increase_ask[dest]
                D_m['number_increase_bid_'+dest] = self.number_increase_bid[dest]
                D_m['number_decrease_ask_'+dest] = self.number_decrease_ask[dest]
                D_m['self.number_decrease_bid_'+dest] = self.number_decrease_bid[dest]
                #voir comment enregistrer ce truc 
                #savemat("C:/matlab_results/mono_dest.mat", D_m, appendmat=True, format='5', long_field_names=False, do_compression=False, oned_as='row')
            #self.append_indicator(D_m, self.t) 
                """ -----------fin enregistrement pour mono ----------------------------"""
        
        #reinitialisation pour les monos 
                self.siz_ask[dest] = 0
                self.size_bid[dest] = 0
                self.number_siz_bid[dest] = 0
                self.number_siz_ask[dest] = 0
                self.number_increase_ask[dest] = 0
                self.number_increase_bid[dest] = 0
                self.number_decrease_ask[dest] = 0
                self.number_decrease_bid[dest] = 0
                self.discover_ask[dest] = 0
                self.discover_bid[dest] = 0
                self.disapear_ask[dest] = 0
                self.disapear_bid[dest] = 0
                self.number_discover_ask[dest] = 0
                self.number_discover_bid[dest] = 0
                self.number_disapear_ask[dest] = 0
                self.number_disapear_bid[dest] = 0
                self.increase_ask[dest] = 0
                self.increase_bid[dest] = 0
                self.decrease_ask[dest] = 0
                self.decrease_bid[dest] = 0
                
                self.volume_trade[dest] = 0
                self.number_trade[dest] = 0
                self.spread[dest] = 0
                self.delta_p_mid[dest] = 0
                self.mean_p_mid[dest] = 0
                self.p_mid[dest] = []
                self.number_events[dest]=0
       
                
            
            
                        
    def last_process(self):
        pass