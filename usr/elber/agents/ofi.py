'''
Created on 21 juin 2011

@author: elber
'''
from simep.core.baseagent import BaseAgent
#from simep.tools import date2num
import numpy as num
from simep.tools import date2num




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


#remarque : si deux events ou plus coinsident, je considere qu'un seul

class PlusieurDestinationOfi(BaseAgent):
    
    
    
    '''######################################################################################################
    ##########################################   STATIC METHODS   ###########################################
    ######################################################################################################'''
    
    @staticmethod
    def public_parameters():
        parameters = {'number_of_events' : {'label' : 'number of events', 'value'  : 100},
                      'use_seuil'       : {'label' : 'use seuil'     , 'value'  : True}}
        return {'setup': {}, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return []
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, parameters, trace):
        super(PlusieurDestinationOfi, self).__init__(setup, context, parameters, trace)
        self.needAllEvts = True
        ba                        = self.ba
        ba['mvrs'] = {'VENUE_IDS'    : self.ba['venue_ids'] ,
                      'tick_size'   : 0.01}
        self.number_of_events  = {}
        self.bid_increase = {}
        self.ask_increase = {}
        self.ask_decrease = {}
        self.bid_decrease = {}
        self.number_ask_increase={}
        self.number_ask_decrease ={}
        self.number_bid_increase = {}
        self.number_bid_decrease={}
        self.mean_spread  = {}
        self.volume_trade = {}
        self.number_of_trades = {}
        self.number_of_event_trade = {}
        self.ofi = {}
        self.number_discover_ask = {}
        self.number_discover_bid ={}
        self.discover_ask = {}
        self.discover_bid = {}
        self.mid_price = {}
        self.siz_bid = {}
        self.siz_ask = {}
        self.disapear_ask = {}
        self.disapear_bid = {}
        self.number_disapear_ask={}
        self.number_disapear_bid = {}
        self.taille_file_bid = {}
        self.taille_file_ask = {}
        for venue_id in ba['mvrs']['VENUE_IDS']:
            destination_id = destination(venue_id)
            dest = 'destination%d' %destination_id
            self.number_of_events[dest]=0
            self.bid_increase[dest]=0
            self.ask_increase[dest]=0
            self.ask_decrease[dest]=0
            self.bid_decrease[dest]=0
            self.number_ask_increase[dest] =0
            self.number_ask_decrease[dest] =0
            self.number_bid_increase[dest]=0
            self.number_bid_decrease[dest] =0
            self.mean_spread [dest]=0
            self.volume_trade[dest]=0
            self.number_of_trades[dest]=0
            self.number_of_event_trade[dest]=0
            self.ofi[dest]=0
            self.number_discover_ask[dest]=0
            self.number_discover_bid[dest] =0
            self.discover_ask[dest]=0
            self.discover_bid [dest]=0
            self.mid_price [dest]=0
            self.siz_bid[dest] = 0
            self.siz_ask [dest]= 0
            self.disapear_ask [dest]= 0
            self.disapear_bid [dest]= 0
            self.number_disapear_ask[dest]= 0
            self.number_disapear_bid[dest]= 0
            self.taille_file_ask[dest] = 0 
            self.taille_file_bid[dest] = 0
        '''--------mono_destination --------------------'''
        for venue_id in ba['mvrs']['VENUE_IDS']:
            destination_id = destination(venue_id)
            dest = 'mono_destination%d' %destination_id
            self.number_of_events[dest]=0
            self.bid_increase[dest]=0
            self.ask_increase[dest]=0
            self.ask_decrease[dest]=0
            self.bid_decrease[dest]=0
            self.number_ask_increase[dest] =0
            self.number_ask_decrease[dest] =0
            self.number_bid_increase[dest]=0
            self.number_bid_decrease[dest] =0
            self.mean_spread [dest]=0
            self.volume_trade[dest]=0
            self.number_of_trades[dest]=0
            self.number_of_event_trade[dest]=0
            self.ofi[dest]=0
            self.number_discover_ask[dest]=0
            self.number_discover_bid[dest] =0
            self.discover_ask[dest]=0
            self.discover_bid [dest]=0
            self.mid_price [dest]=0
            self.siz_bid[dest] = 0
            self.siz_ask [dest]= 0
            self.disapear_ask [dest]= 0
            self.disapear_bid [dest]= 0
            self.number_disapear_ask[dest]= 0
            self.number_disapear_bid[dest]= 0
            self.taille_file_ask[dest] = 0 
            self.taille_file_bid[dest] = 0
        
        
        #ba       = self.ba
        mp       = ba['mvrs']
        n        = len(mp['VENUE_IDS'])
        self.c_fd = {}
        self.p_fd = {}    
        for i in range(n) :
            fd= ba['feed'][mp['VENUE_IDS'][i]]
            destination_id = destination( mp['VENUE_IDS'][i]) 
            self.c_fd['destination%d' %destination_id]= fd[0]
            self.p_fd['destination%d' %destination_id]=fd[1]
        opening = self.c_fd['destination0']['start_time']#context['opening'][context['trading_destination_ids'][1]]    
        self.delta_t = 0#date2num(opening)    
        self.t = 0
        self.last_t = opening
        self.min = self['number_of_events'] - 1
        
            
        
    
        
    

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
            if c_fd[c_dest]['BEST_ASK1']!= None and p_fd[c_dest]['BEST_ASK1']!=None and c_fd[c_dest]['BEST_BID1']!= None and p_fd[c_dest]['BEST_BID1']!=None :
                k = 0
                # pour l ask
                p_ask = p_fd[dest_a]['BEST_ASK1']
                c_ask = c_fd[dest_a]['BEST_ASK1']    
                dest = 'destination%d'%destination(mp['VENUE_IDS'][i])
                pnb = c_fd[dest]['BEST_BID1']
                pnb0 = p_fd[dest]['BEST_BID1']
                pna = c_fd[dest]['BEST_ASK1']
                pna0 = p_fd[dest]['BEST_ASK1']
                qnb = c_fd[dest]['BEST_BSIZ1']
                qnb0 = p_fd[dest]['BEST_BSIZ1']
                qna  = c_fd[dest]['BEST_ASIZ1']
                qna0 = p_fd[dest]['BEST_ASIZ1']
                
                # cas 1
                #je fais comme en mono_dest
                if c_fd[dest]['BEST_ASK1']== c_ask and p_fd[dest]['BEST_ASK1']== p_ask :
                    #tmp = (pnb>=pnb0)*qnb - (pnb<=pnb0)*qnb0 - (pna<=pna0)*qna + (pna>=pna0)*qna0
                    tmp_ask =  (pna<=pna0)*qna - (pna>=pna0)*qna0
                    if  (pna!=pna0 or qna!=qna0)and dest == c_dest: #or (pnb!=pnb0 or qnb!=qnb0): #(pna>pna0 or qna<qna0) or (pnb>pnb0 or qnb>qnb0):
                        self.ofi[dest] -= tmp_ask
                        self.mean_spread[dest] += c_fd[dest]['SPREAD']
                        self.number_of_events[dest] +=1
                        self.mid_price[dest]+= (pnb +pna)/2  
                        self.siz_ask[dest] +=qna
                        
                    if (pna>pna0 or(pna==pna0 and qna<qna0))and dest == c_dest :
                        self.ask_decrease[dest] += tmp_ask   
                        self.number_ask_decrease[dest] +=1
                    if (pna<pna0 or(pna == pna0 and qna>qna0))and dest == c_dest :
                        self.ask_increase[dest] += tmp_ask  
                        self.number_ask_increase[dest] +=1
                                                         
                    
                #cas 2    
                elif c_fd[dest]['BEST_ASK1']== c_ask and p_fd[dest]['BEST_ASK1']!= p_ask :
                    if c_fd[dest]['BEST_ASK1'] >= p_fd[dest]['BEST_ASK1'] :
                        # que le cas d egalite qui peut arriver
                        self.discover_ask[dest] += c_fd[dest]['BEST_ASIZ1']
                        #k+=1
                        self.number_discover_ask[dest] += 1
                    else :
                        if dest == c_dest :
                            self.ask_increase[dest] += c_fd[dest]['BEST_ASIZ1']
                            self.number_ask_increase[dest] += 1
                            
                            self.ofi[dest] -= c_fd[dest]['BEST_ASIZ1']
                            self.mean_spread[dest] += c_fd[dest]['SPREAD']
                            self.number_of_events[dest] +=1
                            self.mid_price[dest]+= (pnb +pna)/2  
                            self.siz_ask[dest] +=qna
                            
                            k+=1
                    
                #cas 3   
                elif c_fd[dest]['BEST_ASK1']!= c_ask and p_fd[dest]['BEST_ASK1'] == p_ask:
                    if c_fd[dest]['BEST_ASK1']== p_fd[dest]['BEST_ASK1']:
                        self.disapear_ask[dest] += p_fd[dest]['BEST_ASIZ1']
                        self.number_disapear_ask[dest] += 1
                        #k+=1
                    elif c_fd[dest]['BEST_ASK1'] > p_fd[dest]['BEST_ASK1']:
                        if dest == c_dest :
                            self.ask_decrease[dest] -= p_fd[dest]['BEST_ASIZ1']
                            self.number_ask_decrease[dest]+=1
                            
                            self.ofi[dest] += p_fd[dest]['BEST_ASIZ1']
                            self.mean_spread[dest] += c_fd[dest]['SPREAD']
                            self.number_of_events[dest] +=1
                            self.mid_price[dest]+= (pnb +pna)/2 
                            self.siz_ask[dest] +=qna 
                            
                            k+=1
                            
                    elif c_fd[dest]['BEST_ASK1'] < p_fd[dest]['BEST_ASK1']:
                        # on fait rien
                        pass

                
                
            
            
                #pour le bid
                p_bid = p_fd[dest_a]['BEST_BID1']
                c_bid = c_fd[dest_a]['BEST_BID1']  
                
                
                # cas 1
                if c_fd[dest]['BEST_BID1']== c_bid and p_fd[dest]['BEST_BID1']== p_bid :
                    tmp_bid = (pnb>=pnb0)*qnb - (pnb<=pnb0)*qnb0
                    if  (pnb!=pnb0 or qnb!=qnb0)and dest == c_dest: 
                        self.ofi[dest] += tmp_bid
                        self.mean_spread[dest] += c_fd[dest]['SPREAD']
                        self.number_of_events[dest] +=1
                        self.mid_price[dest]+= (pnb +pna)/2  
                        self.siz_bid[dest] += qnb
       
                    if ((pnb>pnb0)or (pnb == pnb0 and qnb>qnb0))and dest == c_dest :
                        self.bid_increase[dest] += tmp_bid
                        self.number_bid_increase[dest] +=1  
                    if ((pnb<pnb0) or (qnb<qnb0 and pnb==pnb0)) and dest == c_dest  :
                        self.bid_decrease[dest] += tmp_bid 
                        self.number_bid_decrease[dest] +=1 

                    
                
                #cas 2    
                elif c_fd[dest]['BEST_BID1']== c_bid and p_fd[dest]['BEST_BID1']!= p_bid:
                    if c_fd[dest]['BEST_BID1'] > p_fd[dest]['BEST_BID1'] :
                        if dest == c_dest :
                            self.bid_increase[dest] += c_fd[dest]['BEST_BSIZ1']
                            self.number_bid_increase[dest] += 1
                            
                            self.ofi[dest] += c_fd[dest]['BEST_BSIZ1']
                            self.mean_spread[dest] += c_fd[dest]['SPREAD']
                            self.number_of_events[dest] +=1
                            self.mid_price[dest]+= (pnb +pna)/2
                            self.siz_bid[dest] += qnb  
                            
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
                    elif c_fd[dest]['BEST_BID1'] < p_fd[dest]['BEST_BID1']:
                        if dest == c_dest :
                            self.bid_decrease[dest] -= p_fd[dest]['BEST_BID1']
                            self.number_bid_decrease[dest] += 1
                            
                            
                            self.ofi[dest] -= p_fd[dest]['BEST_BID1']
                            self.mean_spread[dest] += c_fd[dest]['SPREAD']
                            self.number_of_events[dest] +=1
                            self.mid_price[dest]+= (pnb +pna)/2 
                            self.siz_bid[dest] += qnb 
                            
                            k+=1
                    else :
                        pass
                    
                
          
                
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
        if  (pna!=pna0 or qna!=qna0) or (pnb!=pnb0 or qnb!=qnb0): 
            self.ofi[dest_a] += tmp
            self.mean_spread[dest_a] += c_fd[dest_a]['SPREAD']
            self.number_of_events[dest_a] +=1
            self.mid_price[dest_a] += (pnb +pna)/2 
            self.siz_bid[dest_a] += qnb
            self.siz_ask[dest_a] +=qna 
        tmp_bid = (pnb>=pnb0)*qnb - (pnb<=pnb0)*qnb0
        if (pnb>pnb0)or (pnb == pnb0 and qnb>qnb0) :
            self.bid_increase[dest_a] += tmp_bid
            self.number_bid_increase[dest_a] +=1  
        if (pnb<pnb0) or (qnb<qnb0 and pnb==pnb0) :
            self.bid_decrease[dest_a] += tmp_bid 
            self.number_bid_decrease[dest_a] +=1 
        
        tmp_ask =  (pna<=pna0)*qna - (pna>=pna0)*qna0   
        if (pna>pna0 or(pna==pna0 and qna<qna0)) :
            self.ask_decrease[dest_a] += tmp_ask   
            self.number_ask_decrease[dest_a] +=1
        if (pna<pna0 or(pna == pna0 and qna>qna0)) :
            self.ask_increase[dest_a] += tmp_ask  
            self.number_ask_increase[dest_a] +=1
        # calcul des discovers    
        if pnb<pnb0 :
                self.discover_bid[dest_a] += qnb
                self.number_discover_bid[dest_a] += 1
        if pna>pna0 :
                self.discover_ask[dest_a] += qna
                self.number_discover_ask[dest_a] +=1
        '''-------------------------------------mono destinantion -----------------------------'''
        destination_id = destination( mp['VENUE_IDS'][i]) 
        dest = 'mono_'+c_dest
        pnb = c_fd[c_dest]['BEST_BID1']
        pnb0 = p_fd[c_dest]['BEST_BID1']
        pna = c_fd[c_dest]['BEST_ASK1']
        pna0 = p_fd[c_dest]['BEST_ASK1']
        qnb = c_fd[c_dest]['BEST_BSIZ1']
        qnb0 = p_fd[c_dest]['BEST_BSIZ1']
        qna  = c_fd[c_dest]['BEST_ASIZ1']
        qna0 = p_fd[c_dest]['BEST_ASIZ1']
        tmp = (pnb>=pnb0)*qnb - (pnb<=pnb0)*qnb0 - (pna<=pna0)*qna + (pna>=pna0)*qna0
        #cette condition veut dire qu'il y a eu une modif sur au moins un des best
        if  (pna!=pna0 or qna!=qna0) or (pnb!=pnb0 or qnb!=qnb0): 
            self.ofi[dest] += tmp
            self.mean_spread[dest] += c_fd[c_dest]['SPREAD']
            self.number_of_events[dest] +=1
            self.mid_price[dest] += (pnb +pna)/2 
            self.siz_bid[dest] += qnb
            self.siz_ask[dest] +=qna 
        tmp_bid = (pnb>=pnb0)*qnb - (pnb<=pnb0)*qnb0
        if (pnb>pnb0)or (pnb == pnb0 and qnb>qnb0) :
            self.bid_increase[dest] += tmp_bid
            self.number_bid_increase[dest] +=1  
        if (pnb<pnb0) or (qnb<qnb0 and pnb==pnb0) :
            self.bid_decrease[dest] += tmp_bid 
            self.number_bid_decrease[dest] +=1 
        
        tmp_ask =  (pna<=pna0)*qna - (pna>=pna0)*qna0   
        if (pna>pna0 or(pna==pna0 and qna<qna0)) :
            self.ask_decrease[dest] += tmp_ask   
            self.number_ask_decrease[dest] +=1
        if (pna<pna0 or(pna == pna0 and qna>qna0)) :
            self.ask_increase[dest] += tmp_ask  
            self.number_ask_increase[dest] +=1
        # calcul des discovers    
        if pnb<pnb0 :
                self.discover_bid[dest] += qnb
                self.number_discover_bid[dest] += 1
        if pna>pna0 :
                self.discover_ask[dest] += qna
                self.number_discover_ask[dest] +=1
            
                
                
                     
        #pour les trades       
        if  c_fd[c_dest]['TRADE_EVENT']:
                venue_id = event.getVenueId()
                trades = self.marketManager.getTrades(venue_id)
                if len(trades) != 0:
                    self.volume_trade[c_dest] +=c_fd[c_dest]['LAST_TRDS_GROUP']['VOLUME']
                    self.number_of_event_trade[c_dest] += 1
                    self.number_of_trades[c_dest] += len(trades)
                    
                    self.volume_trade['mono_'+c_dest] +=c_fd[c_dest]['LAST_TRDS_GROUP']['VOLUME']
                    self.number_of_event_trade['mono_'+c_dest] += 1
                    self.number_of_trades['mono_'+c_dest] += len(trades)
                    
        #pour la dest agrege
        if  c_fd[dest_a]['TRADE_EVENT']:
                venue_id = event.getVenueId()
                trades = self.marketManager.getTrades(venue_id)
                if len(trades) != 0:
                    self.volume_trade[dest_a] +=c_fd[dest_a]['LAST_TRDS_GROUP']['VOLUME']
                    self.number_of_event_trade[dest_a] += 1
                    self.number_of_trades[dest_a] += len(trades)
                    
                    
        tmp = []         
        for i in range(n if n==1 else n-1):
            destination_id = destination( mp['VENUE_IDS'][i]) 
            dest = 'destination%d' %destination_id
            tmp.append(self.number_of_events[dest])
        self.min = min(tmp)
        
        
                

            
        """---------------enregistrement pour multi destinantion------------------------------------------------"""
        if self['use_seuil'] :
            if self.min >= self['number_of_events'] :
                self.t = event.getTimeStamp()
                self.delta_t = self.t - self.last_t
                self.last_t = self.t
                for i in range(n if n==1 else n-1):
                    destination_id = destination( mp['VENUE_IDS'][i]) 
                    dest = 'destination%d' %destination_id
                    if c_fd[dest]['BEST_ASK1'] == c_fd[dest_a]['BEST_ASK1']:
                        self.taille_file_ask[dest] = c_fd[dest]['BEST_ASIZ1']
                    if c_fd[dest]['BEST_BID1'] == c_fd[dest_a]['BEST_BID1']:
                        self.taille_file_bid[dest] = c_fd[dest]['BEST_BSIZ1']
                    self.taille_file_ask[dest_a] =   c_fd[dest_a]['BEST_ASIZ1'] 
                    self.taille_file_bid[dest_a] = c_fd[dest]['BEST_BSIZ1'] 
                D = {}
                for i in range(n) :
                    destination_id = destination(mp['VENUE_IDS'][i])
                    dest = 'destination%d' %destination_id
                    D['discover_ask_'+dest]          = self.discover_ask[dest]
                    D['discover_bid_'+dest]          = self.discover_bid[dest]
                    D['disapear_ask_'+dest]          = self.disapear_ask[dest]
                    D['disapear_bid_'+dest]          = self.disapear_bid[dest]
                    D['ofi_'+dest]                   = self.ofi[dest]
                    D['number_discover_bid_'+dest]   = self.number_discover_bid[dest]
                    D['number_discover_ask_'+dest]   = self.number_discover_ask[dest]
                    D['number_disapear_ask_'+dest]   = self.number_disapear_ask[dest]
                    D['number_disapear_bid_'+dest]   = self.number_disapear_bid[dest]
                    D['icrease_ask_'+dest]           = self.ask_increase[dest]
                    D['decrease_ask_'+dest]         = self.ask_decrease[dest]
                    D['icrease_bid_'+dest]           = self.bid_increase[dest]
                    D['decrease_bid_'+dest]         = self.bid_decrease[dest]
                    D['volume_trade_'+dest]          = self.volume_trade[dest]
                    D['number_of_trades_'+dest]      = self.number_of_trades[dest]
                    D['number_of_event_trade_'+dest] = self.number_of_event_trade[dest]
                    D['mid_price_'+dest]             = self.mid_price[dest] 
                    D['spread_'+dest]                = self.mean_spread[dest]
                    D['number_events_'+dest]         = self.number_of_events[dest]
                    D['number_increase_ask_'+dest]   = self.number_ask_increase[dest]
                    D['number_increase_bid_'+dest]   = self.number_bid_increase[dest]
                    D['number_decrease_ask_'+dest]   = self.number_ask_decrease[dest]
                    D['number_decrease_bid_'+dest]   = self.number_bid_decrease[dest]
                    D['siz_bid_'+dest]               = self.siz_bid[dest]
                    D['siz_ask_'+dest]               = self.siz_ask[dest]
                    D['zsiz_bid_instantane_'+dest]    = self.taille_file_bid[dest]
                    D['zsiz_ask_instantane_'+dest]    = self.taille_file_ask[dest]
                    #reinitialisation de toutes les varaibles
                    self.number_of_events[dest]      = 0
                    self.bid_increase[dest]          = 0
                    self.ask_increase[dest]          = 0
                    self.ask_decrease[dest]          = 0
                    self.bid_decrease[dest]          = 0
                    self.number_ask_increase[dest]   = 0
                    self.number_ask_decrease[dest]   = 0
                    self.number_bid_increase[dest]   = 0
                    self.number_bid_decrease[dest]   = 0
                    self.mean_spread [dest]          = 0
                    self.volume_trade[dest]          = 0
                    self.number_of_trades[dest]      = 0
                    self.number_of_event_trade[dest] = 0
                    self.ofi[dest]                   = 0
                    self.number_discover_ask[dest]   = 0
                    self.number_discover_bid[dest]   = 0
                    self.discover_ask[dest]          = 0
                    self.discover_bid [dest]         = 0
                    self.mid_price [dest]            = 0
                    self.siz_bid[dest]               = 0
                    self.siz_ask[dest]               = 0
                    self.disapear_ask [dest]         = 0
                    self.disapear_bid [dest]         = 0
                    self.number_disapear_ask[dest]   = 0
                    self.number_disapear_bid[dest]   = 0
                    self.taille_file_ask [dest]      = 0
                    self.taille_file_bid[dest]       = 0
                '''-----------pour mono destination ---------------'''    
                for i in range(n if n==1 else n-1) :
                    destination_id = destination(mp['VENUE_IDS'][i])
                    dest = 'destination%d' %destination_id
                    dest = 'mono_'+dest
                    D['discover_ask_'+dest]          = self.discover_ask[dest]
                    D['discover_bid_'+dest]          = self.discover_bid[dest]
                    D['disapear_ask_'+dest]          = self.disapear_ask[dest]
                    D['disapear_bid_'+dest]          = self.disapear_bid[dest]
                    D['ofi_'+dest]                   = self.ofi[dest]
                    D['number_discover_bid_'+dest]   = self.number_discover_bid[dest]
                    D['number_discover_ask_'+dest]   = self.number_discover_ask[dest]
                    D['number_disapear_ask_'+dest]   = self.number_disapear_ask[dest]
                    D['number_disapear_bid_'+dest]   = self.number_disapear_bid[dest]
                    D['icrease_ask_'+dest]           = self.ask_increase[dest]
                    D['decrease_ask_'+dest]         = self.ask_decrease[dest]
                    D['icrease_bid_'+dest]           = self.bid_increase[dest]
                    D['decrease_bid_'+dest]         = self.bid_decrease[dest]
                    D['volume_trade_'+dest]          = self.volume_trade[dest]
                    D['number_of_trades_'+dest]      = self.number_of_trades[dest]
                    D['number_of_event_trade_'+dest] = self.number_of_event_trade[dest]
                    D['mid_price_'+dest]             = self.mid_price[dest] 
                    D['spread_'+dest]                = self.mean_spread[dest]
                    D['number_events_'+dest]         = self.number_of_events[dest]
                    D['number_increase_ask_'+dest]   = self.number_ask_increase[dest]
                    D['number_increase_bid_'+dest]   = self.number_bid_increase[dest]
                    D['number_decrease_ask_'+dest]   = self.number_ask_decrease[dest]
                    D['number_decrease_bid_'+dest]   = self.number_bid_decrease[dest]
                    D['siz_bid_'+dest]               = self.siz_bid[dest]
                    D['siz_ask_'+dest]               = self.siz_ask[dest]
                    #reinitialisation de toutes les varaibles
                    self.number_of_events[dest]      = 0
                    self.bid_increase[dest]          = 0
                    self.ask_increase[dest]          = 0
                    self.ask_decrease[dest]          = 0 
                    self.bid_decrease[dest]          = 0
                    self.number_ask_increase[dest]   = 0
                    self.number_ask_decrease[dest]   = 0
                    self.number_bid_increase[dest]   = 0
                    self.number_bid_decrease[dest]   = 0
                    self.mean_spread [dest]          = 0
                    self.volume_trade[dest]          = 0
                    self.number_of_trades[dest]      = 0
                    self.number_of_event_trade[dest] = 0
                    self.ofi[dest]                   = 0
                    self.number_discover_ask[dest]   = 0
                    self.number_discover_bid[dest]   = 0
                    self.discover_ask[dest]          = 0
                    self.discover_bid [dest]         = 0
                    self.mid_price [dest]            = 0
                    self.siz_bid[dest]               = 0
                    self.siz_ask[dest]               = 0
                    self.disapear_ask [dest]         = 0
                    self.disapear_bid [dest]         = 0
                    self.number_disapear_ask[dest]   = 0
                    self.number_disapear_bid[dest]   = 0
                
                
                D['ztime_end'] = self.t
                D['zdelta_t']  = self.delta_t        
                self.t                               = 0
                self.delta_t                         = 0
                self.min                             = self['number_of_events'] - 1
                
                self.append_indicator(D, self.t) 
                
        else :
            if self.number_of_events[dest_a] >= self['number_of_events']:
                
                self.t = event.getTimeStamp()
                self.delta_t = self.t - self.last_t
                self.last_t = self.t
                for i in range(n if n==1 else n-1):
                    destination_id = destination( mp['VENUE_IDS'][i]) 
                    dest = 'destination%d' %destination_id
                    if c_fd[dest]['BEST_ASK1'] == c_fd[dest_a]['BEST_ASK1']:
                        self.taille_file_ask[dest] = c_fd[dest]['BEST_ASIZ1']
                    if c_fd[dest]['BEST_BID1'] == c_fd[dest_a]['BEST_BID1']:
                        self.taille_file_bid[dest] = c_fd[dest]['BEST_BSIZ1']
                    self.taille_file_ask[dest_a] =   c_fd[dest_a]['BEST_ASIZ1'] 
                    self.taille_file_bid[dest_a] = c_fd[dest]['BEST_BSIZ1'] 
                        
                D = {}
                for i in range(n) :
                    destination_id = destination(mp['VENUE_IDS'][i])
                    dest = 'destination%d' %destination_id
                    D['discover_ask_'+dest]          = self.discover_ask[dest]
                    D['discover_bid_'+dest]          = self.discover_bid[dest]
                    D['disapear_ask_'+dest]          = self.disapear_ask[dest]
                    D['disapear_bid_'+dest]          = self.disapear_bid[dest]
                    D['ofi_'+dest]                   = self.ofi[dest]
                    D['number_discover_bid_'+dest]   = self.number_discover_bid[dest]
                    D['number_discover_ask_'+dest]   = self.number_discover_ask[dest]
                    D['number_disapear_ask_'+dest]   = self.number_disapear_ask[dest]
                    D['number_disapear_bid_'+dest]   = self.number_disapear_bid[dest]
                    D['icrease_ask_'+dest]           = self.ask_increase[dest]
                    D['decrease_ask_'+dest]         = self.ask_decrease[dest]
                    D['icrease_bid_'+dest]           = self.bid_increase[dest]
                    D['decrease_bid_'+dest]         = self.bid_decrease[dest]
                    D['volume_trade_'+dest]          = self.volume_trade[dest]
                    D['number_of_trades_'+dest]      = self.number_of_trades[dest]
                    D['number_of_event_trade_'+dest] = self.number_of_event_trade[dest]
                    D['mid_price_'+dest]             = self.mid_price[dest] 
                    D['spread_'+dest]                = self.mean_spread[dest]
                    D['number_events_'+dest]         = self.number_of_events[dest]
                    D['number_increase_ask_'+dest]   = self.number_ask_increase[dest]
                    D['number_increase_bid_'+dest]   = self.number_bid_increase[dest]
                    D['number_decrease_ask_'+dest]   = self.number_ask_decrease[dest]
                    D['number_decrease_bid_'+dest]   = self.number_bid_decrease[dest]
                    D['siz_bid_'+dest]               = self.siz_bid[dest]
                    D['siz_ask_'+dest]               = self.siz_ask[dest]
                    D['zsiz_bid_instantane_'+dest]    = self.taille_file_bid[dest]
                    D['zsiz_ask_instantane_'+dest]    = self.taille_file_ask[dest]
                    #reinitialisation de toutes les varaibles
                    self.number_of_events[dest]      = 0
                    self.bid_increase[dest]          = 0
                    self.ask_increase[dest]          = 0
                    self.ask_decrease[dest]          = 0
                    self.bid_decrease[dest]          = 0
                    self.number_ask_increase[dest]   = 0
                    self.number_ask_decrease[dest]   = 0
                    self.number_bid_increase[dest]   = 0
                    self.number_bid_decrease[dest]   = 0
                    self.mean_spread [dest]          = 0
                    self.volume_trade[dest]          = 0
                    self.number_of_trades[dest]      = 0
                    self.number_of_event_trade[dest] = 0
                    self.ofi[dest]                   = 0
                    self.number_discover_ask[dest]   = 0
                    self.number_discover_bid[dest]   = 0
                    self.discover_ask[dest]          = 0
                    self.discover_bid [dest]         = 0
                    self.mid_price [dest]            = 0
                    self.siz_bid[dest]               = 0
                    self.siz_ask[dest]               = 0
                    self.disapear_ask [dest]         = 0
                    self.disapear_bid [dest]         = 0
                    self.number_disapear_ask[dest]   = 0
                    self.number_disapear_bid[dest]   = 0
                    self.taille_file_ask [dest]      = 0
                    self.taille_file_bid[dest]       = 0
                        
                    
                for i in range(n if n==1 else n-1) :
                    destination_id                   = destination(mp['VENUE_IDS'][i])
                    dest                             = 'destination%d' %destination_id
                    dest                             = 'mono_'+dest
                    D['discover_ask_'+dest]          = self.discover_ask[dest]
                    D['discover_bid_'+dest]          = self.discover_bid[dest]
                    D['disapear_ask_'+dest]          = self.disapear_ask[dest]
                    D['disapear_bid_'+dest]          = self.disapear_bid[dest]
                    D['ofi_'+dest]                   = self.ofi[dest]
                    D['number_discover_bid_'+dest]   = self.number_discover_bid[dest]
                    D['number_discover_ask_'+dest]   = self.number_discover_ask[dest]
                    D['number_disapear_ask_'+dest]   = self.number_disapear_ask[dest]
                    D['number_disapear_bid_'+dest]   = self.number_disapear_bid[dest]
                    D['icrease_ask_'+dest]           = self.ask_increase[dest]
                    D['decrease_ask_'+dest]         = self.ask_decrease[dest]
                    D['icrease_bid_'+dest]           = self.bid_increase[dest]
                    D['decrease_bid_'+dest]         = self.bid_decrease[dest]
                    D['volume_trade_'+dest]          = self.volume_trade[dest]
                    D['number_of_trades_'+dest]      = self.number_of_trades[dest]
                    D['number_of_event_trade_'+dest] = self.number_of_event_trade[dest]
                    D['mid_price_'+dest]             = self.mid_price[dest] 
                    D['spread_'+dest]                = self.mean_spread[dest]
                    D['number_events_'+dest]         = self.number_of_events[dest]
                    D['number_increase_ask_'+dest]   = self.number_ask_increase[dest]
                    D['number_increase_bid_'+dest]   = self.number_bid_increase[dest]
                    D['number_decrease_ask_'+dest]   = self.number_ask_decrease[dest]
                    D['number_decrease_bid_'+dest]   = self.number_bid_decrease[dest]
                    D['siz_bid_'+dest]               = self.siz_bid[dest]
                    D['siz_ask_'+dest]               = self.siz_ask[dest]
                    #reinitialisation de toutes les varaibles
                    self.number_of_events[dest]      = 0
                    self.bid_increase[dest]          = 0
                    self.ask_increase[dest]          = 0
                    self.ask_decrease[dest]          = 0
                    self.bid_decrease[dest]          = 0
                    self.number_ask_increase[dest]   = 0
                    self.number_ask_decrease[dest]   = 0
                    self.number_bid_increase[dest]   = 0
                    self.number_bid_decrease[dest]   = 0
                    self.mean_spread [dest]          = 0
                    self.volume_trade[dest]          = 0
                    self.number_of_trades[dest]      = 0
                    self.number_of_event_trade[dest] = 0
                    self.ofi[dest]                   = 0
                    self.number_discover_ask[dest]   = 0
                    self.number_discover_bid[dest]   = 0
                    self.discover_ask[dest]          = 0
                    self.discover_bid [dest]         = 0
                    self.mid_price [dest]            = 0
                    self.siz_bid[dest]               = 0
                    self.siz_ask[dest]               = 0
                    self.disapear_ask [dest]         = 0
                    self.disapear_bid [dest]         = 0
                    self.number_disapear_ask[dest]   = 0
                    self.number_disapear_bid[dest]   = 0
                        
                D['ztime_end'] = self.t
                D['zdelta_t']  = self.delta_t        
                self.t                               = 0
                self.delta_t                         = 0
                
                self.append_indicator(D, self.t) 
            
                    
                    
        