'''
Created on 7 avr. 2011

@author: elber
'''
''' this script help us to calculate the estimators of the Zero intelligence model '''
import os
from simep.core.baseagent import BaseAgent
from collections import defaultdict
from matplotlib.pylab import *
from matplotlib.pyplot import *
from numpy import *
from string import *
from scipy.io import *
import scipy.io.matlab.mio
from simep.core.baseagent import BaseAgent
from simep.tools import date2num

class parametersOfZIMEstimators(BaseAgent):
    """Basic observer in python"""
    
    
    @staticmethod
    def public_parameters():
        parameters = {'time_step'               :{'label': 'time step'              , 'value' : '00:01:00:000000'},
                      'use_time_discretization' :{'label': 'use time discretization', 'value' : False},
                      'list_length'             :{'label': 'lists length'           , 'value' :  10              }}
        return {'setup': {}, 'parameters': parameters}

    def __init__(self, setup, context, params, trace):
        BaseAgent.__init__(self, setup, context, params,trace)
        self.needAllEvts            = True
        self.allTrades              = defaultdict(list)
        self.nbTrades               = 0
        self['time_step']           = date2num(self['time_step'])
        self.venue_id               =  self.ba['venue_ids'][0]
        
        #params['time_step'] = date2num(params['time_step'])
        self.feed                   = self.ba['feed']
        self.start_time             = self.feed[self.venue_id][0]['start_time']          
        self.end_time               = self.feed[self.venue_id][0]['end_time']  
        self.number_time_steps      = float((self.end_time - self.start_time))/self['time_step']
        #market order distribution
        self.list_trades_sizes      =  []
        self.number_of_trades       = 0
        #ask limit order distribution
        # in this we dont use time_step
        self.ask_sizes              =  [0 for i in range(self['list_length'])]
        #in this we use time step
        self.list_ask_sizes         = zeros(shape=(self.number_time_steps,self['list_length']))  
        # bid limit order distribution
        #in this we dont use time step
        self.bid_sizes              =  [0 for i in range(self['list_length'])]
        #in this we use time step
        self.list_bid_sizes         = zeros(shape=(self.number_time_steps,self['list_length']))  

        #get list of tick_sizes
        self.tick_sizes             = self.feed[self.venue_id][0]['tick_sizes']
        self.tick_size              = 0.01
        
    def process(self,evt):
        # check eventType
        if self.update(evt) <= 0:
            return False
        
       
                    
        # the prices and sizes before the event
        best_pasizesb               = self.feed[self.venue_id][0]['BEST_ASK'] 
        best_pbsizesb               = self.feed[self.venue_id][0]['BEST_BID'] 
        best_asizesb                = self.feed[self.venue_id][0]['BEST_ASIZ'] 
        best_bsizesb                = self.feed[self.venue_id][0]['BEST_BSIZ'] 
        # the prices and sizes after the event
        best_pasizesa               = self.feed[self.venue_id][1]['BEST_ASK'] 
        best_pbsizesa               = self.feed[self.venue_id][1]['BEST_BID'] 
        best_asizesa                = self.feed[self.venue_id][1]['BEST_ASIZ'] 
        best_bsizesa                = self.feed[self.venue_id][1]['BEST_BSIZ']
        
        
        t                        = evt.getTimeStamp()
        venue_id                 = evt.getVenueId()
        feed                     = self.marketManager.getFeedInfo(venue_id)
        idx                      = int((t - self.start_time)/ self['time_step'])
        if self['use_time_discretization'] :
            
            if self.feed[self.venue_id][0]['TRADE_EVENT']:
                while len(self.list_trades_sizes) < idx+1:
                    self.list_trades_sizes.append(0)
                self.list_trades_sizes[idx] += feed['LAST_TRDS_GROUP']['VOLUME']
                self.list_number_of_trades[idx] += feed['LAST_TRDS_GROUP']['NUM_MOVES']

            else :        
                for i in range(5):
                    if best_pbsizesa[i]!=None and best_pasizesa[0] !=None :
                        # get the tick_size
                        if self.tick_sizes != [] :
                            if (best_pbsizesa[i]< self.tick_sizes[1][0]):
                                self.tick_size = self.tick_sizes [0][1]
                            elif (best_pbsizesa[i]< self.tick_sizes[2][0]):
                                self.tick_size = self.tick_sizes [1][1]
                            elif (best_pbsizesa[i]< self.tick_sizes[3][0]):
                                self.tick_size = self.tick_sizes [2][1]
                            else :
                                self.tick_size = self.tick_sizes [3][1] 
                                     
                        idx1 = int(((best_pasizesa[0] - best_pbsizesa[i]))/ self.tick_size)
                        if idx1 < self['list_length'] and idx < self.number_time_steps:
                            self.list_bid_sizes[idx,idx1] += max(best_bsizesa[i] - best_bsizesb[i] , 0)
     
                for i in range(5):
                    if best_pasizesa[i]!=None and best_pbsizesa[0] !=None :
                        # get the tick_size
                        if self.tick_sizes != [] :
                            if (best_pasizesa[i]< self.tick_sizes[1][0]):
                                self.tick_size = self.tick_sizes [0][1]
                            elif (best_pasizesa[i]< self.tick_sizes[2][0]):
                                self.tick_size = self.tick_sizes [1][1]
                            elif (best_pasizesa[i]< self.tick_sizes[3][0]):
                                self.tick_size = self.tick_sizes [2][1]
                            else :
                                self.tick_size = self.tick_sizes [3][1] 
                                     
                        idx2 = int(((best_pasizesa[i] - best_pbsizesa[0]))/ self.tick_size)
                        if idx2 < self['list_length'] and idx < self.number_time_steps :
                            self.list_ask_sizes [idx,idx2] += max(best_asizesa[i] - best_asizesb[i] , 0)
        else :  
            self.number_of_trades += feed['LAST_TRDS_GROUP']['NUM_MOVES']          
            
            for i in range(5):
                if best_pbsizesa[i]!=None and best_pasizesa[0] !=None :
                    # get the tick_size
                    if self.tick_sizes != [] :
                        if (best_pbsizesa[i]< self.tick_sizes[1][0]):
                            self.tick_size = self.tick_sizes [0][1]
                        elif (best_pbsizesa[i]< self.tick_sizes[2][0]):
                            self.tick_size = self.tick_sizes [1][1]
                        elif (best_pbsizesa[i]< self.tick_sizes[3][0]):
                            self.tick_size = self.tick_sizes [2][1]
                        else :
                            self.tick_size = self.tick_sizes [3][1] 
                                     
                    idx1 = int(((best_pasizesa[0] - best_pbsizesa[i]))/ self.tick_size)
                    if idx1 < self['list_length'] and idx1>= 0 :
                        self.bid_sizes[idx1] += max(best_bsizesa[i] - best_bsizesb[i] , 0)
     
           
            for i in range(5):
                if best_pasizesa[i]!=None and best_pbsizesa[0] !=None :
                    # get the tick_size
                    if self.tick_sizes != [] :
                        if (best_pbsizesa[i]< self.tick_sizes[1][0]):
                            self.tick_size = self.tick_sizes [0][1]
                        elif (best_pbsizesa[i]< self.tick_sizes[2][0]):
                            self.tick_size = self.tick_sizes [1][1]
                        elif (best_pbsizesa[i]< self.tick_sizes[3][0]):
                            self.tick_size = self.tick_sizes [2][1]
                        else :
                            self.tick_size = self.tick_sizes [3][1] 
                                     
                    idx1 = int(((best_pasizesa[i] - best_pbsizesa[0]))/ self.tick_size)
                    if idx1 < self['list_length'] and idx1>= 0 :
                        self.ask_sizes[idx1] += max(best_asizesa[i] - best_asizesb[i] , 0)    
                
                            
            
            
            
        #pour recuperer directement le volume du trade
        self.list_deals_qtys = self.feed[self.venue_id][1]['DEALS_QTYS']
        self.size_trades     =self.feed[self.venue_id][1]['VOLUME']
    def last_process(self):
        print 'number of trades', float(self.number_of_trades)/(self.end_time - self.start_time)
        self.append_to_m_file('C:/matlab_results/mu.m', {'number_trades_by_day' : float(self.number_of_trades)/(self.end_time - self.start_time)})
        # to be cheked
        self.append_to_m_file('C:/matlab_results/sigma_trade.m', {'sigma_trade' : float(self.size_trades)/(self.end_time - self.start_time)})
        
        #self.append_to_m_file('C:/matlab_results/volume.m', {'volumetrade_daily' : self.number_of_trades})                                                 
        
        #self.append_to_m_file('C:/rendement_obs.m', {'vwap_daily' : feed['VWAP'],
        #                                            'volume'     : feed['VOLUME']})
                                        
        sum_size_ask   = float(sum(self.ask_sizes))
        sum_size_trade =  float(self.size_trades)
        # if self['use_time_discretization']:
        if sum_size_ask != 0 :
            
            sum_size_ask   = float(sum(self.ask_sizes))
            sum_size_bid   = float(sum(self.bid_sizes))
            aux2 = []
            aux3 = aux2
            
            for size in self.ask_sizes :
                aux2.append(float(size)/sum_size_ask)
            self.ask_sizes = aux2
            for size in self.bid_sizes :
                aux3.append(float(size)/sum_size_bid)
            self.bid_sizes = aux3
#        start_time  = self.feed[self.venue_id][0]['start_time']          
#        end_time    = self.feed[self.venue_id][0]['end_time']  
#        time        = long(end_time - start_time)
#        print 'time',time
#        print 'size trade', sum_size_trade
#        alpha       = float(sum_size_trade) / time
#        print 'alpha', alpha
        #=======================================================================
        # fichier = "C:/matlab_results/tradesize_distribution.mat"
        # data = {}
        # data['trade_distribution'] = self.list_trades_sizes
        # savemat(fichier, data, appendmat=True, format='5', long_field_names=False, do_compression=False, oned_as=None)
        # 
        #=======================================================================
            fichier1 = "C:/matlab_results/askLOsize_distribution.mat"
            data = {}
            data['asksize_LO_distribution'] = self.ask_sizes
            savemat(fichier1, data, appendmat=True, format='5', long_field_names=False, do_compression=False, oned_as='row')
            
            fichier2 = "C:/matlab_results/bidsize_LO_distribution.mat"
            data = {}
            data['bidsize_LO_distribution'] = self.bid_sizes
            savemat(fichier2, data, appendmat=True, format='5', long_field_names=False, do_compression=False, oned_as='row')
            time = (self.end_time - self.start_time)
            mu = sum_size_trade / time
            alpha_ask = mean(self.ask_sizes) * sum_size_ask / time
            alpha_bid = mean(self.bid_sizes) * sum_size_bid / time
            alpha     = (alpha_bid+alpha_ask)/2
            print 'mu',mu
            print 'alpha', alpha
            
        else :
            sum_size_ask   = float(sum(self.list_ask_sizes))
            sum_size_bid   = float(sum(self.list_bid_sizes))
            
            for i in range(self.number_time_steps) :
                for j in range(self['list_length']) :
                    self.list_ask_sizes[i,j] = float(self.list_ask_sizes[i,j])/sum_size_ask
                    self.list_bid_sizes[i,j] = float(self.list_bid_sizes[i,j])/sum_size_bid
            fichier  =  "C:/matlab_results/tradesizes_distribution.mat" 
            data = {}
            data['tradesizes_distribution'] = self.list_trades_sizes
            savemat(fichier, data, appendmat=True, format='5', long_field_names=False, do_compression=False, oned_as='row') 
                
            fichier1 = "C:/matlab_results/askLOsize_distribution.mat"
            data = {}
            data['asksize_LO_distribution'] = self.list_ask_sizes
            savemat(fichier1, data, appendmat=True, format='5', long_field_names=False, do_compression=False, oned_as='row')
            
            fichier2 = "C:/matlab_results/bidsize_LO_distribution.mat"
            data = {}
            data['bidsize_LO_distribution'] = self.list_bid_sizes
            savemat(fichier2, data, appendmat=True, format='5', long_field_names=False, do_compression=False, oned_as='row')