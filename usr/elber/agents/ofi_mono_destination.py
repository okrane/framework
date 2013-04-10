'''
Created on 17 juin 2011

@author: elber
'''

from simep.core.baseagent import BaseAgent
from simep.tools import date2num



class MonoDestinationOfi(BaseAgent):
    
    
    
    '''######################################################################################################
    ##########################################   STATIC METHODS   ###########################################
    ######################################################################################################'''
    
    @staticmethod
    def public_parameters():
        parameters = {'number_of_events' : {'label' : 'number of events', 'value'  : 100},
                      'start_time'       : {'label' : 'time to start', 'value' : '08:00:00:000000'}}
        return {'setup': {}, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return []
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, parameters, trace):
        super(MonoDestinationOfi, self).__init__(setup, context, parameters, trace)
        self['start_time']        = date2num(self['start_time'])
        self.needExecReportEvt    = False        
        self.needAllEvts          = True
        self.securityIdEventsOnly = True
        ba                        = self.ba
        ba['mvrs'] = {'VENUE_ID'    : self.ba['venue_ids'][0] if len(self.ba['venue_ids']) == 1 else 'all',
                      'tick_size'   : 0.01}
        
         
        
        self.number_of_events  = 0
        self.bid_increase = 0
        self.ask_increase = 0
        self.ask_decrease = 0
        self.bid_decrease = 0
        self.number_ask_increase=0
        self.number_ask_decrease =0
        self.number_bid_increase = 0
        self.number_bid_decrease=0
        self.mean_spread  = 0
        self.volume_trade = 0
        self.number_of_trades = 0
        self.number_of_event_trade = 0
        self.ofi = 0
        self.number_discover_ask = 0
        self.number_discover_bid =0
        self.discover_ask = 0
        self.discover_bid = 0
        self.mid_price = 0
        self.number_all_events= 0
        self.siz_bid = 0
        self.siz_ask = 0
        self.t_initial = 0
        self.delta_t = 0
        self.delta_p = 0
        self.p_final  = 0
        self.p_initial = 0
        
        
        
    

    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
        
    def process(self, event):
        if self.update(event) <= 0:
            return False
        #self.print_orderbooks()
        if event.getTimeStamp()>= self['start_time'] :
            ba       = self.ba
            mp       = ba['mvrs']
            # fd c est le feed
            fd       = ba['feed'][mp['VENUE_ID']]
            # c_fd current feed p_f : previous feed
            c_fd     = fd[0]
            p_fd     = fd[1]
    
            pnb = c_fd['BEST_BID1']
            pnb0 = p_fd['BEST_BID1']
            pna = c_fd['BEST_ASK1']
            pna0 = p_fd['BEST_ASK1']
            qnb = c_fd['BEST_BSIZ1']
            qnb0 = p_fd['BEST_BSIZ1']
            qna  = c_fd['BEST_ASIZ1']
            qna0 = p_fd['BEST_ASIZ1']
            tmp = (pnb>=pnb0)*qnb - (pnb<=pnb0)*qnb0 - (pna<=pna0)*qna + (pna>=pna0)*qna0
            self.number_all_events +=1
            #cette condition veut dire qu'il y a eu une modif sur au moins un des best
            if  (pna!=pna0 or qna!=qna0) or (pnb!=pnb0 or qnb!=qnb0): 
                self.ofi += tmp
                self.mean_spread += c_fd['SPREAD']
                self.number_of_events +=1
                self.mid_price += (pnb +pna)/2 
                self.siz_bid += qnb
                self.siz_ask +=qna 
            tmp_bid = (pnb>=pnb0)*qnb - (pnb<=pnb0)*qnb0
            if (pnb>pnb0)or (pnb == pnb0 and qnb>qnb0) :
                self.bid_increase += tmp_bid
                self.number_bid_increase +=1  
            if (pnb<pnb0) or (qnb<qnb0 and pnb==pnb0) :
                self.bid_decrease += tmp_bid 
                self.number_bid_decrease +=1 
            
            tmp_ask =  (pna<=pna0)*qna - (pna>=pna0)*qna0   
            if (pna>pna0 or(pna==pna0 and qna<qna0)) :
                self.ask_decrease += tmp_ask   
                self.number_ask_decrease +=1
            if (pna<pna0 or(pna == pna0 and qna>qna0)) :
                self.ask_increase += tmp_ask  
                self.number_ask_increase +=1
            # calcul des discovers    
            if pnb<pnb0 :
                    self.discover_bid += qnb
                    self.number_discover_bid += 1
            if pna>pna0 :
                    self.discover_ask += qna
                    self.number_discover_ask +=1
            
            if self.number_of_events == 1 :
                self.t_initial = event.getTimeStamp()
                self.p_initial = (pna + pnb)/2
            
            if  c_fd['TRADE_EVENT']:
                venue_id = event.getVenueId()
                trades = self.marketManager.getTrades(venue_id)
                if len(trades) != 0:
                    self.volume_trade +=c_fd['LAST_TRDS_GROUP']['VOLUME']
                    self.number_of_event_trade += 1
                    self.number_of_trades += len(trades)
                            
            if self.number_of_events == self['number_of_events']:
                self.t_end = event.getTimeStamp()
                self.p_final = (pna + pnb)/2
                self.delta_t = self.t_end - self.t_initial
                self.delta_p = self.p_final - self.p_initial
                D = {}
                D['delta_price'] = self.delta_p
                D['ofi']= self.ofi
                D['number_trades'] = self.number_of_trades
                D['number_events_trade']= self.number_of_event_trade
                D['volume_trade'] = self.volume_trade
                D['number_all_events'] = self.number_all_events
                D['spread']            = self.mean_spread /self['number_of_events']
                D['discover_bid'] = self.discover_bid
                D['discover_ask'] = self.discover_ask
                D['number_discover_ask']= self.number_discover_ask
                D['number_discover_bid'] = self.number_discover_bid
                D['mid_price'] = self.mid_price/self['number_of_events']
                D['number_ask_decrease']=self.number_ask_decrease
                D['number_ask_increase'] = self.number_ask_increase
                D['number_bid_increase'] = self.number_bid_increase
                D['number_bid_decrease'] = self.number_bid_decrease
                D['bid_decrease'] = self.bid_decrease
                D['bid_increase'] = self.bid_increase
                D['ask_decrease'] = self.ask_decrease
                D['ask_increase'] = self.ask_increase
                D['delta_t']      = self.delta_t
                D['siz_ask']      = self.siz_ask
                D['siz_bid']      = self.siz_bid
                
                self.append_indicator(D,self.t_end)
                
                #re initialisation des varaibles 
                self.ofi = 0
                self.number_of_event_trade = 0
                self.number_of_events = 0
                self.number_of_trades = 0
                self.mean_spread      = 0
                self.number_all_events = 0
                self.number_discover_ask = 0
                self.number_discover_bid =0
                self.discover_ask = 0
                self.discover_bid = 0
                self.mid_price  = 0
                self.bid_increase = 0
                self.ask_increase = 0
                self.ask_decrease = 0
                self.bid_decrease = 0
                self.number_ask_increase=0
                self.number_ask_decrease =0
                self.number_bid_increase = 0
                self.number_bid_decrease=0
                self.volume_trade = 0
                self.siz_bid = 0
                self.siz_ask = 0
                self.t_initial = 0
                self.delta_t = 0
                self.delta_p = 0
                self.p_final  = 0
                self.p_initial = 0


            
            
        
    def last_process(self):
        print 'finish'
        