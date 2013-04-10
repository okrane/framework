'''
Created on 04/07/2012

@author: Julia Nie
'''


from simep.core.baseagent import BaseAgent
from math import pow, log, sqrt
#from simep.funcs.data.pyData import pyData

EPSILON = 1e-8

class VolatilityObserver(BaseAgent):    
    
    @staticmethod
    def public_parameters():
        parameters = {'num_trades' : {'label' : 'number of trades for Garman-Klass volatility', 'value' : 1000},
                      'num_spreads' : {'label' : 'number of observed spreads', 'value' : 60}}
        return {'setup':{}, 'parameters' : parameters}
    
    
    @staticmethod
    def indicators_list():
        return []
    
    
    def __init__(self, setup, context, parameters, trace):
        '''
        constructor
        '''
        '''
        First, we have to call the base class constructor, i.e. BaseAgent
        '''
        BaseAgent.__init__(self, setup, context, parameters, trace)
   
        '''
        Then, we have to set two properties of the agent :
        -> self.needExecReportEvt permits to enable/disable the callback to the function 'processReport'
        -> self.reactOnEvent      permits to call the 'process' callback function every time there is an update
                                  on the market. If you set it to 'False', then you have to control the parameter
                                  self.time2wakeup to have your agent waked up with the process function during 
                                  the day. 
        self.securityIdEventsOnly permits to make sure that the process function will be called only with market 
                                  events of the same security_id as our agent
        '''
        self.needExecReportEvt = True
        self.reactOnEvent = True
        self.securityIdEventsOnly = True     

        #parameter @num_trades: number of trades used for computing volatility
        self.num_trades = parameters['num_trades']
        #parameter @num_spreads: number of spreads
        self.num_spreads = parameters['num_spreads']
        
        #output @volatility: Garman-Klass volatility for @num_trades trades
        self.volatility = None
        #@output @normalized_volatility: normalized volatility in basis point per 10 minutes
        self.normalized_volatility = None
        #list of trading time
        self.times = []
        #list of trading price
        self.prices = []        
        
     
    def compute_gk_volatility(self, garman_klass):
        '''
        ompute the Garman-Klass volatility using open, close, high, low price with 'num_trades' trades
        '''
        open_px = garman_klass['open_price']
        close_px = garman_klass ['close_price']
        high_px = garman_klass['high_price']
        low_px = garman_klass['low_price']
        
        if open_px > EPSILON:
            tmp1 = pow((high_px - low_px), 2) / 2
            tmp2 = (2 * log(2)  - 1) * pow((close_px - open_px), 2)
            tmp1 -= tmp2
            tmp2 = (close_px + open_px) / 2
            return sqrt(tmp1) / tmp2
        else:
            return None


    def compute_normalized_volatility(self, volatility, dt):  
        '''
        compute the normalized Garman-Klass volatility in basis point per 10 minutes (bp/10min)
        ''' 
        if volatility != None and dt > EPSILON:
            return 10000 * volatility * sqrt(10 / dt) 
        else:
            return None
                      
              
    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''  
    def process(self, event):
        try:
            ''' self.update(event) function returns :
            -> -20 : IDLE event (event with no data)
            -> -10 : the event doesn't concern us (other venue)
            -> -5  : the event comes from a mid point dark venue (phase change)
            -> -1  : there is a problem (no time, or we are out of the time limits) : don't do anything
            ->  0  : there is no bid or no ask right now : cancel everythin you have placed !
            ->  1  : everything seems fine, do what you want to do
            '''
            if self.update(event) == 1:      
                venue_id = event.venueId
                feed = self.ba['feed'][venue_id]
                
                #If the latest event contain lit trades, update open/close/high/low price and volatility
                if feed['LIT_TRADE_EVENT']:
                       
                    #computation method 1:        
                    #the number of trades from opening 
                    num_moves = feed['LIT_NUM_MOVES']             
                    trades_times = self.times
                    trades_prices = self.prices
                    for i in feed['LIT_LAST_DEALS_IDXS']:
                        trades_times.append(feed['LIT_DEALS_TIMES'][i])
                        trades_prices.append(feed['LIT_DEALS_PRICES'][i])
                        if num_moves > self.num_trades:
                            trades_times.pop(0)
                            trades_prices.pop(0)       
                              
                    if num_moves < self.num_trades:
                        return                
                    
                    '''
                    #computation method 2:
                    for i in feed['LIT_LAST_DEALS_IDXS']:
                        time = feed['LIT_DEALS_TIMES'][i]
                        price = feed['LIT_DEALS_PRICES'][i]
                        if time > 0L and price > EPSILON:
                            self.times.append(time)
                            self.prices.append(price) 
                   
                    trades_times = self.times[-self.num_trades:]                     
                    trades_prices = self.prices[-self.num_trades:]  
                    #print trades_times                                     
                    #print trades_prices   
                    
                    if len(trades_times) < self.num_trades:
                        return
                    '''
                    
                    gk = {}
                    #set the open/close/high/low trading price
                    gk['open_price'] = trades_prices[0]
                    gk['close_price'] = trades_prices[-1]
                    gk['high_price'] = max(trades_prices)
                    gk['low_price'] = min(trades_prices)
                    #set the open and close time  
                    gk['open_time'] = trades_times[0]
                    gk['close_time'] = trades_times[-1]
                    
                    #compute the Karman-Klass volatility from open/close/high/low price
                    self.volatility = self.compute_gk_volatility(gk)
                    
                    #compute the normalized Karman-Klass volatility
                    dt = float(gk['close_time'] - gk['open_time']) / (6e+7)
                    self.normalized_volatility = self.compute_normalized_volatility(self.volatility, dt)
                    
                    '''
                    #print volatility, normalized_volatility
                    print 'open_time: ', gk['open_time'], 'close_time: ', gk['close_time']
                    print 'open_price: ', gk['open_price'], 'close_price: ', gk['close_price'], 'high_price: ', gk['high_price'], 'low_price: ', gk['low_price']
                    print 'Volatility: ', self.volatility, 'Normalized Volatility: ', self.normalized_volatility
                    '''
                    #date = event.getEvtTime()
                    self.append_indicator({'open_time': gk['open_time'], 'close_time': gk['close_time'],
                                            'open_price': gk['open_price'], 'close_price': gk['close_price'], 
                                            'high_price': gk['high_price'], 'low_price': gk['low_price'], 
                                            'volatility_GK': [self.volatility], 'volatility_normalized': [self.normalized_volatility]},
                                          event.timestamp)

        except:
            import sys, traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout) 
    
