from simep.core.baseagent import BaseAgent
from simep.sched import Order
from simep.funcs.data.pyData import pyData

import math
import scipy as sp
import numpy as np
import random

class Observer(BaseAgent):
    '''
    classdocs
    '''
    @staticmethod
    
    def public_parameters():
        setup      = {'name'         : {'label' : 'Name'             , 'value' : 'Fill001'}}
        parameters = {'side'         : {'label' : 'Side'             , 'value' : Order.Buy}}
        return {'setup': setup, 'parameters': parameters}

    def __init__(self, setup, context, parameters, trace):
        BaseAgent.__init__(self, setup, context, parameters, trace)
        
    def process(self, evt):
        code = self.update(evt)
        if code <= 0:
            return False       
        feed = self.marketManager.getFeedInfo(evt.getVenueId())
        if feed['VOLUME'] != 0:
            if self['side'] == Order.Buy:
                if not feed['TRADE_EVENT']: 
                    self.append_indicator({'Price_bid1': feed['BEST_BID1'],
										'Volume_bid1': feed['BEST_BSIZ1'],
                                        'Price_bid2': feed['BEST_BID2'],
                                        'Volume_bid2': feed['BEST_BSIZ2'],
                                        'Price_bid3': feed['BEST_BID3'],
                                        'Volume_bid3': feed['BEST_BSIZ3'],
                                        'Price_bid4': feed['BEST_BID4'],
                                        'Volume_bid4': feed['BEST_BSIZ4'],
                                        'No_bid1': feed['NO_BIDMMKR'],
                                        'No_ask1': feed['NO_ASKMMKR'],
                                        'Trade_event': feed['TRADE_EVENT'],
										'Price_ask1': feed['BEST_ASK1'],
                                        'Volume_ask1': feed['BEST_ASIZ1'],
                                        'Price_ask2': feed['BEST_ASK2'],
                                        'Volume_ask2': feed['BEST_ASIZ2'],
                                        'Price_ask3': feed['BEST_ASK3'],
                                        'Volume_ask3': feed['BEST_ASIZ3'],
                                        'Price_ask4': feed['BEST_ASK4'],
                                        'Volume_ask4': feed['BEST_ASIZ4'],
                                        'Time_num':feed['TIME_NUM']}, 
                                        timestamp = feed['TIME_NUM'])
                else:
                    for i in feed['LAST_DEALS_IDXS']:
                        self.append_indicator({'Price_bid1': feed['BEST_BID1'],
                                        'Volume_bid1': feed['BEST_BSIZ1'],
                                        'Price_bid2': feed['BEST_BID2'],
                                        'Volume_bid2': feed['BEST_BSIZ2'],
                                        'Price_bid3': feed['BEST_BID3'],
                                        'Volume_bid3': feed['BEST_BSIZ3'],
                                        'Price_bid4': feed['BEST_BID4'],
                                        'Volume_bid4': feed['BEST_BSIZ4'],
                                        'No_bid1': feed['NO_BIDMMKR'],
                                        'No_ask1': feed['NO_ASKMMKR'],
                                        'Trade_event': feed['TRADE_EVENT'],
                                        'Price_ask1': feed['BEST_ASK1'],
                                        'Volume_ask1': feed['BEST_ASIZ1'],
                                        'Price_ask2': feed['BEST_ASK2'],
                                        'Volume_ask2': feed['BEST_ASIZ2'],
                                        'Price_ask3': feed['BEST_ASK3'],
                                        'Volume_ask3': feed['BEST_ASIZ3'],
                                        'Price_ask4': feed['BEST_ASK4'],
                                        'Volume_ask4': feed['BEST_ASIZ4'],
                                        'Time_num':feed['TIME_NUM'],
                                        'Last_trade_side': feed['DEALS_SIDES'][i],
                                        'Last_trade_qty': feed['DEALS_QTYS'][i],
                                        'Last_trade_price': feed['DEALS_PRICES'][i],
                                        'Trade_num': feed['LAST_TRDS_GROUP']['NUM_MOVES'],
                                        'Trade_type': feed['DEALS_TYPES'][i] },
                                        timestamp = feed['TIME_NUM'])
            elif self['side'] == Order.Sell:
                self.append_indicator({'Price_ask': feed['BEST_ASK']}, timestamp = feed['TIME_NUM'])
        
    def processReport(self, evt):  
        feed = self.marketManager.getFeedInfo(evt.getVenueId())       
        self.append_indicator({'size': evt.size, 
                               'price': evt.price}, 
                               timestamp = feed['TIME_NUM'])
        
                                                   
