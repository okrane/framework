'''
Created on 8 sept. 2010

@author: benca
'''

import sys
import os
from simep import __stsim_directory__
sys.path.append(os.path.dirname(__stsim_directory__))
from AlgHoster.lib.pydelegate.scripts.simep.event import Event, MoneyManager
from AlgHoster.lib.pydelegate.scripts.simep.marketmanager import MarketManager, Trade, Order

from simep.core.baseobserver import BaseObserver
from usr.dev.benca.agents.readordersvalidator import ReadOrdersObserver
from simep.funcs.stdio.utils import pyLog
from simep.bin.simepcore import dateNum
from usr.dev.benca.funcs.stdio.orders_loader import OrdersLoader



class BufferizedMarketManager(MarketManager, object):
    
    def __init__(self):
        super(BufferizedMarketManager, self).__init__()
        self.__bufferized_trades = list()
        self.__inicators = {}
    
    def getBufferizedTrades(self):
        return self.__bufferized_trades
    
    def addBufferizedTrade(self, trade):
        self.__bufferized_trades.append(trade)
    
    def clearBufferizedTrades(self):
        del(self.__bufferized_trades)
        self.__bufferized_trades = []
    
    def setIndicators(self, indicators):
        self.__indicators = indicators.copy()
    
    def getIndicators(self):
        return self.__indicators



class OrdersReplayer(BaseObserver):
    
    @staticmethod
    def public_parameters():
        setup      = {'name'           : {'label' : 'Name'                    , 'value' : 'OrdersReplayer001'}}
        parameters = {'delta_t'        : {'label' : 'Delta Time'              , 'value' : '00:00:01:000'},
                      'cmd_filename'   : {'label' : 'Orders Commands Filename', 'value' : 'C:/st_sim/dev/usr/benca/data/detail_occ_3_Jn}0026.txt'},
                      'loading_method' : {'label' : 'Method To Load Orders'   , 'value' : 'headed_text_file'}}
        return {'setup': setup, 'parameters': parameters}
    
    @staticmethod
    def indicators_list():
        return ['avg_spread_bp_t_60', 'avg_trade_size_t_60', 'vwavg_price_m_t_60']
    
    
    
    '''######################################################################################################
    ############################################   CONSTRUCTOR   ############################################
    ######################################################################################################'''
    
    def __init__(self, setup, context, params, trace):
        self._figure_title = '[date=' + context['date'] + ', ric=' + str(context['ric']) + ']'
        params['delta_t']      = dateNum(params['delta_t'])
        super(OrdersReplayer, self).__init__(setup, context, params, trace)
        tmp                    = OrdersLoader.load_orders(self['loading_method'], self['cmd_filename'])
        self._cmd_pydata       = tmp[0]
        self._header           = tmp[1]
        self._cmd_index        = 0
        self._algo_did_actions = False
        self._unbufferize_time = 0.0
        self._algo_buffered_trades = []
        self._event            = Event()
        self._event.marketMgr  = BufferizedMarketManager()
        self._money_mngr       = self._event.getMoneyManager()
        self._money_mngr.setTotalQty(self._header['OrderQty'])
        self._market_mngr      = self._event.getMarketManager()
        self._market_mngr.setVenues({context['ric']:{}})
        self._observers        = []
        setup_observers        = setup.copy()
        setup_observers['name']= 'ReadOrdersValidator000'
        self._observers.append(ReadOrdersObserver(setup_observers, 
                                                  context, 
                                                  {'print' : True,
                                                   'log_filename' : 'C:/obs1.log'}, 
                                                  trace))
    
    
    
    '''######################################################################################################
    #################################   FUNCTIONS CALLED BY THE SCHEDULER   #################################
    ######################################################################################################'''
    
    def process(self, event):
        code = self.update(event)
        if code == 1:
            self._market_mngr.setIndicators(self._bus)
            self._algo_did_actions = False
            self.__unbufferize()
            self.__copy_event(event)
            self.__copy_market_manager(event)
            self.__update_money_manager()
            if not self._algo_did_actions:
                for observer in self._observers:
                    observer.process(self._event)
    
    def processReport(self, eventReport):
        pass
    
    def results(self):
        for observer in self._observers:
            self.indicators = observer.results()
        self.indicators.info['class_name'] = 'OrdersReplayer'
        return self.indicators
    
    
    
    '''######################################################################################################
    #########################################   PRIVATE FUNCTIONS   #########################################
    ######################################################################################################'''
    
    def __unbufferize(self):
        if self._unbufferize_time == 0.0:
            self._unbufferize_time = self._bus['start_time']
        if ba['event']['TIME_NUM'] >= self._unbufferize_time:
            for trade in self._algo_buffered_trades:
                if trade[0] == 'Z':
                    self._money_mngr.updateBook(MoneyManager.EXECUTION, self['ric'], trade[1], trade[2], trade[3])
                elif trade[0] == 'C':
                    self._money_mngr.updateBook(MoneyManager.CANCEL   , self['ric'], trade[1], trade[2], trade[3])
            if len(self._algo_buffered_trades) > 0:
                for observer in self._observers:
                    observer.processReport(self._event)
            self._market_mngr.clearBufferizedTrades()
            del self._algo_buffered_trades[:]
            while ba['event']['TIME_NUM'] >= self._unbufferize_time:
                self._unbufferize_time += self['delta_t']
    
    def __copy_event(self, event):
        self._event.type      = event.eventType
        self._event.timestamp = event.getEvtTimeStr()
        self.orderBookRIC     = event.getOrderBookRIC()
    
    def __copy_market_manager(self, event):
        lob = event.getLob()
        market_image = {}
        # BID
        for i in range(5):
            market_image["BEST_BID" + str(i+1)]  = lob.getBid(i).price
            market_image["BEST_BSIZ" + str(i+1)] = lob.getBid(i).size
        # ASK
        for i in range(5):
            market_image["BEST_ASK" + str(i+1)]  = lob.getAsk(i).price
            market_image["BEST_ASIZ" + str(i+1)] = lob.getAsk(i).size
        # BEST / BEST OPPOSITE
        if self['side'] == Order.Buy:
            for i in range(5):
                market_image['BEST' + str(i+1)]               = lob.getBid(i).price
                market_image['BEST_SIZE' + str(i+1)]          = lob.getBid(i).size
                market_image['BEST_OPPOSITE' + str(i+1)]      = lob.getAsk(i).price
                market_image['BEST_OPPOSITE_SIZE' + str(i+1)] = lob.getAsk(i).size
        elif self['side'] == Order.Sell:
            for i in range(5):
                market_image['BEST' + str(i+1)]               = lob.getAsk(i).price
                market_image['BEST_SIZE' + str(i+1)]          = lob.getAsk(i).size
                market_image['BEST_OPPOSITE' + str(i+1)]      = lob.getBid(i).price
                market_image['BEST_OPPOSITE_SIZE' + str(i+1)] = lob.getBid(i).size
        # TRADES
        trades = event.getTrades()
        if ba['event']['TIME_NUM'] >= dateNum('14:30:39:000'):
            pass
        for i in range(len(trades)):
            if i==0:
                market_image["DEAL_TIME"]  = event.getEvtTimeStr()
                market_image["DEAL_SIZE"]  = trades[i].size
                market_image["DEAL_PRICE"] = trades[i].price
            else:
                market_image["DEAL_TIME_" + str(i)]  = event.getEvtTimeStr()
                market_image["DEAL_SIZE_" + str(i)]  = trades[i].size
                market_image["DEAL_PRICE_" + str(i)] = trades[i].price
            trade              = Trade()
            trade.price        = trades[i].price
            trade.size         = trades[i].size
            trade.orderId1     = trades[i].orderId1
            trade.orderId2     = trades[i].orderId2
            trade.hour         = event.getEvtTimeStr()
            trade.overAsk      = trades[i].overAsk
            self._market_mngr.addBufferizedTrade(trade)
        # UPDATE
        self._market_mngr.updateFeedImage(self['ric'], market_image)
    
    def __update_money_manager(self):
        while self._cmd_index < len(self._cmd_pydata.date) and self._cmd_pydata.date[self._cmd_index] <= ba['event']['TIME_NUM']:
            cmd = self.__get_cmd()
            self._algo_did_actions = True
            # create order
            order = {}
            order['AmountValidation'] = True
            if cmd['Type'] == 'I':
                order['EffectiveTime']= cmd['Timestamp']
            else:
                order['EffectiveTime']= self.__find_INSERT_cmd_key('Timestamp')
            order['Side']             = '1' if cmd['Side']=='B' else '2'
            order['Price']            = str(cmd['Price'])
            order['LeavesQty']        = str(cmd['LeavesQty'])
            order['OrderQty']         = str(cmd['OrderQty'])
            order['OrderType']        = str(cmd['OrderType'])
            order['VenueId']          = str(self['trading_destination_id'])
            order['ExpireTime']       = str(cmd['ExpireTime'])
            order['TimeInForce']      = str(cmd['TimeInForce'])
            # create execution
            execution = {}
            execution['execId']   = str(cmd['MarketOrderId'])
            execution['execRefId']= str(cmd['OrderMarketRef'])
            execution['timestamp']= str(cmd['Timestamp'])
            execution['quantity'] = str(cmd['OrderQty'])
            execution['price']    = str(cmd['Price'])
            # choose case
            if   cmd['Type'] == 'I':
                self._money_mngr.updateBook(MoneyManager.NEW      , self['ric'], cmd['OrderInternalRef'], order, execution)
            elif cmd['Type'] == 'Z':
                self._algo_buffered_trades.append((cmd['Type'], cmd['OrderInternalRef'], order, execution))
            elif cmd['Type'] == 'C':
                self._algo_buffered_trades.append((cmd['Type'], cmd['OrderInternalRef'], order, execution))
            elif cmd['Type'] == 'U':
                self._money_mngr.updateBook(MoneyManager.MODIFY   , self['ric'], cmd['OrderInternalRef'], order, execution)
            # process
            for observer in self._observers:
                observer.process(self._event)
            self._money_mngr.clearLastExecutions()
            # increment
            self._cmd_index += 1
            
    def __get_cmd(self):
        cmd = {}
        for (key,val) in self._cmd_pydata.value.iteritems():
            cmd[key] = val[self._cmd_index]
        cmd['date'] = self._cmd_pydata.date[self._cmd_index]
        return cmd
    
    def __find_INSERT_cmd_key(self, key):
        cmd = None
        index = self._cmd_index-1
        while index >= 0:
            if self._cmd_pydata.value['OrderInternalRef'][index] == self._cmd_pydata.value['OrderInternalRef'][index]:
                cmd = self._cmd_pydata.value[key][index]
                break
            index -= 1
        if cmd == None:
            pyLog("FATAL ERROR : insert order couldn't be found")
        return cmd
    

