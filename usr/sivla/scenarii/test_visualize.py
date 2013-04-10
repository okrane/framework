import sys
import simep
from simep.scenarii.metascenario import MetaScenario
from simep.sched import Order, Trade
from usr.dev.sivla.agents.LobRecorder import LobRecorder
from datetime import datetime, timedelta


def run_visualize(context = {'trading_destination_ids' : [4],
                             'data_type'               : 'TBT2',
                             'ric'                     : 'ACCP.PA'}, 
                  model_type = 'ROBModel', 
                  agent_name = None):
    
    params = {'start': '20110118-14:30:00', 
              'end':   '20110118-15:00:00'}
    date_str = params['start'][0:8]  
    testViz = MetaScenario()
    
    if (model_type == 'ROBModel'):
        testViz.SetEngine(model_type, {'full': True})
    else:
        testViz.SetEngine(model_type, {'number_of_bt': 10, 'full': False})
    
    testViz.SetDates([date_str])
    testViz.SetStocks([context])
    testViz.AddTrader('LobRecorder', {'context'    : {'trading_destination_names' : None, 
                                                      'ric'                       : None},
                                    'parameters' : params})
    testViz.GenerateAndRunSimulations('C:/st_sim/usr/dev/sivla/scenarii')   
    
    
    #b = a[date_str]
    #data = b.values()[0].values()[0]
    #data_interval = data.interval(params['start'], params['end'])
    #LobRecorder.run_visualization(data_interval, title = 'Model: %s <%s: %s>' % ( model_type, context['ric'], params['start'].strftime('%d/%m/%Y') ))



from simep.core.tactic import Tactic
from simep.funcs.data.pyData import pyData, convertStr
class ImpacterAgent(Tactic):    
    def __init__(self, setup, context, params, trace):
        #name, ric, Side, Time, Nb):     
        Tactic.__init__(self, setup, context, params, trace)        
        #self._parameters = {'side': self['side'], 'time': , 'Nb' : Nb, 'ric': ric}
        #self._parameters['time'] = datetime.strptime(self['time'], '%Y-%m-%d %H:%M:%S')
        self.date_str = context['date']
        self.countdown = -1
        self._activeOrder = None
        
        self.needExecReportEvt = True        
        self.needAllEvts = True
        
    def process(self, evt):
        lob = evt.getLob()
        #print lob
        if lob == None: return 
        self.timestamp = evt.getLob().evtTime
        dt = datetime(year   = convertStr(self.date_str[0:4]),  
                      month  = convertStr(self.date_str[4:6]), 
                      day    = convertStr(self.date_str[6:8]), 
                      hour   = convertStr(self.timestamp[0:2]),
                      minute = convertStr(self.timestamp[3:5]),
                      second = convertStr(self.timestamp[6:8]),
                      microsecond = 1000 * convertStr(self.timestamp[9:12]))
        #print dt, self['Time']
        if (dt >= self['time']) and self.countdown < 0: self.countdown = self['nb']
        
        if self.countdown > 0:
            self.countdown -= 1
            if self.countdown % 3 == 0:
                if self['side'] > 0:
                    size = evt.getLob().getAsk(0).size
                else:
                    size = evt.getLob().getBid(0).size
                self._activeOrder = self._createMarketOrder(size , 'a')
                self._orderbook.processCreateOrder(self._activeOrder) 
 
    def processReport(self, evtReport):
        time = evtReport.getEvtTimeStr()
        print "%d@%d - %s" %( evtReport.size, evtReport.price, self.timestamp)
        self.appendIndicator(pyData('init', date = [self.timestamp], value = {'price': [evtReport.price], 'size': [evtReport.size]})) 
        
if __name__ == "__main__":           
    run_visualize(model_type = 'ROBModel', agent_name = None)