import sys
import simep
from simep.scenarii.scenario import Scenario
from simep.sched import Order, Trade
from simep.funcs.dbtools.repository import Repository
from usr.sivla.agents.LobRecorder import LobRecorder
from datetime import datetime, timedelta



def run_visualize(context = {'start': datetime(2010, 06, 29, 9, 0, 00), 
                             'end':   datetime(2010, 06, 29,17, 30, 00), 
                             'RIC': 'FTE.PA'}, 
                  model_type = 'Light', 
                  plot=True):
    
    date_str = context['start'].strftime('%Y%m%d')        
    testMM = Scenario('test MM', Repository('C:/st_sim/simep/projects/databases/repository'), 'C:\st_repository\simep_scenarii')
    
    if (model_type == 'Light'):
        testMM.setEngine(model_type, {'Full': 'True'})
    else:
        testMM.setEngine(model_type, {'NbBT': 400, 'Full': 'False'})
        
    testMM.setUniverse({date_str: [context['RIC']]})
    testMM.registerTrader(secId = context['RIC'], 
                         classname = 'LobRecorder', 
                         params = {'record_only_trade_events': True }, 
                         module="dev.usr.sivla.agents.LobRecorder", 
                         date = date_str)
#    testMM.registerTrader(secId = context['RIC'], 
#                         classname = 'Cycle', 
#                         params = {'size': 500, 'distance': 0,
#                                   'cycle': 5, 'bussinessTime': True,                            
#                                   'side': Order.Sell }, 
#                         module ="dev.usr.sivla.agents.Cycle", 
#                         date = date_str)
    testMM.registerTrader(secId = context['RIC'], 
                         classname = 'MMparam', 
                         params = {'size': 500, 'distance': 0,
                                   'cycle': 5, 'bussinessTime': True }, 
                         module ="dev.usr.malas.agents.MMparam", 
                         date = date_str)
    
    testMM.generateSimulations()
    a = testMM.runSimulations()    
    data = a[0][('trader_MMparam_%s_1_%s' % (context['RIC'].replace('.', '_'), date_str), date_str, context['RIC'])]
    if plot:
        data_interval = data.interval(context['start'], context['end'])
        LobRecorder.run_visualization(data_interval, title = 'Model: %s <%s: %s>' % ( model_type, context['RIC'], context['start'].strftime('%d/%m/%Y') ))
    return data

# import usr.malas.scenarii.test_MMparam
if __name__ == "__main__":           
    data = run_visualize(model_type = 'Light',plot=False)
    data.to_csv('C:\st_sim\dev\usr\malas\scenarii\data_MM.csv')