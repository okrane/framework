# ------------------------------------------------- #
# Create the Scheduler : this is the SIMEP core. It #
# takes as input argument a path in order to store  #
# the log file of the simulation.                   #
# Trace is an object corrersponding to the log      #
# buffer.                                           #
# ------------------------------------------------- #
from simep.sched import Scheduler
from simep.agents.stockobserver import StockObserver
from simep.agents.busmanager import BusManager
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #


file = open('C:/st_sim/dev/tests/demo/scenarii/Free_Model/Free_Model_Wtih_OBS.log', 'w')

# ------------------------------------------------- #
# Create a stock model. Here we use the ZIM, which  #
# means "Zero Intelligence Model". If you want to   #
# have a look at the implementation of this model,  #
# please refer to the zimmodel.py file. Briefly,    #
# this model simply sends random limit orders, in   #
# other words : orders with random quantity,        #
# random price (but centered on 10$), and random    #
# side (buy or sell).                               #
# Basically, you just add an orderbook to the       #
# scheduler ('normal' is the type of orderbook,     #
# 'FTE.PA' is the name of the stock), then          #
# instantiate your model, then add your instance to #
# the scheduler.                                    #
# ------------------------------------------------- #
from usr.tests.demo.agents.dummy_animator import DummyAnimator
sched.addOrderBook('normal', 'FTE.PA')
stock_FTE = DummyAnimator({'name'                   : 'stock_FTE'},
                         {'ric'                    : 'FTE.PA'},
                         {'start_time'             : '07:00:00:000',
                          'delta_time'             : '00:00:10:000',
                          'log_filename'           : 'C:/st_sim/dev/tests/demo/scenarii/Free_Model/Free_Model.log', 
                          'orders_filename'        : 'C:/st_sim/dev/tests/demo/scenarii/Free_Model/Free_Model.txt'},
                         trace)
sched.addAgent(stock_FTE)

# ------------------------------------------------- #
#              Create the bus manager               #
# ------------------------------------------------- #
BusManager.set_sched(sched)
BusManager.set_trace(trace)
# ------------------------------------------------- #
BusManager.new_bus({'trading_destination_id' : 4, 
                    'name'                   : 'stock_ZIM_0', 
                    'trading_destination_name' : 'ZIM', 
                    'date'                   : '20100520', 
                    'security_id'            : 110, 
                    'ric'                    : 'FTE.PA'},
                   ['avg_spread_60', 'garman_klass_15', 'avg_exec_time', 'curves', 'avg_trade_size_300'])
# ------------------------------------------------- #

# ------------------------------------------------- #
# Add a SimpleObserver instance. The 'setup' input  #
# parameters corresponds to a dictionnary with the  #
# name of the instance. The 'context' input         #
# parameter is a dictionnary to identify the stock  #
# to which you want to plug your agent (it usually  #
# contains 'ric', 'security_id','date', and         #
# 'trading_destination_id'). The 'params' input     #
# parameters just contains the 'file_name'          #
# ------------------------------------------------- #
from usr.tests.demo.agents.simpleobserver import SimpleObserver
my_observer = SimpleObserver({'name'                   : 'my_observer'},
                             {'date'                   : '20100520', 
                              'security_id'            : 110, 
                              'trading_destination_id' : 4, 
                              'ric'                    : 'FTE.PA'},
                             {'file'                   : file,
                              'record_vwap_in_log'     : True,
                              'record_orderbook_in_log': True},
                              trace)
sched.addAgent(my_observer)

my_s_observer = StockObserver( {'name'                   : 'my_s_observer'},
                             {'date'                   : '20100520', 
                              'security_id'            : 110, 
                              'trading_destination_id' : 4, 
                              'ric'                    : 'FTE.PA'},
                             {'plot_mode'              : 2,
                              'save_into_file'         : False},
                              trace)

sched.addAgent(my_s_observer)

# ------------------------------------------------- #
# Run the simulation.                               #
# ------------------------------------------------- #
sched.run()
# ------------------------------------------------- #

import matplotlib.pyplot
matplotlib.pyplot.show()

# ------------------------------------------------- #
# Destroy objects.                                  #
# ------------------------------------------------- #
del(stock_FTE)
del(trace)
del(sched)
# ------------------------------------------------- #



