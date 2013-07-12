# ------------------------------------------------- #
# Create the Scheduler : this is the SIMEP core. It #
# takes as input argument a path in order to store  #
# the log file of the simulation.                   #
# Trace is an object corrersponding to the log      #
# buffer.                                           #
# ------------------------------------------------- #
from simep.sched import Scheduler
sched = Scheduler('C:/st_sim/simep/Log')
trace = sched.getTrace()
# ------------------------------------------------- #



# ------------------------------------------------- #
# Create a stock model. Here we use a list of       #
# orders defined in a text file :                   #
# test_XX_demo_XXX_orders.txt                       #
# Each order is sent every second (except for the   #
# initialization of the orderbook, if there is one) #
# ------------------------------------------------- #
from usr.tests.demo.agents.orders_reader import OrdersReader
sched.addOrderBook('normal', 'FTE.PA')
stock_FTE = OrdersReader({'name'                   : 'stock_FTE'},
                         {'ric'                    : 'FTE.PA'},
                         {'start_time'             : '07:00:00:000',
                          'delta_time'             : '00:00:01:000',
                          'log_filename'           : 'C:/st_sim/dev/tests/demo/scenarii/Observer_Demo/Observer_Demo.log', 
                          'orders_filename'        : 'C:/st_sim/dev/tests/demo/scenarii/Observer_Demo/Observer_Demo.txt'},
                         trace)
sched.addAgent(stock_FTE)
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
                              'record_vwap_in_log'     : False,
                              'record_orderbook_in_log': False},
                              trace)
sched.addAgent(my_observer)
# ------------------------------------------------- #



# ------------------------------------------------- #
# Run the simulation.                               #
# ------------------------------------------------- #
sched.run()
# ------------------------------------------------- #



# ------------------------------------------------- #
# Destroy objects.                                  #
# ------------------------------------------------- #
del(stock_FTE)
del(trace)
del(sched)
# ------------------------------------------------- #



