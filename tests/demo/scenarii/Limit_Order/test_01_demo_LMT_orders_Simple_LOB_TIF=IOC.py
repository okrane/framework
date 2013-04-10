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
from usr.tests.demo.agents.orders_reader import OrdersReader
sched.addOrderBook('normal', 'FTE.PA')
stock_FTE = OrdersReader({'name'                   : 'stock_FTE'},
                         {'ric'                    : 'FTE.PA'},
                         {'start_time'             : '07:00:00:000',
                          'delta_time'             : '00:00:01:000',
                          'log_filename'           : 'C:/st_sim/dev/tests/demo/scenarii/Limit_Order/test_01_demo_LMT_orders_simple_LOB_TIF=IOC.log', 
                          'orders_filename'        : 'C:/st_sim/dev/tests/demo/scenarii/Limit_Order/test_01_demo_LMT_orders_simple_LOB_TIF=IOC.txt',
                          'print'                  : True},
                         trace)
sched.addAgent(stock_FTE)
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



