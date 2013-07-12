def test():


	# ------------------------------------------------- #
	# Get Path                                          #
	# ------------------------------------------------- #
	import filecmp
	import sys
	import os.path
	path = os.path.join(os.getenv('HOME'),'st_sim')
	# ------------------------------------------------- #
	
	
	
	# ------------------------------------------------- #
	# Create the Scheduler : this is the SIMEP core. It #
	# takes as input argument a path in order to store  #
	# the log file of the simulation.                   #
	# Trace is an object corrersponding to the log      #
	# buffer.                                           #
	# ------------------------------------------------- #
	from simep.sched import Scheduler
	sched = Scheduler(path + '/simep/Log')
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
	stock_FTE = OrdersReader({'name'               : 'stock_FTE'},
                         {'ric'                    : 'FTE.PA'},
                         {'start_time'             : '07:00:00:000',
                          'delta_time'             : '00:00:01:000',
                          'log_filename'           : path + '/dev/tests/demo/log/test_01_demo_LMT_orders.log', 
                          'orders_filename'        : path + '/dev/tests/demo/scenarii/test_01_demo_LMT_orders.txt',
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
	
	
	
	# ------------------------------------------------- #
	# Return results.                                   #
	# ------------------------------------------------- #
	assert( filecmp.cmp(path + '/dev/tests/demo/log/test_01_demo_LMT_orders.log',path + '/dev/tests/demo/log/test_01_demo_LMT_orders.ref'))
	# ------------------------------------------------- #
	
	
	
	
if not 'auto_run' in locals().keys():
	auto_run = True
	test()


