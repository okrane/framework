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
    sched = Scheduler('C:/st_sim/simep/Log')
    trace = sched.getTrace()
    # ------------------------------------------------- #
    
    
    
    # ------------------------------------------------- #
    # Create a log file for the simulation.             #
    # ------------------------------------------------- #
    file = open('C:/st_sim/dev/tests/demo/log/test_14_demo_ZIM_simulation.log', 'w')
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
    from usr.tests.demo.agents.zimmodel import ZIMModel
    sched.addOrderBook('normal', 'FTE.PA')
    stock_FTE = ZIMModel({'name'                   : 'stock_FTE'},
                         {'date'                   : '20100520', 
                          'ric'                    : 'FTE.PA'},
                         {'mu_delta_time'          : '00:00:08:000', 
                          'number_of_orders'       : 1, 
                          'std_price'              : 1.0, 
                          'start_time'             : '07:00:00', 
                          'std_delta_time'         : '00:00:02:000', 
                          'reference_price'        : 10.0, 
                          'tick_size'              : 0.01, 
                          'mu_order_qty'           : 6.215, 
                          'std_order_qty'          : 0.5, 
                          'half_spread'            : 1.0, 
                          'mu_price'               : 0.5, 
                          'end_time'               : '15:30:00',
                          'file'                   : file},
                         trace)
    sched.addAgent(stock_FTE)
    # ------------------------------------------------- #
    
    
    
    # ------------------------------------------------- #
    # Run the simulation.                               #
    # ------------------------------------------------- #
    sched.run()
    # ------------------------------------------------- #
    
    
    
    # ------------------------------------------------- #
    # Close the log file.                               #
    # ------------------------------------------------- #
    file.close()
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
    assert( filecmp.cmp(path + '/dev/tests/demo/log/test_14_demo_ZIM_simulation.log',path + '/dev/tests/demo/log/test_14_demo_ZIM_simulation.ref'))
    # ------------------------------------------------- #
    
    
    
    
if not 'auto_run' in locals().keys():
    auto_run = True
    test()


