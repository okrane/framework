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
    file = open('C:/st_sim/dev/tests/demo/log/test_13_demo_ROB+OBS_simulation.log', 'w')
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
    # 'ACCPpa.CHI' is the name of the stock), then      #
    # instantiate your model, then add your instance to #
    # the scheduler.                                    #
    # ------------------------------------------------- #
    from simep.robmodel import ROBModel
    sched.addOrderBook('light', 'ACCPpa.CHI')
    stock_ACCP = ROBModel({'login'                  : 'a_lambda_guy', 
                           'name'                   : 'stock_ACCP'},
                          {'trading_destination_id' : 61, 
                           'name'                   : 'stock_ACCP', 
                           'trading_destination_name' : 'ENPA', 
                           'input_file_name'        : 'C:/st_sim/dev/tests/demo/data/lobTrade_2_61_20100524.binary', 
                           'date'                   : '20100524', 
                           'security_id'            : 2, 
                           'ric'                    : 'ACCPpa.CHI'},
                          {'number_of_bt'           : 10, 
                           'full'                   : False},
                          trace)
    sched.addAgent(stock_ACCP)
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
                                  'ric'                    : 'ACCPpa.CHI'},
                                 {'file'                   : file,
                                  'record_vwap_in_log'     : True,
                                  'record_orderbook_in_log': True},
                                  trace)
    sched.addAgent(my_observer)
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
    del(my_observer)
    del(stock_ACCP)
    del(trace)
    del(sched)
    # ------------------------------------------------- #
    
    
    
    # ------------------------------------------------- #
    # Return results.                                   #
    # ------------------------------------------------- #
    assert( filecmp.cmp(path + '/dev/tests/demo/log/test_13_demo_ROB+OBS_simulation.log',path + '/dev/tests/demo/log/test_13_demo_ROB+OBS_simulation.ref'))
    # ------------------------------------------------- #
    
    
    
    
if not 'auto_run' in locals().keys():
    auto_run = True
    test()


