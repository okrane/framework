def test():


    """ STEP 0 : get the right path """
    import filecmp
    import sys
    import os.path
    path = os.path.join(os.getenv('HOME'),'st_sim')
    
    
    
    """ STEP 1 : import the MetaScenario module """
    from simep.scenarii.metascenario import MetaScenario
    
    
    
    """ STEP 2 : create a metascenario instance with the following parameters :
        -> the location of the file st_sim.xml
        -> the location of your simulation file (which is going to be 
           overwritten each time a new simulation is executed inside your
           scenario """
    MyScenario = MetaScenario(path + '/dev/tests/demo/st_sim_demo.xml', 
                              path + '/dev/tests/demo/scenarii/test_16_demo_ROB+SOB_simulation.py')
    
    
    
    """ STEP 3 : set the Simep Engine : 'ROBModel' in this case
     important : strings are CASE SENSITIVE in my code """
    MyScenario.SetEngine('ROBModel')
    
    
    
    """ STEP 4 : set the perimeter. Here you put first all the dates you want to re-play,
        and, for each of these dates, the list of the rics you want to re-play during that
        day.
        
        ex1 : {'20100524' : ['AXAF.PA','BNPP.PA'],
               '20100525' : ['AXAF.PA','BNPP.PA']}
        
        ex2 : {'20100524' : ['ACCP.PA','CAGR.PA'],
               '20100525' : ['RENA.PA']}
        
        ex3 : {'20100524' : ['RENA.PA','EAD.PA' ,'PEO.PA'],
               '20100525' : ['AXAF.PA','CAGR.PA','SGOB.PA','BNPP.PA'],
               '20100526' : ['LVMH.PA','DANO.PA']} 
        
        In the ex1, 4 historic data files will be played.
        In the ex2, 3 historic data files will be played.
        In the ex3, 9 historic data files will be played."""
    MyScenario.SetUniverse({'20100524' : [{'ric'            :'ACCPpa.CHI',
                                           'input_file_name':path + '/dev/tests/demo/data/lobTrade_2_61_20100524.binary'}]})
    
    
    
    """ STEP 5 : define the parameters of your agent. Here we are going to use a simple
        observer agent (called StockObserver) which just displays on the screen what happens. 
        There are two categories of parameters :
        -> 'context' : this corresponds to the stock(s) and date(s) your agent will be plugged 
                       into. 
                       If you put nothing, then your agent will be played on all dates 
                       and all stocks.
                       If you put a ric and no date, then your agent will be played on the
                       designated stock on ALL dates.
                       If you put a date and no ric, then your agent will be played on the
                       designated date and on ALL the stocks for this date.
                       If you put a ric and a date : guess what ? I won't tell you xD !
                       Furthermore, if you write 'ric':None , it is equivalent to all rics.
                       If you write 'date':None , it is equivalent to all dates.
        -> 'parameters': this corresponds to the parameters of your agent.
                       ex : 'limit_price':47.5 , 'asked_qty':100000 , 'order_type':'MARKET'
                       IMPORTANT : if you use the BaseObserver class (which is the case here),
                       you have to put a parameter 'plot_mode' in the following way:
                       -> 'plot_mode' : 0 means no plot
                       -> 'plot_mode' : 1 means plot results at the end of the simulation
                       -> 'plot_mode' : 2 means plot results every 10 or 20 min DURING the 
                                        simulation"""
    MyScenario.AddTrader('StockObserver', {'context'    : {},
                                           'parameters' : {'plot_mode'      : 1,
                                                           'save_into_file' : False}})
    
    
    
    """ STEP 6 : generate your scenario file and run all the simulations.
        You can give a path as an argument if you want to save the results (XLS files) in a
        particular folder. If you don't put anything, results will be saved in the default results
        folder, which corresponds to the 'st_repository_dir' tag in your st_sim.xml file. 
        
        Here, your metascenario is :
        
        for date in ['20100524','20100525']:
            Simulate(date, 'FTE.PA', StockObserver)"""
    MyScenario.GenerateAndRunSimulations(path + '/dev/tests/demo/scenarii')
    
    
    
    """ STEP 7 : check that the test has suceeded """
    assert( filecmp.cmp(path + '/dev/tests/demo/log/test_16_demo_ROB+OBS_simulation.py',path + '/dev/tests/demo/log/test_16_demo_ROB+OBS_simulation.ref'))
    
    
    
    
if not 'auto_run' in locals().keys():
    auto_run = True
    test()




