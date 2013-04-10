import sys
from simep.scenarii.metascenario import MetaScenario
import datetime


def simulate(ric_filename, date_filename, trading_destination_id):
    
    # params
    stock_params = {'data_type'       : 'TBT2'}
    trader1_params = {'context'    : {},
                     'parameters' : {'plot_mode'         : 0, 
                                     'delta_t'           : '00:00:01:000',
                                     'number_of_limits'  : 3,
                                     'tick_size_minimum' : 0.0001,
                                     'price_impact_volumes' : '500|1000|1500'}}
    trader2_params = {'context'    : {},
                     'parameters' : {'plot_mode'         : 0, 
                                     'save_into_file'    : True,
                                     'print_orderbook'   : False}}
    if '.txt' in ric_filename and '.txt' in date_filename:
        dates = get_dates_list(date_filename)
        f = open(ric_filename, 'r')
        line = f.readline()
        while line != '':
            fields  = line.replace('\n','').split('\t')
            ric     = fields[0].replace(' ','')
            trad_id = int(fields[1])
            trader1_params['context']['ric'] = ric
            trader1_params['context']['trading_destination_id'] = trad_id
            trader2_params['context']['ric'] = ric
            trader2_params['context']['trading_destination_id'] = trad_id
            simulation_lowlevel_file = 'C:/st_repository/simep_scenarii/gui/SimepRunner_%s_%d_slave.py' %(ric.replace('_','.'), trad_id)
            stocks = [stock_params.copy()]
            stocks[0]['ric'] = ric
            stocks[0]['trading_destination_id'] = trad_id
            my_scenario = MetaScenario('C:/st_sim/simep/st_sim.xml', simulation_lowlevel_file)
            my_scenario.SetEngine('ROBModel', {'number_of_bt' : 1, 'full' : True})
            my_scenario.SetDates(dates)
            my_scenario.SetStocks(stocks)
            my_scenario.AddTrader('OrderbookObserver', trader1_params)
            my_scenario.AddTrader('StockObserver', trader2_params)
            my_scenario.GenerateAndRunSimulations('C:/st_repository/simep_scenarii/gui')
            del(my_scenario)
            line = f.readline()
        f.close()
    else:
        ric     = ric_filename
        trad_id = int(trading_destination_id)
        trader1_params['context']['ric'] = ric
        trader1_params['context']['trading_destination_id'] = trad_id
        trader2_params['context']['ric'] = ric
        trader2_params['context']['trading_destination_id'] = trad_id
        simulation_lowlevel_file = 'C:/st_repository/simep_scenarii/gui/SimepRunner_%s_%d_slave.py' %(ric.replace('_','.'), trad_id)
        date  = date_filename
        stocks = [stock_params.copy()]
        stocks[0]['ric'] = ric
        stocks[0]['trading_destination_id'] = trad_id
        my_scenario = MetaScenario(simulation_lowlevel_file)
        my_scenario.SetEngine('ROBModel', {'number_of_bt' : 1, 'full' : True})
        my_scenario.SetDates([date])
        my_scenario.SetStocks(stocks)
        my_scenario.AddTrader('OrderbookObserver', trader1_params)
        my_scenario.AddTrader('StockObserver', trader2_params)
        my_scenario.GenerateAndRunSimulations('C:/st_repository/simep_scenarii/gui')
        del(my_scenario)
        
            


def get_dates_list(date_filename):
    f      = open(date_filename, 'r')
    idates = f.readlines()
    f = lambda x : (x.replace('\n','') if (datetime.date(int(x[:4]), int(x[4:6]), int(x[6:8])).weekday() <= 4) else None)
    d = map(f, idates)
    d2 = []
    for date in d:
        if date != None:
            d2.append(date)
    return d2
    


simulate(sys.argv[1], sys.argv[2], sys.argv[3])

