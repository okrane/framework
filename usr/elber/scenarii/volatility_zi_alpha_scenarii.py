#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

import sys
from simep.scenarii.metascenario import MetaScenario

alpha      = float(sys.argv[1])
mu         = float(sys.argv[2])
sigma      = float(sys.argv[3])
price_mean = float(sys.argv[4])
price_std  = float(sys.argv[5])
size_std   = float(sys.argv[6])
n_days     = int(sys.argv[7])

# get end date
from datetime import datetime, timedelta
one_day = timedelta(days=1)
end_datetime   = datetime.strptime('20100101', '%Y%m%d')
for i in range(n_days):
    end_datetime += one_day
end_date = end_datetime.strftime('%Y%m%d')
del one_day
del end_datetime

# build which_param
which_param  = "alpha_%f_mu_%f_sigma_%f_pricemean_%f_pricestd_%f_sizestd_%f"
which_param  = which_param %(alpha, mu, sigma, price_mean, price_std, size_std)

# create metascenario
M = MetaScenario(disable_bufferization=True)
M.SetEngine('ZI3Model', {'print_frequency' : 0.00})
M.SetDates([('20100101', end_date)])
M.SetStocks([{'order_size_std'            : {0: size_std}, 
              'data_type'                 : None, 
              'opening'                   : {0: '09:00:00:000000'}, 
              'trading_destination_names' : ['ENZI3'], 
              'alpha'                     : {0: alpha}, 
              'ric'                       : 'ZI3.PA', 
              'start_price'               : {0: 20.0}, 
              'tick_size'                 : {0: 0.01}, 
              'min_limit_orders_num'      : {0: 50}, 
              'sigma'                     : {0: sigma}, 
              'mu'                        : {0: mu}, 
              'trading_destination_ids'   : [0], 
              'price_mean'                : {0: price_mean}, 
              'execution_start_time'      : {0: '+00:03:00:000000'}, 
              'closing'                   : {0: '17:30:00:000000'}, 
              'price_std'                 : {0: price_std}}])
M.AddTrader('VolatiliteZi', {'parameters' : {'which_param' : which_param}})
M.GenerateAndScheduleSimulations('C:/st_repository/simep_scenarii/volatility_experiment')
del M

# run distributed simulations
from simep.scenarii.run_scheduled_simulations2 import cpu_count, run_processes
wkrs = int(sys.argv[8]) if len(sys.argv) >= 8 else cpu_count()
run_processes('C:/st_repository/simep_scenarii/volatility_experiment/scheduled_simulations', wkrs)



