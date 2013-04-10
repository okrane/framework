#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------
import os
from simep import __python_directory__, __simep_directory__
from simep.scenarii.metascenario import MetaScenario
from usr.dev.joafe.scenarii.parsing_tools import perform_parsing


custom_data = {'file'         : 'C:/st_work/usr/dev/test2_econometrie30new.txt',
               'block_length' : 15}
ric        = 'AXAF.PA'
start_date = '20110602'
end_date   = '20110629'
schedule   = True


M = MetaScenario(disable_bufferization=True)
M.SetEngine('ROBModel', {'include_dark_trades' : False, 
                         'number_of_bt'   : 1, 
                         'full'           : True, 
                         'include_cross_trades' : False, 
                         'include_intradayauction_trades' : False, 
                         'include_tradatlast_trades' : False, 
                         'include_closingauction_trades' : False, 
                         'include_tradafterhours_trades' : False, 
                         'include_stopauction_trades' : False, 
                         'include_openingauction_trades' : False})
M.SetDates([(start_date,end_date)])
M.SetStocks([{'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'ACCP.PA', 
              'data_type'                 : 'TBT2'},
             {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'AXAF.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'BOUY.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'CAPP.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'CARR.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'DANO.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'FTE.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'LAFP.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'PEUP.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'RENA.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'SGOB.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'SGEF.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'SOGN.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'EAD.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'VIE.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'VIV.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'CAGR.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'GSZ.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'ALSO.PA', 
              'data_type'                 : 'TBT2'},
              {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'EDF.PA', 
              'data_type'                 : 'TBT2'},
             {'trading_destination_names' : ['ENPA'], 
              'ric'                       : 'SEVI.PA', 
              'data_type'                 : 'TBT2'}])

#M.SetStocks([{'trading_destination_names' : ['ENPA'], 
#              'ric'                       : 'CAGR.PA', 
#              'data_type'                 : 'TBT2'}])

M.AddTrader('BestExecGLFT', {'parameters' : {'ewma_theta'       : 0.1,
                                         'slice_timestep'            : '00:05:00:000000', 
                                         'asked_qty_multiple'        : 40,  
                                         'plot_mode'                 : 1 if not schedule else 0, 
                                         'max_exec_time'             : ['00:00:20:000000','00:00:30:000000','00:00:40:000000'], 
                                         'algo_end_time'             : '+03:00:00:000000', 
                                         'matrix_size'               : 12, 
                                         'initial_gamma'             : 0.4, 
                                         'ref_order_size'            : [0.8], 
                                         'ewma_theta_ats'            : 0.1, 
                                         'algo_start_time'           : '+01:00:00:000000', 
                                         'max_order_price'           : 3, 
                                         'tolerance'                 : 0.0001, 
                                         'calibration_timestep'      : '00:05:00:000000', 
                                         'slice_sub_timestep'        : '00:00:01:000000',
                                         'delta_target'              : [0.0]},
                        'custom_func' : perform_parsing,
                        'custom_data' : custom_data})

if schedule:
    M.GenerateSplitAndScheduleSimulations('C:/st_repository/simep_scenarii/gui')
    os.system('start %s/python %s/scenarii/run_scheduled_simulations.py C:/st_repository/simep_scenarii/gui/scheduled_simulations' %(__python_directory__, 
                                                                                                                                     __simep_directory__))
else:
    M.GenerateSplitAndRunSimulations('C:/st_repository/simep_scenarii/gui')

