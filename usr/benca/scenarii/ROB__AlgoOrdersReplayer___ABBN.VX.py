#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------

from simep.scenarii.metascenario import MetaScenario

M = MetaScenario(disable_bufferization=True, disable_bus=False)
M.SetEngine('ROBModel', {'number_of_bt'   : 10, 
                         'full'           : True})
M.SetDates(['20100823'])
M.SetStocks([{'trading_destination_names' : ['VIRTX'], 
              'ric'                       : 'ABBN.VX', 
              'data_type'                 : 'BINARY'}])
M.AddTrader('AlgoOrdersReplayer', {'context'    : {'trading_destination_names' : ['VIRTX'], 
                                                   'ric'                       : 'ABBN.VX'},
                                   'parameters' : {'delta_t'                   : '00:00:01:000', 
                                                   'loading_method'            : 'headed_text_file', 
                                                   'cmd_filename'              : 'C:/st_sim/usr/dev/benca/data/detail_occ_3_Jn}0026.txt', 
                                                   'observer_class'            : 'OrdersReplayerObserver', 
                                                   'observer_module'           : 'simep.subagents.orders_replayer_observer'}})
M.GenerateAndRunSimulations('C:/st_sim/usr/dev/benca/scenarii')



