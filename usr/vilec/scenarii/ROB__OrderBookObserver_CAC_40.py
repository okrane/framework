#--------------------------------------------------------------------
# Define meta-scenario
#--------------------------------------------------------------------
import sys
import os
from simep.scenarii.metascenario import MetaScenario
from simep.scenarii import run_scheduled_simulations 
import xml.dom.expatbuilder



def run_cac_40_ob_indicators(simu_date, st_repository_gui_dir):
    M = MetaScenario(disable_bufferization = False, 
                     real_time             = False, 
                     log_mode              = True, 
                     debug_mode            = False, 
                     max_time              = '01:00:00', 
                     rfa_publisher         = {'enable': False, 'single_pipe': True, 'source_hostname': 'padev016'}, 
                     fix_server            = {'reuse_address': True, 'enable': False, 'socket_port': '5003', 'check_latency': False}, 
                     opening_auction       = False, 
                     intraday_auction      = False, 
                     closing_auction       = False, 
                     stop_auction          = False)
    M.SetEngine('ROBModel', {'number_of_bt'   : 1, 
                             'full'           : True, 
                             'include_closingauction_trades' : False, 
                             'include_intradayauction_trades' : False, 
                             'include_tradatlast_trades' : False, 
                             'prefixing_qty_bias' : 0.0, 
                             'include_cross_trades' : False, 
                             'include_stopauction_trades' : False, 
                             'include_openingauction_trades' : False, 
                             'include_tradafterhours_trades' : False})
    M.SetDates([simu_date])
    M.SetStocks([{'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'OREP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'TECF.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'EDF.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'ALUA.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'BNPP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'LAFP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'PEUP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'VIV.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'BOUY.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'SCHN.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'SASY.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'TOTF.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'RENA.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'PRTP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'STM.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'SEVI.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'CNAT.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'SGOB.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'ESSI.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'VIE.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'SGEF.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'UNBP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'CAGR.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'ALSO.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'CAPP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'SOGN.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'DANO.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'PERP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'AXAF.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'VLLP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'ACCP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'GSZ.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'CARR.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'MICP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'EAD.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'FTE.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'PUBP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'AIRP.PA', 
                  'data_type'                 : 'TBT2'}, 
                 {'trading_venue_names'       : ['BATS TRADING EUROPE', 'CHI-X PARIS', 'NYSE EURONEXT PARIS', 'TURQUOISE LIGHT'], 
                  'ric'                       : 'LVMH.PA', 
                  'data_type'                 : 'TBT2'}])
    M.AddTrader('OrderBookObserver', {'context'    : {'trading_venue_names'       : None, 
                                                      'ric'                       : None},
                                      'parameters' : {'dummy_parameter3'          : 'my_string', 
                                                      'round_trip_qty'            : (1, 3, 5, 8, 10, 15, 20), 
                                                      'slice_duration'            : 30}})
  #  M.Run(output_files_dir='C:\st_repository\simep_scenarii\gui', schedule=True, split=True)
    M.Run(output_files_dir= st_repository_gui_dir, schedule=True, split=True)
    


if __name__ == '__main__':
    from simep import __simep_directory__,__executable__, __python_directory__, __xml_config_file__, __stsim_directory__, __simep_directory__, __executable__, __tbtl_format__, __config__
    from simep.funcs.stsim.stsim_tools import load_config_params
    XML_DOC       = xml.dom.expatbuilder.parse(__xml_config_file__, True)
    Params = load_config_params(XML_DOC)
    if len(sys.argv) < 1:
        print("You have to give a date to launch the ob computations")
    the_date = sys.argv[1]
    st_repository_gui_dir = os.path.abspath('%s/simep_scenarii/gui' % Params['st_repository_dir'])
    run_cac_40_ob_indicators(the_date, st_repository_gui_dir)

    the_cmd = '%s %s/scenarii/run_scheduled_simulations.py %s/scheduled_simulations &' %(__executable__,
                                                                                         __simep_directory__,
                                                                                         os.path.abspath('%s/simep_scenarii/gui' % Params['st_repository_dir'])) 
    os.system(the_cmd)