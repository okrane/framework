from simep.scenarii.metascenario import MetaScenario

m = MetaScenario('C:/st_repository/simep_scenarii')
m.SetEngine('ROBModel', {'full' : False})
m.SetDates('20100101', '20100126')
m.SetStocks([{'data_type' : 'TBT2', 'ric' : 'ACCP.PA'}, 
             {'data_type' : 'TBT2', 'ric' : 'AIRP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'ALUA.PA'},
             {'data_type' : 'TBT2', 'ric' : 'AXAF.PA'},
             {'data_type' : 'TBT2', 'ric' : 'BNPP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'BOUY.PA'},
             {'data_type' : 'TBT2', 'ric' : 'CAPP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'CARR.PA'},
             {'data_type' : 'TBT2', 'ric' : 'DANO.PA'},
             {'data_type' : 'TBT2', 'ric' : 'ESSI.PA'},
             {'data_type' : 'TBT2', 'ric' : 'FTE.PA'},
             {'data_type' : 'TBT2', 'ric' : 'OREP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'LAFP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'LAGA.PA'},
             {'data_type' : 'TBT2', 'ric' : 'LVMH.PA'},
             {'data_type' : 'TBT2', 'ric' : 'MICP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'PERP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'PEUP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'PRTP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'RENA.PA'},
             {'data_type' : 'TBT2', 'ric' : 'SGOB.PA'},
             {'data_type' : 'TBT2', 'ric' : 'SCHN.PA'},
             {'data_type' : 'TBT2', 'ric' : 'SGEF.PA'},
             {'data_type' : 'TBT2', 'ric' : 'STM.PA'},
             {'data_type' : 'TBT2', 'ric' : 'SOGN.PA'},
             {'data_type' : 'TBT2', 'ric' : 'TECF.PA'},
             {'data_type' : 'TBT2', 'ric' : 'TOTF.PA'},
             {'data_type' : 'TBT2', 'ric' : 'UNBP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'VLLP.PA'},
             {'data_type' : 'TBT2', 'ric' : 'DEXI.BR'},
             {'data_type' : 'TBT2', 'ric' : 'SASY.PA'},
             {'data_type' : 'TBT2', 'ric' : 'ISPA.AS'},
             {'data_type' : 'TBT2', 'ric' : 'EAD.PA'},
             {'data_type' : 'TBT2', 'ric' : 'VIE.PA'},
             {'data_type' : 'TBT2', 'ric' : 'VIV.PA'},
             {'data_type' : 'TBT2', 'ric' : 'CAGR.PA'},
             {'data_type' : 'TBT2', 'ric' : 'GSZ.PA'},
             {'data_type' : 'TBT2', 'ric' : 'ALSO.PA'},
             {'data_type' : 'TBT2', 'ric' : 'EDF.PA'},
             {'data_type' : 'TBT2', 'ric' : 'SEVI.PA'}])
m.AddTrader('StockObserver', {'parameters' : {'print_orderbook' : False, 
                                              'save_into_file'  : True,
                                              'plot_mode'       : 0}})
#m.AddTrader('Cycle', {'parameters' : {'asked_qty'          : 30000, 
#                                      'side'               : 'Order.Buy',
#                                      'cycle_time'         : 5,
#                                      'half_spread_factor' : 2.0,
#                                      'ats_factor'         : 1.5,
#                                      'limit_price'        : -1.0,
#                                      'use_business_time'  : False,
#                                      'plot_mode'          : 0}})
m.GenerateSplitAndScheduleSimulations('C:/st_repository/simep_scenarii')



