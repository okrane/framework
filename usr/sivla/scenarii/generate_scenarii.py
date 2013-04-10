import sys 
from simep.sched import Order


def create_scenarii_files(ric,date_start,date_end):
	from simep.scenarii.metascenario import MetaScenario
	m = MetaScenario('C:/st_repository/simep_scenarii')
	m.SetEngine('SBBModel', {'full' : False, 'seed' : 4})
	m.SetDates(date_start, date_end)
	m.SetStocks([{'data_type' : 'TBT2', 'ric' : ric}])


	Cycle001_params = {				    
					    'parameters' : {'bussinessTime'          : True, 
					    				'reference'			     : 'best_opposite',
					                    'plot_mode'              : 0, 
					                    'd'                      : 1, 
					                    'cycle'                  : 5, 
					                    'side'                   : 'Order.Buy', 
					                    'size'                   : 10000}
	}
	
	m.AddTrader('CyclePlacement', Cycle001_params) 
	m.GenerateAndScheduleSimulations('C:/st_repository/simep_scenarii/Cycle')
	
	
if __name__ == '__main__':
    create_scenarii_files('FTE.PA', '20101001', '20101030')