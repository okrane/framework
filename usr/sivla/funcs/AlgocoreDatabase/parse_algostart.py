import re
from simep.funcs.dbtools.connections import *

f = open('C:/Documents and Settings/sivla/Desktop/Dashboard Day/alghosters.txt', 'r');
x = 'asafaga'

dico = {}
while x:
    x = f.readline()
    alghoster_pattern = '[A-Z0-9_]+\)'
    alghoster_list = re.findall(alghoster_pattern, x);
    if alghoster_list:
        current_alghoster = alghoster_list[0][:-1]       
        dico[current_alghoster] = {}
        dico[current_alghoster]['strategies'] = []
        
    hostname_pattern = 'ALGOCORE_HOSTNAME=[a-z0-9]+';
    hostname_list = re.findall(hostname_pattern, x);
    if hostname_list:        
        dico[current_alghoster]['hostname'] = hostname_list[0][18:]
        
    strategy_pattern = 'Strategy.[a-zA-Z]+,*'
    strategy_list = re.findall(strategy_pattern, x);
    if strategy_list:
        #print strategy_list
        for strategy in strategy_list:
            if strategy[-1] == ',':
                strategy = strategy[:-1]
            dico[current_alghoster]['strategies'].append(strategy[9:])
            
print dico


g = open('C:/Documents and Settings/sivla/Desktop/Dashboard Day/dash_files_02.sql', 'w')
alghoster_id = 0
for alghoster in dico.keys():
    alghoster_id = alghoster_id + 1
    g.write('insert into dash_alghoster (algoster_id, alghoster_name, hostname) values (%d, "%s", "%s")\n' % (alghoster_id, alghoster, dico[alghoster]['hostname']))
    strategy_id = 0
    for strategy in dico[alghoster]['strategies']:
        strategy_id = strategy_id + 1 
        print strategy
        algo_id = Connections.exec_sql('GANDALF', 'select algo_id from temp_works..dash_strategy_algo where algo_name = "%s"' % (strategy))
        print algo_id
        if algo_id:
            g.write('insert into dash_alghoster_strategy (strategy_id, alghoster_id, algo_id) values (%d, %d, %d)\n' % (strategy_id, alghoster_id, algo_id[0][0]))

g.close()
