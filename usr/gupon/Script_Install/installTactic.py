'''
Created on 17 juil. 2012

@author: gupon
'''

import os
import sys
import ConfigParser_NL
import XMLTestReader
import install_AlgoBox
import shutil

from simep.funcs.dbtools.connections import Connections


def isRecorded(name):
    
    query = """ select algo_name  from dash_strategy_algo where algo_name = '%s' """%name
    data = Connections.exec_sql('GANDALF', query)
    
    if len(data) == 0:
        return False
    else:
        return True
    
def installTactic(type, name, parent_tactic, loc, algobox):
    #install function for news script
    
    query = """select Path from AlgoCore where AlgoCore_Name = '%s'
        """ %algobox
    
    data = Connections.exec_sql('GANDALF', query)
    
    if len(data)!=1:
        print "Erreur : l'AlgoBox : %s n'est pas unique ou n'existe pas !"%algobox
        return False
    
    if type != "delegate" and type != "tactic":
        print "Les types autorises sont 'delegate' et 'tactic'" 
        return False
        
    
    loc_AB = data[0][0]
    
    #copie du script dans l'algobox
    if os.path.exists(loc):
        shutil.copy(loc + name + ".py", loc_AB + "AlgoBox/AlgHoster/scripts/python/")
    else:
        print "Script non trouve !"
        return False
    
    if type == "tactic": 
        #update de list strat
        config = ConfigParser_NL.RawConfigParser()
        config.read(loc_AB + "AlgoBox/AlgHoster/bin/alghoster.ini")
        #config.read("alghoster.ini")
        
        #tester si la strategy est deja dans la liste
        list_strat = config._sections['AlgHoster']['StrategyList'] + ",Strategy."
        if list_strat.find(name) == -1:
            list_strat = list_strat + ",Strategy." + name
            config._sections['AlgHoster']['StrategyList'] = list_strat
        
        conf_alghoster = open(loc_AB + 'AlgoBox/AlgHoster/bin/alghoster.ini', 'w')
        #conf_alghoster = open('alghoster.ini', 'w')
        
        
        for dictio in config._sections:
            aux = config._sections[dictio]
            conf_alghoster.write("[" + dictio + "]\n")
            
            for key in aux:
                if key != "__name__":
                    conf_alghoster.write(key + "=" + str(aux[key]) + "\n")
                    
            conf_alghoster.write("\n")
            
        conf_alghoster.close()
        
        
        if not isRecorded(name):
            query =""" select * from dash_strategy_algo order by algo_id desc
            """
            data = Connections.exec_sql('GANDALF', query)
            
            id = data[0][0] + 1
            
            query =""" select * from dash_strategy_algo_tvfo order by tvfo_id desc
            """
            data = Connections.exec_sql('GANDALF', query)
            
            id2 = data[0][0] + 1
            
            query =""" insert into dash_strategy_algo (algo_id, algo_name, algo_type) values (%d, '%s', 'tvfo')
                """%(id,name)
        
            Connections.exec_sql('GANDALF', query)
            
            query =""" insert into dash_strategy_algo_tvfo (tvfo_id, algo_id, tactic_type, tactic_script) values (%d, %d, 'simep', '${ALGHOSTER_SCRIPT_PATH}/python/%s.py')
                """%(id2,id,name)
                
            Connections.exec_sql('GANDALF', query)
        
        install_AlgoBox.generate_strategy_xml(loc_AB)
        #install_AlgoBox.generate_strategy_xml("C:/")
        
    else:
        return True
    
    
if __name__ == '__main__':
    
    type = sys.argv[1]
    tactic = sys.argv[2]
    loc = sys.argv[3]
    AlgoBox = sys.argv[4]
    
#    tactic = 'BlinkOrder'
#    parent = 'BlinkOrderP'
#    loc ='/home/'
#    AlgoBox ='AlgoCore_gupon1'
    
    installTactic(type, tactic, tactic, loc, AlgoBox)
    
    