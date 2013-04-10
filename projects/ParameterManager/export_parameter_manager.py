# -*- coding: utf-8 -*-

from pymongo import MongoClient

def export_parameter_manager(server, port, target_file):
    client = MongoClient(server, port);
    collection = client.Mercure.parameter_manager
    
    f = open(target_file, "w")
    
    parameter_list = collection.find()  # needs more consistency checks
    
    for elem in parameter_list:
        if not(elem.has_key('strategy') or elem.has_key('key') or elem.has_key('static_parameters')):
            print "Mising mandatory key from database"      
            raise Exception("Mising mandatory key from database")
        
        if elem['key'] == {}:
            line = "strategy=" + elem['strategy'] + "|"
            line += "key=*,*,*,*|"
            for (k, v) in elem['static_parameters'].iteritems():
                line += str(k) + "=" + str(v) + "|"             
        else:
            default = collection.find({'strategy': elem['strategy'], 'key': {}})
            if default.count() == 0:
                print "Missing Default Key for Strategy %s" % elem['strategy']        
                raise Exception ("Missing Default Key for Strategy %s" % elem['strategy'])
            
            static_parameters = default[0]['static_parameters']
            static_parameters.update(elem['static_parameters'])
            key = {"client_id": '*', "parent_algo_id": '*', 'trader_id': '*', "place_id": '*'}                            
            key.update(elem["key"])           
            
            line = "strategy=" + elem['strategy'] + "|"
            
            line += "key={client_id},{trader_id},{place_id},{parent_algo_id}|".format(**key)
            for (k, v) in static_parameters.iteritems():
                line += str(k) + "=" + str(v) + "|"
        print line
        f.write(line + "\n")      
        
    f.close()
    


print "Starting Run"
export_parameter_manager('PARFLTLAB02', 27017, 'test_db_param_manager.kcdb')
print "Ended Run"
                
            
    
    