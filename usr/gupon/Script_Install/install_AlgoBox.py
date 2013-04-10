'''
Created on 9 juil. 2012

@author: gupon
'''

import os
import sys
import shutil
from simep.funcs.dbtools.connections import Connections
import ConfigParser_NL
import random
import datetime
from xml.dom.minidom import Document

def generate_db_config(path):
    '''
        generate file db.ini in 'dev' mode. This file is replacing the database for the default parameters of strategies
        Indeed the file itself is generated from the database GANDALF
    '''
    
    #out_fic = open('C:/Documents and Settings/gupon/db.ini','w')
    out_fic = open(path+'AlgoBox/AlgHoster/cfg/db.ini','w')
    
    query = """
        select * from market..application_section order by section_name
    """
    
    sections = Connections.exec_sql('GANDALF', query)
    for items in sections:
        query = """
            select * from market..application_variable where section_id = %s
        """ %str(items[0])
        temp_dat = Connections.exec_sql('GANDALF', query)
        
        if len(temp_dat)>0:
            out_fic.write("[CATS.%s]\n"%items[1])
            for items2 in temp_dat:
                out_fic.write("%s=%s\n"%(items2[1],str(items2[7])))
                
            out_fic.write("\n")

def generate_strategy_xml(path):
    '''
        generate a xml file where are recorded the available strategies.
    '''
    doc = Document()
    configuration = doc.createElement('configuration')
    doc.appendChild(configuration)
    # -----------------------------------------------
    # ----------------- Delegates -------------------
    # -----------------------------------------------
    delegates = doc.createElement("delegates")
    d = doc.createElement('delegate')
    d.setAttribute('name', 'LUA')
    d.setAttribute('plugin', 'DelegateLua.so')
    delegates.appendChild(d)
    
    d = doc.createElement('delegate')
    d.setAttribute('name', 'PYTHON')
    d.setAttribute('plugin', 'DelegatePython.so')
    delegates.appendChild(d)
    
    d = doc.createElement('delegate')
    d.setAttribute('name', 'CPP')
    d.setAttribute('plugin', 'DelegateCpp.so')
    delegates.appendChild(d)
    
    configuration.appendChild(delegates)
    
    # ------------------------------------------------
    # ----------------- Frameworks -------------------
    # ------------------------------------------------
    
    frameworks = doc.createElement('frameworks')
    
    f = doc.createElement('framework')
    f.setAttribute('name', 'lua')
    f.setAttribute('delegate', 'LUA')
    c = doc.createElement('component') 
    c.setAttribute('file', '${ALGHOSTER_LIB_PATH}/lua/framework.lua')
    f.appendChild(c)
    frameworks.appendChild(f)
    
    f = doc.createElement('framework')
    f.setAttribute('name', 'simep')
    f.setAttribute('delegate', 'PYTHON')
    c = doc.createElement('component') 
    c.setAttribute('file', '${ALGHOSTER_LIB_PATH}/pydelegate')
    f.appendChild(c)
    frameworks.appendChild(f)
    
    f = doc.createElement('framework')
    f.setAttribute('name', 'cpp')
    f.setAttribute('delegate', 'CPP')
    c = doc.createElement('component') 
    c.setAttribute('file', 'none')
    f.appendChild(c)
    frameworks.appendChild(f)
    
    configuration.appendChild(frameworks)
    
    # ------------------------------------------------
    # ----------------- Queries ----------------------
    # ------------------------------------------------
    
    algos   = doc.createElement('algos')
    
    ## For Scripts ##
    query_scripts = """ select s.algo_name, t.tactic_type, t.tactic_script 
                        from dash_strategy_algo s, dash_strategy_algo_tvfo t
                        where s.algo_type = 'tvfo' and
                        s.algo_id = t.algo_id
                    """
    
    script_list = Connections.exec_sql('GANDALF', query_scripts)
    
    scripts = doc.createElement('scripts')
    for e in script_list:
        s = doc.createElement('script')
        s.setAttribute('name', e[0].lower())
        s.setAttribute('framework', e[1])
        comp = doc.createElement('component')
        comp.setAttribute('file', e[2])
        s.appendChild(comp)
        scripts.appendChild(s)
        
        algo = doc.createElement('algo')
        algo.setAttribute('name', e[0])
        algo.setAttribute('tactic', 'tvfo')
        algo.setAttribute('script', e[0].lower())
        algos.appendChild(algo)
        
    ## For tactics ##
    tactics = doc.createElement('tactics')
    query_tactics = """select s.algo_name, t.tactic_type, t.tactic_plugin
                        from dash_strategy_algo s, dash_strategy_algo_tactic t
                        where s.algo_type = 'tactic' and
                        s.algo_id = t.algo_id and s.algo_name != 'MarketOrder' and s.algo_name != 'IcebergMono'
                    """
                    
    tactic_list = Connections.exec_sql('GANDALF', query_tactics)    
    for e in tactic_list:
        t = doc.createElement('tactic')
        t.setAttribute('name', e[0].lower())
        t.setAttribute('type', e[1])
        t.setAttribute('plugin', e[2])
        tactics.appendChild(t)
        
        algo = doc.createElement('algo')
        algo.setAttribute('name', e[0])
        algo.setAttribute('tactic', e[0].lower())
        algos.appendChild(algo)
    
    t = doc.createElement('tactic')
    t.setAttribute('name', 'tvfo')
    t.setAttribute('type', 'TVFO')
    t.setAttribute('plugin', 'tvfo.so')
    tactics.appendChild(t)
    
    ## For templates ##
    templates = doc.createElement('templates') 
    query_templates = """
                    select t.algo_template_id, s.algo_name, k.algo_name
                    from dash_strategy_algo s, dash_strategy_algo_template t, dash_strategy_algo k
                    where s.algo_type = 'template'
                    and t.algo_id = s.algo_id
                    and k.algo_id = t.target_algo_id
                    order by t.algo_template_id
                    """
    
    query_params = """ 
                select algo_template_id, param_name, param_value, param_priority 
                from dash_strategy_algo_param
                order by algo_template_id                
                """                
    
    template_list = Connections.exec_sql('GANDALF', query_templates)
    param_list    = Connections.exec_sql('GANDALF', query_params)
    
    for e in template_list:
        t = doc.createElement('template')
        t.setAttribute('name', e[1])
        t.setAttribute('algo', e[2])
        for p in [k for k in param_list if k[0] == e[0]]:
            param = doc.createElement('param')
            param.setAttribute('name', p[1])
            param.setAttribute('value', p[2])
            if p[3] != 'default':
                param.setAttribute('priority', p[3])
            t.appendChild(param)
        templates.appendChild(t)
    
    
    ## add all to the file    
    configuration.appendChild(scripts)
    configuration.appendChild(tactics)
    configuration.appendChild(algos)
    configuration.appendChild(templates)        
        
    fic = open(path + "AlgoBox/AlgHoster/bin/strategies.xml", 'w')
    doc.writexml(fic, indent = "  ", newl = '\n', addindent = "  ")

def choose_number(mini, maxi, list_const):
    # random number (integer) generator given a range and a list in which the choosen number might not be in 
    
    numb = int(round(random.random()*(maxi-mini) + mini))
    
    while numb in list_const:
        numb = int(round(random.random()*(maxi-mini) + mini))
        
    return numb

def create_arborescence(home_path):
    '''
        creation of the appropriate arborescence in the specified root directory and copy the reference files in the good folders 
    '''
    full_path = "%sAlgoBox"%home_path
    
    if os.path.exists(full_path):
        
        print "Il y a deja une algobox installee dans ce repertoire !\n Voulez-vous ecraser la precedente version ? y or n"
        cont = raw_input(">")
        if cont == "n":
            return 0
        else:
            shutil.rmtree(full_path)
            
            os.makedirs(full_path)
            
            os.makedirs(full_path + "/AlgoCore")
            os.makedirs(full_path + "/AlgoCore/bin")
            os.makedirs(full_path + "/AlgoCore/cfg")
            os.makedirs(full_path + "/AlgoCore/data")
            os.makedirs(full_path + "/AlgoCore/logs")
            os.makedirs(full_path + "/AlgoCore/data/algos")
            os.makedirs(full_path + "/AlgoCore/data/clients")
        
            os.makedirs(full_path + "/AlgHoster")
            os.makedirs(full_path + "/AlgHoster/MMA")
            os.makedirs(full_path + "/AlgHoster/bin")
            os.makedirs(full_path + "/AlgHoster/data")
            os.makedirs(full_path + "/AlgHoster/lib")
            os.makedirs(full_path + "/AlgHoster/data/algos")
            os.makedirs(full_path + "/AlgHoster/data/clients")
            os.makedirs(full_path + "/AlgHoster/data/sdr")
            os.makedirs(full_path + "/AlgHoster/data/strategy")
            os.makedirs(full_path + "/AlgHoster/logs")
            os.makedirs(full_path + "/AlgHoster/scripts")
    else:
        os.makedirs(full_path)
            
        os.makedirs(full_path + "/AlgoCore")
        os.makedirs(full_path + "/AlgoCore/bin")
        os.makedirs(full_path + "/AlgoCore/cfg")
        os.makedirs(full_path + "/AlgoCore/data")
        os.makedirs(full_path + "/AlgoCore/logs")
        os.makedirs(full_path + "/AlgoCore/data/algos")
        os.makedirs(full_path + "/AlgoCore/data/clients")
    
        os.makedirs(full_path + "/AlgHoster")
        os.makedirs(full_path + "/AlgHoster/MMA")
        os.makedirs(full_path + "/AlgHoster/bin")
        os.makedirs(full_path + "/AlgHoster/data")
        os.makedirs(full_path + "/AlgHoster/lib")
        os.makedirs(full_path + "/AlgHoster/data/algos")
        os.makedirs(full_path + "/AlgHoster/data/clients")
        os.makedirs(full_path + "/AlgHoster/data/sdr")
        os.makedirs(full_path + "/AlgHoster/data/strategy")
        os.makedirs(full_path + "/AlgHoster/logs")
        os.makedirs(full_path + "/AlgHoster/scripts")
    
    #copie des dico depuis le repertoire de prod
    tempAlgo_path ="/projet/Linux2.6"
    
    shutil.copy(tempAlgo_path + "/AlgoCore/bin/algocore", full_path + "/AlgoCore/bin")
    #shutil.copy(tempAlgo_path + "/AlgoCore/bin/custom.lua", full_path + "/AlgoCore/bin")
    #shutil.copytree(tempAlgo_path + "/AlgoCore/bin/CLIENT", full_path + "/AlgoCore/bin/CLIENT")
    #shutil.copy(tempAlgo_path + "/AlgoCore/cfg/SLEEP.xml", full_path + "/AlgoCore/cfg")
    
    shutil.copy(tempAlgo_path + "/AlgoCore/cfg/CSAM.xml", full_path + "/AlgoCore/cfg")
    shutil.copy(tempAlgo_path + "/AlgoCore/cfg/ISAM.xml", full_path + "/AlgoCore/cfg")
    shutil.copy(tempAlgo_path + "/AlgoCore/cfg/FIX42.xml", full_path + "/AlgoCore/cfg")
    
    shutil.copy(tempAlgo_path + "/AlgHoster/bin/alghoster", full_path + "/AlgHoster/bin")
    
    tempAlgo_path ="/home/team13/gupon/AlgoBox"
    
    shutil.copytree("/common/packages/QRTactic/1.1.9", full_path + "/AlgHoster/scripts/python")
    shutil.copytree(tempAlgo_path + "/AlgHoster/scripts/lua", full_path + "/AlgHoster/scripts/lua")
    shutil.copy(tempAlgo_path + "/AlgHoster/data/sdr/Referential_Cheuvreux.db", full_path + "/AlgHoster/data/sdr/")
    
#    if mode == 'dev':
#        os.makedirs(full_path + "/AlgHoster/cfg")
    
def create_algocore(username, hostname, path):
    
    query = """
        select 
        AlgoCore_Name, 
        FE_AcceptPortName, 
        DTP_AcceptPortName, 
        PONG_Replication_ListenPort,
        FE_ConnectPortName,
        FE_ConnectPortName_AlgHoster 
        from AlgoCore where user_name = '%s' order by AlgoCore_Name
    """%username
    
    user_AlgoCore = Connections.exec_sql('GANDALF', query)
    
    if len(user_AlgoCore) == 0:
        iter_AlgoCore =1
    else:
        last__AlgoCore = user_AlgoCore[len(user_AlgoCore)-1][0]
        iter_AlgoCore = int(last__AlgoCore[-1])+1
    
    AlgoCore_name = "AlgoCore_" + username + str(iter_AlgoCore)
    Trace_FileName = AlgoCore_name + ".log"
    FE_Sender_to_Hoster = AlgoCore_name + "_Hoster"
    
    list_FE_AcceptPortName =[]
    list_DTP_AcceptPortName =[]
    list_PONG_Replication_ListenPort =[]
    list_FE_ConnectPortName = []
    list_FE_ConnectPortName_AlgHoster = []
    
    for i in range(len(user_AlgoCore)):
        list_FE_AcceptPortName.append(user_AlgoCore[i][1])
        list_DTP_AcceptPortName.append(user_AlgoCore[i][2])
        list_PONG_Replication_ListenPort.append(user_AlgoCore[i][3])
        list_FE_ConnectPortName.append(user_AlgoCore[i][4])
        list_FE_ConnectPortName_AlgHoster.append(user_AlgoCore[i][5])
    
    portname1 = choose_number(1000, 9999, list_FE_AcceptPortName)
    portname2 = choose_number(1000, 9999, list_DTP_AcceptPortName)
    portname3 = choose_number(1000, 9999, list_PONG_Replication_ListenPort)
    portname4 = choose_number(1000, 9999, list_FE_ConnectPortName)
    portname5 = choose_number(1000, 9999, list_FE_ConnectPortName_AlgHoster)
    
    query = """
        INSERT INTO AlgoCore (AlgoCore_Name,user_name,FE_AcceptPortName,DTP_AcceptPortName,PONG_Replication_ListenPort,FE_ConnectHostName,FE_ConnectPortName,Trace_FileName,FE_SenderCompID,FE_ConnectPortName_AlgHoster,FE_Connect_HostName_AlgHoster,Path) VALUES (
        '%s',
        '%s',
        %d,
        %d,
        %d,
        '%s',
        %d,
        '%s',
        '%s',
        %d,
        '%s',
        '%s')
    """%(AlgoCore_name, username, portname1, portname2, portname3, hostname, portname4, Trace_FileName, AlgoCore_name, portname5, FE_Sender_to_Hoster, path)
    
    Connections.exec_sql('GANDALF', query)
    
    return AlgoCore_name

def create_alghoster(username, hostname, nameAC):
    
    query = """
        select * from AlgoCore where AlgoCore_Name = '%s'
    """%nameAC
    
    data_AlgoCore = Connections.exec_sql('GANDALF', query)
    
    query = """
        select * from AlgHoster  order by id desc
    """
    
    data_AlgHoster = Connections.exec_sql('GANDALF', query)
    
    if len(data_AlgHoster) == 0:
        iter_AlgHoster = 1
    else:
        iter_AlgHoster = int(data_AlgHoster[0][0]) +1
    
    AlgHoster_name = "AlgHoster" + str(iter_AlgHoster)
    
    db_server = 'GANDALF'
    db_user = 'bret'
    db_password = 'bret90'
        
    FE_ConnectPortName = int(data_AlgoCore[0][3])
    FE_ConnectHostName = data_AlgoCore[0][11]
    FE_TargetCompID = data_AlgoCore[0][11]
    FE_SenderCompID = AlgHoster_name + "to" + nameAC
    
    query = """
        insert into AlgHoster (AlgoCore_Name,AlgHoster_Name,user_name,db_server,db_user,db_password,FE_ConnectPortName,FE_ConnectHostName,FE_TargetCompID, FE_SenderCompID) values (
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        %d,
        '%s',
        '%s',
        '%s'
        )
    """%(nameAC, AlgHoster_name, username, db_server, db_user, db_password, FE_ConnectPortName, FE_ConnectHostName, FE_TargetCompID, FE_SenderCompID)
    
    Connections.exec_sql('GANDALF', query)
    
    return AlgHoster_name

def generate_strategy_list():
    
    list_strat = ""
    
    query_strategies = """select s.algo_name, t.tactic_type, t.tactic_plugin
                        from dash_strategy_algo s, dash_strategy_algo_tactic t
                        where s.algo_type = 'tactic' and
                        s.algo_id = t.algo_id and s.algo_name != 'MarketOrder' and s.algo_name != 'IcebergMono' """
    
    tactic_list = Connections.exec_sql('GANDALF', query_strategies)
    
    for strat in tactic_list[:-1]:
        list_strat = list_strat + "Strategy." + strat[0] + ","
    
    list_strat = list_strat + "Strategy." + tactic_list[len(tactic_list)-1][0]
    
    return list_strat

def generate_algocoreini(config, path, now):
    
    full_path = path + 'AlgoBox/AlgoCore/bin/algocore.ini'
    
    if os.path.exists(full_path):
        new_path = path + 'AlgoBox/AlgoCore/bin/algocore.' + now + '.ini'
        os.rename(full_path, new_path)
    
    conf_algocore = open(path + 'AlgoBox/AlgoCore/bin/algocore.ini', 'w')
    
    for dictio in config._sections:
        aux = config._sections[dictio]
        conf_algocore.write("[" + dictio + "]\n")
        
        for key in aux:
            if key != "__name__":
                conf_algocore.write(key + "=" + str(aux[key]) + "\n")
                
        conf_algocore.write("\n")
        
    conf_algocore.close()

def generate_alghosterini(config, path, now):
    
    full_path = path + 'AlgoBox/AlgHoster/bin/alghoster.ini' 
    
    if os.path.exists(full_path):
        new_path = path + 'AlgoBox/AlgHoster/bin/alghoster.' + now + '.ini' 
        os.rename(full_path, new_path)
    
    conf_alghoster = open(path + 'AlgoBox/AlgHoster/bin/alghoster.ini', 'w')
    
    for dictio in config._sections:
        aux = config._sections[dictio]
        conf_alghoster.write("[" + dictio + "]\n")
        
        for key in aux:
            if key != "__name__":
                conf_alghoster.write(key + "=" + str(aux[key]) + "\n")
                
        conf_alghoster.write("\n")
        
    conf_alghoster.close()

def update_algocore(AlgoCore_name):
    
    template = "algocore.ini"
    
    #    list_sections = ["AlgHoster", ]
    
    config = ConfigParser_NL.RawConfigParser()
    config.read(template)
    
    query  = """
        select * from AlgoCore where AlgoCore_Name = '%s'
    """%AlgoCore_name
    
    data = Connections.exec_sql('GANDALF', query)
    
    config._sections['AlgoCore']['FE_AcceptPortName'] = data[0][3]
    config._sections['DTP']['AcceptPortName'] = data[0][4]
    config._sections['Persistency']['PONG_Replication_ListenPort'] = data[0][5]
    config._sections['PONG_SLAVE']['FE_ConnectHostName'] = data[0][6]
    config._sections['PONG_SLAVE']['FE_ConnectPortName'] = data[0][7]
    config._sections['Config.Trace.AlgoCore']['Trace_FileName'] = data[0][8]
    config._sections['Session.User.DEFAULT']['FE_SenderCompID'] = data[0][9]
    config._sections['Session.AlgHoster.DEFAULT']['FE_SenderCompID'] = data[0][11]
    config._sections['Session.AlgHoster.AlgHoster_Acceptor']['FE_SenderCompID'] = data[0][11]
    config._sections['Session.AlgHoster.AlgHoster_Acceptor']['FE_ConnectHostName'] = data[0][6]
    config._sections['Session.AlgHoster.AlgHoster_Acceptor']['FE_ConnectPortName'] = data[0][10]
    

    return config

def update_alghoster(AlgHoster_name, hostname, username):
    
    template = "alghoster.ini"
    
    #    list_sections = ["AlgHoster", ]
    
    config = ConfigParser_NL.RawConfigParser()
    config.read(template)
    
    query  = """
        select * from AlgHoster where AlgHoster_Name = '%s'
    """%AlgHoster_name
    
    strat_list = generate_strategy_list()
    
    data = Connections.exec_sql('GANDALF', query)

    config._sections['AlgHoster']['conf_user'] = data[0][3]
    config._sections['AlgHoster']['db_server'] = data[0][4]
    config._sections['AlgHoster']['db_user'] = data[0][5]
    config._sections['AlgHoster']['db_password'] = data[0][6]
    
    config._sections['AlgHoster']['StrategyList'] = strat_list
    config._sections['AlgHoster.SESSION']['FE_ConnectPortName'] = data[0][7]
    config._sections['AlgHoster.SESSION']['FE_ConnectHostName'] = hostname
    config._sections['AlgHoster.SESSION']['FE_TargetCompID'] = data[0][8]
    config._sections['AlgHoster.SESSION']['FE_SenderCompID'] = data[0][10]
    config._sections['DTPServerSLE']['FE_ConnectHostName'] = hostname
    config._sections['MarketAccess']['MA_ConnectionUser'] = username
    config._sections['Config.Trace.AlgHoster']['Trace_FileName'] = "AlgHoster_" + username + ".log"
    
    return config

def generate_users_file(path, username, now):
    
    full_path = path + 'AlgoBox/AlgoCore/cfg/users.ini'
    if os.path.exists(full_path):
        new_path = path + 'AlgoBox/AlgoCore/cfg/users.' + now + ".ini"
        os.rename(full_path, new_path)
    
    user_file = open(full_path, "w")
    
    user_file.write("[users]\n\n")
        
    user_file.write(username+"=:csam:trading:\n")

    user_file.close()

def generate_lances(path):
    
    # generation du lance de l'algocore
    fic = open(path + 'AlgoBox/AlgoCore/bin/lance','w')
    
    fic.write("#!/bin/ksh\n\n")
    
    fic.write("_HERE_WE_ARE=" + path + "AlgoBox/AlgoCore\n")
    fic.write("_sqlite=/home/team03/common03/local/Linux2.6/bin/sqlite3\n")
    fic.write("_CFG_FILE=$_HERE_WE_ARE/bin/algocore.ini\n")
    fic.write("_BIN=$_HERE_WE_ARE/bin/algocore\n")
    fic.write("export ROOT_PATH=$_HERE_WE_ARE\n")
    fic.write("export ALGOCORE_CONF_PATH=$_HERE_WE_ARE/bin/CLIENT\n")
    fic.write("export LD_LIBRARY_PATH=/$PATCH/common/lib/sqlite/:$LD_LIBRARY_PATH\n")
    fic.write("$_BIN -f $_CFG_FILE &\n")
    fic.close()
    
    #generation du lance de l'alghoster
    fic = open(path + 'AlgoBox/AlgHoster/bin/lance','w')
    fic.write("#!/bin/ksh\n\n")
    fic.write("export ROOT_PATH=" + path + "AlgoBox/AlgHoster\n")
    fic.write("export DICO_ROOT_PATH=" + path + "AlgoBox/AlgoCore\n")
    fic.write("BIN=${ROOT_PATH}/bin\n")
    fic.write("ALGHOSTER_SCRIPT_PATH=" + path + "AlgoBox/AlgHoster/scripts\n")
    fic.write("export ALGHOSTER_LIB_PATH=/projet/Linux2.6/AlgHoster/lib/\n")
    fic.write("export SYBASE=/tools/sybase/sybase_15_32bits/\n")
    fic.write("export LD_LIBRARY_PATH=$ALGHOSTER_LIB_PATH:$ALGHOSTER_LIB_PATH/pydelegate/libs\n")
    fic.write("echo $LD_LIBRARY_PATH\n")
    fic.write("export PYTHONHOME=/home/team03/common03/packages/External/python-2.6.5/\n")
    fic.write("ldd $ROOT_PATH/bin/alghoster\n")
    fic.write("cd $ROOT_PATH\n")
    fic.write("ln -s '/projet/Linux2.6/AlgHoster/lib' 'lib'")
    fic.write("cat $ROOT_PATH/data/sdr/queries.sql | sqlite3 $ROOT_PATH/data/sdr/Referential_Cheuvreux.db\n")
    fic.write("$BIN/alghoster -f $BIN/alghoster.ini &\n")
    fic.close()
    
    #generation du start_all
    fic = open(path + 'AlgoBox/start_all','w')
    fic.write("cd " + path +"/AlgoBox/AlgoCore/bin/\n")
    fic.write("chmod +x lance\n")
    fic.write("./lance\n")
    fic.write("sleep 5\n")
    fic.write("cd " + path +"/AlgoBox/AlgHoster/bin/\n")
    fic.write("chmod +x lance\n")
    fic.write("./lance\n")
    fic.close()
    
def config_sle_parameter(path, username):
    
    fic=open(path + 'AlgoBox/AlgHoster/data/sdr/queries.sql','w')
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username +"',1,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',3,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',4,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',5,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',6,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',7,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',13,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',10,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',14,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',15,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',16,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',17,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',19,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',20,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',21,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',22,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',24,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',26,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',27,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',28,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',30,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',37,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',39,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',40,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',41,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',50,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',58,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',60,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',62,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',64,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',65,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',67,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',68,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',69,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',70,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',71,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',72,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',73,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',74,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',75,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',76,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',77,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',78,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',79,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',81,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',86,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',88,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',89,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',90,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',91,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',92,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',93,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    fic.write("INSERT INTO sleparameteritem (userName, marketId, mmaName, port, host, userId, sleId, needPasswd, isDefault, dicoServer, sorConnected, serverId) VALUES('" + username + "',94,'FixCAC','1713','thcor00w','4070','CORE_HC3',0,1,'2713',1,58);\n")
    
    fic.close()
    
if __name__ == '__main__':
    
#    query ="""
#        DROP TABLE AlgoCore"""
#    Connections.exec_sql('GANDALF', query)
#    
#    query = """
#        CREATE TABLE AlgoCore (
#        id INT PRIMARY KEY NOT NULL,
#        AlgoCore_Name VARCHAR(100),
#        user_name VARCHAR(100),
#        FE_AcceptPortName INT,
#        DTP_AcceptPortName INT,
#        PONG_Replication_ListenPort INT,
#        FE_ConnectHostName VARCHAR(100),
#        FE_ConnectPortName INT,
#        Trace_FileName VARCHAR(100),
#        FE_SenderCompID VARCHAR(100),
#        FE_ConnectPortName_AlgHoster INT,
#        FE_Connect_HostName_AlgHoster VARCHAR(100),
#        Path VARCHAR(100)
#        )    
#    """
#    
#    query = """
#        CREATE TABLE AlgHoster (
#        id INT identity NOT NULL,
#        AlgoCore_Name VARCHAR(100),
#        AlgHoster_Name VARCHAR(100),
#        user_name VARCHAR(100),
#        db_server VARCHAR(100),
#        db_user VARCHAR(100),
#        db_password VARCHAR(100),
#        FE_ConnectPortName INT,
#        FE_ConnectHostName VARCHAR(100),
#        FE_TargetCompID VARCHAR(100),
#        FE_SenderCompID VARCHAR(100)
#        )    
#    """
#   
#    Connections.exec_sql('GANDALF', query)
#    
#    INSERT INTO AlgHoster VALUES (2,'AlgoCore1','AlgHoster2','gupon','GANDALF','bret','bret90',2778,'padev282','AlgoCore1_gupon','AlgHoster2_gupon')
#    insert into AlgoCore values (1,'AlgoCore_gupon1','gupon',2777,7778,7775,'AlgoCore1',2779,'AlgoCore1_gupon.log','AlgoCore1_gupon',7773,'padev282','C:/')

    
    username = sys.argv[1]
    hostname = sys.argv[2]
    path     = sys.argv[3]
#    mode     = sys.argv[4]
#    mode = str.lower(mode)
    
#    if mode != "dev" and mode != "homolo":
#        sys.exit( "Le mode specifie n'est pas correct. Attendu : dev ou homolo")

    now = datetime.datetime.now()
    now.toordinal()
    now = now.replace(microsecond = 0)
    now = str(now)
    
    now = now.replace(":","")
    now = now.replace(" ","")
    
    create_arborescence(path)
    
    generate_strategy_xml(path)
    
    nameAC = create_algocore(username,hostname,path)
    
    config = update_algocore(nameAC)
    generate_algocoreini(config, path, now)
    
    nameAH = create_alghoster(username, hostname, nameAC)
    config = update_alghoster(nameAH, hostname, username)
    generate_alghosterini(config, path, now)
    
#    if mode == 'dev':
#        generate_db_config(path)
        
    generate_users_file(path, username, now)
    
    generate_lances(path)
    
    config_sle_parameter(path, username)
    