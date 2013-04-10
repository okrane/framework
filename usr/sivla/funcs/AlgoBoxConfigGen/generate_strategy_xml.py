from simep.funcs.dbtools.connections import Connections
import sys
from xml.dom.minidom import Document



if __name__ == '__main__':    
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
                        s.algo_id = t.algo_id
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
            param.setAttribute('priority', p[3])
            t.appendChild(param)
        templates.appendChild(t)
    
    
    ## add all to the file    
    configuration.appendChild(scripts)
    configuration.appendChild(tactics)
    configuration.appendChild(algos)
    configuration.appendChild(templates)        
        
    file = open(sys.argv[1], 'w')
    print 'Writing XML File to %s' % sys.argv[1]
    doc.writexml(file, indent = "  ", newl = '\n', addindent = "  ")
        