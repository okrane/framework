# -*- coding: utf-8 -*-
import os, os.path
import imp
import pydoc
from inspect import getmembers, isfunction

def to_wiki_string(folder, output_file):
    output_text = ""
    x = pydoc.Doc()
    for root, _, files in os.walk(folder):        
        output_text += "== %s == \n" % root
        for f in files:            
            fullpath = os.path.join(root, f)            
            if fullpath[-3:] == ".py" and f != '__init__.py':                
                print f
                output_text += "=== %s === \n" % f
                foo = imp.load_source('%s' % f, fullpath)
                functions_list = [o for o in getmembers(foo) if isfunction(o[1])]
                print functions_list
                for func in functions_list:
                    output_text += func[0] + "\n"
                    output_text += func[1].__doc__ if func[1].__doc__ else '' + '\n'
                    
                #a = pydoc.help(foo)                
                #
                
    ff = open(output_file, 'w');
    ff.write(output_text)
    ff.close()
                
if __name__ == "__main__":          
    to_wiki_string('C:\st_sim\lib', "C:\Repository\manual_data_files\output.wiki")