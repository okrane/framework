import lib.io.smart_converter as converter
from lib.io.serialize import *
import simplejson
from datetime import datetime
import types
import os

def test_converter():
    script_dir = os.path.dirname(__file__)
    
    file     = open(script_dir + 'orders.json', 'r')
    input    = file.read()
    file.close()
    
    dict_list= simplejson.loads(input, object_hook = as_datetime)
    
    file = open(script_dir + 'fix_types.json', 'r')
    input = file.read()
    file.close()

    list_map = simplejson.loads(input)["fix"]["fields"]["field"]
    
    file = open('enrichment_types.json', 'r')
    input = file.read()
    file.close()
    
    list_map.extend(simplejson.loads(input))
    
    conv     = converter.Converter(list_map)
    
    for dict in dict_list:
        new_dict = {}
        for el in dict:
            new_dict[el] = conv.convert(el, dict[el])
            my_type = conv._map(el)
            assert isinstance(new_dict[el], my_type)
  

if __name__== "__main__":
    test_converter()