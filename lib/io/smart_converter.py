from types import *
from datetime import datetime
import serialize
import simplejson
from lib.logger import *
from lib.io.toolkit import get_traceback

MAPPING = {"FLOAT"                  : FloatType,
           "INT"                    : IntType,
           "STRING"                 : StringType,
           "BOOLEAN"                : BooleanType,
           "UTCTIMESTAMP"           : datetime,
           "CHAR"                   : StringType,
           "LOCALMKTDATE"           : datetime,
           "QTY"                    : FloatType,
           "DATA"                   : StringType,
           "PRICE"                  : FloatType,
           "EXCHANGE"               : StringType,
           "CURRENCY"               : StringType,
           "MULTIPLEVALUESTRING"    : StringType
           }
        
def convert_str(s, date_format = "%Y-%m-%d %H:%M:%S"):
    '''
        Attempts to convert an element to int, 
        if failed it will attempt to convert to float
        if again failed it will return the item as it is.
        
        Use for converting numeric strings into int, float
    '''
        
    if s is None:
        return None
    try:
        ret = int(s)
    except ValueError:
        #Try float.
        try:
            ret = float(s)
        except ValueError:
            # Try datetime            
            try:
                import datetime
                ret = datetime.datetime.strptime(s, date_format)
            except ValueError:
                return s
    return ret  
class Converter:
    def __init__(self, list_of_maps, out_file = 'types_to_add.json'):
        
        self.accepted_date_formats = ["%Y-%m-%d %H:%M:%S", "%Y%m%d"]
        self.to_add     = [] 
        self.out_file   = out_file
        self.unmapped   = list_of_maps
        self.map        = {}
        self._populate_required()
        self.missing    = []
    def _populate_required(self):
        self.required = []
        for el in self.unmapped:
            if "required" in el.keys() and el["required"]:
                self.required.append(el["name"]) 
                   
    def _map(self, key):
        "Return the type of a given key"
        if key not in self.map:
            for el in self.unmapped:
                if el["name"] == key:
                    self.map[key] = MAPPING[el["type"]]
                    return self.map[key]
                
            self.to_add.append({"name" : key, "type" : "STRING"})
            self.map[key] = StringType
        return self.map[key]
        
    
    def _convert_to(self, type_to, value):
        if value is None:
            return None
        if type_to is datetime:
            if isinstance(value, datetime):
                return value
            for date_format in self.accepted_date_formats:       
                try:
                    ret = datetime.strptime(str(value), date_format)
                    return ret
                except ValueError:
                    pass
        elif isinstance(value, type_to):
            return value
        else:
            return type_to(value)
    
    def convert(self, key, value):
        try:
            if isinstance(self._map(key), ListType):
                for el in self._map(key):
                    try:
                        return self._convert_to(el, value)
                    except:
                        continue
            else:
                try:
                    to_return = self._convert_to(self._map(key), value)
                except ValueError:
                    to_return = str(value)
                    logging.error("Impossible to convert this entry(" + key + "): " + str(value) + " to " + str(self.map[key]))
                    get_traceback()
                    logging.warning("Will be kept as string")
                return to_return
                             
        except KeyError, e:
            import traceback
            import StringIO
            fp = StringIO.StringIO()
            traceback.print_exc(file = fp)
            logging.error(fp.getvalue())
            logging.exception("This key " + str(key) + " (= " + str(value) + ") is not specified in your input dict")
            return convert_str(value)
    def convert_dict(self, dict):
        """ 
        Take only the first key into account
        """
        if len(dict.keys()) == 0:
            logging.warning("Got an empty dict")
            return {}
        return self.convert(dict.keys()[0], dict[dict.keys()[0]])
    
    def verify(self, dict):
        new_dict = {}
        missing  = []
        for el in dict:
            new_dict[el] = self.convert(el, dict[el])
        for el in self.required:
            if el not in new_dict.keys():
                missing.append(el)
        if len(missing)>0:
            logging.error("Those keys (" + str(missing) + ") are missing in this dict: " + str(dict))
            self.missing.append({"MissedKey" : missing, "Dict" : new_dict})
        return new_dict
    
    def verify_all(self, dicts):
        new_list = []
        for dict in dicts:
            new_list.append(self.verify(dict))
            
        return new_list
    def add_json(self):
        list_of_dict = simplejson.dumps(self.to_add, separators=(',',':'), sort_keys=True, indent=4 * ' ')
            
        file = open(self.out_file, 'a')
        file.writelines(list_of_dict)
        file.close()
            