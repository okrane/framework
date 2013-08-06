from types import *
from datetime import datetime
import serialize
import simplejson
from lib.logger import *
from lib.io.toolkit import get_traceback
import os
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

class FixTranslator(object):
    def __init__(self, file_transcriptor=None, ignore_tags = []):
        if file_transcriptor is None:
            self.file = os.path.join(os.path.dirname(__file__), 'fix_types.json')
            
        file        = open(self.file, 'r')
        content     = file.read()
        file.close()
        
        my_json             = simplejson.loads(content)
        self.source         = my_json['fix']['fields']['field']
        self.mapping        = {}
        self.ignore_tags    = ignore_tags
        
    def translate_tag(self, value):
        """Two way translator tag fix <-> name
        @param value: either a tagfix Name or a tagfix number, for number, str or int are accepted"""
        value = str(value)
        if value in self.mapping.keys():
            return self.mapping[value]
        try:
            value       = str(int(value))
            key         = 'number'
            value_type  = 'name'
        except ValueError:
            key         = 'name'
            value_type  = 'number'
            
        for tag in self.source:
            if tag[key] == value:
                self.mapping[value]             = tag[value_type]
                self.mapping[tag[value_type]]   = value
                return tag[value_type]
        logging.error('This tag %s is not valid' %value) 
        return ''
    def line_translator(self, line):
        line                = line.rsplit('|')
        dict_order          = {}
        last_correct_date   = None
        for item in line:
            item = item.rsplit('=')
            if len(item) == 2:
                if item[1] != ' ' and item[1] != '':
                    if int(item[0]) not in self.ignore_tags:  
                              
                        nameD = self.translate_tag(item[0])
                        typeD = self.translate_tag(nameD)

                        if typeD != 'UTCTIMESTAMP':
                            temp =convert_str(item[1])
                            if temp is None:
                                logging.warning("This variable is equal to None : "+ str(nameD))
                            else:
                                dict_order[nameD] = temp
                        else:
                            try:
                                data = datetime.strptime(item[1], '%Y%m%d-%H:%M:%S')
                                dict_order[nameD] = data.replace(tzinfo=pytz.timezone('UTC'))
                                last_correct_date = data
                            except ValueError, e:
                                try :
                                    #sometimes instead of getting a date at this format, we get only the time : '%H:%M:%S'
                                    data = datetime.strptime(last_correct_date.strftime("%Y%m%d") + "-" +item[1], '%Y%m%d-%H:%M:%S')
                                    dict_order[nameD] = data.replace(tzinfo=pytz.timezone('UTC'))
                                    logging.warning("This datetime: "+str(item[1]) +"has been found to UTC ")
                                except:
                                    logging.warning("An order has been removed")
                                    continue  
        return dict_order 
                        
    def pretty_print_jsonlike(self, a_string, to_print=True):
        lines = a_string.split('\n')
        s = ''
        for line in lines:
            s+='---------------------------------------\n'
            s += self._pretty_print_jsonlike(line)
        if to_print:
            print s
        return s
            
    def _pretty_print_jsonlike(self, line, to_print=True):
        dic = self.line_translator(line)
        sorted_keys = sorted(dic.keys())
        s = ''
        for el in sorted_keys:
            s += el + ' = ' + str(dic[el]) + '\n'
        return s
    
    def pretty_print_csv(self, a_string, sep = ';', to_print=True):
        lines = a_string.split('\n')
        keys  = []
        l_dic = []
        for line in lines:
            dic = self.line_translator(line)
            l_dic.append(dic)
            for el in dic.keys():
                if el not in keys:
                    keys.append(el)
                    
        s_keys = sorted(keys)
        
        s = sep.join(s_keys) + '\n'
        for dic in l_dic:
            l = []
            for key in s_keys:
                if key in dic.keys():
                    to_add = dic[key]
                else:
                    to_add = ''
                s += str(to_add) + sep
                l.append(str(to_add))
            s += '\n'
        if to_print:
            print s
        return s
            
class Converter:
    def __init__(self, list_of_maps, out_file = 'types_to_add.json', required_tag = 'required'):
        self.accepted_date_formats = ["%Y-%m-%d %H:%M:%S", "%Y%m%d"]
        self.to_add     = [] 
        self.out_file   = out_file
        self.unmapped   = list_of_maps
        self.map        = {}
        self.required_tag = required_tag
        self.missing    = []
        
        self._populate_required()
    def flush(self):
        self.missing    = []   
    def _populate_required(self):
        self.required = []
        for el in self.unmapped:
            if self.required_tag in el.keys() and el[self.required_tag]:
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
            if el not in new_dict.keys() or (el in new_dict.keys() and new_dict[el] == ''):
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

            