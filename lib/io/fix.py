import argparse
import types
from types import *
from datetime import datetime
import logging
import simplejson
import os, sys
logging.getLogger("paramiko").setLevel(logging.WARNING)

def convert_str(s, date_format = ["%Y-%m-%d %H:%M:%S", "%Y%m%d-%H:%M:%S"]):
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
            for format in date_format:       
                try:
                    ret = datetime.strptime(s, format)
                    continue
                except ValueError:
                    ret = s
    return ret  

class FixTranslator(object):
    default_ignore_tags = [8, 21, 22, 9, 34, 49, 56, 10, 47, 369]
    def __init__(self, file_transcriptor=None, ignore_tags = None):
        if file_transcriptor is None:
            self.file = os.path.join(os.path.dirname(__file__), 'fix_types.json')
            
        file        = open(self.file, 'r')
        content     = file.read()
        file.close()
        
        my_json             = simplejson.loads(content)
        self.source         = my_json['fix']['fields']['field']
        self.mapping        = {}
        
        if ignore_tags is None:
            self.ignore_tags    = FixTranslator.default_ignore_tags
        else:
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
                            temp = convert_str(item[1])
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
        lines = a_string.replace('\r','').split('\n')
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
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Translate a fix string or file (not a binary file)')
    parser.add_argument('String_To_Translate_Or_File_To_Translate', type=types.StringType)
    parser.add_argument('-f', '--file', help='Translate a file', action="store_true")
    parser.add_argument('-c', '--csv', help='Display a csv', action="store_true")
    parser.add_argument('-j', '--json', help='Display a json like format', action="store_true")
    parser.add_argument('-s', '--separator', help='Customize the separator for csv, by default it is ";"', default=";")
    args = parser.parse_args()

    ft = FixTranslator()
    separator = args.separator
    if args.separator in ['t', '\\t']:
        separator = '\t'
    elif args.separator in ['n', '\\n']:
        separator = '\n'
        
    string_to_read = args.String_To_Translate
    
    if args.file:
        file == args.String_To_Translate
        f = open(file, 'r')
        string_to_read = f.read()
        f.close()
    
    if args.json:
        s = ft.pretty_print_jsonlike(string_to_read, to_print = True)
    else:
        s = ft.pretty_print_csv(string_to_read, separator, to_print = True)


