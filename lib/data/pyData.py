"""
    @author: Silviu
"""
from simep.funcs.excelerator import Workbook, parse_xls
#from simep.bin.simepcore import dateNum
from simep.funcs.stdio.utils import pyLog
import csv
from datetime import datetime
import types

class pyData:
    def __init__(self, mode, **kwargs):
        """
        From arrays:
            x = pyData('init', date=[1, 2, 3], value = {'price':[10, 10, 11], 'VOLUME':[110, 110, 111]})
            x = pyData('init', date=[1, 2, 3], value = [[10, 10, 11], [110, 110, 111]], colnames = {'price', 'VOLUME'})
        From XLS:    
            x = pyData('xls', filename='C:/st_sim/dev/usr/sivla/a.xls', sheet="Sheet1", columns=[1, 2], colnames = ['col1', 'col2'])
            x = pyData('xls', filename='C:/st_sim/dev/usr/sivla/a.xls', sheet="Sheet1", columns=[1, 2], colnames = 1)
            x = pyData('xls', filename='C:/st_sim/dev/usr/sivla/a.xls', sheet="Sheet1", columns=[1, 2], colnames = 1, rows = range(2, 4))
        From CSV:
            x = pyData('csv', filename = 'C:/st_sim/dev/usr/sivla/b.csv')
            x = pyData('csv', filename = 'C:/st_sim/dev/usr/sivla/b.csv', fieldnames = ['date', 'col1', 'col2'])
        """
        
        if mode == 'init':
            self.date = []
            self.value = {}
            self.info = {}
            
            if kwargs.has_key('value'):
                if isinstance(kwargs['value'], dict):
                    # kwargs is a dictionary: containing names and values, and probably dates as well                    
                    if kwargs['value'].has_key('date'):
                        self.date = kwargs['value']['date']                    
                    self.value = kwargs['value'];
                else:
                    # kwargs is a list: build value dictionary from names + value
                    if kwargs.has_key('colnames'):
                        for (key, val) in zip(kwargs['colnames'], kwargs['value']):
                            self.value[key] = val
                    else:
                        for (key, val) in zip(range(len(kwargs['value'])), kwargs['value']):
                            self.value[key] = val                                        
            else:
                print "pyData: no value array detected"
                            
            if kwargs.has_key('date'):
                self.date = kwargs['date']
            else:
                print "pyData: no date array detected"
            
            if kwargs.has_key('info'):
                self.info = kwargs['info']
            # sort by date            
            self.sort_by('date')
        
        if mode == 'xls':
            if not kwargs.has_key('filename'):
                print "Error: No filename found"
            if not kwargs.has_key('columns'):
                print "Error: No columns found"
            else:
                for index in range(len(kwargs['columns'])):
                    kwargs['columns'][index] -= 1
            
            self.value = {}                            
            data = parse_xls(kwargs['filename'])
            
            colnames = []
            if kwargs.has_key('colnames'):
                #if colnames are found in the .xls file, parse them first                
                if isinstance(kwargs['colnames'], int):                    
                    for sheet, elem in data:                        
                        if kwargs.has_key('sheet') and sheet == kwargs['sheet']:                           
                            for col in kwargs['columns']:                                
                                colnames.append(elem[(kwargs['colnames']-1 , col)])
                else:
                    colnames = kwargs['colnames']
                                
            else:
                colnames = kwargs['columns']
                
            for colname in colnames:
                    self.value[colname] = []
                                 
            for sheet, table in data:                
                if kwargs.has_key('sheet') and sheet == kwargs['sheet']:
                    # if specified rows take only the specified, else take all
                    if kwargs.has_key('rows'):                        
                        #assume columns is int, TODO: def xls_letter_colnames_to_index:
                        for row, col in [(x, y) for x in kwargs['rows'] for y in kwargs['columns']]:
                            self.value[colnames[col]].append(table[(row, col)])
                    else:                        
                        for coord in sorted(table.keys(), key = lambda x : x[0]):                                                        
                            if coord[1] in set(kwargs['columns']):                                                           
                                self.value[colnames[kwargs['columns'].index(coord[1])]].append(table[coord])
            self.date = range(0, len(self.value.keys()[0]))
            self.info = {'filename': kwargs['filename']}    
                                           
        if mode == 'csv':
            self.value = {}
            self.date = []
            if not kwargs.has_key('filename'):
                print  "Error: No filename detected"
            
            if not kwargs.has_key('fieldnames'):
                # if no fieldnames, consider the first element as fieldnames
                reader = csv.DictReader(open(kwargs['filename'], 'r'))
                fieldnames = reader.next()                
                for field in fieldnames:
                    self.value[field] = []
                    self.value[field].append(convertStr(fieldnames[field]))
                
            else:
                # if fieldnames use them as colnames
                delimiter = ',' if not kwargs.has_key('delimiter') else kwargs['delimiter'] 
                reader = csv.DictReader(open(kwargs['filename'], 'r'), fieldnames=kwargs['fieldnames'], delimiter = delimiter)
                for field in kwargs['fieldnames']:
                    self.value[field] = []
          
                        
            for line in reader:                
                for key in line.keys():                                      
                    self.value[key].append(convertStr(line[key]))
            
            #temporary date assignment. TODO: specify the date column        
            if self.value.has_key('date'):
                self.date = self.value['date']
                del self.value['date']
            else:
                key = self.value.keys()[0]
                self.date = range(len(self.value[key]))
            
            self.info = {'filename': kwargs['filename']} 
    def plot(self):
        import matplotlib.pyplot as plt
        ColorMap = ([1, 0.2, 0.2], [0, 0, 1], [170/256, 160/256, 149/256], [0/256, 150/256, 97/256], [153/256, 213/256, 192/256])
        #plt.figure(1)
        for i in range(len(self.value.keys())):
            #if numeric plot it against the date            
            if isinstance(self.value[self.value.keys()[i]][0], int) or isinstance(self.value[self.value.keys()[i]][0], float):                                     
                color = ColorMap[i%5]                
                plt.plot(self.date, self.value[self.value.keys()[i]], color=color)
        plt.show()
    
    def __str__(self):
        if self.is_empty(): return '<pyData: Empty!>'
            
        string = '<pyData: {length: %d; dates: [%s - %s]; ' % (len(self.date), self.date[0], self.date[len(self.date)-1])
        string += 'columns: ['
        for key in self.value.keys():
            string += "'%s' " % key
        string += ']}>'
        return string
        
    def to_vals(self, dates , columns):
        '''
        Returns the values in matrix format:
            @param dates: the dates (will serve as the second index in the result array)
            @param columns: the columns (will serve as the first index in the result array)
            @return: a matrix (double list) containing the values from the specified columns, correspondant to the specified dates
            
        Example:
            x = pyData('init', date=[1, 2, 3], value = {'price':[10, 10, 11], 'ask':[2, 3, 4], 'bid':[1, 2, 3]}, info = {'title': 'Example pyData'})
            print x.to_vals([1, 2], ['bid', 'ask'])
        '''
        if dates == []: # all available dates
            dates = self.date          
        vals = []
        for x in columns:            
            vals.append([self.value[x][self.date.index(d)] for d in dates])        
        return vals
    
    def to_dict(self, dates , columns):
        '''
        Returns the values in matrix format:
            @param dates: the dates (will serve as the second index in the result array)
            @param columns: the columns (will serve as the first index in the result array)
            @return: a dictionary (keys = columns) containing the values from the specified columns, correspondant to the specified dates
            
        Example:
            x = pyData('init', date=[1, 2, 3], value = {'price':[10, 10, 11], 'ask':[2, 3, 4], 'bid':[1, 2, 3]}, info = {'title': 'Example pyData'})
            print x.to_vals([1, 2], ['bid', 'ask'])
        '''
        if dates == []: # all available dates
            dates = self.date          
        vals = {}
        if columns == []:
            columns = self.value.keys()
        for x in columns:            
            vals[x] = [self.value[x][self.date.index(d)] for d in dates]        
        vals['date']= dates
        return vals
            
    def to_xls(self, filename, w=None):
        if w == None:
            w = Workbook()        
        ws = w.add_sheet(self.info['title'] if self.info.has_key('title') else 'pyDataExport')
        if self.date:
            ws.write(0, 0, 'Date')
            for i in range(len(self.date)):
                ws.write(i+1, 0, self.date[i])
        for i in range(len(self.value.keys())):
            ws.write(0, i+1, self.value.keys()[i])
            for j in range(len(self.value[self.value.keys()[i]])):
                try:
                    ws.write(j+1, i+1, self.value[self.value.keys()[i]][j])
                except AssertionError:
                    ws.write(j+1, i+1, self.value[self.value.keys()[i]][j].__str__())
        w.save(filename)
    
    def get_stdata_dict(self, format='%H:%M:%S:000000'):
        import numpy
        n_rows = len(self.date)
        if n_rows == 0:
            return None
        n_cols = len(self.value.keys())
        D = {'date'     : numpy.zeros((n_rows,1), dtype=numpy.float64), 
             'title'    : self.info['title'] if self.info.has_key('title') else '[py_to_st]_data',
             'colnames' : numpy.array([], dtype=numpy.object),
             'info'     : {'coming_from_simep' : True},
             'value'    : numpy.zeros((n_rows,n_cols), dtype=numpy.float64),
             'codebook' : []}
        # TODO : there is a problem with scipy : it seems that it cannot transform several nested dicts
        # into a matlab object (struct)
        # In the next versions of scipy, try to add all the context information
        for k,v in self.info.iteritems():
            if k != 'context':
                D['info'][k] = v
        D['info']['context'] = {}
        for k,v in self.info['context'].iteritems():
            if not isinstance(v, dict):
                D['info']['context'][k] = v  
        if 'title' in D['info']:
            D['info'].pop('title')
        if 'name' in D['info']:
            D['info'].pop('name')
        for (key,val) in D['info'].iteritems():
            if isinstance(val, types.NoneType):
                D['info'][key] = ''
            elif isinstance(val, dict):
                for (key2,val2) in val.iteritems():
                    if isinstance(val2, types.NoneType):
                        val[key2] = ''
        # detect string cases
        integer_cols  = []
        codebook_cols = []
        colnames      = []
        keys          = self.value.keys()
        keys.sort()
        for key in keys:
            val = self.value[key]
            number_type = True
            for i in range(len(val)):
                if not (isinstance(val[i], int) or isinstance(val[i], float) or isinstance(val[i], long) or isinstance(val[i], complex) or isinstance(val[i], types.NoneType)):
                    number_type = False
                    break
            if number_type:
                integer_cols.append(key)
            elif isinstance(val[0], (str,unicode,types.NoneType)):
                codebook_cols.append(key)
        # create unique(codebook)
        if len(codebook_cols) > 0:
            D['codebook'] = numpy.array([(key,
                                          numpy.array(range(1, 1+len(list(frozenset([e if e!=None else '' for e in self.value[key]]))))),
                                          numpy.array(list(frozenset([e if e!=None else '' for e in self.value[key]])), dtype=numpy.object)) for i,key in enumerate(codebook_cols)],
                                          dtype=[('colname',numpy.object) , ('colnum',numpy.ndarray), ('book',numpy.ndarray)] )
            
        # fill dictionnary
        for i in range(n_rows):
            if isinstance(self.date[i], datetime):
                D['date'][i] = dateNum(self.date[i].strftime(format))/86400000000.0
            elif isinstance(self.date[i], str):
                D['date'][i] = dateNum(self.date[i])/86400000000.0
            elif isinstance(self.date[i], int) or isinstance(self.date[i], long):
                D['date'][i] = self.date[i]/86400000000.0
            else:
                pyLog('ERROR in pyData : date is in an unknown format !')
            j = 0
            for key in integer_cols:
                if not key in colnames:
                    colnames.append(key)
                if isinstance(self.value[key][i], list):
                    pass #TO IMPLEMENT
                else:
                    try:
                        D['value'][i,j] = self.value[key][i]
                    except:
                        print self.value[key][i]
                        print '??'
                j += 1
            for k,key in enumerate(codebook_cols):
                val = self.value[key][i]
                if val == None:
                    val = ''
                D['value'][i,j] = numpy.where(D['codebook'][k]['book']==val)[0][0]+1
                j += 1
        for key in codebook_cols:
            colnames.append(key)
        D['colnames'] = numpy.array(colnames, dtype=numpy.object)
        
        if len(D['codebook']) == 0:
            D.pop('codebook')
        return D
    
    def to_mat(self, varname, filename, format=''):
        import scipy.io.matlab
        D = self.get_stdata_dict(format)
        scipy.io.matlab.savemat(filename, {varname:D}, appendmat=True, format='5', do_compression=False, oned_as='row')
    
    
    def to_csv(self, filename, delimiter = ','):
        '''
            @param filename: .csv file
            @param delimiter: custom delimiter to use           
             
        '''
        fieldnames = ['date'] + self.value.keys()
        writer = csv.writer(open(filename, 'w'), delimiter = delimiter)
        writer.writerow(fieldnames)
        for index in range(len(self.date)):
            writer.writerow([self.date[index]] + [self.value[key][index] for key in self.value.keys()])
         
    
    def sort_by(self, name):
        '''
            Sort all columns and date by a specified column (ascendant sort for the moment)
        '''
        if name == 'date':
            self.date, idx = sorted_idx(self.date)            
            for key in self.value.keys():
                self.value[key] = index_ref(self.value[key], idx)
        elif self.value.has_key(name):
            self.value[name], idx = sorted_idx(self.value[name])
            self.date = index_ref(self.date, idx) 
            for key in self.value.keys():
                if key != name:
                    self.value[key] = index_ref(self.value[key], idx)
        else:
            print "Error: Unknown key: %s" % name     
        
    
    def merge(self, pydata_obj):
        #first merge dates:
        temp_date = []
        for date in pydata_obj.date:
            if not date in set(temp_date):
                temp_date.append(date)
        for date in self.date:
            if date not in set(temp_date):
                temp_date.append(date)        
        temp_date.sort()              
          
        for key in self.value.keys():
            self.value[key] = ets(self.date, self.value[key], temp_date)
        for key in pydata_obj.value.keys():
            if not key in set(self.value.keys()):
                self.value[key] = ets(pydata_obj.date, pydata_obj.value[key], temp_date)
            else:
                # in case of conflict between names and dates, keep the original value
                for i in range(len(temp_date)):
                    if self.value[key][i] is None:                        
                        self.value[key][i] = pydata_obj.value[key][pydata_obj.date.index(temp_date[i])]
                                
        self.date = temp_date
        
    
    def stack(self, pydata_obj):
        # fast stacking w/o sorting
        length_date = len(self.date)
        for date in pydata_obj.date:
            self.date.append(date)
        for key in self.value.keys():
            if key in pydata_obj.value.keys():
                for val in pydata_obj.value[key]:
                    self.value[key].append(val)
            else:                
                for i in range(len(pydata_obj.date)):
                    self.value[key].append(None) 
        for key in pydata_obj.value.keys():
            if key not in self.value.keys():
                self.value[key] = []
                for i in range(length_date):
                    self.value[key].append(None)
                for val in pydata_obj.value[key]:
                    self.value[key].append(val)
    
    def interval(self, date_min = None, date_max = None):
        '''
        Returns a copy of a segment of the current object 
        
        '''
        
        date = []
        value = {}
        info = self.info
        
        
        for i in range(len(self.date)):
            if ((date_min == None) or (date_min <= self.date[i])) and ((date_max == None) or (self.date[i] <= date_max)):
                date.append(self.date[i])
                for key in self.value.keys():
                    try: 
                        value[key].append(self.value[key][i])
                    except KeyError:
                        value[key] = [self.value[key][i]]
        
        return pyData('init', date = date, value = value, info = info )   
            
    def is_empty(self):
        '''
        @return: whether the object is empty or not
        '''
        return self.value == {}       
                
    def __getitem__(self, name):
        '''
            @param name: The value-id or 'date'
            @return : Array correspondent to name
        '''        
        if name == 'date':
            return self.date
        if isinstance(name, tuple):
            ret = []        
            for id in name:
                ret.append(self.value[id])
            return ret
        else:  
            return self.value[name]         
    
    def __setitem__(self, name, val):
        '''
            Sets a given field to the specified value 
        '''        
        self.value[name] = val

''' Derived Classes for Plotting'''
class pyDataIntraday(pyData):
    def plot(self):
        import matplotlib.pyplot as plt
        plt.figure(1)
        
        plt.subplot(211)
        plt.plot(self.date, self.value['bid'], 'bv')
        plt.plot(self.date, self.value['ask'], 'r^')
        plt.plot(self.date, self.value['bid'], drawstyle='steps-post', color='black')
        plt.plot(self.date, self.value['ask'], drawstyle='steps-post', color='black')
        
        plt.subplot(212)
        markerline, stemlines, baseline = plt.stem(self.date, self.value['VOLUME'], '--')
        plt.setp(markerline, 'markerfacecolor', 'g')
        plt.setp(stemlines, 'color', 'g')
        plt.setp(baseline, 'color','r', 'linewidth', 2)
        
        plt.show()

''' Helper Functions '''
def index_ref(x, ind):
    '''        
        @param x: an array
        @param ind: an array of indices
        @return: the array x shuffled corresponding to the indices in ind
    '''
    a = []
    for i in ind:
        a.append(x[i])
    return a

def sorted_idx(vec):
    '''
        Returns a sorted ascendantly version of the input as well as 
        a rearrangement of indices matching the new sort
        @return: sorted-value
                 the indices of the older arrangement in the sorted version
        Use: to rearrange other arrays             
    '''
    h = sorted(zip(vec, range(len(vec))), key = lambda x: x[0])   
    srt = []
    idx = []
    for (a, i) in h:
        srt.append(a)
        idx.append(i)
    return srt, idx

def ets(idx, vec, ext_idx):
    '''
        Extended Time Series: Extends the array to a larger index by adding None values
        @param idx: - the current indexing used for the array (can be dates or anything else)
                    - must be sorted 
        @param vec: the array to extend
        @param ext_idx: - the extended indexing;
                        - must be sorted
                        - must contain all values of idx
        @return: an array containing the element of vec corresponding to the indices of idx 
                 and None in the indices found in ext_idx but not in idx 
        
    '''    
    #assert ext_idx contains idx
    ext_vec = []
    ind = 0
    for i in range(len(ext_idx)):
        if ext_idx[i] in set(idx):
            ext_vec.append(vec[ind])
            ind += 1
        else:
            ext_vec.append(None)
    return ext_vec

def convertStr(s):
    '''
        Attempts to convert an element to int, 
        if failed it will attempt to convert to float
        if again failed it will return the item as it is.
        
        Use for converting numeric strings into int, float
    '''
    try:
        ret = int(s)
    except ValueError:
        #Try float.
        try:
            ret = float(s)
        except ValueError:
            return s
    return ret
        
