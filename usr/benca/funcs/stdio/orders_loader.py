'''
Created on 24 sept. 2010

@author: benca
'''

from simep.tools import date2num, num2date, ampm2date
from simep.funcs.data.pyData import pyData


class OrdersLoader:
    
    @staticmethod
    def load_orders(method, filename):
        # initialize
        cmd_pydata = pyData('init', date=[], value = {'Timestamp'          : [],
                                                      'MarketOrderId'      : [],
                                                      'SequenceNumber'     : [],
                                                      'MarketId'           : [],
                                                      'OrderInternalRef'   : [],
                                                      'OrderMarketRef'     : [], 
                                                      'Side'               : [],
                                                      'Type'               : [],
                                                      'OrderQty'           : [],
                                                      'Price'              : [],
                                                      'LeavesQty'          : [],
                                                      'TimeInForce'        : [],
                                                      'ClientOrderId'      : [],
                                                      'OrderType'          : [],
                                                      'ExpireTime'         : []})
        header = {'AutomatonOrderID'      : None,
                  'DarkOnly'              : None,
                  'AutomatonName'         : None,
                  'Sequence'              : None,
                  'InstrumentID'          : None,
                  'Side'                  : None,
                  'EffectiveTime'         : None,
                  'ExpireTime'            : None,
                  'OrderQty'              : None,
                  'ExecutedQuantity'      : None,
                  'Limit'                 : None,
                  'ClientData'            : None,
                  'ExcludePrimary'        : None,
                  'MIFIDBestExecutionReqd': None,
                  'CrossFlag'             : None}
        # load with specific method
        if method == 'simple_text_file':
            (cmd_pydata, header) = load_order_cmds_from_text_file(filename, cmd_pydata, header, False)
        elif method == 'headed_text_file':
            (cmd_pydata, header) = load_order_cmds_from_text_file(filename, cmd_pydata, header, True)
        else:
            raise IOError('Unknown reading method')
        # sort data
        cmd_pydata.sort_by('date')
        sorted_pydata = pyData('init', date=[], value={})
        for key in cmd_pydata.value.keys():
            sorted_pydata.value[key] = []
        i = 0
        while i < len(cmd_pydata.date):
            current_time = cmd_pydata.date[i]
            cmd_list = pyData('init', date=[], value={})
            for key in cmd_pydata.value.keys():
                cmd_list.value[key] = []
            while i < len(cmd_pydata.date) and cmd_pydata.date[i] == current_time:
                cmd_list.date.append(cmd_pydata.date[i])
                for (key,val) in cmd_pydata.value.iteritems():
                    cmd_list.value[key].append(val[i])
                i += 1
            n = len(cmd_list.date)
            if n > 1:
                sorted_types_idx = [(j, OrdersLoader.__sort_types(cmd_list.value['Type'][j]), cmd_list.value['LeavesQty'][j]) for j in range(n)]
                sorted_types_idx.sort(key = lambda x:x[1], reverse = False)
                exec_idx = 0
                while exec_idx < n and sorted_types_idx[exec_idx][1] != 3:
                    exec_idx += 1
                if exec_idx < n-1:
                    tmp = sorted_types_idx[exec_idx:]
                    tmp.sort(key = lambda x:x[2], reverse = True)
                    sorted_types_idx[exec_idx:] = tmp
                for j in sorted_types_idx:
                    sorted_pydata.date.append(cmd_list.date[j[0]])
                    for (key,val) in cmd_list.value.iteritems():
                        sorted_pydata.value[key].append(val[j[0]])
            else:
                sorted_pydata.date.append(cmd_list.date[0])
                for (key,val) in cmd_list.value.iteritems():
                    sorted_pydata.value[key].append(val[0])
        # return
        return (sorted_pydata, header)
    
    @staticmethod
    def visualize_orders_list(sorted_pydata):
        n = len(sorted_pydata.date)
        print '###############################################################################'
        print ''
        orders_id = {}
        count_id  = 1
        for i in xrange(n):
            if sorted_pydata.value['OrderInternalRef'][i] not in orders_id:
                orders_id[sorted_pydata.value['OrderInternalRef'][i]] = count_id
                count_id += 1
            current_string = '%s : ID=%03d  %s %06.03f ORDERQTY=%04d LEAVESQTY=%04d '
            current_string = current_string %(num2date(int(float(sorted_pydata.date[i]))), 
                                              orders_id[sorted_pydata.value['OrderInternalRef'][i]],
                                              sorted_pydata.value['Type'][i], 
                                              sorted_pydata.value['Price'][i],
                                              sorted_pydata.value['OrderQty'][i], 
                                              sorted_pydata.value['LeavesQty'][i])
            print current_string
        print ''
        print ''
    
    @staticmethod
    def __sort_types(c):
        if c == 'I':
            return 1
        elif c == 'U':
            return 2
        elif c == 'C':
            return 5
        elif c == 'Z':
            return 3
        else:
            return 4
        

def load_order_cmds_from_text_file(filename, cmd_pydata, header, use_header):
    f = open(filename, 'r')
    lines = f.readlines()
    f.close()
    if use_header:
        lines[0] = lines[0].replace('\n','')
        fields   = lines[0].split('\t')
        header['AutomatonOrderID']      = fields[0]
        header['DarkOnly']              = fields[1]
        header['AutomatonName']         = fields[2]
        header['Sequence']              = int(fields[3])
        header['InstrumentID']          = int(fields[4])
        header['Side']                  = fields[5]
        time_num  = date2num(ampm2date(fields[6].split(' ')[-1]))
        header['EffectiveTime']         = time_num
        time_num  = date2num(ampm2date(fields[7].split(' ')[-1]))
        header['ExpireTime']            = time_num
        header['OrderQty']              = int(float(fields[8]))
        header['ExecutedQuantity']      = int(float(fields[9]))
        header['Limit']                 = float(fields[10])
        header['ClientData']            = fields[11]
        header['ExcludePrimary']        = fields[12]
        header['MIFIDBestExecutionReqd']= fields[13]
        header['CrossFlag']             = int(fields[14])
        index    = 1
    else:
        index    = 0
    for i in range(index, len(lines)):
        lines[i] = lines[i].replace('\n','')
        fields   = lines[i].split('\t')
        full_date = fields[6]
        time_str  = ampm2date(full_date.split(' ')[-1])
        time_num  = date2num(time_str)
        cmd_pydata.date.append(time_num)
        cmd_pydata.value['Timestamp'].append(time_str)
        cmd_pydata.value['MarketOrderId'].append(int(fields[0]))
        cmd_pydata.value['SequenceNumber'].append(int(fields[1]))
        cmd_pydata.value['MarketId'].append(int(fields[2]))
        cmd_pydata.value['OrderInternalRef'].append(fields[3])
        cmd_pydata.value['OrderMarketRef'].append(fields[4])
        cmd_pydata.value['Side'].append(fields[5])
        cmd_pydata.value['Type'].append(fields[7])
        cmd_pydata.value['OrderQty'].append(int(float(fields[8])))
        cmd_pydata.value['Price'].append(float(fields[9]))
        cmd_pydata.value['LeavesQty'].append(int(float(fields[10])))
        cmd_pydata.value['TimeInForce'].append(fields[11])
        cmd_pydata.value['ClientOrderId'].append(int(fields[12]))
        cmd_pydata.value['OrderType'].append(fields[13])
        full_date = fields[14]
        time_num  = date2num(ampm2date(full_date.split(' ')[-1]))
        cmd_pydata.value['ExpireTime'].append(time_num)
    return (cmd_pydata, header)



if __name__ == "__main__":
    a = OrdersLoader.load_orders('simple_text_file', 'C:/st_sim/usr/dev/benca/data/occurence_2_ME100E5')
    b = OrdersLoader.load_orders('headed_text_file', 'C:/st_sim/usr/dev/benca/data/detail_occ_3_Jn}0026.txt')
    OrdersLoader.visualize_orders_list(a[0])
    OrdersLoader.visualize_orders_list(b[0])
