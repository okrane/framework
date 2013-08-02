#!python2.7
'''
Created on 23 Apr 2013

@author: flexsys
'''

import xml.etree.ElementTree as ET
import paramiko
import pymongo as mongo
import time
from datetime import datetime, timedelta
from lib.dbtools import connections
from pandas import *
from lib.data.pyData import convertStr
import pytz
import lib.dbtools.read_dataset as read_dataset
from lib.logger import *
from lib.io.toolkit import *
from bson.json_util import default
from lib.dbtools.get_repository import get_symbol6_from_ticker
from lib.io.smart_converter import *
from paramiko import ssh_exception
logging.getLogger("paramiko").setLevel(logging.WARNING)
from lib.tca import mapping
    


class ConversionRate:
    data = None
    default_currencies = ['GBX', 'EUR', 'GBP', 'SEK', 'CHF', 'DKK', 'NOK', 'USD']
    dates = []
    @staticmethod        
    def _get_first_time(dates):
        dates_datetime = date_to_datetime(dates)
        ConversionRate.dates.extend(dates)
        if ConversionRate.data is None:
            ConversionRate._get_all(dates_datetime)  
    
    @staticmethod    
    def getRates(curr, dates):
        curr = curr.upper()
        do_get_all = False
        # Initialize if needed
        if ConversionRate.data is None:
            ConversionRate._get_first_time(dates)
            do_get_all = True
        
        # Add dates if needed   
        for d in dates:
            if d not in ConversionRate.dates:
                ConversionRate.dates.append(d)
                do_get_all = True
                
        # Add a currency if needed
        if curr not in ConversionRate.default_currencies:
            ConversionRate.default_currencies.append(curr)
            do_get_all = True
        
        # Convert dates from string to datetime
        dates_datetime = date_to_datetime(dates)
        
        # If needed do a get_all
        if do_get_all:
            ConversionRate._get_all(dates_datetime) 
                   
        rates = []

        for d in dates_datetime:
            try:
                rate = ConversionRate.data[ConversionRate.data['ccy'] == curr].iloc[-1]["rate"]
            except Exception, e:
                rate = None
                import traceback
                import StringIO
                fp = StringIO.StringIO()
                traceback.print_exc(file = fp)
                logging.error(fp.getvalue())
                logging.error("Impossible to get the currency: " + str(curr) + " at the date: " + str(d))
                logging.error("The required rate has been set to None")
                
            rates.append(rate)
        return rates
    
    @staticmethod
    def getRate(curr, date):
        logging.debug("The rate of " + curr + "at date: " + date + " is: "+ str( ConversionRate.getRates(curr, [date])[0] ))
        return ConversionRate.getRates(curr, [date])[0]
    
    @staticmethod  
    def _get_all(dates_datetime):
        connection_name_backup = connections.Connections.connections
        connections.Connections.change_connections('production')
        ConversionRate.data = read_dataset.histocurrencypair(last_date_from   = dates_datetime[len(dates_datetime) - 1].strftime("%Y%m%d"),
                                                             currency         = ConversionRate.default_currencies)
        connections.Connections.change_connections(connection_name_backup)
        logging.info(str(ConversionRate.data))

def get_last_seq(order, l_orders):
    
    primID = order['occ_ID']
    
    res_ord = order
    
    nb_seq = 0
    for ord in l_orders:
        if ord['occ_ID'] == primID and ord['MsgType'] == 'G':
            if nb_seq < ord['nb_replace']:
                nb_seq = ord['nb_replace']
                res_ord = ord
    
    return res_ord
    
def match_trader(trader_name, dico_trader):
    r_trader_name = ""
    if trader_name in dico_trader.keys():
        r_trader_name = dico_trader[trader_name]
        return r_trader_name
    return trader_name

def update_colmap(l_field, client, collection):
    
    database = client['DB_test']
    map_coll = database['field_map']
    
    old_line = map_coll.find({"collection_name":collection})
    
    if old_line.count() == 1:
        old_line = old_line[0]
        old_list = old_line['list_columns']
        new_list = list(set(old_list) | set(l_field))
        old_line['list_columns'] = new_list
        map_coll.save(old_line, safe=True, manipulate=False)
        
    elif old_line.count() == 0:
        map_coll.insert({'collection_name':collection, 'list_columns':l_field})
        
    else:
        logging.warning( '##### Conflict while updating map collection : 2 rows for same collection !! #####')
        
    return 0


def check_EoL(d_msg, reason, day, socket, dico_fix, dico_tags, ignore_tags, import_type='Client'):
    
    done = False
    reason = ''
    
    OrigOrderID = d_msg['ClOrdID']
    logging.warning("Looking for other reason for order : %s" %OrigOrderID)
    
    if import_type == 'Client':
        IN_file = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sI.fix' %(day, day)
        ER_file = '/home/flexsys/logs/trades/%s/FLINKI_CLNT1%sO.fix' %(day, day)
    elif import_type == 'Street':
        IN_file = '/home/flexsys/logs/trades/%s/FLEX_ULPROD%sI.fix' %(day, day)
        ER_file = '/home/flexsys/logs/trades/%s/FLEX_ULPROD%sO.fix' %(day, day)
    
    cmd = "prt_fxlog %s 3 | egrep '41=%s'" %(IN_file, OrigOrderID)
    (stdin, stdout_grep, stderr) = socket.exec_command(cmd)
    

    if stdout_grep.readlines() == 1:
        
        line = stdout_grep.readlines()[0]
        line = line.replace('|\n','')
        [line, dico_tags] = line_translator(line, dico_fix, dico_tags, ignore_tags)
        
        if line == {}:
            return [reason, done]
        
        if line['MsgType'] == 'G':
            reason = 'replaced not acked '
            cmd = "prt_fxlog %s 3 | egrep '11=%s'" %(ER_file, line['ClOrdID'])
            
            (stdin, stdout_grep, stderr) = socket.exec_command(cmd)
            
            for er_line in stdout_grep:
                er_line = er_line.replace('|\n','')
            
            [er_line, dico_tags] = line_translator(er_line, dico_fix, dico_tags, ignore_tags)
            
            if 'ExecType' not in er_line.keys():
                pass
            
            if er_line['ExecType'] == '3':
                reason = '%s and Done for day' %reason
                done = True
                
    return [reason, done]

def update_security_ids(l_kep_secids):
    logging.info( "UPDATE CHEUVREUX SECURITY IDS AND MARKET DATA")
    query = "select SYMBOL1, SYMBOL2, SYMBOL3, SYMBOL4, SYMBOL5, SYMBOL6 from SECURITY where STATUS = 'A'  and SYMBOL3 in ('%s')" % "','".join(map(str,l_kep_secids))
    result = connections.Connections.exec_sql('KGR', query, as_dict = True)
    dict_secs = {}
    for sec in result:
        temp = str(sec['SYMBOL3'])
        dict_secs[temp] = {
                              "gl_secid"            : sec['SYMBOL1'],
                              "isin"                : sec['SYMBOL2'],
                              "sedol_secid"         : sec['SYMBOL3'],
                              "reuters_secid"       : sec['SYMBOL4'],
                              "bloomberg_secid"     : sec['SYMBOL5'],
                              "cheuvreux_secid"     : convert_str(sec['SYMBOL6'])
                           }
        
    return dict_secs

def update_security_ids_symbol(tickers):
    dict = {}
    for ticker in tickers:
        dict[ticker] = get_symbol6_from_ticker(ticker)
        
    return dict

class DatabasePlug:
    def __init__(self, database_server, database, environment, source, dates, mode = 'write'):
        # Arguments
        self.database_server    = database_server
        self.database           = database
        self.environment        = environment
        self.source             = source
        self.dates              = dates
        self.dates_datetime     = date_to_datetime(dates)
        self.mode               = mode
        self.missing_ids_reject = []
        self.missing_ids        = []
        self.strategy_name      = {}
        self.dico_tags          = {}
        self.missing_enrichment = {}
        self.ssh_attempts       = 0
        
        # Constants
        self.ignore_tags            = [8, 21, 22, 9, 34, 49, 56, 58, 10, 47, 369]
        self.missing_id_receivers   = ["alababidi@keplercheuvreux.com", "njoseph@keplercheuvreux.com"]
        self.missing_tags_receivers = ["alababidi@keplercheuvreux.com", "njoseph@keplercheuvreux.com"]
        self.missing_enri_receivers = ["alababidi@keplercheuvreux.com", "njoseph@keplercheuvreux.com"]
        
        # Parameters
        import os
        full_path           = os.path.realpath(__file__)    
        path, f             = os.path.split(full_path)        
 
        self.universe_file  = path + '/../cfg/KC_universe.xml'
        self.dico_FIX       = path + '/../cfg/FIX42.xml'    
        self.conf           = self.get_conf(self.environment, self.universe_file)
        
        
        file = open(path + '/../cfg/fix_types.json', 'r')
        input = file.read()
        file.close()
    
        self.list_of_dict   = simplejson.loads(input)["fix"]["fields"]["field"]
        
        file = open(path + '/../cfg/enrichment_types.json', 'r')
        input = file.read()
        file.close()
        
        self.list_of_dict.extend(simplejson.loads(input))
        self.checker        = Converter(self.list_of_dict)
        
        # Open MONGODB connection
        self.client = connections.Connections.getClient(self.database_server)
        logging.info("Connected to database: " + self.database_server)
        
        self.get_all_strategy_name()
       
    def store_db(self, orders, collection_name, job_id, mode='insert'):
        database = self.client[self.database]
        collection = database[collection_name]
        
        if mode == 'insert':
            logging.info("insert %s Orders into database" %str(len(orders)) )
            
        l = list(self.client['Mars']['field_map'].find({'collection_name':collection_name}))
        fields = []
        if len(l) > 0:
            for el in l:
                if el['collection_name'] == collection_name:
                    fields = el['list_columns']
        to_add = False
        
        for order in orders:
            order.update({'job_id': job_id})
            if mode == 'insert':    
                collection.insert(order, manipulate=False)
            elif mode == 'update':
                collection.save(order)

            for k in order.keys():
                if k not in fields:
                    fields.append(k)
                    to_add = True
        if to_add:
            self.client['Mars']['field_map'].update({'collection_name' : collection_name}, {'collection_name' : collection_name, 'list_columns' : fields})
        logging.info("End Of insertion to the database")

    def get_dico_header(self, day):
        
        mkt_data_fields = ['EXEC_SHARES' ,'ORDER_PERC','INMKT_VOLUME','INMKT_TURNOVER',
                           'PRV_VOLUME','PRV_TURNOVER','AVG_SPRD','IN_VWAS','ARRIVAL_PRICE',
                           'FINAL_PRICE','PERIOD_LOW','PERIOD_HIGH','PRV_WEXEC']
        
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.server['ip_addr'], 
                    username = self.server['list_users']['flexapp']['username'], 
                    password = self.server['list_users']['flexapp']['passwd'])
        
        cmd = 'grep SYM /home/flexapp/ushare/exportprodAV1%s' %day
        
        (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
        
        header = ""
        for line in stdout_grep:
            header = line
        
        dico_header = {}
        f_mkt_data = False
        if header != "":
            header = header[:-1]
            header  = header.rsplit(',')
            f_mkt_data = True
                
            for col in mkt_data_fields:
                if col in header:
                    dico_header[col.lower()] = header.index(col)
        result = {}
        result["f_mkt_data"]  = f_mkt_data
        result["dico_header"] = dico_header
        return result    
    
    def get_conf(self, referential, dico_universe):

        conf        = {}
        struct      = ET.parse(dico_universe)
        raw_data    = struct.getroot()
        servs_dico  = {}
        
        for elt in raw_data.findall('env'):
            if elt.get('name') == referential:
                servs_dico = elt
                
        if servs_dico != {}:
            for attr in servs_dico.findall('server'):
                serv_name   = attr.get('name')
                dict_server = {'ip_addr': attr.get('ip_addr')}
                l_users     = {}
                
                for u_attr in attr.findall('user'):
                    l_users[u_attr.get('username')] = u_attr.attrib
                    
                dict_server['list_users']   = l_users
                conf[serv_name]             = dict_server
                
        return conf
    def line_translator(self, line, dico_tags):
        line                = line.rsplit('|')
        dict_order          = {}
        last_correct_date   = None
        for item in line:
            item = item.rsplit('=')
            if len(item) == 2:
                if item[1] != ' ' and item[1] != '':
                    if int(item[0]) not in self.ignore_tags:
                        if int(item[0]) not in dico_tags.keys():
                            dico_info = self.get_FIX(int(item[0]))
                            dico_tags[int(item[0])] = dico_info
                        
                        nameD = dico_tags[int(item[0])][0]
                        typeD = dico_tags[int(item[0])][1]
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
        return (dict_order, dico_tags)      
      
    def define_unique_id(self, order, my_type):
        day = datetime.strftime(order['TransactTime'], format='%Y%m%d')
        
        if my_type == 'OrderDeals':
            order['p_exec_id'] = day + str(order['ExecID']) + order['server']
            
        if my_type == 'AlgoOrders':
            order['p_occ_id'] = day + order['occ_ID'] + order['server']
            
        order['p_cl_ord_id'] = day + order['ClOrdID'] + order['server']
        return order
    
    def fill(self, order_deals = True, algo_orders = True):
        if order_deals:
            self.fill_order_deals()
        if  algo_orders:
            self.fill_algo_orders()
        
    def fill_order_deals(self):
        self.io      = "O"
        type_order   = ''
        dico_tags    = {}
        trader       = ''
        self.checker        = Converter(self.list_of_dict, required_tag='deals_required')
        for day in self.dates:
            self.logPath    = './logs/trades/%s/FLINKI_%s%s%s.fix' %(day, self.source, day, self.io)
            job_id          = 'OD%s' %day
            orders          = []
            
            for s in self.conf:
                self.server = self.conf[s]
                res_import  = self.import_FIXmsg(type_order, dico_tags, trader)
                new_orders  = res_import[0]
                dico_tags   = res_import[1]
                
                for order in new_orders:
                    order["job_id"] = job_id
                    order["server"] = s
                    order           = self.define_unique_id(order, my_type='OrderDeals')
                orders.extend(new_orders)                
                
            typed_orders        = self.checker.verify_all(orders)
            
            if self.mode == "write":
                self.client[self.database]['OrderDeals'].remove({'job_id':job_id})
                self.store_db(typed_orders, 'OrderDeals', job_id)
            

    def fill_algo_orders(self):
        to_return   = []
        self.io     = "I"
        for day in self.dates:
            
            
            self.logPath    = './logs/trades/%s/FLINKI_%s%s%s.fix' %(day, self.source, day, self.io)
            orders          = []
            job_id          = 'AO%s' %day
            
            for s in self.conf:
                self.server = self.conf[s]
                new_orders  = self.get_algo_orders(day)
                
                for order in new_orders:
                    order["job_id"] = job_id
                    order["server"] = s 
                    order = self.define_unique_id(order, my_type='AlgoOrders')
                    
                orders.extend(new_orders)          

                
            typed_orders    = self.checker.verify_all(orders)
            to_return.append(typed_orders)
            
            if self.mode == "write":
                job_id = 'AO%s' %day
                logging.info("Remove the documents where this job_id is present " + str(job_id))
                self.client[self.database]['AlgoOrders'].remove({'job_id':job_id})
                self.store_db(typed_orders, "AlgoOrders", job_id)
                
                for order in typed_orders:
                    req_for_update = {}
                    
                    if 'rate_to_euro' in order:
                        req_for_update['rate_to_euro'] = order['rate_to_euro']
                    if 'cheuvreux_secid' in order:
                        req_for_update['cheuvreux_secid'] = order['cheuvreux_secid']
                        
                    self.client[self.database]['OrderDeals'].update({'p_cl_ord_id' : order['p_cl_ord_id']}, {'$set': req_for_update }, multi=True)
                        
            self.checker.add_json()
            self.send_missing_ids(day) 
            self.send_missing_tags(day)
            self.send_missing_enrichment(day)
            
            self.missing_ids        = []
            self.missing_ids_reject = []
            self.missing_enrichment = {}
            self.checker.flush()
        return to_return
    def send_missing_tags(self, day):
        if len(self.checker.missing) > 0:
            
            dict = {}
            title = "<h2>Some tags are missing for this date: %s</h2>\n" % day
            m = "<table border='1' cellpadding='1' cellspacing='1' width='580'>\n"
            m += "<TH>Missing tag(s)</TH><TH>ClOrdID</TH><TH>Files</TH>\n" 
            for el in self.checker.missing:
                for miss in el["MissedKey"]:
                    if miss in dict.keys():
                        dict[miss] += 1
                    else:
                        dict[miss]  = 1
                m += "<tr>"
                m += "<td width='30%'>" + ',\n'.join(el["MissedKey"]) + "</td>\n"
                files = []
                try:
                    id = el["Dict"]["ClOrdID"]
                    m += "<td width='10%'>" + str(id) + "</td>\n"
                    day = el["Dict"]["SendingTime"].strftime("%Y%m%d")
                    trader_code = el["Dict"]['TargetSubID']
                    
                    files.append("prt_fxlog /home/flexsys/logs/trades/%s/FLINKI_CLNT1%sI.fix 3" % (day, day) )
                    files.append("/home/flexapp/ushare/exportprod%s%s" %(trader_code, day) )

                    m += "<td width='60%'>" + str("\n".join(files)) + "</td>\n"
                except Exception, e:
                    print el["Dict"]
                    get_traceback()
                    m += "<td></td>\n"
                    m += "<td>" + '\n'.join(map(str,el["Dict"].items())) + "</td>\n"
                m += "</tr>"
            m += "</table>"
            
            summary = "Summary :\n"
            summary += "<ul>" 
            for key, val in dict.iteritems():
                summary += "<li>" + key + " \t=\t " + str(val) + "</li>\n"
            summary += "</ul>"     
            msg = title + summary + m
            send_email(_to = self.missing_tags_receivers, _subject = "[MongoDatabase]Missing tags", _message = msg)
      
    def send_missing_ids(self, day):
        if len(self.missing_ids) > 0:
            m = "<h2>Those Symbol ids cannot be retrieved in KGR for this date: %s</h2>\n" % day
            m += "<ul>"
            for el in set(self.missing_ids):
                m += "<li>" + str(el) + "</li>\n"
            m += "</ul>"
            m += "<h2>Those Symbol has been rejected: %s</h2>\n" % day
            m += "<ul>"
            for el in set(self.missing_ids_reject):
                m += "<li>" + str(el) + "</li>\n"
            m += "</ul>"
            send_email(_to = self.missing_id_receivers, _subject = "[MongoDatabase]Missing ids", _message = m)
            
    def send_missing_enrichment(self, day):
        if len(self.missing_enrichment.keys()) > 0:
            m = "<h2>Could not add data to those orders for this date: %s</h2>\n" % day
            m += "<table border='1' cellpadding='1' cellspacing='1' width='580'>\n"
            
            for el in self.missing_enrichment:
                m += "\t<tr>"
                m += "\t\t<td>" + str(el) + "</td>\n"
                m += "\t\t<td>" + str(self.missing_enrichment[el][0]) + "</td>\n"
                m += "\t\t<td><pre>" + str(self.missing_enrichment[el][1]).replace(',', '\n') + "</pre></td>\n"
                m += "\t</tr>"
            m += "</table>\n"
            send_email(_to = self.missing_enri_receivers, _subject = "[MongoDatabase]Missing Enrichment", _message = m) 
               
    def get_algo_orders(self, day):
        logging.debug('Generating dico for Trader matching')
        temp_dico_trader = self.client[self.database]['map_tagFIX'].find({'tagFIX':'9249', 'ip_server':self.server['ip_addr']})
        
        logging.info("Add default tagFIX to trader dict")
        dico_trader = {}
        for trd in temp_dico_trader:
            dico_trader[trd['tag_value']] = trd['trader_name']
        
        trader = ''
        
        type_order = 'D'
        
        # IMPORT LES LOGS FIX
        # - Single Day import
        res_import = self.import_FIXmsg(type_order, self.dico_tags, trader)
        d_orders = res_import[0]
        self.dico_tags = res_import[1]
         
        l_kep_secids = []
        l_symbol     = []
        job_id = 'AO%s' %day
        

            
        logging.info('Get ' + str(len(d_orders)) + ' orders to add !')
        u_orders = []
        
        if self.mode == "write" :
            i = 0
            for order in d_orders:
                i+=1
                logging.info('Order ID : %s' %order['ClOrdID'])
                l_events = self.order_life(order          = order, 
                                          day            = day, 
                                          dico_tags      = self.dico_tags, 
                                          dico_trader    = dico_trader)
                
                u_order          = l_events[0]
                self.dico_tags   = l_events[1]
                u_orders.extend(u_order)
                
#                 for ord in u_order:
#                     if ord["ClOrdID"] == "FY2BL2SUP00000L0007":
#                         print ord
#                         import lib.io.serialize as serialize
#                         import simplejson
#                         temp = serialize.DateTimeJSONEncoder().encode(ord)
#                         print temp

                for ord in u_order:
                    if 'SecurityID' in ord.keys():
                        l_kep_secids.append(u_order[0]['SecurityID'])
                    if 'Symbol' in ord.keys():
                        l_symbol.append(ord['Symbol'])
            import lib.io.serialize as serialize
            import simplejson
            
            j_orders = serialize.DateTimeJSONEncoder().encode(u_orders)
            j_kep_secids = simplejson.dumps(l_kep_secids, separators=(',',':'))
            j_kep_secids_ticker = simplejson.dumps(l_symbol, separators=(',',':'))
            
            file_orders = open(self.server['ip_addr'] + '.orders.json', 'w')
            file_orders.write(j_orders)
            file_orders.close()
            
            file_ids = open(self.server['ip_addr'] + '.secids.json', 'w')
            file_ids.write(j_kep_secids)
            file_ids.close()
            
            file_ids = open(self.server['ip_addr'] + '.secids.symbol.json', 'w')
            file_ids.write(j_kep_secids_ticker)
            file_ids.close()
        else:
            import lib.io.serialize as serialize
            import simplejson
             
            file_orders = open(self.server['ip_addr'] + '.orders.json', 'r')
            u_orders = simplejson.loads(file_orders.read(), object_hook = serialize.as_datetime)
            file_orders.close()
             
            file_ids = open(self.server['ip_addr'] + '.secids.json', 'r')
            l_kep_secids = simplejson.loads(file_ids.read())
            file_ids.close 
             
            file_ids = open(self.server['ip_addr'] + '.secids.symbol.json', 'r')
            l_symbol = simplejson.loads(file_ids.read())
            file_ids.close()

#         dict_secs = update_security_ids(set(l_kep_secids))
        dict_secs_symbol = update_security_ids_symbol(set(l_symbol))

        # GET WHETHER THERE IS DATA FROM ALGO
        # - GET COLUMN NUMBER IN MKT
        result      = self.get_dico_header(day)
        dico_header = result["dico_header"]
        f_mkt_data  = result["f_mkt_data"]
        
        l_docs = []
        logging.info('Add information from prod to ' + str(len(u_orders)) + ' orders !')   
        job_id          = 'AO%s' %day
        
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.server['ip_addr'],
                    username = self.server['list_users']['flexapp']['username'], 
                    password = self.server['list_users']['flexapp']['passwd'])
                
        # Enrichment Market Data from PROD
        for order in u_orders:
            try:
                if order['MsgType'] == 'D' and f_mkt_data:
                    last_seq = get_last_seq(order, l_docs)
                    
                    Trader_code = last_seq['TargetSubID']
                     
                     
                    ClOrdID = '%sCLNT' %last_seq['ClOrdID']
                    Ticker = order['Symbol']
                    
                    # ------ To note
                    # Be careful, client order id has 29 caracters
                    client_order_id = ClOrdID[0:min(29,len(ClOrdID))]
    
                    cmd = "egrep '%s' /home/flexapp/ushare/exportprod%s%s" %(client_order_id, Trader_code, day)
                     
                    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                    
                    mkt_data = None
                    try:
                        lines       = stdout_grep.readlines()
                        if len(lines) > 1:
                            logging.error("Several lines found for the folowing client order id: " + str(client_order_id))
                        mkt_data    = lines[0]
                    except Exception,e :
                        self.missing_enrichment[order["ClOrdID"]] = [cmd, order]
                        logging.error("Cannot get data from algos for the following strategy: " +  str(order["StrategyName"]))
                        logging.error("This order could not been enriched from the front end: " + str(order) )
                        get_traceback()
                        mkt_data = None  
                    
                    if mkt_data is not None:
                        mkt_data = mkt_data[:-1]
                        mkt_data = mkt_data.rsplit(',')
                         
                        for u, v in dico_header.iteritems():
                            if mkt_data[v] != '':
                                temp = convert_str(mkt_data[v])
                                if temp is None:
                                    logging.warning("This variable is not is equal to None : occ_fe_" + str(u))
                                else:
                                    order['occ_fe_%s'%u] = temp
                                    
                # Strategy Name mapping
                if "StrategyName"  in order.keys():
                    sweep_lit = None
                    
                    if "SweepLit" in order.keys():
                        sweep_lit = order["SweepLit"]
                        
                    order['strategy_name_mapped'] = mapping.StrategyName(id = order["StrategyName"], sweep_lit=sweep_lit)
                else:
                    logging.error("StrategyName is unknown for this order: " + str(order) )                   
                # Tag to add
                order["job_id"] = job_id
                
                # Verifications
#                 if "SecurityID" not in order.keys():
#                     logging.error("The key SecurityID is not present in this order :" +str(order))           
#                 
#                 if str(order['SecurityID']) in dict_secs.keys():
#                     order.update(dict_secs[str(order['SecurityID'])])
#                 else:
#                     self.missing_ids.append(str(order['SecurityID']))
#                     logging.error("The general Ids could not be found for this SecurityID: " + str(order["SecurityID"]) + " and this order:" + str(order))
#                 
#                 if "cheuvreux_secid" in order.keys() and order["cheuvreux_secid"] is None:
#                     self.missing_ids.append(str(order['SecurityID']))
#                     logging.error("The cheuvreux Sec Id could not be found: " + str(order))

                if "Symbol" in order.keys():
                    try:
                        order["cheuvreux_secid"] = dict_secs_symbol[order["Symbol"]]
                    except:
                        order["cheuvreux_secid"] = ''
                                                
                    if len(order["cheuvreux_secid"]) == 0:
                        del order["cheuvreux_secid"]
                        logging.error("chevreux_secid is unknown for this order: " + str(order) )
                        
                        if "BridgeRejection" in order.keys(): # Means rejected
                            self.missing_ids_reject.append(str(order['Symbol']))
                        else:
                            self.missing_ids.append(str(order['Symbol']))
                        
                else:
                    logging.error("Symbol is unknown for this order: " + str(order) )
                    
                    
                try:   
                    rate = ConversionRate.getRate(order['Currency'], day )
                    if rate is not None: 
                        order['rate_to_euro'] = ConversionRate.getRate(order['Currency'], day )
                    else:
                        logging.error("Could not get the rate for the currency " + str(order['Currency']))
                except KeyError, e:
                    get_traceback()
                    logging.error("This order has no currency: " + str(order))
                    
            except ssh_exception.SSHException, e:
                get_traceback()
                logging.error("SSH Connection problem" )
                self.ssh_attempts += 1
                if self.ssh_attempts < 3:
                    import time
                    time.sleep(60)
                    logging.warning("Will run again algo order filling" )
                    return self.get_algo_orders(day)
                else:
                    logging.error("This order could not be enriched: " + str(order))
            except Exception,e:
                get_traceback()
                logging.error("This order could not be enriched: " + str(order))
            finally:
                l_docs.append(order)
        ssh.close()
        logging.info('Store all the orders')
        logging.info(ConversionRate.default_currencies)    
        return l_docs  
    def get_all_strategy_name(self):
        database = self.client[self.database]
        collection = database["map_tagFIX"]
        result   = collection.find({"tag_name" : "StrategyName"})
        l_result = list(result)
        for el in l_result:
            self.strategy_name[int(el["tag_value"])] = el["strategy_name"]
            
    def get_strategy_name(self, num):
        if num not in self.strategy_name.keys():
            logging.error("This Strategy does not exist " + str(num))
            self.strategy_name[int(num)] = None
        return self.strategy_name[int(num)]
    
    def import_FIXmsg(self, type_order, dico_tags={}, trader=''):
        
        # - io :
        #    I for parent order (incoming orders)
        #    O for child(street) orders (outgoing orders from Execution Report)
         
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.server['ip_addr'], 
                    username = self.server['list_users']['flexsys']['username'], 
                    password = self.server['list_users']['flexsys']['passwd'])
        
        path = self.logPath
        
        orders = []
        if self.io == 'I':
            if type_order != '':
                
                if type_order not in ['D', 'G', 'F']:
                    logging.warning( "error no valid type order ! (only D, G or F are allowed)" )
                else:
                    if trader =='':
                        cmd = 'prt_fxlog %s 3 | grep 35=%s' %(path, type_order)
                    else:
                        cmd = "prt_fxlog %s 3 | egrep '35=%s.*57=%s'" %(path, type_order, trader)
                    
                    [orders, dico_tags] = self.run_and_append(orders, dico_tags, ssh, cmd)
            else:
                l_FIXtype = ['D', 'G', 'F']
                for tp in l_FIXtype:
                    if trader == '':
                        cmd = "prt_fxlog %s 3 | grep 35=%s" %(path, tp)
                    else:
                        cmd = "prt_fxlog %s 3 | egrep '35=%s.*57=%s'" %(path, tp, trader)
                        
                    [orders, dico_tags] = self.run_and_append(orders, dico_tags, ssh, cmd)
                    
        elif self.io == 'O':
            if str(type_order) != '':
                
                if str(type_order) not in ['1', '2'] :
                    logging.warning( "error no valid type ! (only 1 or 2 are allowed)")
                else:
                    if trader =='':
                        cmd = "prt_fxlog %s 3 | egrep '35=8.*150=%s'" %(path, str(type_order))
                    else:
                        cmd = "prt_fxlog %s 3 | egrep '35=8.*50=%s.*150=%s'" %(path, trader, str(type_order))
                    
                    [orders, dico_tags] = self.run_and_append(orders, dico_tags, ssh, cmd)
            else:
                l_FIXtype = ['1', '2']
                for tp in l_FIXtype:
                    if trader == '':
                        cmd = "prt_fxlog %s 3 | egrep '35=8.*150=%s'" %(path, str(tp))
                    else:
                        cmd = "prt_fxlog %s 3 | egrep '35=8.*57=%s.*150=%s'" %(path, trader, str(tp))
                    
                    [orders, dico_tags] = self.run_and_append(orders, dico_tags, ssh, cmd) 
        else:
            logging.warning( "wrong connection IO only 'I' or 'O' accepted !")
        
        return [orders, dico_tags]
    
    def run_and_append(self, orders, dico_tags, ssh, cmd):
        (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
        
        for line in stdout_grep:
            line        = line.replace('|\n','')
            
            res_trans   = self.line_translator(line, dico_tags)
            
            dico_tags   = res_trans[1]
            
            is_DMA      = False
            if "OnBehalfOfSubID" in res_trans[0].keys() :
                is_DMA = (res_trans[0]['OnBehalfOfSubID'] == "DMA")
            
            if res_trans[0] != {} and len(res_trans[0]) > 1 and not is_DMA:
                orders.append(res_trans[0])
                
        return [orders, dico_tags]
    def get_FIX(self, fix_number):
        struct = ET.parse(self.dico_FIX)
        raw_data = struct.getroot()
        i=1
        for elt in raw_data.find('fields'):
            if int(elt.get('number')) == fix_number:
                return (elt.get('name'), elt.get('type'))
        
        return
    def order_life(self, order, day,  dico_tags, dico_trader, import_type='Client'):
    
        # - Open SSH connection 
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.server['ip_addr'], 
                    username = self.server['list_users']['flexsys']['username'], 
                    password = self.server['list_users']['flexsys']['passwd'])
        
        ClOrdID = order['ClOrdID']
        Trader  = order['TargetSubID']
        Type    = order['MsgType']
        
        order_life = []
        p_nb_exec = 0
        nb_replace = 0
        p_exec_qty = 0
        p_real_duration = 0
        global_duration = 0
        p_reason = 'unknown'
        p_eff_starttime = 'none'
        
        p_eff_endtime = 'none'
        g_eff_endtime = 'none'
        
        prev_exec = 0
        num_exec_vwap = 0
        prev_num_exec_vwap = 0
        p_num_exec_vwap = 0
        p_volume_at_would = 0
        
        p_ClOrdID = ClOrdID
        
        if Type == 'D':
            
            # - Looking for Execution report sequence
            if import_type == 'Client':
                ER_file = './logs/trades/%s/FLINKI_%s%sO.fix' %(day, self.source, day)
                IN_file = './logs/trades/%s/FLINKI_%s%sI.fix' %(day, self.source, day)
                cmd = "prt_fxlog %s 3 | egrep '35=8.*50=%s.*%s'" %(ER_file, Trader, ClOrdID)
            elif import_type == 'Street':
                ER_file = './logs/trades/%s/FLEX_ULPROD%sI.fix' %(day, day)
                IN_file = './logs/trades/%s/FLEX_ULPROD%sO.fix' %(day, day)
                cmd = "prt_fxlog %s 3 | egrep '35=8.*%s'" %(ER_file, ClOrdID)
            
            
            (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
            
            done = False
            replaced = False
            
            WouldLevel = 0
            Side = 2 * (float(order['Side']) - 1.5)
            
            if "WouldLevel" in order.keys():
                WouldLevel = order['WouldLevel']
            
            for str_msg in stdout_grep:
                
                str_msg = str_msg.replace('|\n','')
                res_trans = self.line_translator(str_msg,dico_tags)
                
                if res_trans[0] != {}:
                    p_msg = res_trans[0]
                else:
                    continue
                
                dico_tags = res_trans[1]
                
                if 'ExecType' not in p_msg.keys():
                    continue
                
                if str(p_msg['ExecType']) == '0':
                    p_reason = 'new acked'
                    
                elif str(p_msg['ExecType']) == '1':
                    p_exec_qty += int(p_msg['LastShares'])
                    num_exec_vwap += float(p_msg['LastShares']) * float(p_msg['LastPx'])
                    p_nb_exec += 1
                    if p_eff_starttime == "none":
                        p_eff_starttime = p_msg['TransactTime']
                    
                    if Side * float(p_msg['LastPx']) < WouldLevel and WouldLevel != 0 and (Side == 1 or Side == -1):
                        p_volume_at_would += int(p_msg['LastShares'])
                    
                elif str(p_msg['ExecType']) == '2':
                    p_nb_exec += 1
                    p_exec_qty += int(p_msg['LastShares'])
                    num_exec_vwap += float(p_msg['LastShares']) * float(p_msg['LastPx'])
                    
                    if Side * float(p_msg['LastPx']) < WouldLevel and WouldLevel != 0 and (Side == 1 or Side == -1):
                        p_volume_at_would += int(p_msg['LastShares'])
                    
                    p_reason = 'filled'
                    if p_eff_starttime == "none":
                        p_eff_starttime = p_msg['TransactTime']
                        
                    done = True
                    
                elif str(p_msg['ExecType']) == '3':
                    p_reason = 'done for day'
                    done = True
                    
                elif str(p_msg['ExecType']) == '4':
                    p_reason = 'cancelled'
                    done = True
                    
                elif str(p_msg['ExecType']) == '5':
                    p_reason = 'replaced'
                    nb_replace += 1
                    replaced = True
                    
                elif str(p_msg['ExecType']) == '8':
                    p_reason = 'rejected'
                    done = True
                    
                elif str(p_msg['ExecType']) == 'A':
                    p_reason = 'pending new'
                    
                elif str(p_msg['ExecType']) == 'E':
                    p_reason = 'pending replace'
                
                p_eff_endtime = p_msg['SendingTime']
                g_eff_endtime = p_eff_endtime
                
            if not done and not replaced :
                [p_reason, done] = check_EoL(order, p_reason, day, ssh, self.dico_FIX, dico_tags, self.ignore_tags, import_type)
                
                if not done:
                    p_reason = 'Front End handling'
                
                
            prev_exec += p_exec_qty
            prev_num_exec_vwap +=  num_exec_vwap
            p_num_exec_vwap = num_exec_vwap
            
            if p_eff_endtime != "none":
                p_real_duration = time.mktime(p_eff_endtime.timetuple()) - time.mktime(order['SendingTime'].timetuple())
            
            if replaced:
                
                loop_count = 0
                while not done and loop_count < 150 and replaced == True:
                    
                    replaced = False
                    loop_count += 1
                    
                    cmd = "prt_fxlog %s 3 | egrep '35=G.*41=%s'" %(IN_file, ClOrdID)
                    (stdin, stdout_grep, stderr) = ssh.exec_command(cmd)
                    
                    for str_msg in stdout_grep:
                        str_msg = str_msg.replace('|\n','')
                    
                    res_trans = self.line_translator(str_msg, dico_tags)
                    c_msg = res_trans[0]
                    dico_tags = res_trans[1]
                    
                    nb_exec = 0
                    exec_qty = 0
                    reason = 'unknown'
                    eff_starttime = "none"
                    real_duration = 0
                    num_exec_vwap = 0
                    volume_at_would = 0
                    
                    ClOrdID = c_msg['ClOrdID']
                    Trader = c_msg['TargetSubID']
                    
                    WouldLevel = 0
                    if "WouldLevel" in c_msg.keys():
                        WouldLevel = c_msg['WouldLevel']
                    
                    if import_type == 'Client':
                        cmd = "prt_fxlog %s 3 | egrep '35=8.*50=%s.*%s'" %(ER_file, Trader, ClOrdID)
                    elif import_type == 'Street':
                        cmd = "prt_fxlog %s 3 | egrep '35=8.*%s'" %(ER_file, ClOrdID)
        
                    (stdin, stdout_grep, stderr)= ssh.exec_command(cmd)
                    rpl_checked = False
                    cc_msg = None
                    for str_msg in stdout_grep:
                        str_msg = str_msg.replace('|\n','')
                        res_trans = self.line_translator(str_msg, dico_tags)
                        
                        if res_trans[0] != {}:
                            cc_msg = res_trans[0]
                        else:
                            continue
                        
                        dico_tags = res_trans[1]
                        
                        if str(cc_msg['ExecType']) == '0':
                            reason = 'new acked'
                            
                        elif str(cc_msg['ExecType']) == '1':
                            exec_qty += int(cc_msg['LastShares'])
                            num_exec_vwap += float(cc_msg['LastShares']) * float(cc_msg['LastPx'])
                            nb_exec += 1
                            
                            if Side * float(cc_msg['LastPx']) < WouldLevel and WouldLevel != 0 and (Side == 1 or Side == -1):
                                volume_at_would += int(cc_msg['LastShares'])
                            
                            if eff_starttime == "none":
                                eff_starttime = cc_msg['TransactTime']
                            
                        elif str(cc_msg['ExecType']) == '2':
                            nb_exec += 1
                            exec_qty += int(cc_msg['LastShares'])
                            num_exec_vwap += float(cc_msg['LastShares']) * float(cc_msg['LastPx'])
                            
                            if Side * float(cc_msg['LastPx']) < WouldLevel and WouldLevel != 0 and (Side == 1 or Side == -1):
                                volume_at_would += int(cc_msg['LastShares'])
                                
                            reason = 'filled'
                            done = True
                            if eff_starttime == "none":
                                eff_starttime = cc_msg['TransactTime']
                            
                        elif str(cc_msg['ExecType']) == '3':
                            reason = 'done for day'
                            done = True
                            
                        elif str(cc_msg['ExecType']) == '4':
                            reason = 'cancelled'
                            done = True
                            
                        elif str(cc_msg['ExecType']) == '5':
                            if rpl_checked == False:
                                rpl_checked =True
                            else:
                                reason = 'replaced'
                                nb_replace += 1
                                replaced = True
                            
                        elif str(cc_msg['ExecType']) == '8':
                            reason = 'rejected'
                            done = True
                            
                        elif str(cc_msg['ExecType']) == 'A':
                            reason = 'pending new'
                            
                        elif str(cc_msg['ExecType']) == 'E':
                            reason = 'pending replace'
                    
                    if not done and not replaced :
                        [reason, done] = check_EoL(c_msg, reason, day, ssh, self.dico_FIX, dico_tags, self.ignore_tags, import_type)
                        
                        if not done:
                            reason = '%s Front End handling' %reason
                    
                    eff_endtime = order['SendingTime']
                    g_eff_endtime = eff_endtime
                    
                    if eff_endtime != 'none':
                        real_duration = time.mktime(eff_endtime.timetuple()) - time.mktime(c_msg['SendingTime'].timetuple())
                    
                    vwap = 0
                    prev_vwap = 0
                    
                    if exec_qty != 0:
                        vwap = num_exec_vwap / float(exec_qty)
                    
                    if prev_exec != 0:
                        prev_vwap = prev_num_exec_vwap / float(prev_exec)
                    
                    if 'TraderName' in c_msg.keys():
                        c_msg['TraderName'] = match_trader(c_msg['TraderName'], dico_trader)
                    
                    
                    enrichment = {'reason': reason, 'nb_exec' : nb_exec, 'exec_qty': exec_qty, 'duration': real_duration, 'occ_ID': p_ClOrdID, 'occ_prev_exec_qty': prev_exec, 'exec_vwap': vwap, 'occ_prev_exec_vwap': prev_vwap, 'turnover': num_exec_vwap, 'occ_prev_turnover': prev_num_exec_vwap, 'nb_replace': loop_count}
                    
                    if eff_starttime != 'none':
                        enrichment.update({'first_deal_time': eff_starttime})
                    
                    if eff_endtime != 'none':
                        enrichment.update({'eff_endtime': eff_endtime})
                    
                    if volume_at_would != 0:
                        enrichment.update({'volume_at_would': volume_at_would})
                        
                    c_msg.update(enrichment)
                    order_life.append(c_msg)
                    
                    prev_exec += exec_qty
                    prev_num_exec_vwap += num_exec_vwap
                    
            if g_eff_endtime != 'none':
                global_duration = time.mktime(g_eff_endtime.timetuple()) - time.mktime(order['SendingTime'].timetuple())
            
            p_vwap = 0
            if p_exec_qty != 0:
                p_vwap = p_num_exec_vwap / float(p_exec_qty)
            
            enrichment = {'reason': p_reason, 'nb_exec' : p_nb_exec, 'occ_nb_replace' : nb_replace, 'exec_qty': p_exec_qty, 'duration': p_real_duration, 'occ_duration': global_duration, 'occ_ID': p_ClOrdID, 'occ_prev_exec_qty': 0, 'exec_vwap': p_vwap, 'occ_prev_exec_vwap': 0, 'turnover': p_num_exec_vwap, 'occ_prev_turnover': 0}
            
            if p_eff_starttime != 'none':
                enrichment.update({'first_deal_time': p_eff_starttime})
            
            if p_eff_endtime != 'none':
                enrichment.update({'eff_endtime': p_eff_endtime})
            
            if p_volume_at_would != 0:
                enrichment.update({'volume_at_would': p_volume_at_would})
                
            order.update(enrichment)
            
            # - append parent order to order_life list
            if 'TraderName' in order.keys():
                order['TraderName'] = match_trader(order['TraderName'], dico_trader)
            
            order_life.append(order)
            
        else:
            logging.warning( 'invalid order type : only NewOrderSingle (D) are allowed !')
        
        ssh.close()
        return [order_life, dico_tags]
                    
if __name__ == '__main__':
    import sys    
#     print ConversionRate.getRate('GBx', '20130803')
#     sys.exit()
    if len(sys.argv) <= 6:
        print "Usage Examples: "
        print "python2.7 import_FIX.py PARFLTLAB02 PARFLTLAB02 dev I"
        print "python2.7 import_FIX.py PARFLTLAB02 PARFLTLAB02 dev O"
        print "python2.7 import_FIX.py HPP PARFLTLAB02 dev I CLNT1"
        print "python2.7 import_FIX.py HPP PARFLTLAB02 dev O CLNT1"
        print "python2.7 import_FIX.py HPP WATFLT01 preprod I CLNT1"
        print "python2.7 import_FIX.py HPP WATFLT01 preprod O CLNT1"
    
    
        database_server    = 'TEST'
        database    = 'DB_test1'
        server_flex = 'WATFLT01'
        environment = 'preprod'
        io          = 'I'
        source      = 'CLNT1'
    connections.Connections.change_connections('dev')    
    dates       =  ['20130603','20130604','20130605','20130606','20130607',
                    '20130610','20130611','20130612','20130613','20130614',
                    '20130617','20130618','20130619','20130620','20130621',
                    '20130624','20130625','20130626','20130627', '20130628',
                    '20130701','20130702','20130703','20130704','20130705',
                    '20130708','20130709','20130710','20130711','20130712']


    
    database_server     = 'MARS'
    database            = 'Mars'
    environment         = 'prod'
    source              = 'CLNT1'

    dates               = ['20130711']
    
    DatabasePlug(database_server    = database_server, 
                 database           = database,
                 environment        = environment, 
                 source             = source, 
                 dates              = dates,
                 mode               = "write").fill(order_deals=False)
    
