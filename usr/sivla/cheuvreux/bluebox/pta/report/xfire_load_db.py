'''
Created on Aug 2, 2012

@author: syarc
'''
from cheuvreux.stdio.ssh import SSHConfiguration, SSHException
import os
import datetime
from cheuvreux.bluebox.pta.report import DB
from cheuvreux.utils.date import workday_range
import pyodbc

def process_file(client, filename, order_ids):
    tmp_order_id = set()
    try:
        client.get('/ext1/RFWK/runtime/parameters/%s' % filename, '.')
        for line in open(filename, 'r'):
            order_id, params = line.split(',')[0:2]
            if order_id in tmp_order_id:
                continue
            tmp_order_id.add(order_id)
            l = len(params.split(';'))
            if l > 2:
                order_ids.add( (order_id, l < 9))
        os.remove(filename)
    except SSHException:
        pass

def load_order_ids(dates):
    pool = SSHConfiguration('prod')
    client = pool.getClient('65.244.97.57')

    order_ids = set()

    for date in dates:
        sdate = date.strftime('%Y%m%d')
        filename = 'parameters_CASE_PROD_FWK_6_%s.txt' % sdate
        process_file(client, filename, order_ids)
        filename = 'parameters_ALGO_MODELS_FWK_1_%s.txt' % sdate
        process_file(client, filename, order_ids)
        filename = 'parameters_ALGO_MODELS_FWK_2_%s.txt' % sdate
        process_file(client, filename, order_ids)
        filename = 'parameters_ALGO_MODELS_FWK_3_%s.txt' % sdate
        process_file(client, filename, order_ids)

    return order_ids

def generate_seq(order_ids):
    
    records = []
    
    query = ''' select parent_order_id, o.destination, o.order_id from quant..orders o
                where parent_order_id in ('%s')
                order by o.parent_order_id, o.order_id 
            ''' % ('\',\''.join([o[0] for o in order_ids]))
            
            
    seq = 1
    last_parent_order_id = last_destination = None
    idx = set ()
    for row in DB.select(query):
        if not last_parent_order_id or row[0] != last_parent_order_id:
            seq = 1
            last_destination = None
        else:
            seq += 1
          
        if (row[2], row[0]) not in idx:
            idx.add( (row[2], row[0]) )
        #print row[2], row[0]
            
        records.append([row[2], row[0], seq, row[1], last_destination])
        
#        tmp = [row[2], row[0], seq, row[1], last_destination]
#        DB.executemany(''' INSERT INTO XFIRE_SEQ 
#                          (ORDER_ID, PARENT_ORDER_ID, SEQ, DESTINATION, LAST_DESTINATION)
#                       VALUES
#                          (?,?,?,?,?)
#                    ''', [tmp])

        last_parent_order_id = row[0]
        last_destination = row[1]
        
    
    DB.executemany(''' INSERT INTO XFIRE_SEQ 
                          (ORDER_ID, PARENT_ORDER_ID, SEQ, DESTINATION, LAST_DESTINATION)
                       VALUES
                          (?,?,?,?,?)
                    ''', [r for r in records])
        
def main(dates):
    
    for date in dates:
        try:
            # load order id from files on fidessa server
            order_ids = load_order_ids([date])
            # Insert them in the database
            #DB.executemany('INSERT INTO quant..XFIRE_ID (ORDER_ID, NEW) VALUES (?, ?)', [g for g in order_ids])
            # Insert data in XFIRE_ORDER_ID_SEQ
            generate_seq(order_ids)
        except pyodbc.IntegrityError, e:
            print date
            raise e

if __name__ == '__main__':
    main(workday_range(datetime.date(2012,6,29), datetime.date(2012,6,29)))