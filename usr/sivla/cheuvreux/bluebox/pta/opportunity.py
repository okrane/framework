'''
Created on Mar 23, 2012

@author: syarc
'''
from cheuvreux.stdio.ssh import SSHConfiguration, SSHException
import os
import datetime
from cheuvreux.fidessa import fidessadb, SecurityIdCache
from cheuvreux.utils.date import workday_range
from cheuvreux.utils.statistics import summary, variance
import math
import traceback
from cheuvreux.dbtools.tickdb import TickDB

def stat():
    db = fidessadb.getODBCConnection()

    query = ''' select  A.order_id, done, destination, side * (parent_gross_price - gross_price) / parent_gross_price,
                side * (vsot - gross_price) / vsot,
                o.forward_signal, o.backward_signal
                from (
                    select o.order_id, parent_gross_price / (1 - perf_vwap / (10000 * o.side)) as vsot
                    from opportunity_view o
                    join tca t on t.order_id = o.parent_order_id
                    where o.done > 0
                ) A
                join opportunity_view o on o.order_id = A.order_id
            '''

    perf_gross_price = []
    perf_vsot = []
    perf_vwap = []

    forward_perf = {'0.5': [], '0.6' : [], '0.7': [], '0.8': [], '0.9': []}
    backward_perf = {'0.5': [], '0.6' : [], '0.7': [], '0.8': [], '0.9': []}

    for row in db.select(query):
        perf_gross_price.append(row[3])
        perf_vsot.append(row[4])

        for_signal = '%.1f' %  row[5]
        if for_signal == '1.0':
            for_signal = '0.9'

        forward_perf[for_signal].append(row[4])
        back_signal = '%.1f' % row[6]
        if back_signal == '1.0':
            back_signal = '0.9'
        backward_perf[back_signal].append(row[4])

    def print_stat(perf):
        print '\t'.join(map(lambda x : '%.0f' % (10000*x), summary(perf) + [math.sqrt(variance(perf))]) + [str(len(perf))])
        #print 'P( > 0) = ', '%.2f' % (100.0 * len(filter(lambda x: x > 0, perf)) / len(perf))

    print_stat(perf_gross_price)
    print_stat(perf_vsot)

    print 'Forward Perf'
    for key in forward_perf:
        print_stat(forward_perf[key])

    print 'Backward perf'
    for key in backward_perf:
        print_stat(backward_perf[key])


def stat_vwap():
    db = fidessadb.getODBCConnection()
    query = ''' SELECT ov.order_id, o.instrument, o.market_id, forward_signal, backward_signal,
                            o.end_time, o.date, ao.end_time, ao.side
                from opportunity_view ov
                join orders o on o.order_id = ov.order_id
                join algo_orders ao on ao.order_id = ov.parent_order_id
                where vsot_after is null
                union
                SELECT ov.order_id, o.instrument, o.market_id, forward_signal, backward_signal,
                            o.end_time, o.date, ao.end_time, ao.side
                from opportunity_view ov
                join algo_orders o on o.order_id = ov.order_id
                join algo_orders ao on ao.order_id = ov.parent_order_id
                where vsot_after is null
                '''

    rows = db.select(query)
    for row in rows:
        try:
            security = SecurityIdCache.getSecurity(row[1], row[2])
        except ValueError, e:
            print "Value Error for", row[1]
            traceback.print_exc()
        if security:
            vsot = TickDB.vwap(security, row[6].replace('-',''), row[5][0:8], row[7][0:8], None, row[8])
        else:
            print 'SECURITY NOT FOUND: ', row
            vsot = None
        print row[0], row[3], row[4], vsot, row[5][0:8], row[7][0:8]
        if vsot:
            db.run('UPDATE opportunity SET VSOT_AFTER = ? WHERE ORDER_ID = ?', vsot, row[0])


def process_vwap(date_range):
    '''
    Insert data in vwap_experiment
    '''
    pool = SSHConfiguration('prod')
    client = pool.getClient('65.244.97.57')

    data = {}

    for date in date_range:
        sdate = date.strftime('%Y%m%d')
        filename = 'parameters_CASE_PROD_FWK_1_%s.txt' % sdate
        try:
            client.get('/ext1/RFWK/runtime/parameters/%s' % filename, '.')
            for line in open(filename, 'r'):
                fields = line.strip().split(',')
                if len(fields) == 3:
                    name, value = fields[1].split('=')
                    if name == 'useOpportunitySignal':
                        if fields[0] not in data:
                            data[fields[0]] = [1, 0, 0]
                        else:
                            data[fields[0]][0] = 1
                        
                    elif name == 'useMidPointPosting':
                        if fields[0] not in data:
                            data[fields[0]] = [0, 1, 0]
                        else:
                            data[fields[0]][1] = 1
                    elif name == 'VWAPuseImbalance':
                        if fields[0] not in data:
                            data[fields[0]] = [0, 0, 1]
                        else:
                            data[fields[0]][2] = 1
                        
        except:
            pass

    stmt = ('INSERT INTO "VWAP_EXPERIMENT" (ORDER_ID, OPPORTUNITY, MIDPOINT, IMBALANCE) VALUES (?, ?, ?, ?)')

    db = fidessadb.getODBCConnection(autocommit=False)
    db.executemany(stmt, [(o,d[0],d[1],d[2]) for o,d in data.iteritems()])
    db._conn.commit()


def process_reduce(date_range):
    pool = SSHConfiguration('prod')
    client = pool.getClient('65.244.97.57')

    db = fidessadb.getODBCConnection(autocommit=False)
    
    records = []

    for date in date_range:
        sdate = date.strftime('%Y%m%d')
        filename = 'parameters_CASE_PROD_FWK_1_%s.txt' % sdate
        
        try:
            client.get('/ext1/RFWK/runtime/parameters/%s' % filename, '.')
            
            for line in open(filename, 'r'):
                fields = line.strip().split(',')
                if len(fields) == 6:
                    record = [None,None]
                    time, parent_order_id = None, fields[0]
                    for i in range(1,len(fields)-1):
                        name, value = fields[i].split('=')
                        if name == 'forwardSignal':
                            record[1] = min(max(float(value),0),1)
                        elif name == 'time':
                            time = value

                    if time: 
                        query = ''' SELECT order_id FROM quant..orders 
                                         WHERE parent_order_id = '%s' AND notes = 'reduced'
                                          and abs(datediff(second,'%s',start_time)) < 2
                                     ''' % (parent_order_id, time)
                                     
                        # look for order id 
                        rows = db.selectOne(query)
                        
                        if not rows: # try algo table
                            query = ''' SELECT ao.order_id FROM quant..algo_orders ao
                                         JOIN fidessa..[High Touch Orders Cumulative] o on o.order_id = ao.order_id and o.version = 1
                                         WHERE ao.parent_order_id = '%s' and order_note_id = 'reduced' 
                                          and abs(datediff(second,'%s',start_time)) < 2
                                     ''' % (parent_order_id, time)
                                     
                            # look for order id 
                            rows = db.selectOne(query)
                            
                        if not rows: 
                            query =  ''' SELECT order_id FROM quant..algo_orders ao
                                         WHERE parent_order_id = '%s' 
                                          and abs(datediff(second,'%s',start_time)) < 2
                                     ''' % (parent_order_id, time)
                                     
                            # look for order id 
                            rows = db.selectOne(query)
                            
                        if rows:
                            record[0] = rows[0] 
                            records.append(record)
                            #print record
                        else:
                            print 'Not found', line.strip()
            os.remove(filename)
        except SSHException:
            pass

    col = {'opportunitySliceId':'', 'forwardSignal':''}
    stmt = ('INSERT INTO "opportunity" (ORDER_ID, BACKWARD_SIGNAL, FORWARD_SIGNAL, IS_REDUCED) VALUES (?,NULL,?,1)')
    db.executemany(stmt, [r for r in records])
    db._conn.commit()
    
def process(date_range):
    pool = SSHConfiguration('prod')
    client = pool.getClient('65.244.97.57')

    def generator():
        for date in date_range:
            sdate = date.strftime('%Y%m%d')
            filename = 'parameters_CASE_PROD_FWK_1_%s.txt' % sdate
            try:
                client.get('/ext1/RFWK/runtime/parameters/%s' % filename, '.')
                for line in open(filename, 'r'):
                    fields = line.strip().split(',')
                    if len(fields) == 5:
                        data = [None,None,None]
                        for i in range(1,4):
                            name, value = fields[i].split('=')
                            if name == 'opportunitySliceId':
                                data[0] = value
                            elif name == 'backwardSignal':
                                data[1] = min(max(float(value),0),1)
                            elif name == 'forwardSignal':
                                data[2] = min(max(float(value),0),1)

                        yield data
                os.remove(filename)
            except SSHException:
                pass

    col = {'opportunitySliceId':'', 'backwardSignal':'', 'forwardSignal':''}
    stmt = ('INSERT INTO "opportunity" (ORDER_ID, BACKWARD_SIGNAL, FORWARD_SIGNAL) VALUES (?,?,?)')

    db = fidessadb.getODBCConnection(autocommit=False)
    db.executemany(stmt, [d for d in generator()])
    db._conn.commit()

if __name__ == '__main__':
    
    process_vwap(workday_range(datetime.date(2012,8,6),datetime.date(2012,8,6)))
    process(workday_range(datetime.date(2012,8,6),datetime.date(2012,8,6)))
    #process_reduce(workday_range(datetime.date(2012,8,6),datetime.date(2012,8,6)))
    #stat_vwap()


