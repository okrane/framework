'''
Created on Apr 4, 2011

@author: syarc
'''
from cheuvreux.fidessa.fidessadb import FidessaDB
from cheuvreux.dbtools.Sqlite import SQLiteBase
import math

tca = SQLiteBase('tca_us.db')
db = FidessaDB()

def curve():
    global tca, db
    
    se_turnover, turnover = [], []
    se_perf, perf = [], []
    se_raw, raw = [], []
    
    vwaps = tca.select("SELECT order_id, quantity, gross_price, perf_vwap FROM tca_us WHERE date >= '20110301' and date < '20110501' and algo = 'VWAP' ")
    
    #vwap_ids = [v[0] for v in vwaps]
    
    se_order_id = set()
    for line in open('C:/cygwin/tmp/se_volume_curve_vwap'):
        se_order_id.add(line.strip())
            
    for vwap in vwaps:
        
        order = db.getFidessaOrder(vwap[0])
        if not order.isMarketOrder or abs(vwap[3]) >= 150:
            continue
        
        gross = vwap[1] * vwap[2]
        
        if vwap[0] in se_order_id:
            se_turnover.append(gross)
            se_perf.append(gross * vwap[3])
            se_raw.append(vwap[3])
        else:
            turnover.append(gross)
            perf.append(gross * vwap[3])
            raw.append(vwap[3])
            
    se_avg, avg = sum(se_perf) / sum(se_turnover), sum(perf) / sum(turnover)
    
    se_error = error = 0
    for p, t in zip(raw, turnover):
        error += t * (p - avg)**2
    for p, t in zip(se_raw, se_turnover):
        se_error += t * (p - se_avg) ** 2
    
    print 'SE VWAP: %.2f [%.2f] %d' % (se_avg, math.sqrt(se_error / sum(se_turnover)), len(se_perf))
    print 'VWAP: %.2f [%.2f] %d' % (avg, math.sqrt(error / sum(turnover)), len(perf))


if __name__ == '__main__':
    curve()

