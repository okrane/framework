'''
Created on Mar 12, 2012

@author: syarc
'''
from cheuvreux.bluebox.pta.report import DB
from xlwt import *
import random

def dump_trade_for_stock(stock, ctpy, start, end, ws):
    query = ''' select instrument, substring(t.timestamp,0,9), substring(t.timestamp,10,12), t.buy_sell, t.gross_price, t.quantity, trade.bid_price, trade.offer_price,executionvenuealias
                from quant..orders o
                join fidessa..[High_touch_data] t on t.order_id = o.order_id
                join fidessa..[High Touch Trade Summary Cumulative] trade on trade.order_id = t.order_id
                            and trade.tradedate = t.tradedate and t.trade_id = trade.trade_id
                where date between '%s' and '%s'
                and counterparty = '%s' and done > 0 and instrument = '%s'
                order by t.timestamp
            ''' % (start, end, ctpy, stock)



    row = 1
    for line in DB.selectBig(query):
        for col, elmt in enumerate(line):
            ws.write(row, col, elmt)
        row+=1

        #for elmt in enumerate(line)
        #print '\t'.join(map(str,line))


def run(ctpy, start, end, size=5):
    query = ''' SELECT instrument, count(*) FROM quant..orders
                WHERE counterparty = '%s' AND date between '%s' and '%s'
                AND done > 0
                GROUP BY instrument having count(*) BETWEEN 50 and 200
                ORDER BY count(*) DESC
            ''' % (ctpy, start, end)

    stocks = []
    for line in DB.select(query):
        stocks.append(line[0])

    while len(stocks) > size:
        del stocks[random.randint(0,len(stocks) - 1)]

    w = Workbook()

    for stock in stocks:
        ws = w.add_sheet(stock)
        for col, header in enumerate(['Stock', 'Date', 'Time', 'Side' , 'Gross Price', 'Quantity',
                                          'Bid', 'Ask', 'Venue']):
            ws.write(0, col, header)
        dump_trade_for_stock(stock, ctpy, start, end, ws)

    w.save('%s_%s_%s.xls' % (ctpy, start, end))


if __name__ == '__main__':
    run('LOOP', '20120305', '20120309')