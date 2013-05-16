'''
Created on Oct 26, 2011

@author: syarc
'''


from cheuvreux.fidessa.audit_trail import AuditTrail
from cheuvreux.fidessa.fidessadb import FidessaDB
from cheuvreux.stdio.html import HtmlTable
from cheuvreux.stdio.mail import Email, HtmlEmail
from cheuvreux.utils.date import workday_range
from collections import defaultdict
import datetime
import locale
import operator
import os
import sys
from cheuvreux.fidessa import fidessa_util
import gzip

fidessa = FidessaDB()

ignore_service = ['CACD2', 'MES', 'ORS', 'MONTAGE']

# Global map for storing security primary market
securityMarketMap = {}

class FlowAggregatorByMarket(object):
    def __init__(self):
        self.market = defaultdict(FlowAggregator)

    def addMarket(self, market, destination):
        self.market[market].addMarket(destination)

    def addLimit(self, market, destination):
        self.market[market].addLimit(destination)

    def addDMA(self, market, destination):
        self.market[market].addDMA(destination)

    def total(self):
        total = 0
        for agg in self.market.values():
            total += agg.total()
        return total

    def __iter__(self):
        return self.market.__iter__()

    def __len__(self):
        return len(self.market)

    def __contains__(self, market):
        return market in self.market

    def __getitem__(self, market):
        return self.market[market]

class FlowAggregator(object):
    def __init__(self):
        self.limit = defaultdict(int)
        self.market = defaultdict(int)
        self.dma = defaultdict(int)

    def addLimit(self, destination):
        self.limit[destination] += 1

    def addMarket(self, destination):
        self.market[destination] += 1

    def addDMA(self, destination):
        self.dma[destination] += 1

    def limitCount(self, destination):
        return self.limit[destination]

    def marketCount(self, destination):
        return self.market[destination]

    def dmaCount(self, destination):
        return self.dma[destination]

    def total(self):
        total = sum(count for count in self.limit.values())
        total += sum(count for count in self.market.values())
        return total

    def __iter__(self):
        return set(self.limit.keys() + self.market.keys() + self.dma.keys()).__iter__()

def getMarketId(orderId):
    return "ALL"

    marketId = None #fidessa.getMarketId(orderId)
    if marketId is None:
        print orderId, ' has no market id'
        return 'OTHER'
    elif marketId.startswith('NAS'):
        return 'NASDAQ'
    elif marketId.startswith('NYS'):
        return 'NYSE'
    else:
        return 'OTHER'

def getFlows(start, end, output=sys.stdout, html=False):
    aggregator = FlowAggregatorByMarket()
    if not end:
        end = start
    for date in workday_range(start, end):
        aggregator = getFlowsForDate(date.strftime("%Y%m%d"), aggregator)

    for market in aggregator:

        agg = aggregator[market]
        result = []

        for destination in agg:
            lim, mkt, dma = agg.limitCount(destination), agg.marketCount(destination), agg.dmaCount(destination)
            subtotal = float(lim + mkt)

            try :
                mkt_pct = 100.0 * mkt / subtotal
                lim_pct = 100.0 * lim / subtotal
            except ZeroDivisionError:
                # can happen due to DMA flow (NYSE stock sent to NASDAQ)
                # It should be very little quantity though
                mkt_pct, lim_pct = 0, 0

            result.append([destination,
                      100.0 * subtotal / agg.total(),
                      "%.2f" % mkt_pct,
                      "%.2f" % lim_pct,
                      locale.format("%d", mkt, grouping=True),
                      locale.format("%d", lim, grouping=True),
                      locale.format("%d", dma, grouping=True), ])

        if html:
            table = HtmlTable()
            table.setBorder()
            locale.setlocale(locale.LC_ALL, '')
            print >> output, "<h2>" + market + "</h2>"
            print >> output, table.header(['Destination', '% Total', '% Market', '% Limit', '# Market', '# Limit', '# DMA'], width=[200,100])
        else:
            print >> output, market
            print >> output, 'destination, %total, %market, %limit, market, limit, dma'


        for line in sorted(result, key=operator.itemgetter(1), reverse=True):
            line[1] = "%.2f" % line[1]

            if html:
                print >> output, table.line(line)
            else:
                print >> output, ",".join(line)

        if html:
            print >> output, table.end()

def getFlowsForDate(date, aggregator):
    file = AuditTrail.searchAuditFile(date)

    if not file or not os.path.exists(file):
        print "Audit trail for %s not found" % date
        return


    if file.endswith('.gz'):
        fd = gzip.open(file, 'rb')
    else:
        fd = open(file, 'r')

    for line in fd:
        fields = line.split('|')

        try:
            isDma = fields[20].strip() == 'Y'
        except IndexError:
            continue

        if fields[6] == 'BLUEBOX':
            continue

        if fields[2] in ('CHLD', 'ASSN', 'AGYE'):
            if fields[6] in ignore_service:
                continue

            price = fields[13]
            if not price:
                price = fields[14]

            destination = fields[6]
            if fields[7] != '':
                destination += ' ' + fields[7]

            if fields[19] not in securityMarketMap:
                securityMarketMap[fields[19]] = getMarketId(fields[0])

            primary_market = securityMarketMap[fields[19]]

            if isDma:
                aggregator.addDMA(primary_market, destination)
            elif fields[10].find(' at market') > 0 and fields[2] != 'AGYE':
                aggregator.addMarket(primary_market, destination)
            elif fields[2] != 'AGYE':
                aggregator.addLimit(primary_market, destination)

    fd.close()
    return aggregator

if __name__ == '__main__':
    mail = HtmlEmail("smtpnotes")
    mail.set_sender("sarchenault@cheuvreux.com")
    mail.set_type('html')
    mail.set_subject('606')

    getFlows(datetime.date(2010, 10, 1), datetime.date(2010, 12, 31), mail, True)

    mail.send('sarchenault')

