'''
Created on Aug 23, 2011

@author: jcmau
'''

import os
from datetime import datetime, timedelta
from cheuvreux.stdio.mail import HtmlEmail
import csv
from collections import defaultdict
from cheuvreux.utils.dataset import Percentage
import locale
from cheuvreux.dbtools.Sqlite import SQLiteBase
import sys
from cheuvreux import fidessa
import gzip
from cheuvreux.stdio.html import HtmlTable

locale.setlocale(locale.LC_ALL, '')

db = SQLiteBase('Cpty_Limits.db')


def initCptyLimits ( limits ):

    db.execute("delete from counterparty_limits ")
    for row in limits:
        if row[0] != "#":
            print row
            query= "INSERT INTO counterparty_limits VALUES (%d,'%s','%s',%d,%d,%d,%d,%d,%d,%d,'%s')" % (int(row[0]), row[1], row[2], int(row[5]),int(row[6]),int(row[8]),int(row[9]),int(row[18]),int(row[19]),int(row[16]),row[17])
            print query
            db.execute(query)
        else:
            pass

def testInt(value):
    try:
        int(value)
        return int(value)
    except ValueError:
        return float(value)

def get_email (demo_mode):
    # send e-mail to say no change occurred
    email = HtmlEmail('smtpnotes')
    email.set_sender('sarchenault')
    if demo_mode == True:
        email.set_dest('sarchenault')
    else:
        email.set_dest('jimperato, jbouquot, cgilbeau, maquino, mgartman, sarchenault, dhegarty, bspillane')

    return email

def insertNewRecords ( newLimit, file_date ):
    global db
    # 0s sometimes come in as 0.0 vs. 0 so must test for float
    sOrdCons = testInt(newLimit[5])
    sDaily   = testInt(newLimit[6])
    dOrdCons = testInt(newLimit[8])
    dDaily   = testInt(newLimit[9])
    cOrdCons = testInt(newLimit[18])
    cDaily   = testInt(newLimit[19])
    #print sOrdCons, sDaily, dOrdCons, dDaily, cOrdCons, cDaily
    query= "INSERT INTO counterparty_limits VALUES (%d,'%s','%s',%d,%d,%d,%d,%d,%d,%d,'%s','%s')" % (int(newLimit[0]), newLimit[1], newLimit[2], sOrdCons,sDaily,dOrdCons,dDaily,cOrdCons,cDaily,int(newLimit[16]),newLimit[17], file_date)
    db.execute(query)

def insertLatestCheck ( client, newLimit ):
    global db
    query= "INSERT INTO counterparty_check VALUES ('%s',%d)" % (client, int(newLimit))
    db.execute(query)

def getRowsToComapre (lastRowProcessed):
    global db
    rows = db.select('''select COUNTERPARTY, STRAIGHT_ORDER_CONS_LIMIT, STRAIGHT_DAILY_CONS_LIMIT, DISC_ORDER_CONS_LIMIT, DISC_DAILY_CONS_LIMIT, COMBINED_ORDER_CONS_LIMIT, COMBINED_DAILY_CONS_LIMIT, VERSION, ENTERED_BY from counterparty_limits where NUMBER > %d group by counterparty''' % (lastRowProcessed))
    return rows

def getLastDetails (currentDetails):
    global db
    rows = db.selectOne('''select COUNTERPARTY, STRAIGHT_ORDER_CONS_LIMIT, STRAIGHT_DAILY_CONS_LIMIT, DISC_ORDER_CONS_LIMIT, DISC_DAILY_CONS_LIMIT, COMBINED_ORDER_CONS_LIMIT, COMBINED_DAILY_CONS_LIMIT, VERSION, ENTERED_BY from counterparty_limits where counterparty = '%s' and version = (select last_version from counterparty_check where counterparty = '%s') ''' % (currentDetails[0],currentDetails[0]))
    return rows

def getMaxRow ():
    global db
    max = db.selectOne('''SELECT max(NUMBER) from counterparty_limits''')
    return max

def getRowNums ():
    global db
    max = db.select('''SELECT NUMBER from counterparty_limits ''')
    return max

def updateLastChecked(client, newVersion):
    global db
    query = "UPDATE counterparty_check set LAST_VERSION = %d where COUNTERPARTY = '%s' " % (int(newVersion), client)
    db.execute(query)


def dollar_html(str):
    return '<span style="color:#000000;font-weight:bold">$%s</span>' % str

def unusual_html(str, diff):
    if diff != 0:
        if diff > 0:
            return '<span style="color:#008000;font-weight:bold">$%s</span>' % str
        else:
            return '<span style="color:#FF0000;font-weight:bold">$%s</span>' % str
    else:
        return '<span style="color:#000000;font-weight:bold">$%s</span>' % str


init = False
demo_mode = False

if demo_mode == True:
    folder_date = ''
    file_date = '20120503'
    counterparty_control = fidessa.find_file('COUNTERPARTY_CONTROL.%s.psv', file_date)
else:
    now = datetime.now()
    folder_date = now.strftime("%Y%m%d")
    if datetime.weekday(now) == 0:
        file_date = (now + timedelta(-3)).strftime("%Y%m%d")
    else:
        file_date = (now + timedelta(-1)).strftime("%Y%m%d")

    counterparty_control = fidessa.find_file('COUNTERPARTY_CONTROL.%s.psv', file_date)

#print counterparty_control

if counterparty_control.endswith('.gz'):
    fd = gzip.open(counterparty_control, 'rb')
else:
    fd = open(counterparty_control,'r')

column_data = csv.reader(fd, delimiter='|')
column_data_dict = csv.DictReader(fd)

if (init):
    initCptyLimits(column_data)
else:
    pass

lastRowProcessed = getMaxRow()
#print lastRowProcessed[0]
latestRowAvailable = 0
rowsCurrentFile = []
for row in column_data:
    if row[0] != "#":
        rowsCurrentFile.append(row)
        latestRowAvailable = int(row[0])

#print rowsCurrentFile
#print latestRowAvailable

if lastRowProcessed[0] < latestRowAvailable:
    print "Greater than last row processed "
    for row in rowsCurrentFile:
        if row[0] != "#" and int(row[0]) > lastRowProcessed[0]:
            insertNewRecords(row, file_date)
else:
    email = get_email(demo_mode)
    email.set_subject('No credit limits changed ' + file_date)
    email.flush()
    sys.exit(0)

#print "lastRowProcessed: ", lastRowProcessed[0]
rowsToProcess = getRowsToComapre(lastRowProcessed[0])
#print "rowsToProcess: ", rowsToProcess
#sys.exit(0)

headerC = ['Combined daily limit','Combined daily %','Combined order limit','Combined order %','Entered by']
headerNC = ['Dir daily limit','Dir daily %','Dir order limit','Dir order %','Care daily limit','Care daily %','Care order limit','Care order %','Entered by']

client_list = defaultdict(lambda: defaultdict(int))
client_list2 = defaultdict(lambda: defaultdict(int))



for eachNewLimit in rowsToProcess:

    previousRecord = getLastDetails(eachNewLimit)
    #print "previous record: ", previousRecord
    #print "new limit: ", eachNewLimit

    if previousRecord == None:
        sOrderLim = abs(eachNewLimit[1])
        if sOrderLim == 0:
            sOrderPct = 0
        else:
            sOrderPct = 1

        sDailyLim = abs(eachNewLimit[2])
        if sDailyLim == 0:
            sDailyPct = 0
        else:
            sDailyPct = 1

        dOrderLim = abs(eachNewLimit[3])
        if dOrderLim == 0:
            dOrderPct = 0
        else:
            dOrderPct = 1

        dDailyLim = abs(eachNewLimit[4])
        if dDailyLim == 0:
            dDailyPct = 0
        else:
            dDailyPct = 1

        cOrderLim = abs(eachNewLimit[5])
        if cOrderLim == 0:
            cOrderPct = 0
        else:
            cOrderPct = 1

        cDailyLim = abs(eachNewLimit[6])
        if cDailyLim == 0:
            cDailyPct = 0
        else:
            cDailyPct = 1
    else:

        sOrderLim = eachNewLimit[1] - previousRecord[1]
        if sOrderLim == 0:
            sOrderPct = 0
        elif previousRecord[1] == 0:
            if eachNewLimit[1] != 0:
                sOrderPct = 1
            else:
                sOrderPct = 0
        elif sOrderLim > 0:
            sOrderPct = float(sOrderLim) / float(previousRecord[1])
        else:
            sOrderPct = float(abs(sOrderLim)) / float(previousRecord[1])

        sDailyLim = eachNewLimit[2] - previousRecord[2]
        if sDailyLim == 0:
            sDailyPct = 0
        elif previousRecord[2] == 0:
            if eachNewLimit[2] !=0:
                sDailyPct = 1
            else:
                sDailyPct = 0
        elif sDailyLim > 0:
            sDailyPct = float(sDailyLim) / float(previousRecord[2])
        else:
            sDailyPct = float(abs(sDailyLim)) / float(previousRecord[2])

        dOrderLim = eachNewLimit[3] - previousRecord[3]
        if dOrderLim == 0:
            dOrderPct = 0
        elif previousRecord[3] == 0:
            if eachNewLimit[3] !=0:
                dOrderPct = 1
            else:
                dOrderPct = 0
        elif dOrderLim > 0:
            dOrderPct = float(dOrderLim) / float(previousRecord[3])
        else:
            dOrderPct = (float(abs(dOrderLim)) / float(previousRecord[3]))

        dDailyLim = eachNewLimit[4] - previousRecord[4]
        if dDailyLim == 0:
            dDailyPct = 0
        elif previousRecord[4] == 0:
            if eachNewLimit[4] !=0:
                dDailyPct = 1
            else:
                dDailyPct = 0
        elif dDailyLim > 0:
            dDailyPct = float(dDailyLim) / float(previousRecord[4])
        else:
            dDailyPct = float(abs(dDailyLim)) / float(previousRecord[4])

        # for future use once STAR migrated to use consolidated limits or
        # automatic upload from Paris is in place
        cOrderLim = previousRecord[5] - eachNewLimit[5]
        if cOrderLim == 0:
            cOrderPct = 0
        elif previousRecord[5] == 0:
            if eachNewLimit[5] !=0:
                cOrderPct = 1
            else:
                cOrderPct = 0
        elif cOrderLim > 0:
            cOrderPct = float(cOrderLim) / float(previousRecord[5])
        else:
            cOrderPct = float(abs(cOrderLim)) / float(previousRecord[5])

        cDailyLim = previousRecord[6] - eachNewLimit[6]
        if cDailyLim == 0:
            cDailyPct = 0
        elif previousRecord[6] == 0:
            if eachNewLimit[6] !=0:
                cDailyPct = 1
            else:
                cDailyPct = 0
        elif cDailyLim > 0:
            cDailyPct = float(cDailyLim) / float(previousRecord[6])
        else:
            cDailyPct = float(abs(cDailyLim)) / float(previousRecord[6])

    #print sOrderLim
    #print sOrderPct
    #print sDailyLim
    #print sDailyPct

    #print dOrderLim
    #print dOrderPct
    #print dDailyLim
    #print dDailyPct
    #print "cOrderLim: ", cOrderLim
    #print "cOrderPct: ", cOrderPct
    #print "cDailyLim: ", cDailyLim
    #print "cDailyPct: ", cDailyPct

    if (sOrderLim != 0 or sDailyLim !=0 or dOrderLim != 0 or dDailyLim != 0
        or cOrderLim != 0 or cDailyLim != 0):
        # set the previous version info
        #if previousRecord != None:
            #client_list[previousRecord[0] + '|' + str(previousRecord[7])][header[0]] = dollar_html(locale.format("%d", int(previousRecord[2]), grouping=True))
            #client_list[previousRecord[0] + '|' + str(previousRecord[7])][header[1]] = dollar_html(locale.format("%d", int(previousRecord[1]), grouping=True))
            #client_list[previousRecord[0] + '|' + str(previousRecord[7])][header[2]] = dollar_html(locale.format("%d", int(previousRecord[4]), grouping=True))
            #client_list[previousRecord[0] + '|' + str(previousRecord[7])][header[3]] = dollar_html(locale.format("%d", int(previousRecord[3]), grouping=True))
            #client_list[previousRecord[0] + '|' + str(previousRecord[7])][header[4]] = dollar_html(locale.format("%d", int(previousRecord[6]), grouping=True))
            #client_list[previousRecord[0] + '|' + str(previousRecord[7])][header[5]] = dollar_html(locale.format("%d", int(previousRecord[5]), grouping=True))
            #client_list[previousRecord[0] + '|' + str(previousRecord[7])][header[6]] = previousRecord[7]
            #client_list[previousRecord[0] + '|' + str(previousRecord[7])][header[7]] = previousRecord[8]
        #else:
            #pass
            # set the new version info
        # only create a report if entery by not "OMAR"
        user = eachNewLimit[8].split('@')
        if (cOrderLim != 0 or cDailyLim != 0):
            if eachNewLimit[8] != "OMAR":
                #print eachNewLimit[8] , " combined"
                if (cDailyLim != 0):
                    client_list[eachNewLimit[0]][headerC[0]] = unusual_html(locale.format("%d", int(eachNewLimit[6]), grouping=True), cDailyLim)
                    client_list[eachNewLimit[0]][headerC[1]] = Percentage(cDailyPct, True)
                else:
                    client_list[eachNewLimit[0]][headerC[0]] = locale.format("%d", int(eachNewLimit[6]), grouping=True)
                    client_list[eachNewLimit[0]][headerC[1]] = Percentage(cDailyPct, True)

                if cOrderLim <= 0:
                    client_list[eachNewLimit[0]][headerC[2]] = unusual_html(locale.format("%d", int(eachNewLimit[5]), grouping=True), abs(cOrderLim))
                else:
                    client_list[eachNewLimit[0]][headerC[2]] = unusual_html(locale.format("%d", int(eachNewLimit[5]), grouping=True), (-1 * cOrderLim))

                client_list[eachNewLimit[0]][headerC[3]] = Percentage(cOrderPct, True)
                client_list[eachNewLimit[0]][headerNC[8]] = user[0]
            else:
                pass
        else:
            if eachNewLimit[8] != "OMAR":
                #print eachNewLimit[8] , " not combined"
                client_list2[eachNewLimit[0]][headerNC[0]] = unusual_html(locale.format("%d", int(eachNewLimit[2]), grouping=True), sDailyLim)
                client_list2[eachNewLimit[0]][headerNC[1]] = Percentage(sDailyPct, True)
                client_list2[eachNewLimit[0]][headerNC[2]] = unusual_html(locale.format("%d", int(eachNewLimit[1]), grouping=True), sOrderLim)
                client_list2[eachNewLimit[0]][headerNC[3]] = Percentage(sOrderPct, True)
                client_list2[eachNewLimit[0]][headerNC[4]] = unusual_html(locale.format("%d", int(eachNewLimit[4]), grouping=True), dDailyLim)
                client_list2[eachNewLimit[0]][headerNC[5]] = Percentage(dDailyPct, True)
                client_list2[eachNewLimit[0]][headerNC[6]] = unusual_html(locale.format("%d", int(eachNewLimit[3]), grouping=True), dOrderLim)
                client_list2[eachNewLimit[0]][headerNC[7]] = Percentage(dOrderPct, True)
                client_list2[eachNewLimit[0]][headerNC[8]] = user[0]
            else:
                pass
        #client_list[eachNewLimit[0] + '|' + str(eachNewLimit[7])][header[8]] = unusual_html(locale.format("%d", int(eachNewLimit[6]), grouping=True), cDailyLim)
        #client_list[eachNewLimit[0] + '|' + str(eachNewLimit[7])][header[9]] = Percentage(cDailyPct, True)
        #client_list[eachNewLimit[0] + '|' + str(eachNewLimit[7])][header[10]] = unusual_html(locale.format("%d", int(eachNewLimit[5]), grouping=True), cOrderLim)
        #client_list[eachNewLimit[0] + '|' + str(eachNewLimit[7])][header[11]] = Percentage(cOrderPct, True)
        #client_list[eachNewLimit[0] + '|' + str(eachNewLimit[7])][header[6]] = eachNewLimit[7]
        #print user[0]

        if previousRecord == None:
            print "insert a new latest check", eachNewLimit[0], eachNewLimit[7]
            insertLatestCheck(eachNewLimit[0], eachNewLimit[7])
        else:
            print "update latest check",eachNewLimit[0], eachNewLimit[7]
            updateLastChecked(eachNewLimit[0], eachNewLimit[7])
    else:
        pass

#sys.exit(0)

email = get_email(demo_mode)
email.set_subject('Credit limit changes: ' + file_date)
table = HtmlTable()
table.setBorder()

table2 = HtmlTable()
table2.setBorder()

print >> email, '<h2>Combined credit limits</h2>'
print >> email, table.header(['Client'] + headerC)
for client, value in sorted(client_list.items(), key=lambda item: item[1]['Client'], reverse=False):
    print >> email, table.line([client] +  (map(str,[client_list[client][h] if h in client_list[client] else 0 for h in headerC])), "style='text-align:right;'")

print >> email, table.end()

print >> email, '<h2>Credit Limits Not Combined</h2>'
print >> email, table2.header(['Client'] + headerNC)
for client, value in sorted(client_list2.items(), key=lambda item: item[1]['Client'], reverse=False):
    print >> email, table2.line([client] +  (map(str,[client_list2[client][h] if h in client_list2[client] else 0 for h in headerNC])), "style='text-align:right;'")

print >> email, table2.end()

email.flush()
#db.close()
print datetime.now()
