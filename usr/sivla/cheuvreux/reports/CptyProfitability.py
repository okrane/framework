'''
Created on Jul 30, 2010

@author: syarc / JCM
This script is meant to capture stats on Fraggles.
It looks for stats whenever a trader takes the following
action on orders:

Manually splits an order to a destination (including to another trader)
Amends / enriches an order (parent or child)
Cancels (Parent or child)

'''
#from cheuvreux.fidessa.fidessadb import FidessaDB
#from cheuvreux.fidessa.audit_trail import AuditTrail

from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.fidessa import FIDESSA_BASE_DIR, get_directory_for_date
from cheuvreux.stdio.html import HtmlTable, table_style
from cheuvreux.stdio.mail import HtmlEmail
from collections import defaultdict
from datetime import datetime, timedelta
import gzip
import os
import re
import sys
import time

print datetime.now()
demo_mode = False
if demo_mode == True:
    FIDESSA_BASE_DIR = "C:\Documents and Settings\jcmau\Desktop\My Specs\extracts"
    folder_date = ''
    file_date = '20110912'
    order_filename = os.path.join(FIDESSA_BASE_DIR, 'ORDER.%s.psv' % file_date)
    audit_filename = os.path.join(FIDESSA_BASE_DIR, 'AUDIT_TRAIL.%s.psv' % file_date)
else:
    now = datetime.now()
    folder_date = now.strftime("%Y%m%d")
    if datetime.weekday(now) == 0:
        file_date = (now + timedelta(-3)).strftime("%Y%m%d")
    else:
        file_date = (now + timedelta(-1)).strftime("%Y%m%d")
    audit_filename = os.path.join(get_directory_for_date(file_date), 'AUDIT_TRAIL.%s.psv' % file_date)
    order_filename = os.path.join(get_directory_for_date(file_date),'ORDER.%s.psv' % file_date)

    if not os.path.exists(audit_filename):
        if os.path.exists(audit_filename + '.gz'):
            audit_filename += '.gz'

    if not os.path.exists(order_filename):
        if os.path.exists(order_filename + '.gz'):
            order_filename += '.gz'

print audit_filename
print order_filename

if order_filename.endswith('gz'):
    orderDetails = gzip.open(order_filename,'rb')
else:
    orderDetails = open(order_filename)
order_tuple = {}

for order_row in orderDetails:
    order_row = order_row.strip().split('|')
    if order_row[3] == '1':
        order_tuple[order_row[2]]=order_row


print audit_filename
print order_filename
if  not os.path.exists(audit_filename) or not os.path.exists(order_filename):
    #sys.exit(0)
    print ' exit '

'''
audit events we track
'''
nonAmendEvents = ["CHLD" , "ASSN" , "COMM" , "EXED" , "EXEE" , "ALGO" , "ESOA" , "CEXD"]
'''
permissioned users that interact with fidessa (FDSA)
'''
starUserID = [ "DMA@CRAG.US" , "SMART@CRAG.US"]

cptyCounter = defaultdict(int)
cptyCounter_2 = defaultdict(int)
cptyActionCounter = defaultdict(int)

rows = 0

if demo_mode == True:
    audit_filename = os.path.join(FIDESSA_BASE_DIR, 'AUDIT_TRAIL.%s.psv' % file_date)
    order_filename = os.path.join(FIDESSA_BASE_DIR, 'ORDER.%s.psv' % file_date)
    auditTrail_events = open(audit_filename)
    orderDetails = open(order_filename)
else:
    if audit_filename.endswith('gz'):
        auditTrail_events = gzip.open(audit_filename, 'rb')
    else:
        auditTrail_events = open(audit_filename)

    if order_filename.endswith('gz'):
        orderDetails = gzip.open(order_filename,'rb')
    else:
        orderDetails = open(order_filename)

def checkOrderDetails_2( orderId, Cpty, event):

    return Cpty


def checkOrderDetails( orderId, Cpty, event):

    def_order_rows = 0
    order_tuple.has_key(orderId)


    if orderId in order_tuple:
        order_fields = order_tuple[orderId]
        if event == "CHLD":
            if order_fields[3] == "1" and (order_fields[10].find("@CRAG.US") > -1 and order_fields[10] not in starUserID):
                if order_fields[6] == '' and order_fields[30] == "HSE":
                    Cpty[order_fields[5] + ' '] += 1
                    Cpty[order_fields[5] + '_' + event ] += 1
                elif order_fields[6] != '':
                    Cpty[order_fields[6] + ' '] += 1
                    Cpty[order_fields[6] + '_' + event ] += 1
                else:
                    pass
            elif ((order_fields[10].find("@CRAG.US") == -1 or
                  order_fields[10] in starUserID) and order_fields[3] == "1"):
                pass
        else:
            if order_fields[128].find("BTCG") and event == "ASSN":
                pass
            else:
                if order_fields[6] == '' and order_fields[30] == "HSE":
                    Cpty[order_fields[5] + ' '] += 1
                    Cpty[order_fields[5] + '_' + event ] += 1
                else:
                    Cpty[order_fields[6] + ' '] += 1
                    Cpty[order_fields[6] + '_' + event ] += 1
    else:
        pass


    return Cpty

for eachLine in auditTrail_events:
    eachLine = eachLine.strip()
    event_fields = eachLine.strip().split('|')

    rows = rows + 1
    #print rows

    if event_fields[2] in nonAmendEvents:

        if event_fields[2] == "CHLD":
            cptyCounter_2 = checkOrderDetails( event_fields[0] , cptyCounter_2, event_fields[2])
        elif (event_fields[2] == "ALGO" and
              ((event_fields[10].find("Sent pause to") != -1 or
                event_fields[10].find("Sent resume to") != -1 or
                event_fields[10].find("Sent stop to") != -1))):
            cptyCounter_2 = checkOrderDetails( event_fields[0] , cptyCounter_2, event_fields[2])
        elif (event_fields[2] == "ASSN" and event_fields[6] == "MES"
              and (event_fields[7].find("@CRAG.US")!= -1) and event_fields[12].find("Routed by rule") == -1):
            cptyCounter_2 = checkOrderDetails( event_fields[0] , cptyCounter_2, event_fields[2])
        elif ((event_fields[2] in ["EXEE" , "EXED" , "CEXE" , "CEXD"]) and
              event_fields[6] == "MES" and (event_fields[7].find("@CRAG.US")!= -1)):
            cptyCounter_2 = checkOrderDetails( event_fields[0] , cptyCounter_2, event_fields[2])
        elif (event_fields[2] == "COMM" and
              event_fields[6] == "MES" and (event_fields[7].find("@CRAG.US")!= -1)):
            cptyCounter_2 = checkOrderDetails( event_fields[0] , cptyCounter_2, event_fields[2])
        elif (event_fields[2] == "ESOA" and event_fields[10].find("Manual action by") != -1):
            print ' -- ESOA -- '
            cptyCounter_2 = checkOrderDetails( event_fields[0] , cptyCounter_2, event_fields[2])
        else:
            continue
    else:

        continue
'''
look for all amends by a non-system user
'''
rows = 0
for eachOrderLine in orderDetails:
    eachOrderLine = eachOrderLine.strip()
    orders = eachOrderLine.strip().split('|')

    rows = rows + 1

    if (orders[10].find("@CRAG.US") > -1 and orders[10] not in starUserID) and orders[3].strip() != "1":
        if orders[6] == '' and orders[30] == "HSE":
            cptyCounter_2[orders[5] + '_' + 'AGYA' ] += 1
            cptyCounter_2[orders[5]] += 1
        else:
            cptyCounter_2[orders[6] + '_' + 'AGYA' ] += 1
            cptyCounter_2[orders[6]] += 1
    else:

        continue

auditTrail_events.close()
orderDetails.close()



db = SQLiteBase('Fraggles_info.db')
db.execute("delete from fraggles where date = '%s'" % (file_date))
for key, value in cptyCounter_2.iteritems():
    if key.find('_') > 0:
        client, event = key.split('_')
    else:
        client, event = key, 'Total'
    query= "INSERT INTO fraggles VALUES ('%s','%s','%s',%d)" % (file_date, client.strip(), event, value)
    print query
    db.execute(query)

rows = db.select(''' select client, eg.`group`, sum(count) from fraggles f
                     left join event_group eg on eg.event = f.event
                     group by client, eg.`group`
                     order by client
                 ''')

report_date = db.select(''' select min(DATE), max(DATE) from fraggles''')
starting_date = report_date[0][0][:4] + '-' + report_date[0][0][4:6] + '-' + report_date[0][0][6:] + ' to '
current_date = report_date[0][1][:4] + '-' + report_date[0][1][4:6] + '-' + report_date[0][1][6:]

data = defaultdict(lambda: defaultdict(int))

header = ['Total','Split', 'Order Amends','Trades','Other','Market','Algo']

for row in rows:
    data[row[0]][row[1]] = row[2]

email = HtmlEmail('smtpnotes', style=table_style)
email.set_sender('jmauras-ext')
if demo_mode == True:
    email.set_dest('jmauras-ext')
else:
    email.set_dest('jmauras-ext,mrowley,sarchenault,lmassoulard-ext,lmassoulard-ext')
email.set_subject('Trader actions report: ' + starting_date + current_date)
table = HtmlTable()
table.setBorder()
print >> email, table.header(['Client'] + header)

for client, value in sorted(data.items(), key=lambda item: item[1]['Total'], reverse=True):
    print >> email, table.line([client] +  (map(str,[data[client][h] if h in data[client] else 0 for h in header])))

print >> email, table.end()
email.flush()
db.close()
print datetime.now()
