'''
Created on Aug 23, 2011

@author: jcmau
'''

from cheuvreux.dbtools.odbc import ODBC
from cheuvreux.stdio.html import HtmlTable, table_style
from cheuvreux.stdio.mail import HtmlEmail
from collections import defaultdict
import locale
from datetime import datetime, timedelta
from cheuvreux.dbtools.Sqlite import SQLiteBase
from cheuvreux.utils.dataset import Dollar, Percentage
locale.setlocale(locale.LC_ALL, '')

demo_mode = False
if demo_mode == True:
    date = '20120329'
else:
    now = datetime.now()
    date = now.strftime("%Y%m%d")
    if datetime.weekday(now) == 0:
        date = (now + timedelta(-3)).strftime("%Y%m%d")
    else:
        date = (now + timedelta(-1)).strftime("%Y%m%d")

print date
db = ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=fidessa;UID=syarc;PWD=syarc')
local_db = SQLiteBase('Cpty_Limits.db')
def unusual_html(str):
    return '<span style="color:#000000;font-weight:normal">$%s</span>' % str

def clearUpActivity():
    db = SQLiteBase('Cpty_Limits.db')
    db.execute("delete from cptyOneDayActivity ")

def insertActivity ( client, cons ):
    db = SQLiteBase('Cpty_Limits.db')
    query= "INSERT INTO cptyOneDayActivity VALUES ('%s',%d)" % (client, int(cons))
    db.execute(query)

# account for the latest version / amendments
clients = db.select(''' with cte as (
select *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY version DESC) as RowNumber
from [fidessa].[dbo].[High Touch Orders Cumulative]
where parent_order_id is null
and tradedate = '%s'
)
SELECT COUNT(*) "PARENT_ORDERS", sum(VOLUME_DONE), sum(TRADING_QUANTITY), COUNTERPARTY_CODE, sum(VOLUME_DONE * GROSS_PRICE)  FROM cte
WHERE RowNumber = 1 and PARENT_ORDER_ID IS NULL group by COUNTERPARTY_CODE''' % date)

client_list = defaultdict(lambda: defaultdict(int))
global_list = defaultdict(lambda: defaultdict(int))


header = [' # Parent orders','Qty','done',' exec ratio']
header2 = [' $ done']
clearUpActivity()
for eachOrder in clients:

    numOrders = eachOrder[0]

    if numOrders > 0:
        client_list[eachOrder[3]][header[0]] = locale.format("%d", int(eachOrder[0]), grouping=True)
        client_list[eachOrder[3]][header[1]] = locale.format("%d", int(eachOrder[2]), grouping=True)
        client_list[eachOrder[3]][header[2]] = locale.format("%d", int(eachOrder[1]), grouping=True)
        client_list[eachOrder[3]][header[3]] = Percentage( float(eachOrder[1]) / float(eachOrder[2]), True)
        insertActivity(eachOrder[3], eachOrder[4])
    else:
        continue


cptyLimitsGrid = local_db.select(''' select COUNTERPARTY, CONSIDERATION_DONE from cptyOneDayActivity where COUNTERPARTY
not in (select COUNTERPARTY from cptylimitsgrid)''')

for eachCpty in cptyLimitsGrid:
    #print eachCpty[0], eachCpty[1]
    global_list[eachCpty[0]][header2[0]] = unusual_html(locale.format("%d", int(eachCpty[1]), grouping=True))

email = HtmlEmail('smtpnotes', style=table_style)
email.set_sender('jimperato')

if demo_mode == True:
    email.set_dest('jimperato')
else:
    email.set_dest('cgilbeau, jimperato, jbouquot, dhegarty, bspillane')

email.set_subject('Client activity: ' + date)
table = HtmlTable()
table.setBorder()

table2 = HtmlTable()
table2.setBorder()

print >> email, '<h2>All Client Activity</h2>'
print >> email, table.header(['Client'] + header)
for client, value in sorted(client_list.items(), key=lambda item: item[1]['Client'], reverse=False):
    print >> email, table.line([client] +  (map(str,[client_list[client][h] if h in client_list[client] else 0 for h in header])), "style='text-align:right;'")

print >> email, table.end()

print >> email, '<h2>Client activity using global credit defaults</h2>'
print >> email, table2.header(['Client'] + header2)
for client, value in sorted(global_list.items(), key=lambda item: item[1]['Client'], reverse=False):
    print >> email, table2.line([client] +  (map(str,[global_list[client][h] if h in global_list[client] else 0 for h in header2])), "style='text-align:right;'")

print >> email, table2.end()

# Pie chart
import matplotlib
from pylab import *
from matplotlib.pyplot import figure, show
from matplotlib import font_manager as fm

# make a square figure and axes
#f = figure(1, figsize=(6,6))
#ax = axes([0.1, 0.1, 0.8, 0.8])

#labels = 'BBG-Portal', 'BBG-Xp','ITG', 'MixIt', 'STAR', 'ETP/EMS', 'BNET','Ullink','IG','JSSF','LOOP','Nexus','Real','TrdScrn'
#fracs = [15,30,45,10,5,3,3,3,4,1,1,1,6,2]

#dynamic_lables = 'BBG-Portal', 'BBG-Xp'
#pct1 = 40
#pct2 = 60
#fracs = []
#fracs.append(pct1)
#fracs.append(pct2)


#explode=(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
#explode=()
#patches, texts, autotexts = ax.pie(fracs, explode=None, labels=dynamic_lables, colors=('#008B8B', '#FF1493','#FFD700','#228B22','#FFFFFF','#FF0000'), autopct='%1.1f%%', shadow=True)
#title('Order flow by gateway', bbox={'facecolor':'0.8', 'pad':5})
#proptease = fm.FontProperties()
#proptease.set_size('xx-small')
#setp(autotexts, fontproperties=proptease)
#setp(texts, fontproperties=proptease)

#filename = 'pie_count.png'

#f.savefig(filename)

#print >> email, '<img src="cid:%s">' % filename
#email.attachImage(filename, filename)
#show()

email.flush()
