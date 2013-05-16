'''
Created on Nov 2, 2011

@author: syarc
'''
import datetime
from collections import defaultdict
from cheuvreux.fidessa import fidessadb
from cheuvreux.utils.dataset import Dataset, Quantity, Dollar, to_html
from cheuvreux.stdio.mail import HtmlEmail
import locale

def report (range, output, html=False):

    DB = fidessadb.getODBCConnection('fidessa')
    start, end = range[0], range[-1]

    DB.execDirect ('CREATE TABLE #USER_GRP (username varchar(32), grp varchar(32))')
    DB.run (''' INSERT INTO #USER_GRP VALUES ('DMA@CRAG.US','DMA');
                INSERT INTO #USER_GRP VALUES ('SMART@CRAG.US','DSA (ex CAS)');
                INSERT INTO #USER_GRP VALUES ('EXPRESSIN','DSA (ex CAS)');
                INSERT INTO #USER_GRP VALUES ('FIXPIN','DSA (ex CAS)');
                INSERT INTO #USER_GRP VALUES ('dhegarty@CRAG.US','CARE');
                INSERT INTO #USER_GRP VALUES ('bspillane@CRAG.US','CARE');
                INSERT INTO #USER_GRP VALUES ('jimperato@CRAG.US','CARE');
                INSERT INTO #USER_GRP VALUES ('mwalczuk@CRAG.US','CARE');
                INSERT INTO #USER_GRP VALUES ('jbouquot@CRAG.US','CARE');
                INSERT INTO #USER_GRP VALUES ('sarchenault@CRAG.US','CARE');
            ''')

    DB.execDirect('CREATE TABLE #CTPY_GRP (counterparty varchar(64), usr varchar(32), grp varchar(64))')
    DB.run ('''
                INSERT INTO #CTPY_GRP VALUES ('LYON','DSA (ex CAS)','High Touch (CAS)');
                INSERT INTO #CTPY_GRP VALUES ('LYON','CARE','High Touch (CAS)');
                INSERT INTO #CTPY_GRP VALUES ('CACHEUVREUX','DSA (ex CAS)','High Touch (CACNA)');
                INSERT INTO #CTPY_GRP VALUES ('CACHEUVREUX','CARE','High Touch (CACNA)');
                INSERT INTO #CTPY_GRP VALUES ('GPT','DSA (ex CAS)', 'Global PT');
                INSERT INTO #CTPY_GRP VALUES ('GPT','CARE', 'Global PT');
                INSERT INTO #CTPY_GRP VALUES ('CACPA-CHMA','CARE','High Touch (CACNA)');
                INSERT INTO #CTPY_GRP VALUES ('CACPA-CHMA','DSA (ex CAS)','High Touch (CACNA)');
            ''')
    query = ''' with cte
                as (
                    SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY version DESC) as RowNumber
                    FROM [High Touch Orders Cumulative]
                        WHERE  parent_order_id IS NULL AND tradedate BETWEEN '%s' AND '%s'
                          AND volume_done > 0
                          AND instrument_code not in ('ZVZZT','ZXZZT','ZWZZT','ZYZZT')
                          AND SETTLEMENT_CURRENCY_ID in ('USD','CAD','BRL','CLP','MXN','COP','ARS', 'PEN')
                    )
                SELECT isnull(cg.grp, isnull(ug.grp,entered_by)), SETTLEMENT_CURRENCY_ID, sum(volume_done), sum(volume_done * gross_price), count(distinct counterparty_code)
                 FROM cte
                LEFT JOIN #USER_GRP ug ON username = entered_by
                LEFT JOIN #CTPY_GRP cg ON cte.COUNTERPARTY_CODE = cg.counterparty and ug.grp = cg.usr
                WHERE RowNumber = 1
                GROUP BY isnull(cg.grp, isnull(ug.grp,entered_by)), SETTLEMENT_CURRENCY_ID
            ''' % (start, end)

    ccy_map = defaultdict(list)

    rows = DB.select(query)
    if len(rows) == 0:
        raise ValueError("No data")

    for row in rows:
        ccy_map[row[1]].append((row[0],row[2],row[3],row[4]))

    for ccy in sorted(ccy_map.keys(), reverse = True):
        print >> output, '<h2>%s</h2>' % ccy if html else ccy

        ds = Dataset(['Flow type', 'Volume','Gross ($)','# Clients'])
        sum_volume = sum_gross = 0
        for row in ccy_map[ccy]:
            ds.append([row[0], Quantity(row[1],0), Dollar(row[2],0), Quantity(row[3],0)])
            sum_volume += row[1]
            sum_gross += row[2]

        query = ''' SELECT count(distinct counterparty_code)
                      FROM [High Touch Orders Cumulative]
                     WHERE parent_order_id IS NULL AND tradedate BETWEEN '%s' AND '%s'
                       AND volume_done > 0
                       AND instrument_code not in ('ZVZZT','ZXZZT','ZWZZT','ZYZZT')
                       AND SETTLEMENT_CURRENCY_ID = '%s'
                ''' % (start, end, ccy)

        nb_clients = DB.selectOne(query)
        ds.add_total_row(['Total', Quantity(sum_volume,0), Dollar(sum_gross,0), Quantity(nb_clients[0],0)])

        ds.sort('Gross ($)', True)
        ds.set_extra_style(['', "style='text-align:right;'"])
        print >> output, str(ds) if not html else to_html(ds)


if __name__ == '__main__':
    import sys

    locale.setlocale(locale.LC_ALL, '')


    email = HtmlEmail('smtpnotes')
    email.set_subject('Overall activity')
    email.set_dest('sarchenault@cheuvreux.com')
    report([datetime.date(2012,3,12)], email, True)
    email.flush()