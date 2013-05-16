'''
Created on Dec 13, 2011

@author: syarc
'''
from cheuvreux.fidessa.fidessa import FidessaDB
from cheuvreux.dbtools.odbc import ODBC

rebate = {'XNAS' : -0.0029, 'ARCX': -0.0028, 'ARCA': -0.0028, 'Nasdaq': -0.0029}

mapping = {'QR' : 'Nasdaq',
               'QA' : 'Nasdaq',
               'QO' : 'Nasdaq',
               'ZR' : 'BATS',
               'NR' : 'NYSE',
               'GR' : 'Getco ATS',
               'PR' : 'ARCA',
               'PA' : 'ARCA',
               'PO' : 'ARCA',
               'KR' : 'EDGX',
               'KO' : 'EDGX',
               'FR' : 'CSFB',
               'JR' : 'EDGA',
               'JO' : 'EDGA',
               'ER' : 'Knight Match',
               'XR' : 'Philladelphia Stock Exchange',
               'IR' : 'ITG',
               'SR' : 'Sigma-X',
               'HR' : 'Barclays ATS',
               'YR' : 'BATS-Y',
               'MR' : 'Chicago Stock Exchange',
               'MO' : 'Chicago Stock Exchange',
               'VR' : 'Level ATS',
               'BR' : 'Nasdaq-BX',
               'CR' : 'National Stock Exchange',
               'LR' : 'Knight Link',
               'AR' : 'AMEX',
               'UR' : 'UBS',
               'WR' : 'Chicago Board of Options Exchange',
               'WO' : 'Chicago Board of Options Exchange',
               'DR' : 'Deutsche-Bank ATS',
               'DO' : 'Deutsche-Bank ATS',
               'DA' : 'Deutsche-Bank ATS',
               'RR' : 'CSFB Light Pool'   }

DB = ODBC('DRIVER={SQL Server};SERVER=nysql001;DATABASE=fidessa;UID=syarc;PWD=syarc')

rows = DB.select('''  select "last-mkt", h.liquidity_indicator, sum(quantity), max(k.rate)
from [High Touch Trade Summary Cumulative] h
join KYCDatabase..[RebateChargeRate Table] k on h.execution_venue = k.exchange and k.liquidity = h.liquidity_indicator
where tradedate between '%s' and '%s' and execution_venue = 'GESX'
group by "last-mkt", h.liquidity_indicator ''' % ('20111201', '20111231')   )

result = {}

for row in rows:

    ind = row[1][1]
    idx = 1
    if ind == 'A':
        idx = 0
    elif ind == 'O':
        idx = 2

    mkt = row[0].strip()

    #mkt = mapping[row[1].strip()]

    if mkt not in result:
        result[mkt] = [0,0,0,0]
    result[mkt][idx] += int(row[2])

    if idx == 1:
        result[mkt][3] = row[3]



print '\t'.join(['Mkt', 'Add', 'Remove', 'Other', 'RemoveCost', 'AddRebate', 'TotalFee'])

total_cost = total_rebate = 0.0
total_add = total_remove = total_other = 0

for mkt in result:

    remove_cost = result[mkt][1] * result[mkt][3]
    add_rebate = 0
    if mkt in rebate:
        add_rebate = result[mkt][0] * rebate[mkt]
    print '\t'.join(map(str,([mkt] + result[mkt][0:3] + [remove_cost, add_rebate, remove_cost + add_rebate])))
    total_cost += remove_cost
    total_rebate += add_rebate
    total_add += result[mkt][0]
    total_remove += result[mkt][1]
    total_other += result[mkt][2]


print '\t'.join(map(str,['Total:',total_add, total_remove, total_other, total_cost, total_rebate, total_cost + total_rebate]))



