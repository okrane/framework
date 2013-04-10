from tickdb import *
from security import *
from SE import *

Connections.change_connections('production')

data = TickDB.trade_list(Security(2, 4), '20100104')
data.plot()

"""
db = Sybase.connect('SIRIUS','batch','delphi','quant')
c = db.cursor()
c.execute('select * from quant..estimator')
a = c.fetchall()
#a = Connections.exec_sql('BILBO', 'select security_id, trading_destination_id, avg_deal_size from market..trading_security_market_view4 where security_id=110')
"""
