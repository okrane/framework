# -*- coding: utf-8 -*-



from lib.dbtools.connections import Connections

print Connections.exec_sql('BSIRIUS', 'select * from security_market')