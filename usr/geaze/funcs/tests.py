from simep.funcs.dbtools.connections import Connections

(a,s) = Connections.exec_sql_schema('BSIRIUS', 'select * from tick_db..trading_daily where security_id = 2 and trading_destination_id = NULL and date = "20120301" ')

print a

print s

b = Connections.exec_sql('BSIRIUS', 'select indicator_value from quant_data..ci_security_indicator where security_id = 110 and trading_destination_id = 4 and indicator_id = 25 ');

print b[0][0]

print Connections.exec_sql('BSIRIUS', 'select indicator_value from quant_data..ci_security_indicator where security_id = 110 and trading_destination_id = 4 and indicator_id = 25 ')[0][0]