# -*- coding: utf-8 -*-

from lib.dbtools.connections import Connections
print Connections.exec_sql('BSIRIUS', 'select top 10 * from security_market')
"""
import lib.dbtools.get_repository as get_repository 

print dir(get_repository)
print "security_id(%s)->glid=%s"% (110, get_repository.convert_symbol(source = 'security_id', dest = 'glid', value = 110, exchgid='SEPA'))
print "security_id(%s)->SECID=%s"% (110, get_repository.convert_symbol(source = 'security_id', dest = 'SECID', value = 110, exchgid='SEPA'))
print "security_id(%s)->ISIN=%s"% (110, get_repository.convert_symbol(source = 'security_id', dest = 'ISIN', value = 110, exchgid='SEPA'))
"""
"""

def test_type(column, coltype):
    return isinstance(column, coltype)
    
v = []
columns = ['1', 2, 2.0]

for c in columns:
    def f():
        print c
        assert test_type(c, int), str(type(c))
    v.append(f)
    print v
    
for g in v:
    print g()

"""