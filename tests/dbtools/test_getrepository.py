# -*- coding: utf-8 -*-

import lib.dbtools.get_repository as get_repository 

print dir(get_repository)
print "security_id(%s)->glid=%s"% (110, get_repository.convert_symbol(source = 'security_id', dest = 'glid', value = 110, exchgid='SEPA'))
print "security_id(%s)->SECID=%s"% (110, get_repository.convert_symbol(source = 'security_id', dest = 'SECID', value = 110, exchgid='SEPA'))
print "security_id(%s)->ISIN=%s"% (110, get_repository.convert_symbol(source = 'security_id', dest = 'ISIN', value = 110, exchgid='SEPA'))