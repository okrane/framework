'''
Created on 27 mai 2011

@author: benca
'''

import string

delete_table  = string.maketrans(string.digits, ' '*len(string.digits))

s = '467389bzyeu47839fezui327'
print s
print delete_table
print s.translate(None, delete_table)