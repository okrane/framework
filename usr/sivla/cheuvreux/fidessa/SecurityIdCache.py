'''
Created on Aug 24, 2010

This module provides a cache mechanism for security_id, trading_destination_id and ISIN.

@author: syarc
'''
from cheuvreux.dbtools.repository import Repository, DictionaryId
import atexit
import os
import pickle
from cheuvreux.utils import collections
from cheuvreux.dbtools.security import Security

cache_dir = os.path.join(os.path.split(__file__)[0], 'cache')

# Global cache dictionaries
sec_id_cache = dict()
td_cache = dict()
isins = dict()
code_cache = dict()

# Not disk persisted
securities = dict()

def getCodeFromSecurity(sec_id):
    ''' Get the US Code for a security id. '''

    if sec_id in code_cache:
        return code_cache[sec_id]

    for uscode, secid in sec_id_cache.items():
        if secid[0] == sec_id:
            return uscode

    # sec_id not found, look for it
    uscode = Repository.security_dict_code(sec_id, DictionaryId.USCODE)
    if uscode:
        sec_id_cache[(uscode, None)] = (sec_id,)
        code_cache[sec_id] = uscode
        return uscode

    return None

def getSecurityId(uscode, market=None):
    ''' Get the security id for a US code. '''
    if (uscode, market) not in sec_id_cache:
        sec_id = Repository.us_security_id(uscode, True, market)
        if sec_id:
            sec_id_cache[(uscode, market)] = sec_id
        else:
            return (None, None)
    return sec_id_cache[(uscode, market)]

def getSecurity(uscode, market=None):

    if (uscode, market) not in securities:
        sec_id, td_id = getSecurityId(uscode, market)
        if not sec_id or not td_id:
            return None
        else:
            securities[(uscode, market)] = Security(sec_id, td_id)

    return securities[(uscode, market)]

def getSecurityIdFromIsin(isin):
    ''' Get the security for an ISIN. '''
    if isin not in isins:
        sec_id = Repository.dict_id_to_security_id(isin, DictionaryId.ISIN)
        if sec_id:
            sec_id = sec_id.keys()[0]
            td_id = getTradingDestId(sec_id)
            isins[isin] = (sec_id, td_id)
        else:
            return None, None
    return isins[isin]


def getTradingDestId(sec_id):
    ''' Get the main trading destination for a security_id. '''
    if sec_id not in td_cache:
        td_cache[sec_id] = Repository.trading_destination(sec_id)[0][0]
    return td_cache[sec_id]


def flush2disk():
    ''' Flush cache to disk. '''
    global sec_id_cache, td_cache, isins
    pickle.dump(sec_id_cache, open(os.path.join(cache_dir, 'sec_id.cache'), 'wb'))
    pickle.dump(td_cache, open(os.path.join(cache_dir, 'td.cache'), 'wb'))
    pickle.dump(isins, open(os.path.join(cache_dir, 'isins.cache'), 'wb'))


def load():
    ''' Load cache data into memory. '''
    global sec_id_cache, td_cache, isins, code_cache
    if os.path.exists(os.path.join(cache_dir, 'sec_id.cache')):
        sec_id_cache = pickle.load(open(os.path.join(cache_dir, 'sec_id.cache'), 'rb'))
        code_cache = dict((v[0],k) for k, v in sec_id_cache.iteritems())
    if os.path.exists(os.path.join(cache_dir, 'td.cache')):
        td_cache = pickle.load(open(os.path.join(cache_dir, 'td.cache'), 'rb'))
    if os.path.exists(os.path.join(cache_dir, 'isins.cache')):
        isins = pickle.load(open(os.path.join(cache_dir, 'isins.cache'), 'rb'))


def clean():
    global sec_id_cache, td_cache, isins
    for key in sec_id_cache.keys():
        if sec_id_cache[key][0] is None:
            print 'Remove %s' % str(key)
            del sec_id_cache[key]
    for key in isins:
        if isins[key] is None:
            print 'Remove %s' % key
            del isins[key]

# Load the cache automatically
load()
# Save the cache automatically
atexit.register(flush2disk)
