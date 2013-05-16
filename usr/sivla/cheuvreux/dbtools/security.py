'''
Created on 20 avr. 2010

@author: syarc
'''
from cheuvreux.dbtools.repository import Repository, DictionaryId
from tickdb import TickDB
import datetime

class Security(object):

    def __init__(self, security_id, trading_destination_id):
        self._security_id = security_id
        self._trading_destination_id = trading_destination_id
        self._suffix = None
        self._isin = None
        self._code = None

    @property
    def security_id(self):
        return self._security_id

    @property
    def trading_destination_id(self):
        return self._trading_destination_id

    @property
    def suffix(self):
        ''' Returns the suffix needed for tick data

            This suffix could be:
                '' for europe
                'ameri' for US stocks
                'asia' for asia stocks
        '''
        if self._suffix is None:
            tmp = Repository.table_suffix_sec_id(self.security_id)
            self._suffix = tmp
            if tmp != '':
                self._suffix = tmp

        return self._suffix

    @property
    def isin(self):
        if self._isin is None:
            self._isin = Repository.security_dict_code(self.security_id, DictionaryId.ISIN)
        return self._isin

    def market_cap(self, date=None):
        if date is None:
            date = datetime.date.today().strftime("%Y%m%d")

        nb_shares = Repository.outstanding_shares(self.security_id, date)
        close = TickDB.ohlc(self, date)
        if nb_shares is None or close is None:
            return None

        return nb_shares * close[3]

    def __str__(self):
        return "%d @ %d" % (self.security_id, self.trading_destination_id)


def buildFromISINs(isins):
    securities = []
    sec_ids = Repository.isin_to_security_id(isins)
    for sec_id, isin in sec_ids.items():
        s = Security(sec_id, None)
        s._isin = isin
        securities.append(s)

    return securities

