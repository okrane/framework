'''
Created on 20 avr. 2010

@author: syarc
'''
import unittest
from usr.dev.sivla.funcs.DBTools.repository import Repository
from usr.dev.sivla.funcs.DBTools.security import Security


class Test(unittest.TestCase):

    def testTradingDestination(self):
        self.assertEqual(Repository.trading_destination(110),
                         [(4,), (61,), (81,), (80,), (90,), (92,), (89,), (128,)])
        self.assertEqual(Repository.trading_destination('FTE.PA', 'idn_selectfeed'),
                         [(4, 110)])


    def testSecurityId(self):

        self.assertEqual(Repository.security_id('FTE.PA', 'idn_selectfeed'), 110)
        self.assertEqual(Repository.us_security_id('C'),
                          [(11679, 25), (11679, 96), (11679, 98), (11679, 99), (11679, 100), (11679, 102), (11679, 103), (11679, 104), (11679, 105), (11679, 106), (11679, 109), (11679, 108)])
        self.assertEqual(Repository.us_security_id('C', True),
                         (11679, 25))
        self.assertEqual(Repository.table_suffix(11679), 'ameri')
        self.assertEqual(Repository.table_suffix(110), '')

        # Test Isin
        sec = Security (110, 4) #FTE.PA
        self.assertEqual(sec.isin, 'FR0000133308')

        self.assertEqual(Repository.outstanding_shares(110, '20100428'), 2602070176.0)

        self.assertEqual(int(sec.market_cap('20100427')), 42947168254L)

    def testIndex(self):

        self.assertEqual(Repository.index_id('CAC40'), 1)
        self.assertEqual(Repository.index_id('S&P500 STOCK INDEX'), 46)

        self.assertEqual(Repository.index_components(6),
                       [10673, 10804, 10826, 12281, 12666, 12668, 12670, 12671, 12672, 12673, 12675, 12676, 12679, 13375, 13381, 13382, 15876, 15877, 44626, 44765, 45595, 45799, 189276, 202330, 240005, 286745, 291815, 296373, 303139, 428026])

    def testSector(self):
        self.assertEqual(Repository.get_sector(10673), ('GICS35', 'Health Care'))
        self.assertEqual(Repository.get_sector('US0017651060'), ('GICS20', 'Industrials'))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testSecurityId']
    unittest.main()
