'''
Created on Sep 13, 2010

@author: syarc
'''
import unittest
from cheuvreux.reports.auditstream import get_stock_flows
from dev.usr.syarc.fidessa.fidessadb import FidessaDB
from cheuvreux.dbtools.repository import Repository

class Test(unittest.TestCase):

    def test_stock_flows(self):

        fidessa_db = FidessaDB()
        rows = fidessa_db._backend.select(''' select instrument_code, sum(quantity)
                            from [High Touch Trade Summary Cumulative]
                            where tradedate = '20100902' and version = 1
                            and trade_part_index = 1
                            group by instrument_code ''')

        flows = get_stock_flows('cheuvreux.db_20100902-seb')
        for row in rows:
            isin = Repository.us_code_to_isin(row[0])
            if isin and isin[0] in flows:
                try:
                    self.assertEqual(int(row[1]), flows[isin[0]].total)
                except:
                    print row[0], isin, row[1], flows[isin[0]].total, ' failed !'
            else:
                print row[0], isin, 'not found'



if __name__ == "__main__":
    unittest.main()
