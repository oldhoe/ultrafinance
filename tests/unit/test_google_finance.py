'''
Created on July 30, 2011

@author: ppa
'''

import logging
import unittest

from ultrafinance.dam.googleFinance import GoogleFinance
from ultrafinance.lib.errors import UfException
from ultrafinance.lib.util import *

LOG = logging.getLogger(__name__)


class testGoogleFinance(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @log
    def testGetQuotes(self):
        googleFinance = GoogleFinance()
        data = googleFinance.getQuotes('NASDAQ:EBAY', getDateString(40), None)
        LOG.info([str(q) for q in data])
        assert len(data)

    @log
    def testGetAll(self):
        googleFinance = GoogleFinance()
        data = googleFinance.getAll('EBAY')
        LOG.info(data)
        self.assertNotEqual(0, len(data))

    @log
    def testGetAll_badSymbol(self):
        googleFinance = GoogleFinance()
        self.assertRaises(UfException, googleFinance.getAll, 'fasfdsdfasf')

    @log
    def testGetQuotes_badSymbol(self):
        googleFinance = GoogleFinance()
        self.assertRaises(UfException, googleFinance.getQuotes, *['AFSDFASDFASDFS', '20110101', '20110110'])

    @log
    def testGetFinancials(self):
        googleFinance = GoogleFinance()
        # ret = googleFinance.getFinancials('NASDAQ:EBAY', ['Net Income', 'Total Revenue', 'Diluted Normalized EPS', 'Total Common Shares Outstanding'], False)
        ret = googleFinance.getFinancials('NASDAQ:EBAY')
        LOG.info(ret)

    @log
    def testGetTicks(self):
        googleFinance = GoogleFinance()
        ret = googleFinance.getTicks('EBAY', start='20140101', end='20140110')
        LOG.info(ret)


if __name__ == "__main__":
    unittest.main()
