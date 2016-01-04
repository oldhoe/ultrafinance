# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 23:42:53 2015

@author: pchaos
"""
import logging
import unittest
from unittest import TestCase

import dam
from ultrafinance.dam.TDXDAM import TDXDAM
from ultrafinance.dam.mongoDAM import MongoDAM

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class TestTDXDAM(TestCase):
    def setUp(self):
        self.dam = TDXDAM()
        self.testPath = './data'

    def tearDown(self):
        self.dam = None
        self.testPath = None

    def test_targetPath(self):
        self.test_setDir()
        self.dam.setSymbol('600001')
        dir = self.dam.targetPath(None)
        LOG.info('targetPath:{0}'.format(dir))
        self.assertNotEqual(0, len(dir))

    def test_readQuotes(self):
        symbol= '600177'
        dam = TDXDAM()
        dam.setDir('./data')
        dam.setSymbol(symbol)
        start = 20131101
        end = 20131115
        data = dam.readQuotes(start, end)
        LOG.debug([str(q) for q in data])
        self.assertNotEqual(0, len(data))
        #数据数量不可能大于相隔日期
        self.assertTrue(len(data) <= end - start + 1, '数据数量太大！')

    def test_readQuotesAndWriteQuotesToMongo(self):
        symbol= '600177'
        start = 20141101
        end = 20141231
        data = self._readQuotesAndWriteQuotesToMongo(symbol, end, start)
        start = None
        end = None
        data1 = self._readQuotesAndWriteQuotesToMongo(symbol, end, start)
        self.assertTrue(len(data1) >= len(data), '{0} data must be error!'.format(symbol))

    def _readQuotesAndWriteQuotesToMongo(self, symbol, end, start):
        dam = TDXDAM()
        dam.setDir('./data')
        dam.setSymbol(symbol)
        data = dam.readQuotes(start, end)
        LOG.debug([str(q) for q in data])
        self.assertNotEqual(0, len(data))
        dam = MongoDAM()
        dam.setup('mongodb://127.0.0.1', 'testdb')
        dam.setSymbol(symbol)
        quotes = data
        dam.writeQuotes(quotes)
        # print([str(quotes) for symbol, quotes in dam.readBatchTupleQuotes(["test"], 0, None).items()])
        print([str(quote) for quote in dam.readQuotes(0, None)])
        return data

    def test_writeQuotes(self):
        self.fail()

    def test_readTicks(self):
        self.fail()

    def test_writeTicks(self):
        self.fail()

    def test_setDir(self):
        self.dam.setDir(self.testPath)
        self.assertTrue(1 == 1)


if __name__ == "__main__":
    unittest.main()
