# -*- coding: utf-8 -*-
"""
Created on %(date)s

@author: pchaosgit
"""
import os
from unittest import TestCase
from ultrafinance.dam.mongoDAM import MongoDAM
from ultrafinance.model import Tick, Quote

import ultrafinance.dam.googleDAM

import logging

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


class TestMongoDAM(TestCase):
    '''
    MongoDAM测试类
from ultrafinance.model import Tick, Quote
from pymongo import MongoClient
from pymongo import IndexModel
import pymongo
import json
from ultrafinance.dam.mongoLib import QuoteMongo
from ultrafinance.dam.mongoLib import QuoteMongos
from ultrafinance.dam.mongoDAM import MongoDAM
from ultrafinance.dam.mongoDAM import MongoDAM
client = MongoClient()
db = client.stockdb
collection = db.quotes
quotes = [Quote(*['1320676200', '32.59', '32.59', '32.58', '32.58', '65213', None]),
     Quote(*['1320676201', '32.60', '32.60', '32.59', '32.59', '65214', None])]

dam = MongoDAM()

quotes
quotes[0]
for quote in quotes:
print(quote)

mq =[QuoteMongo('testsymbol', quote.time, quote.open, quote.high, quote.low, quote.close, quote.volume, quote.adjClose) for quote in quotes]
mq

a = mq[0]

a= QuoteMongos('test')
a.append(quotes)
b = json.dumps(a, default=lambda o: o.__dict__)
b
c= json.loads(b)
collection.insert(c)
a= QuoteMongos('test1')
quotes = [Quote(*['1340676200', '32.59', '32.59', '32.58', '32.58', '65213', None]),
     Quote(*['1340676201', '32.60', '32.60', '32.59', '32.59', '65214', None])]
a.append(quotes)
b = json.dumps(a, default=lambda o: o.__dict__)
c= json.loads(b)
v.insert(c)
a= QuoteMongos('test')
quotes = [Quote(*['1340676200', '3.59', '3.59', '3.58', '3.58', '65211', None]),
     Quote(*['1340676201', '32.0', '3.60', '32.9', '3.59', '65210', None])]
a.append(quotes)
b = json.dumps(a, default=lambda o: o.__dict__)
c= json.loads(b)
collection.insert(c)

query = {'symbol': 'test'}
query = {'symbol': c['symbol']  }

query = {'symbol': 'test'}
tb =collection.find(query)
for d in tb:
    print(d)

    '''
    def setUp(self):
        self.symbol = 'ebay'

    def tearDown(self):
        pass

    def test_setup(self):
        dam = MongoDAM()
        self.assertTrue(dam.client is None, 'MongoDAM client is not None')
        dam.setup()
        LOG.debug('dam.setup() client: {0}'.format(dam.client))
        self.assertTrue(dam.client is not None, 'MongoDAM client is None')
        # 默认dbname = 'stockdb'
        self.assertTrue(dam.db.name == 'stockdb', 'MongoDAM client is not {0}'.format(dam.client.database_names()))

        dam.setup('mongodb://127.0.0.1', 'testdb')
        LOG.debug('damm mongodb://127.0.0.1 client: {0}'.format(dam.client))
        self.assertTrue(dam.client is not None, 'MongoDAM client is None')
        self.assertTrue(dam.db.name == 'testdb', 'MongoDAM client is not {0}'.format(dam.client.database_names()))

    def test_readQuotes(self):
        dam = MongoDAM()
        dam.setup('mongodb://127.0.0.1')
        dam.setSymbol('test')
        data = dam.readQuotes('20131101', '20131110')
        print([str(q) for q in data])
        self.assertNotEqual(0, len(data))

    def test_readTupleQuotes(self):
        self.fail()

    def test_readBatchTupleQuotes(self):
        self.fail()

    def test_readTupleTicks(self):
        self.fail()

    def test_readTicks(self):
        self.fail()

    def test_writeQuotes(self):
        dam = MongoDAM()
        dam.setup('mongodb://127.0.0.1', 'testdb')
        dam.setSymbol("test")

        quotes = [Quote(*['1320676200', '32.59', '32.59', '32.58', '32.58', '65213', None]),
                  Quote(*['1320676201', '32.60', '32.60', '32.59', '32.59', '65214', None])]
        dam.writeQuotes(quotes)
        quotes = [Quote(*['1320676210', '32.59', '32.59', '32.58', '32.58', '65213', None]),
                  Quote(*['1320676211', '32.60', '32.60', '32.59', '32.59', '65214', None]),
                  Quote(*['1320676201', '32.60', '32.60', '32.59', '32.59', '65214', None])]
        dam.writeQuotes(quotes)
        # print([str(quotes) for symbol, quotes in dam.readBatchTupleQuotes(["test"], 0, None).items()])
        print([str(quote) for quote in dam.readQuotes(0, None)])

    def test_writeTicks(self):
        self.fail()

    def test_writeFundamental(self):
        self.fail()

    def test_readFundamental(self):
        self.fail()

    def test_saveToMongo(self):
        self.fail()

    def test_dropMongo(self):
        dam = MongoDAM()
        dam.setup('mongodb://127.0.0.1', 'testdb')
        dam.setSymbol("test")
        dam.dropCollection('quotes')

    def test_ReadQuotesFromGooglewriteQuotes(self):
        damGoogle = ultrafinance.dam.googleDAM.GoogleDAM()
        symbol = 'NASDAQ:EBAY'
        # symbol = 'SHA:600000'
        damGoogle.setSymbol(symbol)
        start = '20131111'
        endDate = '20131231'
        data = damGoogle.readQuotes(start, endDate)

        dam = MongoDAM()
        dam.setup('mongodb://127.0.0.1', 'testdb')
        dam.setSymbol(symbol)

        quotes = data
        dam.writeQuotes(quotes)
        data = damGoogle.readQuotes(start, endDate)
        quotes = data
        dam.writeQuotes(quotes)
        # print([str(quotes) for symbol, quotes in dam.readBatchTupleQuotes(["test"], 0, None).items()])
        print([str(quote) for quote in dam.readQuotes(0, None)])

