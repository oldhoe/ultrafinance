# -*- coding: utf-8 -*-
"""
Created on %(date)s

@author: pchaosgit
"""
import os
import tempfile
from unittest import TestCase

from ultrafinance.dam.tdxDAM import TDXDAM
from ultrafinance.dam.mongoDAM import MongoDAM
from ultrafinance.model import Tick, Quote
from ultrafinance.dam.googleDAM import GoogleDAM
from random import randint
import logging

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


class TestMongoDAM(TestCase):
    '''
    MongoDAM测试类
import os
import tempfile
from ultrafinance.dam.tdxDAM import TDXDAM
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
collection = db.symbols
symbols = [Quote(*['1320676200', '32.59', '32.59', '32.58', '32.58', '65213', None]),
     Quote(*['1320676201', '32.60', '32.60', '32.59', '32.59', '65214', None])]

dam = MongoDAM()

symbols
symbols[0]
for quote in symbols:
print(quote)

mq =[QuoteMongo('testsymbol', quote.time, quote.read, quote.high, quote.low, quote.close, quote.volume, quote.adjClose) for quote in symbols]
mq

a = mq[0]

a= QuoteMongos('test')
a.append(symbols)
b = json.dumps(a, default=lambda o: o.__dict__)
b
c= json.loads(b)
collection.insert(c)
a= QuoteMongos('test1')
symbols = [Quote(*['1340676200', '32.59', '32.59', '32.58', '32.58', '65213', None]),
     Quote(*['1340676201', '32.60', '32.60', '32.59', '32.59', '65214', None])]
a.append(symbols)
b = json.dumps(a, default=lambda o: o.__dict__)
c= json.loads(b)
v.insert(c)
a= QuoteMongos('test')
symbols = [Quote(*['1340676200', '3.59', '3.59', '3.58', '3.58', '65211', None]),
     Quote(*['1340676201', '32.0', '3.60', '32.9', '3.59', '65210', None])]
a.append(symbols)
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
        dam.setup('mongodb://127.0.0.1', 'testdb')
        # dam.setup('mongodb://127.0.0.1')
        dam.setSymbol('600177')
        data = dam.readQuotes(20131101, 20131110)
        len1 = len(data)
        self.assertNotEqual(0, len1)
        print([q for q in data])
        data = dam.readQuotes(20131101)
        self.assertTrue(len(data) > len1)

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
        dam.setSymbol("testNew")
        quotes = [Quote(*[20130101, 3259, 3259, 3258, 3258, 65213, None]),
                  Quote(*[20130102, 3260, 3260, 3259, 3259, 65214, None])]
        dam.writeQuotes(quotes)
        quotes = [Quote(*[20130101, 3333, 4444, 5555, 6666, 65213, None]),
                  Quote(*[20130111, 3011, 3012, 3013, 3014, 65214, None]),
                  Quote(*[20130112, 3012, 3013, 3014, 3259, 65214, None])]
        dam.writeQuotes(quotes)
        LOG.debug([str(quote) for quote in dam.readQuotes(0, None)])

    def test_deleteQuotes(self):
        self.test_writeQuotes()
        dam = MongoDAM()
        dam.setup('mongodb://127.0.0.1', 'testdb')
        dam.setSymbol("testNew")
        data = dam.readQuotes('20130101', '20131110')
        quotes = []
        quotes.append(data[randint(0, len(data) - 1)])
        dam.deleteQuotes(quotes)

    def test_writeTicks(self):
        self.fail()

    def test_writeFundamental(self):
        self.fail()

    def test_readFundamental(self):
        self.fail()

    def test_saveToMongo(self):
        self.fail()

    def test_dropCollection(self):
        dam = MongoDAM()
        dam.setup('mongodb://127.0.0.1', 'testdb')
        dam.setSymbol("test")
        dam.dropCollection('symbols')
        dam.setup('mongodb://127.0.0.1', 'testdb')
        dam.dropCollection('quotes')
        # dam.setup('mongodb://127.0.0.1')
        # dam.dropCollection('quotes')

    def test_ReadQuotesFromGoogle_WriteQuotes(self):
        damGoogle = GoogleDAM()
        symbol = 'NASDAQ:EBAY'
        # symbol = 'SHA:600000'
        damGoogle.setSymbol(symbol)
        start = '20141111'
        end = '20141211'
        data = damGoogle.readQuotes(start, end)
        len1 = len(data)
        quotes = data
        self.assertNotEqual(0, len1, 'read equal to 0')
        dam = MongoDAM()
        dam.setup('mongodb://127.0.0.1', 'testdb')
        dam.setSymbol(symbol)

        dam.writeQuotes(quotes)
        end = '20141231'
        data = damGoogle.readQuotes(start, end)
        quotes = data
        dam.writeQuotes(quotes)
        # print([str(symbols) for symbol, symbols in dam.readBatchTupleQuotes(["test"], 0, None).items()])
        print([str(quote) for quote in dam.readQuotes(0, None)])

    def test_readZXG_WriteQuotes(self):
        '''
        从通达信下载日线数据包，根据自选股列表保存到mongoDB
        :return:
        todo 150210?
        '''
        damTDX = TDXDAM()
        dam = MongoDAM()
        damTDX.setDir(tempfile.gettempdir())
        dam.setup('mongodb://127.0.0.1', 'testdb')
        zxgName = 'ZXG'
        zxg = damTDX.readZXG('./data/{0}.blk'.format(zxgName))
        from numpy import array
        zxg = {'name': 'ZXG', 'symbols': array([(0, '150228'),
                                                (0, '150210'), (1, '510900'), (1, '600401'), (0, '000998')],
                                               dtype=[('market', 'i1'), ('symbol', '<U20')])}
        for a in zxg['symbols']:
            dam.setSymbol(a['symbol'])
            damTDX.setSymbol(a['symbol'])
            data = damTDX.readQuotes()
            self.assertTrue(len(data) > 0, '未取得数据')
            dam.writeQuotes(data)
            LOG.info('Writed {0}'.format(a['symbol']))
