# -*- coding: utf-8 -*-
"""
Created on 2016-01-02

@author: pchaos
"""
import logging
import sys

import pymongo
from pymongo import MongoClient
from pymongo import IndexModel
from ultrafinance.dam.sqlDAM import TickSql
from ultrafinance.dam.baseDAM import BaseDAM
from ultrafinance.dam.mongoLib import QuoteMongos
from ultrafinance.lib.util import splitListEqually
from ultrafinance.model import Quote, Tick, TupleQuote
from ultrafinance.dam.mongoNumpyLib import MongoNumpyLib
import json


LOG = logging.getLogger()

class MongoNumpyDAM(BaseDAM):
    '''
    MongoNumpy DAM

    '''

    def __init__(self):
        '''
        constructor
        '''
        super(MongoNumpyDAM, self).__init__()
        self.__mg = MongoNumpyLib()
        self.first = True

    def setup(self, setting='', dbname='stockdb'):
        '''
        set up
        setting = 'mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]'
        :param setting: monggDB setting
        :param dbname: db name
        '''
        self.__mg.setup(setting, dbname)

    def __sqlToQuote(self, row):
        ''' convert row result to Quote '''
        return Quote(row.time, row.read, row.high, row.low, row.close, row.volume, row.adjClose)

    def __quoteToMongo(self, quote):
        ''' convert tick to QuoteSql '''
        return QuoteMongo(self.symbol, quote.time, quote.read, quote.high, quote.low, quote.close, quote.volume,
                          quote.adjClose)

    def __quoteToMongos(self, quote):
        ''' convert tick to QuoteSql '''
        return QuoteMongo(self.symbol, quote.time, quote.read, quote.high, quote.low, quote.close, quote.volume,
                          quote.adjClose)

    def readQuotes(self, start = None, end = None):
        ''' read symbols '''
        if self.symbol is None:
            LOG.debug('Symbol is None')
            return []
        if type(start) == str:
            start = int(start)
        if type(end) == str:
            end = int(end)
        mg = self.__findBySymbol()
        if mg is None:
            return None
        else:
            return self.__listToQuotes(mg['symbols'], start, end)

    def __findBySymbol(self):
        '''
        查找证券代码对应的数据
        :return:返回值为找到的dict对象， 返回None未找到
        '''
        collection = self.getTable(None)
        query = {'symbol': self.symbol}
        mg = collection.find_one(query)
        return mg

    def __listToQuotes(self, quoteList, start = None, end = None):
        '''
        将数据列表转换为Quote列表
        :param quoteList: 数据列表
        :return: 转换成Quote列表
        '''
        if start is None:
            start = 0
        if end is None:
            end = sys.maxsize
        quotes = []
        for q in quoteList:
            if start <= q['time'] <= end:
                quotes.append(Quote(q['time'], q['read'], q['high'], q['low'], q['close'], q['volume'], q['adjClose']))
        return quotes

    def writeQuotes(self, quotes):
        '''
        write symbols
        '''
        # self.saveQuotesToMongo([self.__quoteToMongo(quote) for quote in symbols])
        collection = self.getTable(quotes[0])
        qm = QuoteMongos(self.symbol)
        qm.extend(quotes)
        self.saveQuotesToMongo(collection, qm)


    def destruct(self):
        ''' destructor '''
        self.db = None
        self.client = None


    def _checkConnection(self):
        '''
        判断连接是否有效
        '''
        try:
            info = self.client.server_info()
            if info['ok'] > 0:
                pass
        except:
            raise Exception('mongoDB not started!\nplease Start monogoDB.')

    def getTable(self, quote):
        self._checkConnection()
        # db = self.client[self._dbName]
        qm = QuoteMongos(self.symbol)
        table = self.db[qm.getTableName()]
        return table

    def saveQuotesToMongo(self, collection, quoteMongos, CRUDmode='insert'):
        '''
        保存dataFrame到mongoDB
        CRUDmode='insert': 插入
        CRUDmode='update': 更新
        CRUDmode='delete': 删除
        '''
        result = None

        if CRUDmode.lower() == 'insert':
            mg = self.__findBySymbol()
            if mg is None:
                # 插入操作
                qmj = self._to_json(quoteMongos)
                result = collection.insert_one(qmj)
                LOG.info('Write {1} find_and_modify:{0}'.format(
                        result.inserted_id, collection.name))
                self.createIndex(collection)
            else:
                result = self._updateQuoteMongoCollection(collection, mg, quoteMongos, result)
        elif CRUDmode.lower() == 'update':
            mg = self.__findBySymbol()
            result = self._updateQuoteMongoCollection(collection, mg, quoteMongos, result)

        elif CRUDmode.lower() == 'delete':
            # 删除操作
            result = collection.find_one_and_delete({'symbol': quoteMongos.symbol})
            LOG.info('Write {1} find_and_modify:{0}'.format(
                    result, collection.name))
        else:
            # todo raise exception
            result = None
        return result

    def _updateQuoteMongoCollection(self, collection, mg, quoteMongos, result):
        ql = self.__listToQuotes(mg['symbols'])
        quoteMongos.append(ql)
        qmj = self._to_json(quoteMongos)
        result = self.updateMongoTable(collection, qmj)
        return result

    def _to_json(self, quoteMongos):
        qmjs = json.dumps(quoteMongos, default=lambda o: o.__dict__)
        return json.loads(qmjs)

    def updateMongoTable(self, collection, quoteMongosJson):
        '''
        更新操作
        默认更新关键字为股票代码（symbol）
        :param quoteMongosJson: DataFrame转换成json值
        :param collection:
        :return:
        '''
        if len(quoteMongosJson) == 2:
            LOG.info('Write {1} find_and_modify:{0}'.format(
                    quoteMongosJson, self.db.name))
            query = {'symbol': quoteMongosJson['symbol']}
            result = collection.find_one_and_replace(query, quoteMongosJson)
        return result

    def createIndex(self, collection):
        if collection != None:
            try:
                index1 = IndexModel([('symbol', pymongo.ASCENDING)], unique=True)
                index2 = IndexModel([('time', pymongo.ASCENDING)])
                collection.create_indexes([index1, index2])
            except:
                pass

    def dropCollection(self, collectionName):
        '''
        删除collection
        :param collectionName: 需要删除collection的名称
        :return:
        '''
        collection = self.db[collectionName]
        collection.drop()

    def deleteQuotes(self, quotes):
        oldQuotes = self.readQuotes()

        collection = self.getTable(quotes[0])
        qm = QuoteMongos(self.symbol)
        qm.extend(oldQuotes)
        self.saveQuotesToMongo(collection, qm, 'delete')
        qm.remove(quotes)

        self.saveQuotesToMongo(collection, qm)

