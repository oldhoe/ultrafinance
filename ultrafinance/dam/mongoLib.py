'''
Created on Wed Dec  9 18:12:34 2015

@author: pchaosgit
'''
import pandas as pd
from ultrafinance.model import Tick, Quote
import json

import logging

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

QUOTEMONGO_FIELDS = ['symbol', 'time', 'close', 'volume', 'low', 'high']

class QuoteMongos(object):
    '''
    QuoteMongo集合
    todo 返回排序的quotes list
    '''
    __tablename__ = 'quotes'

    def __init__(self, symbol):
        super(QuoteMongos, self).__init__()
        if symbol is None:
            raise Exception("symbol can't be Null.")
        self.symbol = symbol
        self.quotes = []

    def getQuotes(self, start, end):
        '''
        Get historical prices for the given ticker symbol.
        Date format is 'YYYYMMDD'

        Returns a nested list.
        '''
        # symbols = self.__read(start, end)
        return self.quotes

    def getAll(self, symbol):
        """
        Get all available quote data for the given ticker symbol.
        Returns a dictionary.
        """
        pass

    def getTableName(self):
        return self.__tablename__

    def append(self, Quotes):
        for q in Quotes:
            self._append(q)
        self.quoteUniqueByDelete(self.quotes, self.getKey)

    def _append(self, quote):
        if not (quote in self.quotes):
            self.quotes.append(quote)
            LOG.debug('_append:{0}'.format(quote))


    def extend(self,Quotes):
        self.quotes.extend(Quotes)
        self.quoteUniqueByDelete(self.quotes, self.getKey)

    def insert(self, i= None, quote = None):
        if not(quote in self.quotes):
            self.quotes.insert(i, quote)

    def remove(self, quoteList):
        for q in quoteList:
            self._remove(q, self.getKey)

    @staticmethod
    def getKey(x):
        return x.time

    def _remove(self, quote, quoteKey=None):
        '''
        
        :param quoteKey: 
        :return: 
        '''
        if quoteKey is None:
            def idfun(x): return x
        for item in reversed(self.quotes):
            if quoteKey(quote) == quoteKey(item):
                assert isinstance(item, object)
                self.quotes.remove(item)

    @staticmethod
    def quoteUniqueByDelete(quotes, quoteKey=None):
        '''
        order preserving
        :param quotes: 原列表
        :param quoteKey: 列表比较函数
        :return: 返回无重复值的列表
        '''
        if quoteKey is None:
            def idfun(x): return x
        seen = {}
        for item in reversed(quotes):
            marker = quoteKey(item)
            # in old Python versions:
            # if seen.has_key(marker)
            # but in new ones:
            if marker in seen:
                seen[marker] += 1
            else:
                seen[marker] = 1
        for item in reversed(quotes):
            marker = quoteKey(item)
            # in old Python versions:
            # if seen.has_key(marker)
            # but in new ones:
            if seen[marker] > 1:
                # seen[marker] += 1
                quotes.remove(item)
                seen[marker] -= 1

class QuoteMongo(object):
    __tablename__ = 'symbols'

    def __init__(self, symbol, time, open, high, low, close, volume, adjClose):
        ''' constructor '''
        self.symbol = symbol
        self.time = time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.adjClose = adjClose

    def __str__(self):
        ''' convert to string '''
        return json.dumps({
            "symbol": self.symbol,
            "time": self.time,
            "read": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "adjClose": self.adjClose})

    def to_json(self):
        return self.__str__()

    def __repr__(self):
        return "<QuoteMongo('%s', '%s','%s', '%s', '%s','%s', '%s', '%s')>" \
               % (self.symbol, self.time, self.open, self.high, self.low, self.close, self.volume, self.adjClose)

    def getTableName(self):
        return self.__tablename__


class TickMongo(object):
    __tablename__ = 'ticks'

    def __init__(self, symbol, time, open, high, low, close, volume):
        ''' constructor '''
        self.symbol = symbol
        self.time = time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __repr__(self):
        return "<Tick('%s', '%s', '%s', '%s', '%s', '%s', '%s')>" \
               % (self.symbol, self.time, self.open, self.high, self.low, self.close, self.volume)


class fmsql(object):
    __tablename__ = 'fundamental'

    def __init__(self, symbol, field, timeStamp, value):
        ''' constructor '''
        self.symbol = symbol
        self.field = field
        self.timeStamp = timeStamp
        self.value = value

    def __repr__(self):
        return "<Fundamentals('%s', '%s', '%s', '%s')>" \
               % (self.symbol, self.field, self.timeStamp, self.value)
