# -*- coding: utf-8 -*-
"""
Created on %(date)s

@author: %(username)s
"""
from pymongo import MongoClient
from ultrafinance.lib.mongowrapper import MongoWrapper

class MongoNumpyLib(object):
    '''
    Quote using Numpy
    need https://github.com/dattalab/mongowrapper
    '''
    def __init__(self):
        super(MongoNumpyLib, self).__init__()
        # if symbol is None:
        #     raise Exception("symbol can't be Null.")
        self.symbol = None
        # quotes is numpy array
        self.quotes = None

    def setup(self, setting='', dbname='stockdb'):
        '''
        set up
        setting = 'mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]'
        '''
        if 'mongodb' not in setting:
            if len(setting) < len('db'):
                self.client = MongoClient()
            else:
                raise Exception("db not specified in setting")
        else:
            self.client = MongoClient(setting)
        if dbname is not None:
            self.db = self.client[dbname]

    def getAll(self, symbol):
        """
        Get all available quote data for the given ticker symbol.
        Returns a dictionary.
        """
        url = 'http://www.google.com/finance?q=%s' % symbol
        page = self.__request(url)

        soup = BeautifulSoup(page.content, 'lxml')
        snapData = soup.find("table", {"class": "snap-data"})
        if snapData is None:
            raise UfException(Errors.STOCK_SYMBOL_ERROR, "Can find data for stock %s, symbol error?" % symbol)
        data = {}
        for row in snapData.findAll('tr'):
            keyTd, valTd = row.findAll('td')
            data[keyTd.getText()] = valTd.getText()

        return data