# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 23:42:53 2015

@author: pchaos
"""
import os

from ultrafinance.dam.baseDAM import BaseDAM
from ultrafinance.dam.TDXLib import TDXLib
from ultrafinance.model import TICK_FIELDS, QUOTE_FIELDS, Quote, Tick
from ultrafinance.lib.errors import UfException, Errors
from os import path
import logging

LOG = logging.getLogger()


class TDXDAM(BaseDAM):
    ''' TDX DAO
    TDX 通达信
    '''
    QUOTE = 'quote'
    TICK = 'tick'

    def __init__(self):
        ''' constructor '''
        super(TDXDAM, self).__init__()
        self.__dir = None

    def targetPath(self, kind):
        return path.join(self.__dir, "%s-%s.xls" % (self.symbol, kind))

    def __findRange(self, TDXLib, start, end):
        ''' return low and high as excel range '''
        inc = 1
        low = 0
        high = 0
        dates = TDXLib.readCol(0, 1)

        for index, date in enumerate(dates):
            if int(start) <= int(date):
                low = index + inc
                break

        if low:
            for index, date in reversed(list(enumerate(dates))):
                if int(date) <= int(end):
                    high = index + inc
                    break

        return low, high

    def __readData(self, basePath, start, end):
        ''' read data '''
        ret = []
        if not path.exists(basePath):
            LOG.error("Target file doesn't exist: %s" % path.abspath(basePath))
            return ret
        with TDXLib(basePath, self.__symbol, TDXLib.READ_MODE) as TDX:
           ret = TDX.read(start, end)
        return ret

    def __writeData(self, targetPath, fields, rows):
        ''' write data '''
        if path.exists(targetPath):
            LOG.error("Target file exists: %s" % path.abspath(targetPath))
            raise UfException(Errors.FILE_EXIST, "can't write to a existing file")  # because xlwt doesn't support it

        with TDXLib(fileName=targetPath, mode=TDXLib.WRITE_MODE) as excel:
            excel.writeRow(0, fields)
            for index, row in enumerate(rows):
                excel.writeRow(index + 1, row)

    def readQuotes(self, start, end):
        ''' read quotes '''
        startInt = int(start)
        endInt = int(end)
        quotes = self.__readData(self.__dir, startInt, endInt)
        return quotes

    def writeQuotes(self, quotes):
        ''' write quotes '''
        self.__writeData(self.targetPath(TDXDAM.QUOTE),
                         QUOTE_FIELDS,
                         [[getattr(quote, field) for field in QUOTE_FIELDS] for quote in quotes])

    def readTicks(self, start, end):
        ''' read ticks '''
        ticks = self.__readData(self.targetPath(TDXDAM.TICK), start, end)
        return [Tick(*tick) for tick in ticks]

    def writeTicks(self, ticks):
        ''' read quotes '''
        self.__writeData(self.targetPath(TDXDAM.TICK),
                         TICK_FIELDS,
                         [[getattr(tick, field) for field in TICK_FIELDS] for tick in ticks])

    def setDir(self, path):
        ''' set dir '''
        if os.path.exists(path):
            self.__dir = path
        else:
            raise UfException(Errors.INVALID_TDX_PATH, "通达信路径错误！\n{0}".format(path))


    def setSymbol(self, symbol):
        ''' set symbol '''
        self.__symbol = symbol