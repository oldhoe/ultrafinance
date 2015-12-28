# -*- coding: utf-8 -*-
"""
Created on Sat Nov  7 11:25:43 2015

@author: pchaos
"""
import logging
import os
import struct
from os import path
from os.path import sep

import time

from xlrd import open_workbook

from ultrafinance.model import Quote, Tick, TupleQuote
from ultrafinance.lib.errors import UfException, Errors

LOG = logging.getLogger()


class TDXLib(object):
    ''' lib for accessing TDX Data '''
    READ_MODE = 'r'
    WRITE_MODE = 'w'

    def __init__(self, basePath='', symbols=None, mode=READ_MODE):
        ''' constructor '''
        if TDXLib.READ_MODE == mode:
            self.__operation = TDXRead(basePath, symbols)
        elif TDXLib.WRITE_MODE == mode:
            self.__operation = TDXWrite(symbols)
        else:
            raise UfException(Errors.INVALID_TDX_MODE,
                              "Invalid operation mode, only %s and %s are accepted" \
                              % (TDXLib.READ_MODE, TDXLib.WRITE_MODE))

    def __enter__(self):
        ''' call enter of operation '''
        self.__operation.pre()
        return self

    def __exit__(self, type, value, traceback):
        ''' call exit of operation '''
        self.__operation.post()
        return

    def getOperation(self):
        ''' get operation'''
        return self.__operation

    def read(self, start, end):
        return self.__operation.read(start, end)



class TDXOpertion(object):
    ''' TDX operation '''
    DEFAULT_START_DATE = 20100101

    def open(self, name):
        ''' open sheet '''
        raise UfException(Errors.UNDEFINED_METHOD, "openSheet function is not defined")

    def post(self):
        ''' post action as exit'''
        return

    def pre(self):
        ''' pre action as pre '''
        return


class TDXWrite(TDXOpertion):
    ''' class to write excel '''

    def __init__(self, fileName):
        if path.exists(fileName):
            raise UfException(Errors.FILE_EXIST, "File already exist: %s" % fileName)

        self.__fileName = fileName

    def open(self, name):
        ''' set a sheet to write '''
        if name not in self.__sheetNameDict:
            sheet = self.__workbook.add_sheet(name)
            self.__sheetNameDict[name] = sheet

    def post(self):
        ''' save workbook to excel file '''
        self.__fileName.save(self.__fileName)


class TDXRead(TDXOpertion):
    ''' class to read TDX file '''
    america_path = sep + "ds" + sep + "lday" + sep
    # on linux : /sh/lday ; on windows : \sh\lday
    sh_path = "sh" + sep + "lday" + sep
    # on linux : /sz/ldayreadCell
    sz_path = "sz" + sep + "lday" + sep

    def __init__(self, basePath, symbol):
        ''' constructor '''
        fileName = self.getfileName(basePath, symbol)
        if not path.exists(fileName):
            raise UfException(Errors.FILE_NOT_EXIST, "File doesn't exist: %s" % fileName)
        self.__file = fileName

    def getfileName(self, basePath, symbol):
        '''
        根据不同的代码返回相应的目录+文件
        :param basePath: 数据跟目录
        :param symbol: 股票代码
        :return: 文件全路径+文件名
        '''
        pt = None
        if symbol[0] == '6':
            pt = os.path.join(basePath, self.sh_path)
            fileName = 'sh{0}.day'.format(symbol)
        else:
            pt = os.path.join(basePath, self.sz_path)
            fileName = 'sz{0}.day'.format(symbol)
        return os.path.join(pt, fileName)

    def change_path(self, pval, pname='tdxpath'):
        global tdxpath
        if pname == 'tdxpath':
            tdxpath = pval
            LOG.debug('tdx2.change.tdxpath =', pval)
        # below is not used mostly
        if pname == 'america_path':
            america_path = pval
        if pname == 'sh_path':
            sh_path = pval
        if pname == 'sz_path':
            sz_path = pval

    def parse_time_reverse(self, ft):
        # 418x turn into 20101007
        s = ft
        s = (s + 10956) * 86400
        s -= 1000 * 60 * 60 * 6
        s = time.gmtime(s)
        s = time.strftime('%Y%m%d', s)
        s = int(s)
        return s

    def parse_time(self, ft):
        # 20101007 turn into 418x
        s = str(ft)
        s = time.strptime(s, '%Y%m%d')
        s = time.mktime(s)
        s += 1000 * 60 * 60 * 6
        s = s / 86400 - 10956
        s = int(s)
        return s

    def get_dayline_by_fid(self, str_fid='sh000001', restrictSize=0):

        str_fid = str_fid.replace('.day', '')

        str_tdx_dayline_format = "iiiiifii"
        size_of_tdx_dayline = 32  # 32 bytes per struct

        if str_fid[0:2] == 'sh':
            spath = tdxpath + sh_path
        else:
            spath = tdxpath + sz_path

        filename = spath + str_fid + ".day"
        f = open(filename, 'rb')

        if restrictSize != 0:
            fsize = os.path.getsize(filename)
            sr = fsize - 32 * restrictSize
            if sr < 0: sr = 0
            f.seek(sr)

            # for i in range(0,9999):
        dayline = []
        while 1:
            rd = f.read(32)
            if not rd: break
            # print i, len(rd)
            st = struct.unpack(str_tdx_daylineBaseException_format, rd)
            # print st

            q = libqda_struct.fdaydata()
            q.pars(parse_time(st[0]),
                   int(st[1]) / 100.,
                   int(st[2]) / 100.,
                   int(st[3]) / 100.,
                   int(st[4]) / 100.,
                   float(st[5]),
                   int(st[6]),
                   int(st[7]))

            dayline.append(q)

        return dayline

        f.close()

    def read(self, start, end):
        datalist = []
        with open(self.__file, 'rb') as f:
            text = f.read()
            startpos = 0
            total_length = len(text)
            while startpos < total_length:
                mydate, open_price, high, low, close, amount, vol, reservation = struct.unpack("iiiiifii", text[startpos:startpos + 32])
                if start <= mydate <= end:
                    datalist.append(Quote(mydate, open_price, high, low, close, vol, None))
                startpos += 32
        return datalist