# -*- coding: utf-8 -*-
"""
Created on Sat Nov  7 11:25:43 2015

@author: pchaos
"""
import logging
from os import path
from os.path import sep

from ultrafinance.lib.errors import UfException, Errors

LOG = logging.getLogger()


class TDXLib(object):
    ''' lib for accessing TDX Data '''
    READ_MODE = 'r'
    WRITE_MODE = 'w'

    def __init__(self, basePath='', symbols=None, mode=READ_MODE):
        ''' constructor '''
        if TDXLib.READ_MODE == mode:
            self.__operation = TDXRead(symbols)
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

    def openSheet(self, name):
        ''' open a sheet by name '''
        self.__operation.open(name)

    def getOperation(self):
        ''' get operation'''
        return self.__operation


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
    sh_path = sep + "sh" + sep + "lday" + sep
    # on linux : /sz/ldayreadCell
    sz_path = sep + "sz" + sep + "lday" + sep

    def __init__(self, fileName):
        ''' constructor '''
        if not path.exists(fileName):
            raise UfException(Errors.FILE_NOT_EXIST, "File doesn't exist: %s" % fileName)

        self.__book = open_workbook(fileName)
        self.__sheet = None

    def change_path(pval, pname='tdxpath'):
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

    def parse_time_reverse(ft):
        # 418x turn into 20101007
        s = ft
        s = (s + 10956) * 86400
        s -= 1000 * 60 * 60 * 6
        s = time.gmtime(s)
        s = time.strftime('%Y%m%d', s)
        s = int(s)
        return s

    def parse_time(ft):
        # 20101007 turn into 418x
        s = str(ft)
        s = time.strptime(s, '%Y%m%d')
        s = time.mktime(s)
        s += 1000 * 60 * 60 * 6
        s = s / 86400 - 10956
        s = int(s)
        return s

    def get_dayline_by_fid(str_fid='sh000001', restrictSize=0):

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
            st = struct.unpack(str_tdx_dayline_format, rd)
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
