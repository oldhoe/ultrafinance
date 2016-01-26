# -*- coding: utf-8 -*-
"""
Created on Sat Nov  7 11:25:43 2015

@author: pchaos
"""
import logging
import os
import re
import struct
import tempfile
import threading
import zipfile
from os import path
from os.path import sep
import numpy as np

import time as tm

import sys

import subprocess
from xlrd import open_workbook

from ultrafinance.dam.symbolLib import Market, SymbolLib
from ultrafinance.model import Quote, Tick, TupleQuote
from ultrafinance.lib.errors import UfException, Errors
from ultrafinance.designPattern.singleton import Singleton

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
        ''' read sheet '''
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
            # 找不到通达信数据文件

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
        sl = SymbolLib.getInstance()
        if sl.checkSHSymbol(symbol):
            # 上证代码
            pt = os.path.join(basePath, self.sh_path)
            symbol = 'sh{0}'.format(symbol)
        elif sl.checkSZSymbol(symbol):
            # 深证代码
            pt = os.path.join(basePath, self.sz_path)
            symbol = 'sz{0}'.format(symbol)
        else:
            # 其他代码
            pass
        if basePath == tempfile.gettempdir():
            '''
            根目录为临时目录
            '''
            tdxSource = TDXSource.getInstance()
            ktype = 'D'  # 日线
            filename = tdxSource.extract(symbol, ktype)
            return filename
        else:
            fileName = '{0}.day'.format(symbol)
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
        s = tm.gmtime(s)
        s = tm.strftime('%Y%m%d', s)
        s = int(s)
        return s

    def parse_time(self, ft):
        # 20101007 turn into 418x
        s = str(ft)
        s = tm.strptime(s, '%Y%m%d')
        s = tm.mktime(s)
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
        '''

        :param start:
        :param end:
        :return:
        '''
        datalist = []
        if start is None:
            start = 0
        if end is None:
            end = sys.maxsize
        with open(self.__file, 'rb') as f:
            text = f.read()
            startpos = 0
            total_length = len(text)
            while startpos < total_length:
                mydate, open_price, high, low, close, amount, vol, reservation = struct.unpack("iiiiifii", text[
                                                                                                           startpos:startpos + 32])
                if start <= mydate <= end:
                    # todo Quote对象内部使用的是浮点数，改成整数
                    datalist.append(Quote(mydate, open_price, high, low, close, vol, None))
                startpos += 32
        return datalist


class TDXSource(Singleton):
    '''
    通达信数据源
    从http://www.tdx.com.cn/list_66_69.html下载相应的数据：
    上证常见指数日线、深证常见指数日线、上证所有证券日线、深证所有证券日线、上证所有证券5分钟线、深证所有证券5分钟线

    '''
    tdxSourceList = [{'name': 'shlday', 'url': 'http://www.tdx.com.cn/products/data/data/vipdoc/shlday.zip'}  # 上证所有证券日线
        , {'name': 'szlday', 'url': 'http://www.tdx.com.cn/products/data/data/vipdoc/szlday.zip'}  # 深证所有证券日线
        , {'name': 'sh5fz', 'url': 'http://www.tdx.com.cn/products/data/data/vipdoc/sh5fz.zip'}  # 上证所有证券5分钟线
        , {'name': 'sz5fz', 'url': 'http://www.tdx.com.cn/products/data/data/vipdoc/sz5fz.zip'}  # 深证所有证券5分钟线
                     ]

    sh_path = "sh" + sep + "lday" + sep
    sh_min5_path = "sh" + sep + "fzline" + sep
    # on linux : /sz/lday
    sz_path = "sz" + sep + "lday" + sep
    sz_min5_path = "sz" + sep + "fzline" + sep

    def runShell(self, command, timeout=5):
        # proz http://www.tdx.com.cn/products/data/data/shzsday.zip -P /tmp/
        with open(os.devnull, 'w') as fnull:
            result = subprocess.call(command, shell=True, stdout=fnull, stderr=fnull, timeout=timeout)
            if result:
                return False
            else:
                return True

    def download(self, name='shlday', timeout=500):
        '''
        使用shell下载数据
        :param name: 要下载的名称
            shlday: 上证所有证券日线
            szlday: 深证所有证券日线
            sh5fz: 上证所有证券5分钟线
            sz5fz:  深证所有证券5分钟线
        :param timeout:超时秒数
        :return: True下载成功, Fasle 下载失败
            fileName 下载文件名
        '''
        tmpPath = tempfile.gettempdir()
        fileName = os.path.join(tmpPath, name + '.zip')
        if os.path.exists(fileName):
            # 已存在下载的文件
            # todo 文件超过24小时，则重新下载
            return True, fileName
        url = self.getUrl(name)
        if url is not None:
            # 使用proz多线程下载，也可以使用wget
            command = 'proz {0} -P {1}'.format(url, tmpPath)
            LOG.info('runShell:{0}'.format(command))
            return self.runShell(command, timeout), fileName
        else:
            # url 为空
            return False, fileName

    def __getStockSymbols(self, name='shlday', timeout=500):
        '''
        返回name对应的压缩包内的股票代码列表
        :param name: 要下载的名称
            shlday: 上证所有证券日线
            szlday: 深证所有证券日线
        :param timeout: 下载超时秒数
        :return:
        '''
        result, downloadZipName = self.download(name, timeout)
        symbolList = []
        if result:
            # 对下载后的压缩文件文件名整理
            archive = zipfile.ZipFile(downloadZipName)
            for x in archive.namelist():
                if len(x) == len(archive.namelist()[0]):
                    symbolList.append(x.split('.')[0])
        return symbolList

    def getSHStockSymbols(self, timeout=500):
        '''
        通达信上海证券代码
        :param timeout: 下载超时秒数
        :return:
        '''
        name='shlday'
        symbolList = self.__getStockSymbols(name, timeout)
        symbolDict = self.__symbolListToDict(symbolList)
        return symbolDict

    def getSZStockSymbols(self, timeout=500):
        '''
        通达信深圳证券代码
        :param timeout: 下载超时秒数
        :return:
        '''
        name='szlday'
        symbolList = self.__getStockSymbols(name, timeout)
        symbolDict = self.__symbolListToDict(symbolList)
        return symbolDict

    def __symbolListToDict(self, symbolList):
        '''
        symbol List to Dict
        :param symbolList:
        :return:
        '''
        symbolDict = {}
        if len(symbolList) > 0:
            symbolDict =  dict((k, str(Market[k[0:2].upper()].value) + k[2:]) for k in symbolList)
            # symbolDict = [dict('{0}{1}'.format(str(Market.SH.value), k[2:]), [Market.SZ)] for k in symbolList)]
        return symbolDict

    def getUrl(self, downloadName):
        '''
        根据name属性返回对应的通达信url地址
        :param downloadName:
        :return: name对应的url，没有对应则返回None
        '''
        url = None
        for item in self.tdxSourceList:
            if item['name'] == downloadName:
                url = item['url']
                break
        if url is None:
            LOG.info('downloadName: {0} not found url.'.format(downloadName))
        return url

    def extract(self, symbol='', ktype='D', targetPath=''):
        '''
        获取股票代码、周期对应的文件
        :param symbol: 股票代码 例如: 'sh000001', 'sz000001'
        :param ktype: 数据周期
            ktype='D':获取周k线数据 默认值
            ktype='W':获取周k线数据
            ktype='M':获取月k线数据
            ktype='5':获取5分钟k线数据
            ktype='15':获取15分钟k线数据
            ktype='30':获取30分钟k线数据
            ktype='60':获取60分钟k线数据
        :return: 返回文件名
                 当返回N时，表示不成功
        '''
        symbol = symbol.lower()
        downloadName = self.getDownloadnameBySymbol(symbol, ktype)
        result, downloadZipName = self.download(downloadName)
        fileNameResult = None
        if result:
            # 已下载数据包
            fileName = self.getTargetFileName(symbol, ktype, targetPath)
            filePath, f = os.path.split(fileName)
            archive = zipfile.ZipFile(downloadZipName)
            for file in archive.namelist():
                if file.startswith(symbol):
                    archive.extract(file, filePath)
            if os.path.exists(fileName):
                fileNameResult = fileName
        # 删除old Files
        self.deleteFileOfACertainAge(filePath)
        return fileNameResult

    def getTargetFileName(self, symbol, ktype, targetPath=''):
        ktype = ktype.upper()
        fileSuffix = ''
        if ktype in 'DWM':
            # 日线数据
            fileSuffix = '.day'
        else:
            # 五分钟数据
            fileSuffix = '.lc5'
        subFolder = ''
        symbol = symbol.lower()
        if 'sh' in symbol:
            if fileSuffix == '.day':
                subFolder = self.sh_path
            elif fileSuffix == '.lc5':
                subFolder = self.sh_min5_path
        elif 'sz' in symbol:
            if fileSuffix == '.day':
                subFolder = self.sz_path
            elif fileSuffix == '.lc5':
                subFolder = self.sz_min5_path
        else:
            pass
        if len(targetPath) == 0:
            fileName = os.path.join(os.path.join(tempfile.gettempdir(), subFolder), symbol + fileSuffix)
        else:
            fileName = os.path.join(os.path.join(targetPath, subFolder), symbol + fileSuffix)
        return fileName

    def getDownloadnameBySymbol(self, symbol, ktype='D'):
        '''
        根据股票代码、周期返回对应的要下载的名称
        :param symbol:
        :param ktype: 默认为日线数据
        :return:
        '''
        downloadName = ''
        symbol = symbol.lower()
        if 'sh' in symbol:
            downloadName = 'sh'
        elif 'sz' in symbol:
            downloadName = 'sz'
        else:
            pass
        ktype = ktype.upper()
        if ktype in 'DWM':
            # 日线数据
            downloadName += 'lday'
        else:
            # 五分钟数据
            downloadName += '5fz'
        return downloadName

    def beeps(self, times=1, freq=1600):
        if sys.platform == 'linux':
            command = '( speaker-test -t sine -f {0} )& pid=$! ; sleep 0.1s ; kill -9 $pid'.format(freq)
            i = 0
            while i < times:
                i += 1
                self.runShell(command)
                if i + 1 < times:
                    tm.sleep(0.025)

    def delays(self, times=1, yesOrNo=True):
        '''
        延迟times秒
        :param times: 秒数
        :param yesOrNo: 是否延迟
        :return:
        '''
        delaySec = 1
        if yesOrNo:
            tm.sleep(delaySec * times)

    # @staticmethod
    def deleteFileOfACertainAge(self, filePath, seconds=300):
        '''
        删除目录filePath中seconds秒前的文件
        :param filePath: 目录
        :param seconds: 秒
        :return:
        '''
        ago = tm.time() - seconds
        for somefile in os.listdir(filePath):
            filename = os.path.join(filePath, somefile)
            st = os.stat(filename)
            mtime = st.st_mtime
            if mtime < ago:
                os.remove(filename)


class Command(object):
    '''
    超时关闭运行中的命令行
    command = Command("echo 'Process started'; sleep 2; echo 'Process finished'")
    command.run(timeout=3)
    command.run(timeout=1)
    The output of this snippet in my machine is:

        Thread started
        Process started
        Process finished
        Thread finished
        0
        Thread started
        Process started
        Terminating process
        Thread finished
        -15
    '''

    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        def target():
            LOG.info('Thread started')
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()
            LOG.info('Thread finished')

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            LOG.info('Terminating process')
            self.process.terminate()
            thread.join()
        LOG.info(self.process.returncode)


class TDXZXG(object):
    '''
    通达信自选股
    '''
    def __init__(self):
        super(TDXZXG, self).__init__()
        self.name = None
        # symbols is numpy array
        self.symbols = None

    def read(self, fileName):
        '''
        读取
        :param fileName:
        :return: 返回
        '''
        #  x = np.zeros(3, dtype={'col1':('i1',0,'title 1'), 'col2':('f4',1,'title 2')})
        with open(fileName) as f:
            lines = f.read().splitlines()
            q = []
            pattern = re.compile(r'0|1')
            for s in lines:
                match = pattern.match(s)
                if match is not None:
                    q.append((int(s[0]), s[1:]))
                else:
                    #todo 港股 美股。。。
                    pass
            if len(lines) > 0:
                dtype=([('market', 'i4'), ('symbol',  'U20')])
                self.symbols = np.asarray(q, dtype)

        filepath, self.name = os.path.split(fileName)
        self.name, filepath = self.name.split('.')
        return {'name': self.name,
                'symbols': self.symbols}
