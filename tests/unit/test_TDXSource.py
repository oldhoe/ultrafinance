# -*- coding: utf-8 -*-
"""
2016 01 04  10:48:24 CST

@author: pchaos
"""
import os
import unittest

from unittest import TestCase
from ultrafinance.dam.tdxLib import TDXSource
import numpy as np

import logging

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class TestTDXSource(TestCase):
    def setUp(self):
        self.tdxSource = TDXSource.getInstance()
        self.downloadName = ''

    def tearDown(self):
        from ultrafinance.designPattern.singleton import forgetAllSingletons
        # 释放所有的singletons
        forgetAllSingletons()

        self.tdxSource = None

    def test_getUrl(self):
        self.downloadName = 'sz5fz'
        url = self._getUrl(self.downloadName)
        self.assertTrue(len(url) > 0, "Can't getUrl")
        self.downloadName = None
        url = self._getUrl(self.downloadName)
        self.assertTrue(url is None, '当downloadName=None,不可能取得url值')

    def _getUrl(self, downloadName):
        url = self.tdxSource.getUrl(downloadName)
        LOG.debug('getUrl: {0} {1}'.format(downloadName, url))
        return url

    def test_download(self):
        self.downloadName = 'szlday'
        self.downloadName = 'sh5fz'
        result, fileName = self.tdxSource.download(self.downloadName)
        self.assertTrue(result and os.path.exists(fileName), '当downloadName={0},不可能取得url值'.format(self.downloadName))
        self.downloadName = ''
        result, fileName = self.tdxSource.download(self.downloadName)
        self.assertTrue(result == False, '当downloadName={0},不可能取得url值'.format(self.downloadName))

    def test_getDownloadnameBySymbol(self):
        symbol = 'sh000001'
        ktype = 'd'
        self.downloadName = self.tdxSource.getDownloadnameBySymbol(symbol, ktype)
        self.assertTrue(self.downloadName == 'shlday', '返回下载名称不对！')
        symbol = 'SZ000001'
        ktype = 'w'
        self.downloadName = self.tdxSource.getDownloadnameBySymbol(symbol, ktype)
        self.assertTrue(self.downloadName == 'szlday', '返回下载名称不对！')
        symbol = 'sh000001'
        ktype = '30'
        self.downloadName = self.tdxSource.getDownloadnameBySymbol(symbol, ktype)
        self.assertTrue(self.downloadName == 'sh5fz', '返回下载名称不对！')
        symbol = ''
        ktype = '30'
        self.downloadName = self.tdxSource.getDownloadnameBySymbol(symbol, ktype)
        self.assertTrue(len(self.downloadName) != len('shlday'), '返回下载名称不对！ {0} != {1}'.format(self.downloadName, ''))

    def test_getTargetFileName(self):
        symbol = 'sh600001'
        ktype = 'd'
        filename = self.tdxSource.getTargetFileName(symbol, ktype)
        predict = '/tmp/sh/lday/{0}.day'.format(symbol)
        self.assertEqual(filename, predict, '目标文件名不匹配：{0} {1}'.format(filename, predict))
        symbol = 'sz000001'
        ktype = 'd'
        filename = self.tdxSource.getTargetFileName(symbol, ktype)
        predict = '/tmp/sz/lday/{0}.day'.format(symbol)
        self.assertEqual(filename, predict, '目标文件名不匹配：{0} {1}'.format(filename, predict))
        symbol = 'sh000001'
        ktype = '5'
        filename = self.tdxSource.getTargetFileName(symbol, ktype)
        predict = '/tmp/sh/fzline/{0}.lc5'.format(symbol)
        self.assertEqual(filename, predict, '目标文件名不匹配：{0} {1}'.format(filename, predict))

    def test_extract(self):
        symbol = 'sh600001'
        ktype = 'd'
        filename = self.tdxSource.extract(symbol, ktype)
        self.assertTrue(filename is not None, '未取得文件')
        predict = '/tmp/sh/lday/{0}.day'.format(symbol)
        self.assertEqual(filename, predict, '目标文件名不匹配：{0} {1}'.format(filename, predict))
        symbol = 'sh600401'
        ktype = 'd'
        filename = self.tdxSource.extract(symbol, ktype)
        self.assertTrue(filename is not None, '未取得文件')
        predict = '/tmp/sh/lday/{0}.day'.format(symbol)
        self.assertEqual(filename, predict, '目标文件名不匹配：{0} {1}'.format(filename, predict))
        symbol = 'sz000998'
        ktype = 'd'
        filename = self.tdxSource.extract(symbol, ktype)
        self.assertTrue(filename is not None, '未取得文件')
        predict = '/tmp/sz/lday/{0}.day'.format(symbol)
        self.assertEqual(filename, predict, '目标文件名不匹配：{0} {1}'.format(filename, predict))

    def test_deleteFileOfACertainAge(self):
        symbol = 'sh600401'
        ktype = 'd'
        filename = self.tdxSource.extract(symbol, ktype)
        # 延迟秒数
        delaySeconds = 4
        self.tdxSource.delays(delaySeconds)
        symbol = 'sh600705'
        ktype = 'd'
        filename = self.tdxSource.extract(symbol, ktype)
        filePath, f = os.path.split(filename)
        dirlist = len(os.listdir(filePath))
        self.tdxSource.deleteFileOfACertainAge(filePath, delaySeconds)
        dirlist1 = len(os.listdir(filePath))
        self.assertTrue(dirlist1 < dirlist, '{0}文件没删除！'.format(filePath))

    def test_getSymbolList(self):
        symbolDict = self.tdxSource.getSZStockSymbols()
        LOG.debug("sz symbol:{0}".format(symbolDict))
        sizeOfList = len(symbolDict)
        self.assertTrue(sizeOfList > 3000, '证券代码数量不足:{0}'.format(sizeOfList))
        fileName = './data/szSymbols.npy'
        np.save(fileName, symbolDict)
        read_dictionary = np.load(fileName).item()
        self.assertEquals(symbolDict, read_dictionary, '读取和保存不匹配')
        symbolDict = self.tdxSource.getSHStockSymbols()
        LOG.debug("sh symbol:{0}".format(symbolDict))
        sizeOfList = len(symbolDict)
        self.assertTrue(sizeOfList > 3000, '证券代码数量不足:{0}'.format(sizeOfList))
        np.save(fileName, symbolDict)
        read_dictionary = np.load(fileName).item()
        self.assertEquals(symbolDict, read_dictionary, '读取和保存不匹配')

if __name__ == "__main__":
    unittest.main()
    # '''
