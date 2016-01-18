# -*- coding: utf-8 -*-
"""
2016 01 04  23：40：07 CST

@author: pchaosgit
"""
import os
import tempfile
from unittest import TestCase
from ultrafinance.dam.tdxLib import TDXLib, TDXOpertion, TDXRead, TDXZXG


class TestTDXLib(TestCase):
    def setUp(self):
        self.tdxLib = None
        self.downloadName = ''
        self.symbol = '600177'

    def tearDown(self):
        self.tdxLib = None

    def test_getOperation(self):
        tdxLib = TDXLib('./data', self.symbol)
        self.assertTrue(type(tdxLib.getOperation()) == TDXRead, '类型不匹配')
        tdxLib = TDXLib(tempfile.gettempdir(), self.symbol)
        # 文件名
        filename = os.path.join(os.path.join(os.path.join(tempfile.gettempdir(), 'sh'), 'lday'),
                                'sh{0}.day'.format(self.symbol))
        self.assertTrue(type(tdxLib.getOperation()) == TDXRead, '类型不匹配')
        self.assertTrue(os.path.exists(filename), '文件未找到！ {0}'.format(filename))

    def test_TDXZXGread(self):
        name = 'ZXG'
        filename = './data/{0}.blk'.format(name)
        tdxZXG = TDXZXG()
        zxg = tdxZXG.read(filename)
        self.assertTrue(len(zxg) > 0, '没有读取出自选股')
        self.assertEquals(zxg['name'], name, '读取文件名称不匹配：{0} {1}'.format(zxg['name'], name))
