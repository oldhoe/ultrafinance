# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 23:42:53 2015

@author: pchaos
"""
import logging
import unittest
from unittest import TestCase

import dam
from ultrafinance.dam.TDXDAM import TDXDAM

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class TestTDXDAM(TestCase):
    def setUp(self):
        self.dam = TDXDAM()
        self.testPath = './data'

    def tearDown(self):
        self.dam = None
        self.testPath = None

    def test_targetPath(self):
        self.test_setDir()
        self.dam.setSymbol('600001')
        dir = self.dam.targetPath(None)
        LOG.info('targetPath:{0}'.format(dir))
        self.assertNotEqual(0, len(dir))

    def test_readQuotes(self):
        dam = TDXDAM()
        dam.setDir('./data')
        dam.setSymbol('600177')
        data = dam.readQuotes('20131101', '20131110')
        LOG.debug([str(q) for q in data])
        self.assertNotEqual(0, len(data))

    def test_writeQuotes(self):
        self.fail()

    def test_readTicks(self):
        self.fail()

    def test_writeTicks(self):
        self.fail()

    def test_setDir(self):
        self.dam.setDir(self.testPath)
        self.assertTrue(1 == 1)


if __name__ == "__main__":
    unittest.main()
