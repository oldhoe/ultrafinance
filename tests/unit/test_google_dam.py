'''
Created on Nov 27, 2011

@author: ppa
'''
import os
import logging
import sys

LOG = logging.getLogger(__name__)

import unittest
from unittest import TestCase

try:
    import ultrafinance.dam.googleDAM
    from ultrafinance.lib.util import *
except:
    import ultrafinance


class testGoogleDam(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @log
    def testReadQuotes(self):
        dam = ultrafinance.dam.googleDAM.GoogleDAM()
        dam.setSymbol('NASDAQ:EBAY')
        data = dam.readQuotes('20131101', '20131110')
        print([str(q) for q in data])
        self.assertNotEqual(0, len(data))

    def testReadTicks(self):
        dam = ultrafinance.dam.googleDAM.GoogleDAM()
        dam.setSymbol('EBAY')
        data = dam.readTicks('20111120', '20111201')
        print(data)
        self.assertNotEqual(0, len(data))

    def testReadFundamental(self):
        dam = ultrafinance.dam.googleDAM.GoogleDAM()
        dam.setSymbol('EBAY')
        keyTimeValueDict = dam.readFundamental()
        print(keyTimeValueDict)
        self.assertNotEqual(0, len(keyTimeValueDict))
