# -*- coding: utf-8 -*-
"""
Created on Sat Nov  7 11:25:43 2015

@author: pchaos
"""

import os
import unittest

from ultrafinance.dam.TDXLib import TDXLib
from ultrafinance.lib.util import LOG


class testTDXLib(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testReadTDX(self):
        dataSourcePath = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                                      'dataSource')
        LOG.debug("dataSourcePath: {0}".format(dataSourcePath))
        with TDXLib(fileName=os.path.join(dataSourcePath, 'hoursing_interestRate.xls'),
                    mode=TDXLib.READ_MODE) as excel:
            print("sheet numbers: %s" % excel.getOperation().getTotalSheetNumber())
            print("sheetNames %s" % excel.getOperation().getSheetNames())
            excel.openSheet('Data')
            data = excel.readRow(0)
            print(data)
            self.assertNotEqual(0, len(data))

            data = excel.readCol(0, 7)
            print(data)
            self.assertNotEqual(0, len(data))

    def testWriteTDX(self):
        targetPath = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')
        targetFile = os.path.join(targetPath, "writeTest.xls")
        sheetName = "testSheet"

        if os.path.exists(targetFile):
            os.remove(targetFile)

        with TDXLib(fileName=targetFile,
                    mode=TDXLib.WRITE_MODE) as excel:
            excel.openSheet(sheetName)
            excel.writeRow(0, [1, 2, 3, "4", "5"])

        self.assertTrue(os.path.exists(targetFile))
