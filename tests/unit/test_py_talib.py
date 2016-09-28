'''
Created on Dec 18, 2011

@author: ppa
'''
import unittest

from ultrafinance.pyTaLib.indicator import Sma


class testPyTaLib(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testSma(self):
        #周期1
        sma = Sma(period=1)
        expectedAvgs = [1.0, 2, 3, 4, 5]
        for index, number in enumerate(range(0, 4)):
            nm= number+1
            self.assertEqual(expectedAvgs[index], sma(nm), '{0} Equal {1}'.format(expectedAvgs[index], nm))
        # 周期3
        sma = Sma(period=3)
        expectedAvgs = [1, 1.5, 2.5, 3.5, 4.5]
        for index, number in enumerate(range(1, 6)):
            self.assertNotEqual(expectedAvgs[index], sma(number), '{0} Equal {1}'.format(expectedAvgs[index], number))
        sma = Sma(period=3)
        expectedAvgs = [None, None, 2, 3, 4, 5, 6]
        for index, number in enumerate(range(1, 7)):
            self.assertEqual(expectedAvgs[index], sma(number), '{0} not Equal {1}'.format(expectedAvgs[index], number))
        # 周期4
        sma = Sma(period=4)
        expectedAvgs = [None, None, None, 2.5, 3.5, 4.5]
        for index, number in enumerate(range(1, 6)):
            self.assertEqual(expectedAvgs[index], sma(number), '{0} not Equal {1}'.format(expectedAvgs[index], number))

