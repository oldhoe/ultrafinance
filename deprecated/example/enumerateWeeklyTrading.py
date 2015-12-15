'''
Created on July 18, 2010

@author: ppa
'''
from os.path import join, dirname, abspath

from ultrafinance.lib.historicalDataStorage import HistoricalDataStorage
from ultrafinance.lib.tradingStrategy.tradeInWeek import TradeInWeek

if __name__ == '__main__':
    outputPrefix = join(dirname(dirname(abspath(__file__))), 'dataSource', 'SPY', 'spy')
    print
    outputPrefix
    storage = HistoricalDataStorage(outputPrefix)
    storage.buildExls(['SPY'], 1)

    tradeInWeek = TradeInWeek('%s0.xls' % outputPrefix, 0)
    for value in tradeInWeek.tryAllStrategy():
        print
        value
