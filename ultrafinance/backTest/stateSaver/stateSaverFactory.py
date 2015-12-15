'''
Created on Nov 6, 2011

@author: ppa
'''
import logging

from ultrafinance.designPattern.singleton import Singleton
from ultrafinance.lib.errors import Errors, UfException

LOG = logging.getLogger()


class StateSaverFactory(Singleton):
    ''' factory for output saver '''

    @staticmethod
    def createStateSaver(name, setting):
        ''' create state saver '''
        if 'sql' == name:
            from ultrafinance.backTest.stateSaver.sqlSaver import SqlSaver
            saver = SqlSaver()
        else:
            raise UfException(Errors.INVALID_SAVER_NAME,
                              "Saver name is invalid %s" % name)

        saver.setup(setting)
        return saver
