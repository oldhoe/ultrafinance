'''
Created on Dec 18, 2010

@author: ppa
'''
import sys
import time
from datetime import date
from datetime import datetime
from datetime import timedelta

from bs4 import BeautifulSoup
# from time import gmtime, strftime

import functools
import logging

logging.basicConfig(
        level=logging.DEBUG, format='%(asctime)s%(name)s[line:%(lineno)d] %(levelname)s %(message)s',
        datefmt='%a, %d %b %Y %H:%M:%S')

# logging.basicConfig(
#         level=logging.INFO, format='%(asctime)s%(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')

LOG = logging.getLogger(__name__)

googCSVDateformat = "%d-%b-%y"


def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        print('Call %s():' % func.__name__)
        return func(*args, **kw)

    return wrapper


def importClass(path, moduleName, className=None):
    ''' dynamically import class '''
    if not className:
        className = capitalize(moduleName)
    sys.path.append(path)

    mod = __import__(moduleName)
    return getattr(mod, className)


def capitalize(inputString):
    ''' capitalize first letter '''
    if not inputString:
        return inputString
    elif 1 == len(inputString):
        return inputString[0].upper()
    else:
        return inputString[0].upper() + inputString[1:]


def deCapitalize(inputString):
    ''' capitalize first letter '''
    if not inputString:
        return inputString
    elif 1 == len(inputString):
        return inputString[0].lower()
    else:
        return inputString[0].lower() + inputString[1:]


def splitByComma(inputString):
    ''' split string by comma '''
    return [name.strip() for name in inputString.split(',')]


def convertGoogCSVDate(googCSVDate):
    ''' convert date 25-Jul-2010 to 20100725'''
    # googCSVDateformat = "%d-%b-%y"
    # d = datetime.strptime(googCSVDate, googCSVDateformat)
    # d = str(d.date())
    # return d.replace("-", "")
    struct_time = time.strptime(googCSVDate, googCSVDateformat)
    return '{0}{1}{2}'.format(struct_time.tm_year, struct_time.tm_mon, struct_time.tm_mday)
    # return convert_date(googCSVDate, googCSVDateformat, '%Y%m%d')

def convert_date(string, in_format, out_format):
    '''
    Helper method to convert a string date in format dd MMM YYYY to YYYY-MM-DD
    year = convert_date('2011-01-01', '%Y-%m-%d', '%Y')
    '''

    if hasattr(datetime, 'strptime'):
        strptime = datetime.strptime
    else:
        strptime = lambda date_string, format: datetime(*(time.strptime(date_string, format)[0:6]))

    try:
        a = strptime(string, in_format).strftime(out_format)
    except Exception:
        print('Date conversion failed: %s' % Exception)
        return None
    return a

def findPatthen(page, pList):
    datas = [BeautifulSoup(page)]
    for key, pattern in pList:
        newDatas = findPattern(datas, key, pattern)

        datas = newDatas
        if not datas:
            break

    return datas


def findPattern(datas, key, pattern):
    newDatas = []
    for data in datas:
        if 'id' == key:
            newDatas.extend(data.findAll(id=pattern, recursive=True))
        if 'text' == key:
            newDatas.extend(data.findAll(text=pattern, recursive=True))

    return newDatas


def string2EpochTime(stingTime, format='%Y%m%d'):
    ''' convert string time to epoch time '''
    return int(time.mktime(datetime.strptime(stingTime, '%Y%m%d').timetuple()))


def string2datetime(stringTime, format='%Y%m%d'):
    ''' convert string time to epoch time'''
    return datetime.strptime(stringTime, '%Y%m%d')


def splitListEqually(inputList, chunks):
    return [inputList[i: i + chunks] for i in range(0, len(inputList), chunks)]


def splitDictEqually(inputDict, chunks):
    '''Splits dict by keys. Returns a list of dictionaries.
    from http://enginepewpew.blogspot.com/2012/03/splitting-dictionary-into-equal-chunks.html
    '''
    return_list = [dict() for idx in xrange(chunks)]
    idx = 0
    for k, v in inputDict.iteritems():
        return_list[idx][k] = v
        if idx < chunks - 1:  # indexes start at 0
            idx += 1
        else:
            idx = 0
    return return_list


def getDateString(numDaysBefore):
    ''' return string represent date of n days ago '''
    t = date.today() - timedelta(days=numDaysBefore)
    return t.strftime("%Y%m%d")
