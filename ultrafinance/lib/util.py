'''
Created on Dec 18, 2010

@author: ppa
'''
import sys
import time as tm
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
    # d = datetime.strptime(googCSVDate, googCSVDateformat)
    # d = str(d.date())
    # return d.replace("-", "")
    struct_time = tm.strptime(googCSVDate, googCSVDateformat)
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
        strptime = lambda date_string, format: datetime(*(tm.strptime(date_string, format)[0:6]))

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
    return int(tm.mktime(datetime.strptime(stingTime, '%Y%m%d').timetuple()))


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

def __request(url):
    try:
        # TODO, 访问google需要代理.代理加入到配置文件
        import socket
        import socks
        import requests
        socks.set_default_proxy(socks.SOCKS5, "localhost", 9150)
        socket.socket = socks.socksocket
        uagent = [
            'Mozilla/5.0 (iPad; CPU OS 7_0_4 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B554a Safari/9537.53',
            'Mozilla/5.0 (iPad; CPU OS 7_0_4 like Mac OS X) AppleWebKit/539.51.1 (KHTML, like Gecko) CriOS/34.0.1847.118 Mobile/11B554a Safari/9537.53',
            'Mozilla/5.0 (iPhone; CPU OS 7_0_5 like Mac OS X) AppleWebKit/557.51.1 (KHTML, like Gecko) CriOS/34.0.1847.18 Mobile/11B554a Safari/9537.53',
            'Mozilla/5.0 (Linux; U; Android 4.0.4; en-gb; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
            'Mozilla/5.0 (Android; Mobile; rv:29.0) Gecko/29.0 Firefox/29.0',
            'Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.114 Mobile Safari/537.36',
            "Mozilla/5.0 (iPhone; U; fr; CPU iPhone OS 4_2_1 like Mac OS X; fr) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148a Safari/6533.18.5",
            "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_1 like Mac OS X; zh-tw) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8G4 Safari/6533.18.5",
            "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3 like Mac OS X; pl-pl) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8F190 Safari/6533.18.5",
            "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3 like Mac OS X; fr-fr) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8F190 Safari/6533.18.5",
            "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3 like Mac OS X; en-gb) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8F190 Safari/6533.18.5",
            "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_2_1 like Mac OS X; nb-no) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148a Safari/6533.18.5",
            "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_2_1 like Mac OS X; it-it) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148a Safari/6533.18.5",
            "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_2_1 like Mac OS X; fr) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148a Safari/6533.18.5",
            "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_2_1 like Mac OS X; fi-fi) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148a Safari/6533.18.5",
            "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_2_1 like Mac OS X; fi-fi) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5"
        ]
        from random import choice
        user_agent = {'User-agent': choice(uagent)}
        headers = user_agent
        page = requests.get(url, headers=headers, timeout = 5)
        if page.status_code == 400:
            return ''
        return page.text
    except requests.exceptions.RequestException:
        return ''

def wxcmmo():
    import time
    j=i=0
    while i < 1000 :
        i +=1
        r = __request('http://wx.cmmo.cn/news/api.php?op=vote&id=23&modelid=1')
        r = __request('http://wx.cmmo.cn/news/api.php?op=vote&id=23&modelid=1')
        if len(r) > 100:
            j +=1
            print(i,j, (i-j)*10000/i//1/100, end=" ")
            tm.sleep(0.05)
        else:
            print(0,end=" ")
            tm.sleep(0.5)