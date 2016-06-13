'''
Created on Wed Dec  9 18:12:34 2015

@author: pchaosgit
'''
import logging
import sys

import pymongo
from pymongo import MongoClient
from pymongo import IndexModel
from ultrafinance.dam.sqlDAM import TickSql
from ultrafinance.dam.baseDAM import BaseDAM
from ultrafinance.dam.mongoLib import QuoteMongos
from ultrafinance.lib.util import splitListEqually
from ultrafinance.model import Quote, Tick, TupleQuote
import json

LOG = logging.getLogger()


class MongoDAM(BaseDAM):
    '''
    Mongo DAM
    '''

    def __init__(self):
        ''' constructor '''
        super(MongoDAM, self).__init__()
        # self.__mg = QuoteMongo()
        self.first = True
        self.client = None
        self.db = None

    def setup(self, setting='', dbname='stockdb'):
        ''' set up
        setting = 'mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]'
        '''
        if 'mongodb' not in setting:
            if len(setting) < len('db'):
                self.client = MongoClient()
            else:
                raise Exception("db not specified in setting")
        else:
            self.client = MongoClient(setting)
        if dbname is not None:
            self.db = self.client[dbname]

    def __sqlToQuote(self, row):
        ''' convert row result to Quote '''
        return Quote(row.time, row.read, row.high, row.low, row.close, row.volume, row.adjClose)

    def __sqlToTupleQuote(self, row):
        ''' convert row result to tuple Quote '''
        # return TupleQuote(row.time, row.read, row.high, row.low, row.close, row.volume, row.adjClose)
        # TODO -- remove type conversion, crawler should get the right type
        return TupleQuote(row.time, row.close, int(row.volume), row.low, row.high)

    def __sqlToTick(self, row):
        ''' convert row result to Tick '''
        return Tick(row.time, row.read, row.high, row.low, row.close, row.volume)

    def __sqlToTupleTick(self, row):
        ''' convert row result to tuple Tick '''
        return Tick(row.time, row.read, row.high, row.low, row.close, row.volume)

    def __tickToSql(self, tick):
        ''' convert tick to TickSql '''
        return TickSql(self.symbol, tick.time, tick.read, tick.high, tick.low, tick.close, tick.volume)

    def __quoteToMongo(self, quote):
        ''' convert tick to QuoteSql '''
        return QuoteMongo(self.symbol, quote.time, quote.read, quote.high, quote.low, quote.close, quote.volume,
                          quote.adjClose)

    def __quoteToMongos(self, quote):
        ''' convert tick to QuoteSql '''
        return QuoteMongo(self.symbol, quote.time, quote.read, quote.high, quote.low, quote.close, quote.volume,
                          quote.adjClose)

    def readQuotes(self, start = None, end = None):
        '''
        read symbols
        :param start: 开始日期，例如： 20151201
        :param end: 截至日期，例如： 20151231
        :return: 返回股票价格列表
            # sample values:[['Date', 'Open', 'High', 'Low', 'Close', 'Volume'], \
            #              ['20091231', '11277', '11280', '11139', '11144', '90637900']...]
        '''
        if self.symbol is None:
            LOG.debug('Symbol is None')
            return []
        end, start = self.__chkTime(end, start)
        mg = self.__findBySymbol()
        if mg is None:
            return None
        else:
            return self.__listToQuotes(mg['quotes'], start, end)

    def __chkTime(self, end, start):
        if type(start) == str:
            start = int(start)
        if type(end) == str:
            end = int(end)
        if start is None:
            start = 0
        if end is None:
            end = sys.maxsize
        return end, start

    def __findBySymbol(self):
        '''
        查找证券代码对应的数据
        :return:返回值为找到的dict对象， 返回None未找到
        '''
        collection = self.getTable(None)
        query = {'symbol': self.symbol}
        mg = collection.find_one(query)
        return mg

    def __listToQuotes(self, quoteList, start = None, end = None):
        '''
        将数据列表转换为Quote列表
        :param quoteList: 数据列表
        :return: 转换成Quote列表
           [time, read, high, low, close, volume, adjClose]\
        '''
        end, start = self.__chkTime(end, start)
        quotes = []
        for q in quoteList:
            if start <= q['time'] <= end:
                quotes.append(Quote(q['time'], q['open'], q['high'], q['low'], q['close'], q['volume'], q['adjClose']))
        sortQuotes = sorted(quotes, key=lambda x: x.time)
        return sortQuotes

    def readTupleQuotes(self, start, end):
        ''' read symbols as tuple '''
        if end is None:
            end = sys.maxint

        session = self.getReadSession()()
        try:
            rows = session.query(QuoteSql).filter(and_(QuoteSql.symbol == self.symbol,
                                                       QuoteSql.time >= int(start),
                                                       QuoteSql.time < int(end)))
        finally:
            self.getReadSession().remove()

        return [self.__sqlToTupleQuote(row) for row in rows]

    def readBatchTupleQuotes(self, symbols, start, end):
        '''
        read batch symbols as tuple to save memory
        '''
        if end is None:
            end = sys.maxint

        ret = {}
        session = self.getReadSession()()
        try:
            symbolChunks = splitListEqually(symbols, 100)
            for chunk in symbolChunks:
                rows = session.query(QuoteSql.symbol, QuoteSql.time, QuoteSql.close, QuoteSql.volume,
                                     QuoteSql.low, QuoteSql.high).filter(and_(QuoteSql.symbol.in_(chunk),
                                                                              QuoteSql.time >= int(start),
                                                                              QuoteSql.time < int(end)))

                for row in rows:
                    if row.time not in ret:
                        ret[row.time] = {}

                    ret[row.time][row.symbol] = self.__sqlToTupleQuote(row)
        finally:
            self.getReadSession().remove()

        return ret

    def readTupleTicks(self, start, end):
        ''' read ticks as tuple '''
        if end is None:
            end = sys.maxint

        session = self.getReadSession()()
        try:
            rows = session.query(TickSql).filter(and_(TickSql.symbol == self.symbol,
                                                      TickSql.time >= int(start),
                                                      TickSql.time < int(end)))
        finally:
            self.getReadSession().remove()

        return [self.__sqlToTupleTick(row) for row in rows]

    def readTicks(self, start, end):
        ''' read ticks '''
        if end is None:
            end = sys.maxint

        session = self.getReadSession()()
        try:
            rows = session.query(TickSql).filter(and_(TickSql.symbol == self.symbol,
                                                      TickSql.time >= int(start),
                                                      TickSql.time < int(end)))
        finally:
            self.getReadSession().remove()

        return [self.__sqlToTick(row) for row in rows]

    def writeQuotes(self, quotes):
        '''
        write symbols
        '''
        # self.saveQuotesToMongo([self.__quoteToMongo(quote) for quote in symbols])
        collection = self.getTable(quotes[0])
        qm = QuoteMongos(self.symbol)
        qm.extend(quotes)
        self.saveQuotesToMongo(collection, qm)

    def writeTicks(self, ticks):
        ''' write ticks '''
        if self.first:
            Base.metadata.create_all(self.client, checkfirst=True)
            self.first = False

        session = self.getWriteSession()
        session.add_all([self.__tickToSql(tick) for tick in ticks])

    def destruct(self):
        ''' destructor '''
        self.db = None
        self.client = None

    '''
    read/write fundamentals
    TODO: when doing fundamentals and quote/tick operation together,
    things may mess up
    '''

    def writeFundamental(self, keyTimeValueDict):
        ''' write fundamental '''
        if self.first:
            Base.metadata.create_all(self.__getEngine(), checkfirst=True)
            self.first = False

        sqls = self._fundamentalToSqls(keyTimeValueDict)
        session = self.Session()
        try:
            session.add_all(sqls)
        finally:
            self.Session.remove()

    def readFundamental(self):
        ''' read fundamental '''
        rows = self.__getSession().query(FmSql).filter(and_(FmSql.symbol == self.symbol))
        return self._sqlToFundamental(rows)

    def _sqlToFundamental(self, rows):
        keyTimeValueDict = {}
        for row in rows:
            if row.field not in keyTimeValueDict:
                keyTimeValueDict[row.field] = {}

            keyTimeValueDict[row.field][row.timeStamp] = row.value

        return keyTimeValueDict

    def _fundamentalToSqls(self, keyTimeValueDict):
        ''' convert fundament dict to sqls '''
        sqls = []
        for key, timeValues in keyTimeValueDict.iteritems():
            for timeStamp, value in timeValues.iteritems():
                sqls.append(FmSql(self.symbol, key, timeStamp, value))

        return sqls

    def _checkConnection(self):
        '''
        判断连接是否有效
        '''
        try:
            info = self.client.server_info()
            if info['ok'] > 0:
                pass
        except:
            raise Exception('mongoDB not started!\nplease Start monogoDB.')

    def getTable(self, quote):
        self._checkConnection()
        # db = self.client[self._dbName]
        qm = QuoteMongos(self.symbol)
        table = self.db[qm.getTableName()]
        return table

    def saveQuotesToMongo(self, collection, quoteMongos, CRUDmode='insert'):
        '''
        保存dataFrame到mongoDB
        CRUDmode='insert': 插入
        CRUDmode='update': 更新
        CRUDmode='delete': 删除
        '''
        result = None

        if CRUDmode.lower() == 'insert':
            mg = self.__findBySymbol()
            if mg is None:
                # 插入操作
                qmj = self._to_json(quoteMongos)
                result = collection.insert_one(qmj)
                LOG.info('Write {1} find_and_modify:{0}'.format(
                        result.inserted_id, collection.name))
                self.createIndex(collection)
            else:
                result = self._updateQuoteMongoCollection(collection, mg, quoteMongos, result)
        elif CRUDmode.lower() == 'update':
            mg = self.__findBySymbol()
            result = self._updateQuoteMongoCollection(collection, mg, quoteMongos, result)

        elif CRUDmode.lower() == 'delete':
            # 删除操作
            result = collection.find_one_and_delete({'symbol': quoteMongos.symbol})
            LOG.info('Write {1} find_and_modify:{0}'.format(
                    result, collection.name))
        else:
            # todo raise exception
            result = None
        return result

    def _updateQuoteMongoCollection(self, collection, mg, quoteMongos, result):
        ql = self.__listToQuotes(mg['quotes'])
        quoteMongos.append(ql)
        qmj = self._to_json(quoteMongos)
        result = self.updateMongoTable(collection, qmj)
        return result

    def _to_json(self, quoteMongos):
        qmjs = json.dumps(quoteMongos, default=lambda o: o.__dict__)
        return json.loads(qmjs)

    def updateMongoTable(self, collection, quoteMongosJson):
        '''
        更新操作
        默认更新关键字为股票代码（symbol）
        :param quoteMongosJson: DataFrame转换成json值
        :param collection:
        :return:
        '''
        if len(quoteMongosJson) == 2:
            LOG.info('Write {1} find_and_modify:{0}'.format(
                    quoteMongosJson, self.db.name))
            query = {'symbol': quoteMongosJson['symbol']}
            result = collection.find_one_and_replace(query, quoteMongosJson)
        return result

    def createIndex(self, collection):
        if collection != None:
            try:
                index1 = IndexModel([('symbol', pymongo.ASCENDING)], unique=True)
                index2 = IndexModel([('time', pymongo.ASCENDING)])
                collection.create_indexes([index1, index2])
            except:
                pass

    def dropCollection(self, collectionName):
        '''
        删除collection
        :param collectionName: 需要删除collection的名称
        :return:
        '''
        collection = self.db[collectionName]
        collection.drop()

    def deleteQuotes(self, quotes):
        oldQuotes = self.readQuotes()

        collection = self.getTable(quotes[0])
        qm = QuoteMongos(self.symbol)
        qm.extend(oldQuotes)
        self.saveQuotesToMongo(collection, qm, 'delete')
        qm.remove(quotes)

        self.saveQuotesToMongo(collection, qm)

def get_hist_data(code=None, start=None, end=None,
                  ktype='D', retry_count=3,
                  pause=0.001):
    """
        获取个股历史交易记录
    Parameters
    ------
      code:string
                  股票代码 e.g. 600848
      start:string
                  开始日期 format：YYYY-MM-DD 为空时取到API所提供的最早日期数据
      end:string
                  结束日期 format：YYYY-MM-DD 为空时取到最近一个交易日数据
      ktype：string
                  数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
      retry_count : int, 默认 3
                 如遇网络等问题重复执行的次数
      pause : int, 默认 0
                重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
    return
    -------
      DataFrame
          属性:日期 ，开盘价， 最高价， 收盘价， 最低价， 成交量， 价格变动 ，涨跌幅，5日均价，10日均价，20日均价，5日均量，10日均量，20日均量，换手率
    """
    symbol = _code_to_symbol(code)
    url = ''
    if ktype.upper() in ct.K_LABELS:
        url = ct.DAY_PRICE_URL%(ct.P_TYPE['http'], ct.DOMAINS['ifeng'],
                                ct.K_TYPE[ktype.upper()], symbol)
    elif ktype in ct.K_MIN_LABELS:
        url = ct.DAY_PRICE_MIN_URL%(ct.P_TYPE['http'], ct.DOMAINS['ifeng'],
                                    symbol, ktype)
    else:
        raise TypeError('ktype input error.')

    for _ in range(retry_count):
        time.sleep(pause)
        try:
            request = Request(url)
            lines = urlopen(request, timeout = 10).read()
            if len(lines) < 15: #no data
                return None
        except Exception as e:
            print(e)
        else:
            js = json.loads(lines.decode('utf-8') if ct.PY3 else lines)
            cols = []
            if (code in ct.INDEX_LABELS) & (ktype.upper() in ct.K_LABELS):
                cols = ct.INX_DAY_PRICE_COLUMNS
            else:
                cols = ct.DAY_PRICE_COLUMNS
            if len(js['record'][0]) == 14:
                cols = ct.INX_DAY_PRICE_COLUMNS
            df = pd.DataFrame(js['record'], columns=cols)
            if ktype.upper() in ['D', 'W', 'M']:
                df = df.applymap(lambda x: x.replace(u',', u''))
                df[df==''] = 0
            for col in cols[1:]:
                df[col] = df[col].astype(float)
            if start is not None:
                df = df[df.date >= start]
            if end is not None:
                df = df[df.date <= end]
            if (code in ct.INDEX_LABELS) & (ktype in ct.K_MIN_LABELS):
                df = df.drop('turnover', axis=1)
            df = df.set_index('date')
            df = df.sort_index(ascending = False)
            return df
    raise IOError(ct.NETWORK_URL_ERROR_MSG)