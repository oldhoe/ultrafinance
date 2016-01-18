# -*- coding: utf-8 -*-
"""
2016 01 08  12:21:51 CST

@author: pchaos
"""
import re
from enum import Enum, unique
from ultrafinance.designPattern.singleton import Singleton , SingletonException

@unique
class Market(Enum):
    '''
    -1： 未知
    0：深证
    1：上证
    2：港股
    3:美股
    '''
    Unknow = -1 # unknow的value被设定为-1
    SZ = 0
    SH = 1
    HK = 71
    USA = 74
    Thu = 4
    Fri = 5
    Sat = 6

class ListedCompany(object):
    '''
    证券公司
    所属市场代码见：class Market
    '''
    def __init__(self, symbol = None, market = None ):
        self.symbol = symbol
        self.market = market
        self.name = None
        self.remark = None



class SymbolLib(Singleton):
    '''
股票编码规则

由于我国证券市场的不断繁荣壮大，目前在沪、深两地证券交易所挂牌交易的品种愈来愈多，加之现在证券交易的无纸化和电子化的普及，交易品种都有其挂牌代码来进行处理、交易的，所以从其代码就可分辨其交易类别及种类。在上海证券交易所挂牌的交易品种皆由6位数字来进行编码的，而深圳皆由4位数字来进行编码的。其编码范围如下：

沪市A股票买卖的代码是以600或601打头，如：运盛实业，股票代码是600767，中国国航(7.72,0.32,4.32%)是601111。

B股买卖的代码是以900打头，如：上电B股(0.448,0.00,0.90%)，代码是900901。

深市A股票买卖的代码是以000打头，如：顺鑫农业(10.66,-0.05,-0.47%)，股票代码是000860。

B股买卖的代码是以200打头，如：深中冠B(4.04,-0.03,-0.74%)股，代码是200018。

沪市新股申购的代码是以730打头。如：中信证券(40.89,-0.88,-2.11%)申购的代码是730030。

深市新股申购的代码与深市股票买卖代码一样，如：中信证券在深市市值配售代码是003030。

配股代码，沪市以700打头，深市以080打头。如：运盛实业配股代码是700767。深市草原兴发配股代码是080780。

中小板股票代码以002打头，如：东华合创(33.06,0.27,0.82%)代码是002065。

股票代码除了区分各种股票，也有其潜在的意义，比如600***是大盘股，6006**是最早上市的股票，有时候，一个公司的股票代码跟车牌号差不多，能够显示出这个公司的实力以及知名度，比如000088盐田港，000888峨眉山。
　　在上海证券交易所上市的证券，根据上交所"证券编码实施方案"，采用6Market.Unknow位数编制方法，前3位数为区别证券品种，具体见下表所列：
　　001×××国债现货；110×××120×××企业债券；129×××100×××可转换债券；
　　201×××国债回购；
　　310×××国债期货；
　　500×××550×××基金；
　　600×××A股；
　　700×××配股；710×××转配股；
　　701×××转配股再配股；711×××转配股再转配股；
　　720×××红利；730×××新股申购；735×××新基金申购；737×××新股配售。
　　900×××B股；
　　以前深交所的证券代码是四位，前不久已经升为六位具体变化如下：
　　深圳证券市场的证券代码由原来的4位长度统一升为6位长度。
　　1、新证券代码编码规则
　　升位后的证券代码采用6位数字编码，编码规则定义如下：
　　顺序编码区：6位代码中的第3位到第6位，取值范围为0001-9999。
　　证券种类标识区：6位代码中的最左两位，其中第1位标识证券大类，第2位标识该大类下的衍生证券。
　　第1位 第2位第3-6位定义
　　00 xxxxA股证券
　　3 xxxxA股A2权证
　　7 xxxxA股增发
　　8 xxxxA股A1权证
　　9 xxxxA股转配
　　10 xxxx国债现货
　　1 xxxx债券
　　2 xxxx可转换债券
　　3 xxxx 国债回购
　　17 xxxx 原有投资基金
　　8 xxxx 证券投资基金
　　20 xxxx B股证券
　　7 xxxx B股增发
　　8 xxxx B股权证
　　30 xxxx 创业板证券
　　7 xxxx 创业板增发
　　8 xxxx 创业板权证
　　39 xxxx 综合指数/成份指数
　　2、新旧证券代码转换
　　此次A股证券代码升位方法为原代码前加“00”，但有两个A股股票升位方法特殊，分别是“0696 ST联益”和“0896 豫能控股”，升位后股票代码分别为“001696”和“001896”。
    '''
    market = None
    symbol = None
    def getSymbol(self, symbol, market = Market.Unknow):
        '''

        :param symbol: 股票代码
        :param market: 市场号码
          -1： 未知
          0：深证
          1：上证
          2：港股

        :return:无
        '''
        symbol = symbol.strip()
        if market == Market.Unknow:
            if len(symbol) == 6 and symbol.isnuber:
                #china Stock code

                pass
            elif len(symbol) == 7 and symbol.isnuber:
                # 通达信自选股格式，第一位为市场编码，后面为股票代码
                lc = ListedCompany(symbol[1:6], Market(symbol[0]))
                return lc

    def checkSHSymbol(self, symbol):
        '''
        判断是否符合上海股市代码特征
        :param symbol:
        :return: 符合上海股市代码特征返回 True, 否则 False
        '''
        patternText = ['600', '601', '603', '500', '502', '510', '512', '518', '900', '999', '720', '730', '550', '201', '204']
        return self.__checkSymbol(symbol, patternText)

    def checkSZSymbol(self, symbol):
        '''
        判断是否符合深圳股市代码特征
        :param symbol:
        :return: 符合深圳股市代码特征返回 True, 否则 False
        '''
        patternText = ['00', '30', '39', '20', '17', '10', '13', '15', '7', '8', '1']
        return self.__checkSymbol(symbol, patternText)

    def __checkSymbol(self, symbol, patternText):
        for pt in patternText:
            match = self.__isMatch(symbol, pt) is not None
            if match:
                return match
        return False

    def __isMatch(self, symbol, patternText):
        pattern = re.compile(r'^{0}'.format(patternText))
        match = pattern.match(symbol)
        return match

