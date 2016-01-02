ultrafinance
============
Python project for real-time financial data collection, analyzing && backtesting trading strategies.

============
todolist:
解压缩单独通达信日线 五日线文件，倒入mongoDB
数据地址： http://www.tdx.com.cn/list_66_69.html
import zipfile,os.path
with zipfile.ZipFile('shlday.zip') as zf:
    for member in zf.infolist():
        words = member.filename.split('/')
        print(words)
import os
import shutil
with zipfile.ZipFile('shlday.zip') as z:
    with z.open(icon[1]) as zf, open(os.path.join(tDir, os.path.basename(icon[1])), 'wb') as f:
        shutil.copyfileobj(zf, f)

计算股票ma并保存
自选股从通达信中读取，保存到mongoDB

============
Changelog
version 1.0.5
    修改Quote内部结构：time, open, high, low, close, adjClose类型为整型，修改对应的GoogleFinance。修复QuoteMongos.quoteUniqueByDelete中的bug
    修改MongoDAM.readQuotes返回数据逻辑
    修复TDXRead.read逻辑错误

version 1.0.4
    增加保存到mangoDB

version 1.0.3
    增加通达信类文件


version 1.0.2
2015-11-01
    ultrafinance/dam/googleFinance.py 支持python 3
    修复split 'str' does not support the buffer interface
    替换urllib为requests
    GoogleFinance中，访问google使用代理模式 http_proxy = "http://127.0.0.1:7777"
    文件googleCrawler.py中，多写了一个“/”，修改为self.sqlLocation = 'sqlite://%s' % self.__getOutputSql()
    测试/tests/unit/test_google_dam.py，test_google_finance.py


<!--
平时切换到dev分支开发
update git to master
-->
git checkout master
git merge dev
<!-- delete brach -->
git branch -d dev
git push
git checkout -b dev
git push --set-upstream origin dev
