ultrafinance
============
Python project for real-time financial data collection, analyzing && backtesting trading strategies.

============
todolist:
从通达信文件中获取数据
增加保存到mangoDB

============
Changelog

version 1.0.2
2015-11-01 ultrafinance/dam/googleFinance.py 支持python 3
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
