# -*- coding: utf-8 -*-
import httplib
import traceback
import urllib
from ConfigParser import *
from pymongo import *
import pandas as pd
import random
from time import sleep
from base_collector import *
import MySQLdb
import json
import itertools
from WindPy import w as WindClient

# block name in configure file
MYSQL_SETTING = 'mysql'
MONGO_SETTING = 'mongo'
SECIDS_SETTING = 'secIDs'
UPDATE_SETTING = 'update'

# name to replace in configure file
REPL_STOCK_ID = '%{STOCK_ID}'
REPL_BEGIN_DATE = '%{BEGIN_DATE}'
REPL_END_DATE = '%{END_DATE}'

# wind api exception
class WindApiError(CollectorException):
    def __init__(self, message=None):
        self.message = message
        pass
    def __str__(self):
        output = "Wind api error"
        if self.message:
            output = output + "[%s]"%(self.message)
        return output

#JY data collector
class WindCollector(BaseDayCollector):
    """ 聚源数据 Collector """
    def __init__(self, configFile):
        BaseDayCollector.__init__( self, 'WindCollector')
        # instance variable that read from config file
        self.secIDsFun = None
        self.tableList = None
        self.mongoSetting = None
        self.mysqlSetting = None

        # instance variable used by JYCollector
        self.windClient = None
        self.mongoClient = None
        self.mysqlClient = None

        # read configure
        self.configFile = configFile
        self.config = ConfigParser()
        self.config.optionxform = str
        self.config.read(self.configFile)
        self.readConfig()

        # initialize wind client connection and database connnection
        self.windClient = WindClient
        self.windClient.start()
        self.mongoClient = MongoClient(**self.mongoSetting)
        self.mysqlClient  = MySQLdb.connect(**self.mysqlSetting)

    def __del__( self ):
        if self.windClient is not None:
            self.windClient.close()
            self.windClient = None

    def readConfig(self):
        self.secIDsFun = self.config.get(SECIDS_SETTING, 'fun')
        tableString = self.config.get(UPDATE_SETTING, 'table')
        self.tableList = [x.strip() for x in tableString.split(',')]

        # mysql setting
        self.mysqlSetting = dict(self.config.items(MYSQL_SETTING))
        if self.mysqlSetting.has_key('port'):
            self.mysqlSetting['port'] = int(self.mysqlSetting['port'])

        # mongo setting
        mongoSetting = dict(self.config.items(MONGO_SETTING))
        for key in mongoSetting.keys():
            if mongoSetting[key].isdigit():
                mongoSetting[key] = int(mongoSetting[key])
        self.mongoSetting = mongoSetting



    def reconnect(self):
        """reconnect"""
        if self.windClient is not None:
            self.windClient.close()
            self.windClient = None
        self.windClient = WindClient

    def updateData(self, beginDate, endDate, updateList = None):
        """update wind data"""
        failList = []
        if updateList is None:
            secIDs = self.__GetSecIDs()
            updateList = itertools.product(self.tableList, secIDs, [beginDate],[endDate],['dummy'])
        for tablename, secID, _beginDate, _endDate, _ in updateList:
            try:
                self.__updateTable(secID, tablename, _beginDate, _endDate)
            except Exception, e:
                errorMessage = str(e)
                failList.append([tablename, secID, _beginDate, _endDate, errorMessage])
                print 'update %s in table %s fail: %s' %(secID, tablename, errorMessage)
        return failList

    def __updateTable(self, secID, tablename, beginDate, endDate):
        """update table"""
        fun = self.config.get(tablename, 'fun')
        db = self.config.get(tablename, 'db')
        table = self.config.get(tablename, 'table')
        saveToDB = int(self.config.get(tablename, 'saveToDB'))
        fun = fun.replace(REPL_STOCK_ID, secID)
        fun = fun.replace(REPL_BEGIN_DATE, beginDate)
        fun = fun.replace(REPL_END_DATE, endDate)
        primaryKey = None
        try:
            primaryKey = self.config.get(tablename,'primaryKey')
            primaryKey = [x.strip() for x in primaryKey.split(',')]
        except:
            pass

        fun = "result = self.windClient." + fun
        exec(fun)
        if result.ErrorCode == 0:
            data = pd.DataFrame(result.Data).T.dropna(how='all')
            if data.empty:
                return
            data.columns = result.Fields
            #特殊逻辑，万德api wsd函数返回的Data没有日期，需要自己引入
            if tablename == "marketDay":
                data = data.assign(**{'TIME': np.array(result.Times)[data.index]})
            data = self.__FilterData(data, primaryKey)
            if not data.empty and saveToDB:
                self.saveToMysql(data, db, table)
        else:
            errorMessage = 'WindApiError: result code = %d, result msg = %s, fun = %s' %(result.ErrorCode, str(result.Data), fun)
            raise WindApiError(errorMessage)

    def __GetSecIDs(self):
        """获取所有股票代码"""
        fun = "result = self.windClient." + self.secIDsFun
        print fun
        exec(fun)
        if result.ErrorCode == 0:
            data = pd.DataFrame(result.Data).T
            data.columns = result.Fields;
            if data.empty:
                errorMessage = 'GetSecIDsError: result code=%d, result msg =%s' %(result.ErrorCode, str(result.Data))
                raise WindApiError(errorMessage)
            return list(data['wind_code'])
        else:
            errorMessage = 'GetSecIDsError: result code=%d, result msg =%s' %(result.ErrorCode, str(result.Data))
            raise WindApiError(errorMessage)

    def __FilterData(self, data, primaryKey):
        """ filter data """
        # drop all data if missing primary key
        for key in primaryKey:
            if key not in data.columns:
                return pd.DataFrame()
        # drop duplicate row
        data = data.drop_duplicates()
        # convert string time to datetime
        data = DfStringToDatetime(data)
        # drop rows if primary key is null
        if primaryKey:
            data = data.dropna(axis=0, how = 'any', subset = primaryKey)
        return data

    def saveToMongo(self, data, dbName, tableName):
        """saving data to mongodb"""
        db = self.mongoClient[dbName]
        collection = db[tableName]
        collection.insert_many(data.T.to_dict().values())

    def saveToMysql(self, data, dbName, tableName):
        """saving data to mysql"""
        data.to_sql(tableName, self.mysqlClient, flavor = 'mysql', if_exists = 'append', index=None)

if __name__ == "__main__":
    wind = WindCollector('./conf/wind.conf')
    wind.updateData('19900101','19991231')

