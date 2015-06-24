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

# http ok code
HTTP_OK = 200

# block name in configure file
WMCLOUD_SETTING = 'wmcloud'
MYSQL_SETTING = 'mysql'
MONGO_SETTING = 'mongo'
SECIDS_SETTING = 'secIDs'
UPDATE_SETTING = 'update'

# name to replace in configure file
REPL_STOCK_ID = '%{STOCK_ID}'
REPL_BEGIN_DATE = '%{BEGIN_DATE}'
REPL_END_DATE = '%{END_DATE}'

# wmcloud exception
class WmcloudError(CollectorException):
    def __init__(self, message=None):
        self.message = message
        pass
    def __str__(self):
        output = "Wmcloud server error"
        if self.message:
            output = output + "[%s]"%(self.message)
        return output

#JY data collector
class JYCollector(BaseDayCollector):
    """
    聚源数据 Collector
    Wmcloud Response Example:
    1. '{"code":-403,"message":"Need privilege","data":null}'
    2. '{"retCode":-1,"retMsg":"No Data Returned"}'
    3. '{"retCode":1,"retMsg":"Success","data":[{...},{...},...]}'
    """
    def __init__(self, configFile):
        BaseDayCollector.__init__( self, 'JYCollector')
        # instance variable that read from config file
        self.domain = None
        self.port = None
        self.token = None
        self.secIDsUrl = None
        self.tableList = None
        self.mongoSetting = None
        self.mysqlSetting = None

        # instance variable used by JYCollector
        self.httpClient = None
        self.mongoClient = None
        self.mysqlClient = None

        # read configure
        self.configFile = configFile
        self.config = ConfigParser()
        self.config.optionxform = str
        self.config.read(self.configFile)
        self.readConfig()

        # initialize http connection and database connnection
        self.httpClient = httplib.HTTPSConnection(self.domain, self.port)
        self.mongoClient = MongoClient(**self.mongoSetting)
        self.mysqlClient  = MySQLdb.connect(**self.mysqlSetting)

    def __del__( self ):
        if self.httpClient is not None:
            self.httpClient.close()
            self.httpClient = None

    def __encodepath(self, path):
        """encode url"""
        start=0
        n=len(path)
        re=''
        i=path.find('=',start)
        while i!=-1 :
            re+=path[start:i+1]
            start=i+1
            i=path.find('&',start)
            if(i>=0):
                re+=urllib.quote(path[start:i])
                re+='&'
                start=i+1
            else:
                re+=urllib.quote(path[start:n])
                start=n
            i=path.find('=',start)
        return re

    def readConfig(self):
        """parse config"""
        self.domain = self.config.get(WMCLOUD_SETTING, 'domain')
        self.port = self.config.get(WMCLOUD_SETTING, 'port')
        self.token = self.config.get(WMCLOUD_SETTING, 'token')
        self.secIDsUrl = self.config.get(SECIDS_SETTING, 'url')
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

    def getData(self, path):
        """
        DESC
            send request and get response
        INPUT
            path: relative path with respect to api.wmcloud.com
        OUTPUT
            status: int type, http result status, e.g. 200
            result: string type, response data, e.g.
            '{"retCode":1,"retMsg":"Success","data":[{...},{...},...]}'
        """
        result = None
        path='/data'+path
        path=self.__encodepath(path)
        retry_interval = [0, 1, 2, 4, 8]
        retry_count = 0

        # fetch data from wmcloud server and catch BadStatusLine exception.
        # if BadStatusLine is raise, reconnect to wmcloud server.
        # try 5 times at most
        while True:
            try:
                #set http header here and make request
                self.httpClient.request('GET', path, headers = {"Authorization": "Bearer " + self.token})
                response = self.httpClient.getresponse()
                #read result, parse json into python primitive object
                result = response.read()
                if(path.find('.csv?')!=-1):
                    result=result.decode('GB2312').encode('utf-8')
                break
            except Exception, e:
                if retry_count >= len(retry_interval):
                    raise e
                else:
                    sleep(retry_interval[retry_count])
                    self.httpReconnect()
                    retry_count += 1

        return response.status, result

    def httpReconnect(self):
        """
        DESC
            reconnect to wmcloud server
        INPUT
            None
        OUTPUT
            None
        """
        if self.httpClient is not None:
            self.httpClient.close()
            self.httpClient = None
        self.httpClient = httplib.HTTPSConnection(self.domain, self.port)

    def updateData(self, beginDate = None, endDate = None, updateList = None):
        """
        DESC
            update data, if updateList is None, update all stock data
            between beginDate and endDate. otherwise, update data
            according to updateList
        INPUT
            beginDate:  string type, begin date, support format are
                        %Y-%m-%d, %Y/%m/%d, %Y-%m-%d %H:%M:%S and
                        %Y/%m/%d %H:%M:%S
            endDate:    string type, support format are as same as beginDate
            updateList: list type, elements is
                        [tablename, secID, beginDate, endDate]
        OUTPUT
            failList:   list type, if update all data success return [],
                        otherwise, return the fail list like
                        [[tablename, secID, beginDate, endDate], ..., ...]
        """
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
        url = self.config.get(tablename, 'url')
        db = self.config.get(tablename, 'db')
        table = self.config.get(tablename, 'table')
        saveToDB = self.config.getboolean(tablename, 'saveToDB')
        removeSuffix = self.config.getboolean(tablename, 'removeSuffix')
        url = url.replace(REPL_STOCK_ID, (lambda x: x.split('.')[0] if removeSuffix else x)(secID) )
        url = url.replace(REPL_BEGIN_DATE, beginDate)
        url = url.replace(REPL_END_DATE, endDate)
        primaryKey = None
        try:
            primaryKey = self.config.get(tablename,'primaryKey')
            primaryKey = [x.strip() for x in primaryKey.split(',')]
        except:
            pass

        code, result = self.getData(url)
        if code != HTTP_OK:
            errorMessage = 'http return code=%d' %(code)
            raise WmcloudError(errorMessage)

        # convert string format to json object
        result = json.loads(result)
        if result.has_key('retCode') and result['retCode'] == 1:
            data = self.__FilterData(pd.DataFrame(result['data']), primaryKey)
            if not data.empty and saveToDB:
                self.saveToMysql(data, db, table)
                # self.saveToMongo(data, db, table)
        # retCode =-1 means no data return
        elif result.has_key('retCode') and result['retCode'] == -1:
            print 'http return code=%d, result code=%d, result msg=%s'%(code, result['retCode'], result['retMsg'])
        else:
            errorMessage = 'http return code=%d, result string=%d'%(code, result['retCode'], str(result['data']))
            raise WmcloudError(errorMessage)

    def __GetSecIDs(self):
        """获取所有股票代码"""
        secInfo = pd.DataFrame()
        code, result = self.getData(self.secIDsUrl)
        if code != HTTP_OK:
            errorMessage = 'http return code=%d' %(code)
            raise WmcloudError(errorMessage)
        # convert string format to json object
        result = json.loads(result)
        if result.has_key('retCode') and result['retCode'] == 1:
            secInfo = pd.DataFrame(result['data'])
            return list(self.__FilterSecID(secInfo['secID']))
        else:
            errorMessage = 'http return code=%d, result string=%d'%(code, result['retCode'], str(result['data']))
            raise WmcloudError(errorMessage)

    def __FilterData(self, data, primaryKey = None):
        """ filter data """
        # drop all data if missing primary key
        if primaryKey is not None:
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

    def __IsStockA(serlf, secID):
        stockId, suffix = secID.split('.')
        if not stockId.isdigit():
            return False
        if suffix != 'XSHG' and suffix != 'XSHE':
            return False
        if suffix == 'XSHE' and int(stockId[0:3]) == 200:
            return False
        if suffix == 'XSHG' and int(stockId[0]) != 6:
            return False
        return True

    def __FilterSecID(self,secIDs):
        """过滤B股跟港股"""
        secIDs = secIDs.apply(lambda x: x if self.__IsStockA(x) else 'wrong')
        secIDs = secIDs[secIDs != 'wrong']
        return secIDs

    def saveToMongo(self, data, dbName, tableName):
        """saving data to mongodb"""
        db = self.mongoClient[dbName]
        collection = db[tableName]
        collection.insert_many(data.T.to_dict().values())

    def saveToMysql(self, data, dbName, tableName):
        """saving data to mysql"""
        data.to_sql(tableName, self.mysqlClient, flavor = 'mysql', if_exists = 'append', index=None)

if __name__ == "__main__":
    pass

