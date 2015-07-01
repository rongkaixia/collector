# -*- coding: utf-8 -*-
import httplib
import traceback
import urllib
import pandas as pd
import numpy as np
import random
import MySQLdb
import json
import itertools
import yaml
from time import sleep
from ConfigParser import *
from pymongo import *

from base_collector import *
import dfutils

# http ok code
HTTP_OK = 200

# block name in configure file
WMCLOUD_SETTING = 'exchangeSH'
STOCKIDS_SETTING = 'stockIDs'
STOCK_DIVIDEND = 'stockDividend'
CASH_DIVIDEND = 'cashDividend'
RIGHTS_ISSUE= 'rightsIssue'

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
class JYCollector(BaseCollector):
    """
    聚源数据 Collector
    Wmcloud Response Example:
    1. '{"code":-403,"message":"Need privilege","data":null}'
    2. '{"retCode":-1,"retMsg":"No Data Returned"}'
    3. '{"retCode":1,"retMsg":"Success","data":[{...},{...},...]}'
    """
    def __init__(self, configFile, dbtype='mongo', database=None, **dbsetting):
        BaseCollector.__init__( self, 'JYCollector')
        self.configFile = configFile
        self.dbtype = dbtype
        self.database = database
        self.dbsetting = dbsetting

        # instance variable
        self.yamlSetting = None
        self.stockIDs = None
        self.domain = None
        self.port = None
        self.token = None

        # read configure
        self.readConfig(configFile)

        # initialize http connection and database connnection
        if dbtype == 'mongo':
            self.dbConnection = MongoClient(**dbsetting)[database]
        elif dbtype == 'mysql':
            self.dbConnection = MySQLdb.connect(**dbsetting)
        else:
            raise ValueError("'{0}' is not valid for dbtype".format(dbtype))
        self.httpClient = httplib.HTTPConnection(self.domain, self.port)

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

    def readConfig(self,configFile):
        """parse config"""
        self.yamlSetting = yaml.safe_load(open(configFile, 'r'))
        self.domain = self.yamlSetting[WMCLOUD_SETTING]['domain']
        self.port = self.yamlSetting[WMCLOUD_SETTING]['port']

    def getData(self, path, referer):
        """
        Describe
        ----------
        send request and get response

        Parameters
        ----------
        path: relative path with respect to api.wmcloud.com

        Returns
        ----------
        status: int type, http result status, e.g. 200
        result: json type, response data
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
                self.httpClient.request('GET', path, headers = {"Referer": referer})
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

        # convert string format to json object
        return response.status, result

    def httpReconnect(self):
        """
        Describe
        ----------
        reconnect to wmcloud server
        """
        if self.httpClient is not None:
            self.httpClient.close()
            self.httpClient = None
        self.httpClient = httplib.HTTPSConnection(self.domain, self.port)

    def update(self, updateList = None):
        """
        Describe
        ----------
        Update data
        Implement by children class

        Parameters
        ----------
        updateList: list type, elements is
                    [[tablename, stockID, beginDate, endDate], ..., ...]
        tablename:  update table name
        stockID:    stock ID, '*' means update all data
        beginDate:  string type, begin date, support format are
                    %Y-%m-%d, %Y/%m/%d, %Y-%m-%d %H:%M:%S and
                    %Y/%m/%d %H:%M:%S
        endDate:    string type, support format are as same as beginDate

        Returns:
        ----------
        failList:   list type, if update all data success return [],
                    otherwise, return the fail list like
                    [[tablename, stockID, beginDate, endDate], ..., ...]
        succList:   success list, same format as failList

        """
        failList = []
        succList = []
        if updateList is None:
            return (failList,succList)

        if self.stockIDs is None:
            self.stockIDs = self.getStockIDs()

        for tablename, stockID, beginDate, endDate in updateList:
            print tablename
            # rightsIssue
            if tablename == RIGHTS_ISSUE:
                for year in range(int(beginDate),int(endDate)+1):
                    data = None
                    print year
                    try:
                        data = self.fetchData(stockID, tablename, str(year), endDate)
                        if data.empty :
                            continue
                        print data
                        data['allotmentRatio'] = data['allotmentRatio'].apply(lambda x: float(x)/10.0)
                        data['allotmentPrice'] = data['allotmentPrice'].apply(lambda x: float(x))
                        dfutils.DfToDatabase(data, tablename, self.dbConnection, dbtype=self.dbtype)
                        succList.append([tablename, stockID, beginDate, endDate, "OK"])
                    except Exception, e:
                        errorMessage = str(e)
                        failList.append([tablename, stockID, beginDate, endDate, errorMessage])
                        print 'update %s in table %s fail: %s' %(stockID, tablename, errorMessage)
                        print data
                        traceback.print_exc()
                        raise e
                continue

            # cashDividend and stockDividend
            stockList = [stockID]
            if stockID == "*":
                stockList = self.stockIDs
            for sid in stockList:
                data = None
                try:
                    data = self.fetchData(sid, tablename, beginDate, endDate)
                    if data.empty :
                        continue
                    if tablename == CASH_DIVIDEND:
                        data['perCashDivAfTax'] = data['perCashDivAfTax'].apply(lambda x: float(x) if x!='-' else np.nan)
                        data['perCashDiv'] = data['perCashDiv'].apply(lambda x: float(x) if x!='-' else np.nan)
                    if tablename == STOCK_DIVIDEND:
                        data['perStockDiv'] = data['perStockDiv'].apply(lambda x: float(x)/10.0)
                    dfutils.DfToDatabase(data, tablename, self.dbConnection, dbtype=self.dbtype)
                    succList.append([tablename, sid, beginDate, endDate, "OK"])
                except Exception, e:
                    errorMessage = str(e)
                    failList.append([tablename, sid, beginDate, endDate, errorMessage])
                    print 'update %s in table %s fail: %s' %(sid, tablename, errorMessage)
                    print data
                    traceback.print_exc()
                    raise e
        return (succList, failList)

    def fetchData(self, stockID, tablename, beginDate, endDate):
        """
        Describe
        ----------
        Fetch data

        Parameters
        ----------
        stockID:    stock ID
        tablename:  update table name
        beginDate:  string type, begin date, support format are
                    %Y-%m-%d, %Y/%m/%d, %Y-%m-%d %H:%M:%S and
                    %Y/%m/%d %H:%M:%S
        endDate:    string type, support format are as same as beginDate

        Returns
        --------
        pandas.DataFrame
        """
        data = pd.DataFrame()
        url = self.yamlSetting[tablename]['url']
        referer = self.yamlSetting[tablename]['referer']
        removeSuffix = self.yamlSetting[tablename]['removeSuffix']
        url = url.replace(REPL_STOCK_ID, (lambda x: x.split('.')[0] if removeSuffix else x)(stockID) )
        url = url.replace(REPL_BEGIN_DATE, beginDate)
        url = url.replace(REPL_END_DATE, endDate)
        print url
        print referer

        primaryKey = None
        renameColumns = None
        if self.yamlSetting[tablename].has_key('primaryKey'):
            primaryKey = self.yamlSetting[tablename]['primaryKey']
        if self.yamlSetting[tablename].has_key('columnMapping'):
            renameColumns = self.yamlSetting[tablename]['columnMapping']

        code, result = self.getData(url, referer)
        if code == 403:
            return data
        if code != HTTP_OK:
            errorMessage = 'http return code=%d' %(code)
            raise WmcloudError(errorMessage)

        try:
            idx = result.find('(')+1
            result = result[idx:-1]
            result = json.loads(result)
        except Exception, e:
            errorMessage = 'http return code=%d, result=%s'%(code, str(result))
            raise WmcloudError(errorMessage)

        # retCode =-1 means no data return
        if result.has_key('pageHelp') and result['pageHelp'].has_key('data'):
            data = self.FilterData(pd.DataFrame(result['pageHelp']['data']), primaryKey, renameColumns)
        else:
            errorMessage = 'http return code=%d, result=%s'%(code, str(result))
            raise WmcloudError(errorMessage)

        return data

    def getStockIDs(self):
        """获取所有股票代码"""
        stockIDs = []
        for i in range(600000,605000):
            stockIDs.append('%06d'%(i))
        return stockIDs

    def FilterData(self, data, primaryKey = None, renameColumns = None):
        """ filter data """
        # drop all data if missing primary key column
        if primaryKey is not None:
            for key in primaryKey:
                if key not in data.columns:
                    return pd.DataFrame()
        # drop duplicate row
        data = data.drop_duplicates()
        # convert string time to datetime
        dfutils.DfStrColToDatetime(data, inplace=True)
        # drop rows if primary key is null
        if primaryKey is not None:
            data = data.dropna(axis=0, how = 'any', subset = primaryKey)
        if renameColumns is not None:
            data.rename(columns = renameColumns, inplace = True)
        return data

    def __IsStockA(serlf, stockID):
        stockID, suffix = stockID.split('.')
        if not stockID.isdigit():
            return False
        if suffix != 'XSHG' and suffix != 'XSHE':
            return False
        if suffix == 'XSHE' and int(stockID[0:3]) == 200:
            return False
        if suffix == 'XSHG' and int(stockID[0]) != 6:
            return False
        return True

    def FilterSecID(self,stockIDs):
        """过滤B股跟港股"""
        stockIDs = stockIDs.apply(lambda x: x if self.__IsStockA(x) else 'wrong')
        stockIDs = stockIDs[stockIDs != 'wrong']
        return stockIDs

if __name__ == "__main__":
    s = yaml.safe_load(open('./conf/sh.conf','r'))
    jy = JYCollector('./conf/sh.conf', dbtype='mongo',database=s['mongo']['db'], **s['mongo']['settings'])
    updateList=[
            # ['rightsIssue','*','1990','2014'],
            ['cashDividend','*','19900101','20141231']
            # ['stockDividend','*','19900101','20141231']
            ]
    s,f = jy.update(updateList)

