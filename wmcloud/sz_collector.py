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
from bs4 import BeautifulSoup

# http ok code
HTTP_OK = 200

# block name in configure file
WMCLOUD_SETTING = 'exchangeSZ'
ISSUEIDS_SETTING = 'issueValue'

SZ_DIVIDEND = 'szDividend'

# name to replace in configure file
REPL_ISSUE_ID = '%{ISSUE_ID}'

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
        if path.find('=',0) == -1:
            return path
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

    def getData(self, path, postData=None, headers=None, method='GET'):
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
        path=self.__encodepath(path)
        retry_interval = [0, 1, 2, 4, 8]
        retry_count = 0

        # fetch data from wmcloud server and catch BadStatusLine exception.
        # if BadStatusLine is raise, reconnect to wmcloud server.
        # try 5 times at most
        while True:
            try:
                #set http header here and make request
                if method == 'POST':
                    self.httpClient.request('POST', path, postData, headers)
                else:
                    print 'path=%s'%(path)
                    self.httpClient.request('GET', path)
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
        self.httpClient = httplib.HTTPConnection(self.domain, self.port)

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
        for issueID in self.yamlSetting[ISSUEIDS_SETTING]:
            tablename = SZ_DIVIDEND
            print SZ_DIVIDEND
            # cashDividend and stockDividend
            data = None
            try:
                data = self.fetchData(issueID, SZ_DIVIDEND, '*', '*')
                if data.empty :
                    continue
                dfutils.DfToDatabase(data, SZ_DIVIDEND, self.dbConnection, dbtype=self.dbtype)
                succList.append([tablename, '*', '*', '*', "OK"])
            except Exception, e:
                errorMessage = str(e)
                failList.append([tablename, '*', '*', '*', errorMessage])
                print 'update %s in table %s fail: %s' %('*', tablename, errorMessage)
                print data
                traceback.print_exc()
                raise e
        return (succList, failList)

    def fetchData(self, issueID, tablename, beginDate, endDate):
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
        postData = self.yamlSetting[tablename]['postData']
        url = self.yamlSetting[tablename]['postUrl']
        headers = self.yamlSetting[tablename]['headers']
        postData = postData.replace(REPL_ISSUE_ID, issueID)
        print url
        print postData
        print headers

        primaryKey = None
        renameColumns = None
        if self.yamlSetting[tablename].has_key('primaryKey'):
            primaryKey = self.yamlSetting[tablename]['primaryKey']
        if self.yamlSetting[tablename].has_key('columnMapping'):
            renameColumns = self.yamlSetting[tablename]['columnMapping']

        # 获取报告链接
        code, result = self.getData(url, postData, headers,method='POST')
        if code == 403:
            print 'Error: http return code=%d' %(code)
            return data

        soup = BeautifulSoup(result)
        url = None
        for a_href in soup.find_all('a'):
            if a_href.string.find(u'分红') !=-1:
                url = a_href['href']
                break
        if url is None:
            print 'Warning: http dividend url is None'
            return data
        print u'分红url=%s'%(url)
        # 首先返回303， 再获取真实的url
        code, result = self.getData(url)
        if code == 403:
            print 'Error: real url http return code=%d' %(code)
            raise ValueError

        soup = BeautifulSoup(result)
        url = None
        url = soup.find('a')['href']
        print u'分红 real url=%s'%(url)
        if url is None:
            print 'Error: http real dividend url is None'
            return data

        self.httpClient = httplib.HTTPConnection(self.domain, self.port)
        code, result = self.getData(url)
        if code != HTTP_OK:
            errorMessage = 'http return code=%d' %(code)
            raise WmcloudError(errorMessage)

        soup = BeautifulSoup(result)
        if soup.find('tbody') is not None:
            for row in soup.tbody.find_all('tr'):
                row_data=[]
                if len(row.find_all('td')) !=14 or len(row.find_all('td'))!=15:
                    continue
                for a in row.find_all('td'):
                    row_data.append(unicode(a.string))
                if all([c ==  u'None' for c in row_data]):
                    print 'WARNING: all None'
                    continue
                if len(row_data)>14:
                    print 'WARNING: len(row_data)=%d'%(len(row_data))
                    print row_data
                    row_data=row_data[1:]
                data=data.append([row_data],ignore_index=True)
        else:
            for row in soup.find_all('tr')[1:]:
                row_data=[]
                if len(row.find_all('td')) !=14 or len(row.find_all('td'))!=15:
                    continue
                for a in row.find_all('td'):
                    row_data.append(unicode(a.string))
                if all([c== u'None' for c in row_data]):
                    print 'WARNING: all None'
                    continue
                if len(row_data)>14:
                    print 'WARNING: len(row_data)=%d'%(len(row_data))
                    print row_data
                    row_data=row_data[1:]
                data=data.append([row_data],ignore_index=True)
        if data.empty:
            return data
        data.columns = ['ticker', 'secName', 'bonus', 'perStockDiv', 'totalCashDiv', 'perCashDiv', 'allotmentNum', 'allotmentRatio', 'allotmentPrice', 'fundsRaised', 'exDivRightsDate', 'regDate', 'exPrice', 'preClosePrice']
        print data
        data['perStockDiv'] = data['perStockDiv'].apply(lambda x: float(x) if all(c in "0123456789.+-" for c in x) else np.nan)
        data['perCashDiv'] = data['perCashDiv'].apply(lambda x: float(x) if all(c in "0123456789.+-" for c in x) else np.nan)
        data['allotmentRatio'] = data['allotmentRatio'].apply(lambda x: float(x) if all(c in "0123456789.+-" for c in x) else np.nan)
        data['allotmentPrice'] = data['allotmentPrice'].apply(lambda x: float(x) if all(c in "0123456789.+-" for c in x) else np.nan)

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
    s = yaml.safe_load(open('./conf/sz.conf','r'))
    jy = JYCollector('./conf/sz.conf', dbtype='mongo',database=s['mongo']['db'], **s['mongo']['settings'])
    updateList=[SZ_DIVIDEND, '*', '0','0']
    s,f = jy.update(updateList)

