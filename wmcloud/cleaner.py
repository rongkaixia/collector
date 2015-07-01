# -*- coding: utf-8 -*-
from ConfigParser import *
from pymongo import *
import pandas as pd
import random
from time import sleep
import MySQLdb
import json
import itertools
from collections import Counter, OrderedDict
import numpy as np
import matplotlib.pyplot as plt


# block name in configure file
MYSQL_SETTING = 'mysql'
MONGO_SETTING = 'mongo'
TARGET_SETTING = 'target'

# name to replace in configure file
REPL_STOCK_ID = '%{STOCK_ID}'
REPL_BEGIN_DATE = '%{BEGIN_DATE}'
REPL_END_DATE = '%{END_DATE}'

# Cleaner exception

# Data Cleaner
class Cleaner():
    """
    Cleaner
    """
    def __init__(self, configFile):
        # instance variable that read from config file
        self.mongoSetting = None
        self.mysqlSetting = None

        # instance variable used by cleaner
        self.mongoClient = None
        self.mysqlClient = None

        # read configure
        self.parseConfig(configFile)

        # initialize database connnection
        self.mongoClient = MongoClient(**self.mongoSetting)
        self.mysqlClient  = MySQLdb.connect(**self.mysqlSetting)

    def __del__( self ):
        pass

    def __readOneDataMysql(self, **kwargs):
        '''
        kwargs
            db:
            table:
            columns:
            primaryKeys:
            constraints:
        '''
        mysqlParams = self.mysqlSetting
        mysqlParams['db'] = kwargs['db']
        con = MySQLdb.connect(**mysqlParams)

        query = 'SELECT %s FROM %s WHERE %s' %(','.join(kwargs['columns']), kwargs['table'], kwargs['constraints'])
        print query
        data = pd.io.sql.read_sql(query, con)
        # drop duplicate row
        data.drop_duplicates(inplace=True)
        # drop nan row
        data.dropna(axis=0, how='any', inplace=True)
        # drop zero row
        columns = kwargs['columns']
        primaryKeys = kwargs['primaryKeys']
        compareColumns = list(set(columns).difference(set(primaryKeys)))
        zeroNanIndex = data[compareColumns].apply(self.__isZeroNanRow, axis=1)
        data = data[~zeroNanIndex]
        return data

    def __isZeroNanRow(self, row):
        for x in row:
            if  ~np.isnan(x) and x!=0:
                return False
        return True

    def clean(self, data, targetInfo):
        '''
        data: join data
        targetInfo
            columns: [...]
            primaryKeys: [...]
            datasets: [{...}, ... ,{...}]
        Returns
        --------
        (sameData, diffData)
        '''
        columns = list(set(data.columns).difference(set(targetInfo['primaryKeys'])))
        compareData = data[columns]
        # set precision
        compareData = compareData.applymap(lambda x: np.round(x,3))
        # 一致信息
        compareColumns = set(targetInfo['columns']).difference(set(targetInfo['primaryKeys']))
        compareColumnsMap = {x: [] for x in compareColumns}
        print compareColumns
        for dataset in targetInfo['datasets']:
            [compareColumnsMap[x].append(x + '_' + dataset['__name__']) for x in compareColumns]
        print compareColumnsMap
        idx = None
        for _,cols in compareColumnsMap.items():
            sub_idx = compareData[cols].apply(lambda x: True if len(Counter(x))==1 else False, axis=1)
            if idx is None:
                idx = sub_idx
            else:
                idx = idx & sub_idx
        sameData = data[idx]

        # 不一致信息
        addedColumn = compareData[~idx].apply(lambda x: str(dict(Counter(x))), axis=1)
        anyDiffData = data[~idx]
        anyDiffData = anyDiffData.assign(**{'note':addedColumn})

        # 如果有两个信息是一致的，则去掉
        def has_nan(xx):
            if Counter(xx).most_common(1)[0][1]<2:
                return False
            for x in xx:
                if np.isnan(x):
                    return True
            return False
        # mostCommIdx = compareData[~idx].apply(lambda x: True if Counter(x).most_common(1)[0][1]>=2 else False, axis=1)
        mostCommIdx = compareData[~idx].apply(has_nan, axis=1)

        allDiffData = data[~idx][~mostCommIdx] #表示三个都不一致，nan != nan

        def has_two(xx):
            count = 0
            for x in xx:
                if not np.isnan(x):
                    count = count+1;
            return count>=2
        twoDiffIdx = compareData[~idx][~mostCommIdx].apply(has_two, axis=1)
        twoDiffData = allDiffData[twoDiffIdx]
        oneDiffData = allDiffData[~twoDiffIdx]

        print 'same data shape ',
        print sameData.shape
        print 'any diff data shape ',
        print anyDiffData.shape
        print 'all diff data shape ',
        print allDiffData.shape
        print 'one diff data shape ',
        print oneDiffData.shape
        print 'two diff data shape ',
        print twoDiffData.shape
        print twoDiffData

        # use most common
        return (sameData, anyDiffData, oneDiffData, allDiffData)

    def compareTarget(self, secID, beginDate, endDate, targetInfo):
        '''
        targetInfo
            columns: [...]
            primaryKeys: [...]
            datasets: [{...}, ... ,{...}]
        '''
        joinData = pd.DataFrame()
        for dataset in targetInfo['datasets']:
            params = {}
            params['primaryKeys'] = [dataset[x] for x in targetInfo['primaryKeys']]
            params['columns'] = [dataset[x] for x in targetInfo['columns']]
            params['db'] = dataset['db']
            params['table'] =dataset['table']
            constraints = dataset['constraints']
            constraints = constraints.replace(REPL_STOCK_ID, secID)
            constraints = constraints.replace(REPL_BEGIN_DATE, beginDate)
            constraints = constraints.replace(REPL_END_DATE, endDate)
            params['constraints'] = constraints
            # read data from database
            dfData = self.__readOneDataMysql(**params)
            dfData.columns = targetInfo['columns']
            compareColumns = set(targetInfo['columns']).difference(set(targetInfo['primaryKeys']))
            renameColumns = {x: x + '_'+ dataset['__name__'] for x in compareColumns}
            dfData.rename(columns = renameColumns, inplace = True)
            print '%s: %d rows' %(dataset['__name__'], dfData.shape[0])
            # join data
            if joinData.empty:
                joinData = dfData
            else:
                joinData = joinData.merge(dfData, on = targetInfo['primaryKeys'], how='outer')
        print 'joinData: %d rows' %(joinData.shape[0])

        return self.clean(joinData, targetInfo)

    def doCompare(self, secID, beginDate, endDate):
        fmt = ['r-','b--','g-..','k-+','m:.']
        count = 0
        plt.subplot(121)
        for targetName, targetInfo in self.targetsSetting.items():
            print '--------------%s-----------' %(targetName)
            (sameData, diffData, _, _) = self.compareTarget(secID, beginDate, endDate, targetInfo)
            # print diffData

            tmp = list(diffData['exDivRightsDate'].apply(lambda x: int(x.strftime('%Y'))))
            stat = OrderedDict(Counter(tmp))
            plt.plot(stat.keys(),stat.values(),fmt[count], label=targetName)
            # plt.
            count = count + 1
        plt.yscale('log')
        plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
        plt.xlabel('year')
        plt.ylabel('difference')
        # plt.ylim(0,200)
        plt.show()

    def parseConfig(self, configFile):
        """read config"""
        self.configFile = configFile
        self.config = ConfigParser()
        self.config.optionxform = str
        self.config.read(self.configFile)

        # read db setting
        self.mysqlSetting = dict(self.config.items(MYSQL_SETTING))
        if self.mysqlSetting.has_key('port'):
            self.mysqlSetting['port'] = int(self.mysqlSetting['port'])

        #convert some configure value into int type rather than string type
        mongoSetting = dict(self.config.items(MONGO_SETTING))
        for key in mongoSetting.keys():
            if mongoSetting[key].isdigit():
                mongoSetting[key] = int(mongoSetting[key])
        self.mongoSetting = mongoSetting

        # read target setting
        self.targetsSetting={}
        target = self.config.get(TARGET_SETTING, 'targets')
        self.targetList = [x.strip() for x in target.split(',')]
        for target in self.targetList:
            datasets = [x.strip() for x in self.config.get(target, 'datasets').split(',')]
            primaryKeys = [x.strip() for x in self.config.get(target, 'primaryKeys').split(',')]
            columns = [x.strip() for x in self.config.get(target, 'columns').split(',')]

            dbSettingList = []
            for data in datasets:
                dataSetting = {}
                targetDataSection = target+'_'+data
                dataSetting['__name__'] = targetDataSection
                dataSetting['type'] = self.config.get(targetDataSection, 'type')
                dataSetting['db'] = self.config.get(targetDataSection, 'db')
                dataSetting['table'] = self.config.get(targetDataSection, 'table')
                dataSetting['constraints'] = self.config.get(targetDataSection, 'constraints')
                for key in primaryKeys:
                    dataSetting[key] = self.config.get(targetDataSection, key)
                for col in columns:
                    dataSetting[col] = self.config.get(targetDataSection, col)
                dbSettingList.append(dataSetting)

            self.targetsSetting[target] = {
                    'primaryKeys': primaryKeys,
                    'columns': columns,
                    'datasets': dbSettingList
                    }

        print self.targetsSetting


    def saveToMongo(self, data, dbName, tableName):
        """saving data to mongodb"""
        db = self.mongoClient[dbName]
        collection = db[tableName]
        collection.insert_many(data.T.to_dict().values())

    def saveToMysql(self, data, dbName, tableName):
        """saving data to mysql"""
        data.to_sql(tableName, self.mysqlClient, flavor = 'mysql', if_exists = 'append', index=None)

if __name__ == "__main__":
    jy = Cleaner('./conf/cleaner.conf')
    jy.doCompare('1', '19900101','20141231')

