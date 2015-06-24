import re
import pandas as pd
import time
import datetime

class BaseDayCollector:
    def __init__(self, moduleName = ''):
        self.name = moduleName
        # self.error_code = 0
        # self.error_msg = 'OK'

    def getCollectorName(self):
        return self.name

    def readConfig(self):
        """read config interface"""
        pass

    def updateData(self, beginDate, endDate, updateList = None):
        """update data, call collector to fetch and save data"""
        pass

class CollectorException(Exception):
    pass

class GetSecIDsError(CollectorException):
    def __init__(self, moduleName):
        self.moduleName = moduleName
    def __str__(self):
        return self.moduleName + ": GetSecIDsError"


def StringToDatetime(s):
    """
    Check whether the string s is a time format string.
    return datetime or s
    Example: '2014/02/03' return '%Y/%m%d'
             '2014-02-03 00:00:00' return '%Y-%m-%d %H:%M:%S'
    """
    if type(s) != str and type(s) != unicode:
        return s
    if type(s) == str:
        s = unicode(s)
    mapFormat = {
            u'^\s*\d{4}-\d{2}-\d{1,2}\s*\d{1,2}:\d{2}:\d{2}\s*$': '%Y-%m-%d %H:%M:%S',
            u'^\s*\d{4}-\d{1,2}-\d{1,2}\s*$': '%Y-%m-%d',
            u'^\s*\d{4}/\d{1,2}/\d{1,2}\s*\d{2}:\d{2}:\d{2}\s*$': '%Y/%m/%d %H:%M:%S',
            u'^\s*\d{4}/\d{1,2}/\d{1,2}\s*$': '%Y/%m/%d'
            }
    for k, v in mapFormat.items():
        if re.search(k, s) != None:
            return datetime.datetime.strptime(s,v)
    return s

def DfStringToDatetime(data):
    """
    convert pandas.DataFrame columns from time string format to datetime type format
    """
    if type(data) != pd.DataFrame:
        return data
    for col in data.columns:
        data[col] = data[col].apply(StringToDatetime)
    return data


