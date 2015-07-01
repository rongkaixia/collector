import re
import pandas as pd
import numpy as np
import time
import datetime
from pymongo import *
import MySQLdb
import json

def _isTimeFormat(s):
    """
    Describe
    ----------
    Check whether the string s is a time format string.
    Just support following time format

    Return
    ----------
    Boolean
    """
    if not isinstance(s,str) and not isinstance(s,unicode):
        return False
    s = unicode(s)
    mapFormat = {
            u'^\s*\d{4}-\d{1,2}-\d{1,2}\s*\d{2}:\d{2}:\d{2}\s*$': '%Y-%m-%d %H:%M:%S',
            u'^\s*\d{4}-\d{1,2}-\d{1,2}\s*$': '%Y-%m-%d',
            u'^\s*\d{4}/\d{1,2}/\d{1,2}\s*\d{2}:\d{2}:\d{2}\s*$': '%Y/%m/%d %H:%M:%S',
            u'^\s*\d{4}/\d{1,2}/\d{1,2}\s*$': '%Y/%m/%d'
            }
    for fmt, v in mapFormat.items():
        if re.search(fmt, s) != None:
            return True
    return False

def DfStrColToDatetime(data, inplace=False):
    """
    Describe
    ----------
    Convert pandas.DataFrame columns from time string format to datetime type format

    Parameters
    ----------
    data: a pandas.DataFrame instance

    Return
    ----------
    pandas.DataFrame
    """
    if not isinstance(data, pd.DataFrame):
        raise NotImplementedError("'data' argument should be either a "
                                  "Series or a DataFrame")
    if inplace not in [True, False]:
        raise ValueError("'{0}' is not valid for inplace".format(dbtype))
    if inplace:
        frame = data
    else:
        frame = data.copy()
    for name in frame.columns:
        col = frame[name].dropna()
        if col.size == np.sum(col.apply(_isTimeFormat)):
            frame[name] = pd.to_datetime(frame[name])

    if not inplace:
        return frame

def DftoMongo(data, tablename, con, date_format='iso', date_unit='s', **kwargs):
    """
    Describe
    ----------
    pandas.DataFrame to Mongo databases
    each row of data represent a document in mongodb by default
    you can change this by setting 'orient' parameter
    for 'orient', please check pandas.DataFrame.to_json descrition

    Parameters
    ----------
    data: a pandas.DataFrame instance
    tablename: string, name of SQL table
    con: SQLAlchemy engine or DBAPI2 connection (legacy mode)

    date_format, date_unit and other options
    please check pandas.DataFrame.to_json descrition
    """
    if not isinstance(data, pd.Series) and not isinstance(data, pd.DataFrame):
        raise NotImplementedError("'data' argument should be either a "
                                  "Series or a DataFrame")
    con[tablename].insert_many(
            json.loads(data.T.to_json(date_format=date_format, date_unit=date_unit, **kwargs)).values()
            )

def DftoSQL(data, tablename, con, flavor='mysql', if_exists='append', index=False, **kwargs):
    """
    Describe
    ----------
    pandas.DataFrame to MySQL databases
    each row of data represent a document in mongodb by default,
    you can change this by setting index = True or call
    DftoSQL(data.T, ...)

    Parameters
    ----------
    data: a pandas.DataFrame instance
    tablename: string, name of SQL table
    con: SQLAlchemy engine or DBAPI2 connection (legacy mode)

    flavor, if_exists, index and other options
    please check pandas.DataFrame.to_sql descrition
    """
    if not isinstance(data, pd.Series) and not isinstance(data, pd.DataFrame):
        raise NotImplementedError("'data' argument should be either a "
                                  "Series or a DataFrame")
    data.to_sql(tablename, con, flavor=flavor, if_exists=if_exists, index=index, **kwargs)

def DfToDatabase(data, tablename, con, dbtype='mysql', **kwargs):
    """
    Save pandas.DataFrame to databases
    """
    if not isinstance(data, pd.Series) and not isinstance(data, pd.DataFrame):
        raise NotImplementedError("'data' argument should be either a "
                                  "Series or a DataFrame")

    if dbtype == 'mysql':
        DftoSQL(data, tablename, con, **kwargs)
    elif dbtype == 'mongo':
        DftoMongo(data, tablename, con, **kwargs)
    else:
        raise ValueError("'{0}' is not valid for dbtype".format(dbtype))

def test():
    a = pd.DataFrame([{'id':0,'date1':'2015-01-01'}])
    b = pd.DataFrame([{'id':1,'date2':'2015-01-01'}])
    c = a.merge(b, on='id',how='outer')
    d = DfStrColToDatetime(c)
    db = MongoClient()['test']
    sql_db = MySQLdb.connect(host='127.0.0.1', user='rk',passwd='rk',db='test')
    DfToMongo(c,'test',db)
    DfToSQL(c,'test',db)

