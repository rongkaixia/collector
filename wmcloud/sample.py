# -*- coding: utf-8 -*-
import pandas as pd
import csv
import datetime
import numpy as np
import random
from pymongo import MongoClient
import MySQLdb

if __name__ == "__main__":
    # GetAllData()
    db = MongoClient('localhost',27017)['SXData']
    con = MySQLdb.connect(host="127.0.0.1", user='rk', passwd='rk',port=3306, db='SXData',charset='utf8')
    cashData = pd.DataFrame()
    for post in db['cashDividend'].find(projection={'_id': False}):
        cashData = cashData.append(post, ignore_index=True)
    stockData = pd.DataFrame()
    for post in db['stockDividend'].find(projection={'_id': False}):
        stockData = stockData.append(post, ignore_index=True)
    rightsData = pd.DataFrame()
    for post in db['rightsIssue'].find(projection={'_id': False}):
        rightsData = rightsData.append(post, ignore_index=True)
    print rightsData.shape
    print stockData.shape
    print cashData.shape
    dividendData = cashData.merge(stockData, how = 'outer', on =['ticker','exDivDate'])
    rightsData.rename(columns = {'exRightsDate': 'exDivDate'}, inplace = True)
    dividendData = dividendData.merge(rightsData, how = 'outer', on =['ticker','exDivDate'])
    dividendData = dividendData[['ticker','exDivDate','perCashDiv','perCashDivAfTax','perStockDiv','allotmentPrice','allotmentRatio']]
    print dividendData.columns
    print dividendData.shape
    dividendData = dividendData.drop_duplicates()
    dividendData = dividendData.dropna(how='all')
    dividendData.to_sql('dividend', con, flavor='mysql', if_exists='append', index=False)
