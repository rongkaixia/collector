# -*- coding: utf-8 -*-
import pandas as pd
import csv
import datetime
import numpy as np
import random
import time

def mergeData(allotData, divData):
    """
    合并聚源的分红数据与配股数据:
    分红信息(聚源数据)
    1、除权除息日 --> exDivDate
    2、每股派现（税前）--> perCashDiv
    3、每股送股比例 --> perShareDivRatio
    4、每股转增股比例 --> perShareTransRatio
    配股信息(聚源数据)
    1、除权日 --> exRightsDate
    2、配股价 --> allotmentPrice
    3、配股比例 --> allotmentRatio
    """
    divColumns = ['ticker', 'exDivDate', 'perCashDiv', 'perShareDivRatio', 'perShareTransRatio']
    allotColumns = ['ticker', 'exRightsDate', 'allotmentPrice', 'allotmentRatio']

    divAddOn ={ x:0 for x in allotColumns[2:] }
    allotAddOn = { x:0 for x in divColumns[2:] }

    # get columns
    allot = allotData[allotColumns]
    div = divData[divColumns]

    # add and rename columns
    allot = allot.assign(**allotAddOn)
    allot = allot.rename(columns={'exRightsDate': 'exDivRightsDate'})
    div = div.assign(**divAddOn)
    div = div.rename(columns={'exDivDate': 'exDivRightsDate'})

    data = div.append(allot,ignore_index = True)
    return data

def compareDiv(jyData, windData):
    """
    比较聚源与wind除权除息信息
    比较内容为：除权出息日，分红派现，送股，转增股，配股价，配股比例
    """
    columnConverter = {
            'ticker': 'wind_code',
            'exDivRightsDate': 'ex_dividend_date',
            'perCashDiv': 'cash_payout_ratio',
            'perShareDivRatio': 'stock_dividend_ratio',
            'perShareTransRatio': 'stock_split_ratio',
            'allotmentPrice': 'rights_issue_price',
            'allotmentRatio': 'rights_issue_ratio'
            }
    windData = windData[columnConverter.values()]
    windData.columns = columnConverter.keys()
    jyData = jyData[columnConverter.keys()]
    # fill nan value
    jyData = jyData.fillna('missing')
    windData = windData.fillna('missing')
    # convert wind sercurity code 000599.SZ to 599
    windData['ticker'] = windData['ticker'].apply(lambda x: np.int64(x.split('.')[0]))
    # lambda function for convert datetime
    # JY format: 2005-01-01 00:00:00
    # wind format: 2005/01/01
    jyDateFun = lambda x: x if x=='missing' else time.strptime(x.split(' ')[0], '%Y-%m-%d')
    windDateFun = lambda x: x if x=='missing' else time.strptime(x, '%Y/%m/%d')
    jyData['exDivRightsDate'] = jyData['exDivRightsDate'].apply(jyDateFun)
    windData['exDivRightsDate'] = windData['exDivRightsDate'].apply(windDateFun)
    # remove duplicate
    jyData = jyData.drop_duplicates()
    windData = windData.drop_duplicates()

    # return (jyData, windData)
    return findDiff(jyData, windData)

def isEqualRow(row_a, row_b):
    """compare row"""
    dict_a = row_a.to_dict()
    dict_b = row_b.to_dict()
    cols_name = ['perShareTransRatio', 'allotmentPrice', 'allotmentRatio', 'perShareDivRatio', 'perCashDiv']
    for col in cols_name:
        if dict_a[col] != dict_b[col] and dict_a[col] != 'missing' and dict_b[col] != 'missing' and round(dict_a[col],2) != round(dict_b[col],2):
            return False
    return True

def matrixRowSetDiff(m_a, m_b):
    """ find different rows in matrix a and matrix b base on secID and exDivRightsDate"""
    set_a = pd.DataFrame()
    set_b = pd.DataFrame()
    set_same = pd.DataFrame()
    set_diff_a = pd.DataFrame()
    set_diff_b = pd.DataFrame()
    for _, row_a in m_a.iterrows():
        idx = m_b['exDivRightsDate'] == row_a['exDivRightsDate']
        # print idx
        # print type(m_b.ix[idx])
        # print type(row)
        # print m_b.ix[idx] == row
        if np.sum(idx) == 0:
            set_a = set_a.append(row_a, ignore_index=True)
        # else if np.sum(np.sum(m_b.ix[idx] == row)) !=7:
        #     set_diff_a = set_diff_a.append(row, ignore_index=True)

    for _, row_b in m_b.iterrows():
        idx = m_a['exDivRightsDate'] == row_b['exDivRightsDate']
        if np.sum(idx) == 0:
            set_b = set_b.append(row_b, ignore_index=True)
        elif not isEqualRow(m_a.ix[idx].squeeze(), row_b):
            set_diff_a = set_diff_b.append(m_a.ix[idx].squeeze(), ignore_index=True)
            set_diff_b = set_diff_b.append(row_b, ignore_index=True)
        else:
            set_same = set_same.append(row_b, ignore_index=True)

    return (set_a, set_same, set_b, set_diff_a, set_diff_b)

def findDiff(jyData, windData):
    """ find different rows in matrix a and matrix b """

    # just compare data between 2005/01/01 to 2014/12/31
    idx = jyData['exDivRightsDate'] >= datetime.datetime(2005,1,1).timetuple()
    jyData = jyData.ix[idx]
    idx = jyData['exDivRightsDate'] <= datetime.datetime(2014,12,31).timetuple()
    jyData = jyData.ix[idx]

    jySet = pd.DataFrame()
    windSet = pd.DataFrame()
    sameSet = pd.DataFrame()
    diffSetJy = pd.DataFrame()
    diffSetWind =  pd.DataFrame()

    allSecIDs = np.union1d(np.unique(jyData['ticker']), np.unique(windData['ticker']))
    for secID in allSecIDs:
        jySecData = jyData.ix[jyData['ticker'] == secID]
        windSecData = windData.ix[windData['ticker'] == secID]
        [set_a, set_same, set_b, set_diff_a, set_diff_b] = matrixRowSetDiff(jySecData, windSecData)
        jySet = jySet.append(set_a, ignore_index = True)
        windSet = windSet.append(set_b, ignore_index = True)
        sameSet = sameSet.append(set_same, ignore_index = True)
        diffSetJy = diffSetJy.append(set_diff_a, ignore_index = True)
        diffSetWind= diffSetWind.append(set_diff_b, ignore_index = True)
        print 'secID=%s, jySet=%d, windSet=%d, sameSet=%d diffSetJy=%d diffSetWind=%d' %(secID, jySet.shape[0], windSet.shape[0], sameSet.shape[0], diffSetJy.shape[0], diffSetWind.shape[0])

    # save difference to csv file
    cols_name = ['ticker', 'exDivRightsDate', 'perCashDiv', 'perShareDivRatio', 'perShareTransRatio', 'allotmentPrice', 'allotmentRatio']
    windSet = windSet[cols_name]
    windSet = reformat(windSet)
    windSet.to_csv('/home/rk/host/diff/windSet.csv', index=None)
    jySet = jySet[cols_name]
    jySet = reformat(jySet)
    jySet.to_csv('/home/rk/host/diff/jySet.csv', index=None)

    diffSetJy = diffSetJy[cols_name]
    diffSetWind = diffSetWind[cols_name]
    diffSetWind.columns = ['wind_'+x for x in cols_name]
    diffSet = pd.concat([diffSetJy, diffSetWind], axis=1)
    diffSet = reformat(diffSet)
    diffSet.to_csv('/home/rk/host/diff/diffSet.csv', index=None)
    return (jySet, windSet, sameSet, diffSetJy, diffSetWind)

def reformat(data):
    if 'ticker' in data.columns:
        data['ticker'] = data['ticker'].apply(lambda x: '%06d' %(x))
    if 'exDivRightsDate' in data.columns:
        data['exDivRightsDate'] = data['exDivRightsDate'].apply(lambda x: time.strftime('%Y-%m-%d',x))
    if 'wind_ticker' in data.columns:
        data['wind_ticker'] = data['wind_ticker'].apply(lambda x: '%06d' %(x))
    if 'wind_exDivRightsDate' in data.columns:
        data['wind_exDivRightsDate'] = data['wind_exDivRightsDate'].apply(lambda x: time.strftime('%Y-%m-%d',x))
    return data

if __name__ == "__main__":
    # GetAllData()
    a = 1

