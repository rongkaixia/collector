# -*- coding: utf-8 -*-
from dataapiclient import Client
import pandas as pd
import csv
import datetime
import numpy as np
import random


def GetSecIDs():
    secIDs = pd.DataFrame()
    client= Client()
    client.init('911de8a6b2cd91d6012ce06a40dde6158bc0b5e74c4a41d6524494b70332b1a0')
    try:
        url1='/api/master/getSecID.json?field=&assetClass=E&ticker=&partyID=&cnSpell='
        # url1='/api/equity/getEqu.json?field=&listStatusCD=&secID=&ticker=&equTypeCD=A'
        code, result = client.getData(url1)
        if(code==200 and result['retCode']==1):
            row=pd.DataFrame(result['data'])
            secIDs=secIDs.append(row)
        else:
            print 'get secIDs error, http_ret=%d, ret_code=%d, ret_msg=%s' %(code, result['retCode'], result['retMsg'])
    except Exception, e:
        raise e

    return secIDs

def GetStockMarketByDay(secIDs, beginDate, endDate):
    """docstring for Get"""
    client= Client()
    client.init('911de8a6b2cd91d6012ce06a40dde6158bc0b5e74c4a41d6524494b70332b1a0')
    data=pd.DataFrame()
    perc=list(np.array(np.linspace(0,len(secIDs),20),dtype='int'))
    success_count=0
    for idx,secID in enumerate(secIDs):
        try:
            # url= '/api/market/getMktEqudJY.json?field=&secID=' + secID + '&startDate=' + beginDate +  '&endDate=' + endDate
            # url= '/api/listedCorp/getEquAllotaJY.json?field=&secID=' + secID +'&ticker=&exchangeCD='
            url = '/api/listedCorp/getEquSPOaJY.json?field=&secID=' + secID + '&ticker=000001&exchangeCD=&eventProcessCD=&spoTypeCD=&isExright='
            url = '/api/listedCorp/getNeeqDivJY.json?field=&secID=' + secID +'&ticker=&exchangeCD=&endDateStart=&endDateEnd=&isDiv=&eventProcessCD='
            code, result = client.getData(url)
            if(code==200):
                if(result['retCode']==1):
                    row=pd.DataFrame(result['data'])
                    data=data.append(row, ignore_index=True)
                    success_count+=1
            else:
                print 'fetch secID[%s] error, http_ret=%d, ret_code=%d, ret_msg=%s' %(secID, code, result['retCode'], result['retMsg'])
                return None
        except Exception, e:
            #traceback.print_exc()
            raise e
        if idx in perc:
            print '%%%d done(%d)' %(idx*100/len(secIDs), success_count)

    return data

def GetStockMarketByPeriod(secID, beginDate, endDate):
    """docstring for Get"""
    client= Client()
    client.init('911de8a6b2cd91d6012ce06a40dde6158bc0b5e74c4a41d6524494b70332b1a0')
    data=pd.DataFrame()
    try:
        url= '/api/market/getMktEqudJY.json?field=&secID=' + secID + '&startDate=' + beginDate +  '&endDate=' +endDate
        code, result = client.getData(url)
        if(code==200):
            if(result['retCode']==1):
                row=pd.DataFrame(result['data'])
                data=data.append(row, ignore_index=True)
            else:
                return None
        else:
            print 'fetch secID[%s] error, http_ret=%d, ret_code=%d, ret_msg=%s' %(secID, code, result['retCode'], result['retMsg'])
            return None
    except Exception, e:
        #traceback.print_exc()
        raise e
    return data

def GetAllData2():
    print 'getting secIDs...',
    secInfo = GetSecIDs()
    secIDs = list(secInfo['secID'])
    print 'done(%d stocks)' %(len(secIDs))
    startDate = '19900101'
    endDate = '20150605'
    # all_data=pd.DataFrame()
    for idx, secID in enumerate(secIDs):
        print 'fetching stock[%s] data...' %(secID),
        data = GetStockMarketByPeriod(secID, startDate, endDate)
        print 'done.[%d/%d]' %(idx+1,len(secIDs))
        if data is not None:
            data.to_csv('/home/rk/host/data/'+secID + '_'+startDate+'to' +endDate+'.csv',index=None,float_format="%.2f", encoding="utf-8")
            # all_data=all_data.append(data, ignore_index=True)

    # all_data.to_csv('/home/rk/host/data/all_'+startDate+'to' +endDate+'.csv',index=None,float_format="%.2f", encoding="utf-8")

def GetAllData():
    print 'getting secIDs...',
    secInfo = GetSecIDs()
    secIDs = list(secInfo['secID'])
    # random.shuffle(secIDs)
    # secIDs = secIDs[0:300]
    print 'done(%d stocks)' %(len(secIDs))
    startDate = datetime.datetime(1990, 1, 1)
    endDate = datetime.datetime(1990, 12, 31)
    while   startDate.year <= 2015:
        str_startDate = startDate.strftime('%Y%m%d')
        str_endDate = endDate.strftime('%Y%m%d')
        data = GetStockMarketByDay(secIDs, str_startDate, str_endDate)
        data.to_csv('/home/rk/host/data2/'+str(startDate.year)+'.csv',index=None,float_format="%.2f")
        startDate = datetime.datetime(startDate.year+1,startDate.month, startDate.day)
        endDate = datetime.datetime(endDate.year+1,endDate.month, endDate.day)

def GetEquAllota():
    """docstring for GetEquAllota"""
    print 'getting secIDs...',
    secInfo = GetSecIDs()
    secIDs = list(secInfo['secID'])
    # random.shuffle(secIDs)
    # secIDs = secIDs[0:100]
    print 'done(%d stocks)' %(len(secIDs))
    data = GetStockMarketByDay(secIDs, '111', '111')
    data.to_csv(u'/home/rk/host/股票分红.csv',index=None,float_format="%.2f")

if __name__ == "__main__":
    # GetAllData()
    GetEquAllota()

