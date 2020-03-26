# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 15:12:49 2020

@author: Kyle Cheng
"""

import pandas as pd
import time

#产生一个长度为30秒，间隔为500毫秒的序列
def in_half_min_listing(start_time):
    if start_time[-2:] == "30":
        if start_time[3:5] == "59":
            end_time = "%02d:00:00" % (int(start_time[0:2]) + 1)
        else:
            end_time = start_time[0:2] + ":%02d:00" % (int(start_time[3:5]) + 1)
    else:
        end_time = start_time[0:6] + "30"  
    timelist = [x.strftime("%T:%f")[:12] for x in list(pd.date_range(\
                start='2019-1-1 %s'%start_time, end="2019-1-1 %s"%end_time,freq='500L'))]
    return timelist[1:], end_time


def close_price(data, time1, time2):
    data0 = data.loc[(data.UpdateTime > time1) & \
                    (data.UpdateTime <= time2)]
    if data0.shape[0] > 0:
        if data.iloc[data0.index.tolist()[0] - 1, 1] == time1:
            data.drop([data0.index.tolist()[0] - 1], inplace = True)
            data.iloc[data0.index.tolist()[0] - 1, 1] = time1
        else:
            data.iloc[data0.index.tolist()[0], 1] = time1
    data = data.reset_index(drop = True)
    return data



#生成一个间隔为30秒的check时间序列
def half_min_list():
    timelist1 = [x.strftime("%T:%f")[:8] for x in list(pd.date_range(\
                start='2019-1-1 09:29:30',end="2019-1-1 11:29:30",freq='30S'))]
    timelist2 = [x.strftime("%T:%f")[:8] for x in list(pd.date_range(\
                start='2019-1-1 13:00:00',end="2019-1-1 14:59:30",freq='30S'))]
    timelist = timelist1 + timelist2
    return timelist

#获取开盘价
def open_price(data):
    data0 = data.loc[(data.UpdateTime < "09:30:00:000")]
    
    data = data.iloc[data0.shape[0] - 1:data0.shape[0]]
    data.iloc[0,1] = "09:30:00:000"
    data = data.rename(columns = {'UpdateTime':'TradingTime','OpenInterest':'Position'})
    return data
                     
                      
#将处理好时间的数据和标准时间序列合并
def half_min_time_insert(half_min_empty, data):
    merged = pd.merge(half_min_empty, data, how = "left")
    #删除不需要的列,重命名某些列
    if merged.shape[1] == 26:
        merged.drop(['BidPrice2', 'BidVolume2', 'AskPrice2', 'AskVolume2', 
                     'BidPrice3', 'BidVolume3', 'AskPrice3', 'AskVolume3', 
                     'BidPrice4', 'BidVolume4', 'AskPrice4', 'AskVolume4', 
                     'BidPrice5', 'BidVolume5', 'AskPrice5','AskVolume5',
                     'Turnover'], axis = 1, inplace = True)
    else:
        merged.drop(['BidPrice2', 'BidVolume2', 'AskPrice2', 'AskVolume2', 
                     'BidPrice3', 'BidVolume3', 'AskPrice3', 'AskVolume3', 
                     'BidPrice4', 'BidVolume4', 'AskPrice4', 'AskVolume4', 
                     'BidPrice5', 'BidVolume5', 'AskPrice5','AskVolume5',
                     'Turnover','PreSettlementPrice','SettlementPrice'],\
                     axis = 1, inplace = True)
    return merged

#找到某段指定的时间内的序列（模拟check得到的数据）
def time_num(start_time, end_time, data):
    half_min_data = data.loc[(data.UpdateTime > start_time) & \
                             (data.UpdateTime < end_time)]
    return half_min_data

#时间进位
def timechange(time):
    time = str(time)
    if time[6:8] == "59":
        if time[3:5] == "59":
            time ="%02d:00:00:000" % (int(time[0:2]) + 1)
        else:
            time = time[0:2] + ":%02d:00:000" % (int(time[3:5]) + 1)
    else:
        time = time[0:6] + "%02d:000" % (int(time[6:8]) + 1)
    return time

#把时间归到半秒整
def time_adjust(data):
    if int(data.iloc[0, 1][9:]) != 000 or int(data.iloc[0, 1][9:]) != 500:
        data['ms'] = data['UpdateTime'].map(lambda x: int(x[9:]))
        data.loc[(data['ms'] < 500),['UpdateTime']] = data.loc[(data['ms'] < 500),['UpdateTime']]\
        .applymap(lambda x: x[0:9] + "500" )
        data.loc[(data['ms'] > 500),['UpdateTime']] = data.loc[(data['ms'] > 500),['UpdateTime']]\
        .applymap(lambda x: timechange(x) )
        data.drop(['ms'], axis = 1, inplace = True)
    return data

#差分得到交易量
def volume_adjust(data):
    data["Volume"] = data["Volume"].diff()
    return data

#对缺失值处理
def missing_values_halfmin(data_all, data, start_time):
    #合约名字填充
    data["ContractID"].fillna(method = "ffill", inplace = True)
    data["ContractID"].fillna(method = "bfill", inplace = True)
    #其他数据填充
    #拼已有数据的最后一行上去
    if start_time != "09:30:00":
        data = data_all[-1:].append(data, ignore_index = True) 
    data['BidPrice1'].fillna(method = "ffill", inplace = True)
    data['AskPrice1'].fillna(method = "ffill", inplace = True)
    data['OpenInterest'].fillna(method = "ffill", inplace = True)
    data['LastPrice'].fillna(method = "ffill", inplace = True)
    data['BidVolume1'].fillna(0, inplace = True)
    data['AskVolume1'].fillna(0, inplace = True)
    data['Volume'].fillna(0, inplace = True)
    #顺序处理
    order = ["ContractID", 'UpdateTime', 'LastPrice', 'Volume','BidPrice1',
             'BidVolume1', 'AskPrice1', 'AskVolume1', 'OpenInterest']
    data = data[order]
    data.drop([0], inplace = True)
    return data


# =============================================================================
"""执行部分："""
# =============================================================================


#对于20151015的IC1910进行操作
file = "Y:\\CTP\\MarketData\\StockIndex\\20191015\\IC2003.csv"
write = "V:\\DataConfig\\halfmin.csv"
data = pd.read_csv(file)
#产生一个空的标准时间dataframe来存放清洗完的数据
data_all = pd.DataFrame()


#生成一个间隔为30秒的check时间序列（这里的end_time实际上才是check发生的时间）
#第一次check获得开盘价
openprice = open_price(data)
data = close_price(data)
start = time.clock()

for start_time in half_min_list():
    #对于之后每一个check点进行操作
    end_time = in_half_min_listing(start_time)[1]
    if start_time == '09:29:30':
        data_all = open_price(data)
        
    #为了模拟实盘，提取start_time之后，end_time前的所有序列作为已知数据    
    if start_time == '11:29:30':
        half_min_data = time_num(start_time, "11:30:01:000", data)
        half_min_data = close_price(half_min_data, "11:30:00:000", "11:30:01:000")
    elif start_time == '14:59:30':
        half_min_data = time_num(start_time, "15:00:01:000", data)
        half_min_data = close_price(half_min_data, "15:00:00:000", "15:00:01:000")
    else:
        half_min_data = time_num(start_time, end_time, data)

    #将时间归到半秒整
    half_min_data = time_adjust(half_min_data)

    #交易量调整
    half_min_data = volume_adjust(half_min_data)
    half_min_data = half_min_data.loc[1:]
    half_min_data.reset_index(drop=True)
   
    #产生一个空的半分钟序列
    in_half_min_list = in_half_min_listing(start_time)[0]
    half_min_empty = pd.DataFrame()
    half_min_empty = half_min_empty.append(in_half_min_list)
    half_min_empty.columns = ['UpdateTime']
    #合并两个序列
    merged = half_min_time_insert(half_min_empty, half_min_data)   

    merged = missing_values_halfmin(data_all, merged, start_time)
    merged = merged.rename(columns = {'UpdateTime':'TradingTime','OpenInterest':'Position'})
 
    data_all = data_all.append(merged, ignore_index = True)
    print("checked" + end_time, end = "")

#顺序处理    
order = ["ContractID", 'TradingTime', 'LastPrice', 'Volume','BidPrice1',
             'BidVolume1', 'AskPrice1', 'AskVolume1', 'Position']
data_all = data_all[order]
data_all.to_csv(write, index = False)
    
    
    