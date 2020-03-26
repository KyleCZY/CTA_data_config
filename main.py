# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 12:57:20 2020

@author: Kyle Cheng
"""

import pandas as pd
import time
import os
import numpy as np


#计算指定日期指定合约的最后一小时平均持仓量     
def last_hour_open_interest(date, contract):
    data = pd.read_csv(filepath + date + '\\' + contract)
    #为了减少运算量，不先对数据的时间进行处理，粗糙地计算一下平均持仓量
    if data.shape[0] < 10:
        return 0
    elif data.iloc[-1, 1][0:2] < "14":
        return 0
    else:    
        average = data.loc[(data.UpdateTime >= "14:00:00:000") & \
                           (data.UpdateTime <= "15:00:00:000"),
                           ['OpenInterest']].mean()
        return float(average)


#get main contract 获取主力合约
def get_mc(start_date, end_date, contract_name):
    
    #获得日期列表
    datelist = os.listdir(filepath)
    datelist = [date for date in datelist if (date >= start_date) & (date <= end_date)]
    mclist = []
    for date in datelist:
        #读取该日所有合约列表
        allcontractlist = os.listdir(filepath + date + "\\")
        #筛选该日指定品种合约列表
        contractlist = []
        for contract in allcontractlist:
            if contract[0:2] == contract_name:
                contractlist.append(contract)
        #计算该品种前一交易日最后一小时平均持仓量最大的合约
        #先判断这些合约在前一交易日是否存在
        lastallcontractlist = os.listdir(filepath + datelist[datelist.index(date) - 1] + "\\")
        lastcontractlist = []
        for contract in contractlist:
            if contract in lastallcontractlist:
                lastcontractlist.append(contract)
        if len(lastcontractlist) == 0:
            break
        #lastcontractlist即为这两天都交易的合约
        #用last_hour_open_interest()计算最后一小时平均持仓量
        open_interests = []
        for contract in lastcontractlist:
            open_interests.append(last_hour_open_interest(datelist[datelist.index(date) - 1], contract))
        main_contract = lastcontractlist[open_interests.index(max(open_interests))]
        mclist.append([date,main_contract])
        print(contract_name + date + "成功", end = " ")
    return mclist

#产生一个含有标准的时间序列的空白表
def get_time_list():
    timelist1 = [x.strftime("%T:%f")[:12] for x in list(pd.date_range(\
                start='2019-1-1 9:30:00',end="2019-1-1 11:30:00",freq='500L'))]
    timelist2 = [x.strftime("%T:%f")[:12] for x in list(pd.date_range(\
                start='2019-1-1 13:00:00',end="2019-1-1 15:00:00",freq='500L'))]
    timelist = timelist1 + timelist2[1:]
    return timelist


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

#将时间向后归到半秒整
def time_adjust(data):
    if int(data.iloc[0, 1][9:]) != 000 or int(data.iloc[0, 1][9:]) != 500:
        data['ms'] = data['UpdateTime'].map(lambda x: int(x[9:]))
        data.loc[(data['ms'] < 500),['UpdateTime']] = data.loc[(data['ms'] < 500),['UpdateTime']]\
        .applymap(lambda x: x[0:9] + "500" )
        data.loc[(data['ms'] > 500),['UpdateTime']] = data.loc[(data['ms'] > 500),['UpdateTime']]\
        .applymap(lambda x: timechange(x) )
        data.drop(['ms'], axis = 1, inplace = True)
    return data


#累计成交量做差得到成交量
def volume_adjust(data):
    data["Volume"] = data["Volume"].diff()
    return data

#对于收盘价专门处理
def close_price(data):
    data0 = data.loc[(data.UpdateTime > "11:30:00:000") & \
                    (data.UpdateTime <= "11:30:01:000")]
    
    if data0.shape[0] > 0:
        if data.iloc[data0.index.tolist()[0] - 1, 1] == "11:30:00:000":
            data.drop([data0.index.tolist()[0] - 1], inplace = True)
            data.iloc[data0.index.tolist()[0] - 1, 1] = "11:30:00:000"
        else:
            data.iloc[data0.index.tolist()[0], 1] = "11:30:00:000"
    data = data.reset_index(drop = True)
    
    data0 = data.loc[(data.UpdateTime > "15:00:00:000") & \
                    (data.UpdateTime <= "15:00:01:000")]

    if data0.shape[0] > 0:
        if data.iloc[data0.index.tolist()[0] - 1, -1] == "15:00:00:000":
            data.drop([data0.index.tolist()[0] - 1], inplace = True)
            data.iloc[data0.index.tolist()[0] - 1, 1] = "15:00:00:000"
        else:
            data.iloc[data0.index.tolist()[0], 1] = "15:00:00:000"
    data = data.reset_index(drop = True)
    return data


#将时间标准化
def time_insert(data, timelist):
    #产生一个包括标准时间序列的dataframe
    empty = pd.DataFrame()
    empty = empty.append(timelist)
    empty.columns = ['UpdateTime']
    #将data中的信息填入标准的dataframe中
    merged = pd.merge(empty, data, how = "left")
    #删除不需要的列,重命名某些列
    if merged.shape[1] == 26:
        merged.drop(['BidPrice2', 'BidVolume2', 'AskPrice2', 'AskVolume2', 
                     'BidPrice3', 'BidVolume3', 'AskPrice3', 'AskVolume3', 
                     'BidPrice4', 'BidVolume4', 'AskPrice4', 'AskVolume4', 
                     'BidPrice5', 'BidVolume5', 'AskPrice5','AskVolume5',
                     'Turnover'], axis = 1, inplace = True)
    elif merged.shape[1] == 11:
        merged.drop(['PreSettlementPrice','SettlementPrice'],\
                     axis = 1, inplace = True)
    else:
        merged.drop(['BidPrice2', 'BidVolume2', 'AskPrice2', 'AskVolume2', 
                     'BidPrice3', 'BidVolume3', 'AskPrice3', 'AskVolume3', 
                     'BidPrice4', 'BidVolume4', 'AskPrice4', 'AskVolume4', 
                     'BidPrice5', 'BidVolume5', 'AskPrice5','AskVolume5',
                     'Turnover','PreSettlementPrice','SettlementPrice'],\
                     axis = 1, inplace = True)
    merged.rename(columns={'UpdateTime':'TradingTime', 'OpenInterest':'Position'}, 
                  inplace = True)
    print("时间标准化成功，", end = "")
    return merged


#对缺失值处理
def missing_values(date, data):
    #合约名字填充
    data["ContractID"].fillna(method = "ffill", inplace = True)
    data["ContractID"].fillna(method = "bfill", inplace = True)
    #其他数据填充
    data['BidPrice1'].fillna(method = "ffill", inplace = True)
    data['AskPrice1'].fillna(method = "ffill", inplace = True)
    data['Position'].fillna(method = "ffill", inplace = True)
    data['LastPrice'].fillna(method = "ffill", inplace = True)
    data['BidVolume1'].fillna(0, inplace = True)
    data['AskVolume1'].fillna(0, inplace = True)
    data['Volume'].fillna(0, inplace = True)
    #顺序处理
    order = ["ContractID", 'TradingTime', 'LastPrice', 'Volume','BidPrice1',
             'BidVolume1', 'AskPrice1', 'AskVolume1', 'Position']
    data = data[order]
    #时间前加上日期
    data['TradingTime'] = data['TradingTime'].map(lambda x: date + " " + x)
    #对开盘进行处理
    #获得日期列表
    datelist = os.listdir(filepath)
    
    #检查开盘数据是否缺失
    if np.isnan(data.iloc[0, 2]):
        print("开盘数据缺失，", end = "")
        raw_data = pd.read_csv(filepath + date + '\\' + data.iloc[0, 0] + ".csv")
        #在原价格序列中找到开盘数据对应的位置
        i = 0
        while True:
            if raw_data.iloc[i, 1][0:5] == "09:30":
                break
            i += 1
        i -= 1
        #检查当日九点半前的数据能不能用
        indicator = False
        if i >= 1:
            while True:
                #检查交易量是否为0
                if raw_data.iloc[i , 3] - raw_data.iloc[i - 1, 3] != 0:
                    indicator = True
                    break
                if i == 0:
                    break
                i -= 1
        #如果当日九点半前的数据可用,则使用
        if indicator == True:
            print("使用当日数据，", end = "")
            data.iloc[0, 2] = raw_data.iloc[i,2]
            data.iloc[0, 3] = raw_data.iloc[i,3]
            data.iloc[0, 4] = raw_data.iloc[i,4]
            data.iloc[0, 5] = raw_data.iloc[i,5]
            data.iloc[0, 6] = raw_data.iloc[i,6]
            data.iloc[0, 7] = raw_data.iloc[i,7]
            data.iloc[0, 8] = raw_data.iloc[i,-2]
        else:
            #当日数据不可用，只能使用前一日的结算价
            #如果有结算价，使用结算价
            if raw_data.shape[1] == 28 or raw_data.shape[1] == 11:
                print("使用结算价", end = "")
                data.iloc[0, 2] = raw_data.iloc[10,-4]
                data.iloc[0, 3] = 0
                data.iloc[0, 4] = data.iloc[0, 2] - 0.2
                data.iloc[0, 5] = 0
                data.iloc[0, 6] = data.iloc[0, 2] + 0.2
                data.iloc[0, 7] = 0
                data.iloc[0, 8] = raw_data.iloc[0,-2]
            else:
                #没有结算价
                print("使用昨日价", end = "")
                last_raw_data = pd.read_csv\
                    (filepath + datelist[datelist.index(date) - 1] + '\\' + \
                    data.iloc[0, 0] + ".csv")
                i = 0
                while True:
                    if last_raw_data.iloc[i, 1][0:5] == "15:00":
                        break
                    i += 1
                data.iloc[0, 2] = last_raw_data.iloc[i,2]
                data.iloc[0, 3] = 0
                data.iloc[0, 4] = data.iloc[0, 2] - 0.2
                data.iloc[0, 5] = 0
                data.iloc[0, 6] = data.iloc[0, 2] + 0.2
                data.iloc[0, 7] = 0
                data.iloc[0, 8] = last_raw_data.iloc[i,-2]
    print("合约数据处理成功，", end = "")
    return data


# =============================================================================
"""季度化处理函数"""
# =============================================================================


#输出特定年份和季度的数据，quarter参数为"2020Q1"形式
def quarterize_data(contract,quarter):
    datapath = 'V:\\DataConfig\\'
    data = pd.read_csv(datapath + contract + ".csv")
    #寻找该季度的第一个数据
    i = 0
    found = True
    while True:
        if data.iloc[i,1][0:4] == quarter[0:4] and int(data.iloc[i,1][4:6]) == \
        int(quarter[-1]) * 3 - 2:
            break
        elif i == data.shape[0] - 1:
            print("不包含该季度数据")
            found = False
            break
        i += 1
    if found == True:
        #如果找到就找下一个季度的第一个数据作为结尾
        j = i
        if quarter[-1] == "4":
            #第四季度涉及年份转换，特殊处理
            while True:
                if int(data.iloc[j,1][0:4]) == int(quarter[0:4]) + 1:
                    break
                elif j == data.shape[0] - 1:
                    break
                j += 1
        else:
            while True:
                if data.iloc[i,1][0:4] == quarter[0:4] and int(data.iloc[i,1][4:6]) == \
                int(quarter[-1]) * 3 + 1:
                    break
                elif j == data.shape[0] - 1:
                    break
                j += 1
        print("找到该季度数据")        
        #写入独立的csv
        file = savepath + contract + quarter + ".csv"
        data.loc[i:j].to_csv(file, index = False)
        print(contract + quarter + "写入成功")
    return 0


# =============================================================================
"""数据清洗执行部分:"""
# =============================================================================


global filepath
global savepath
filepath = 'Y:\\CTP\\MarketData\\StockIndex\\'
savepath = 'V:\\DataConfig\\'

#获得指定合约的主力合约，输出为列表，列表中每个元素是一个[日期，主力合约]的二元列表
contract_name = "IC"
start = time.clock()
#指定日期(包括首尾)
start_date = "20191015"
end_date = "20200117"
mclist = get_mc(start_date, end_date, contract_name)
end = time.clock()
averagetime = (end-start) / len(mclist)
print("")
print(contract_name + "主力合约列表产生成功，平均每份用时: %s秒" \
      % averagetime)

#产生后面反复用到的标准时间序列
time_list = get_time_list()
#min_list = get_min_list()

#数据清洗和写入
file = savepath + contract_name + ".csv"
header = 0


for date_contract in mclist:
    start = time.clock()
    data = pd.read_csv(filepath + date_contract[0] + '\\' + date_contract[1])
    data = time_adjust(data)

    data = close_price(data)
    data = volume_adjust(data)
    merged = time_insert(data, time_list)
    merged = missing_values(date_contract[0], merged)
    if header == 0:
        merged.to_csv(file, index = False)
    else:
        merged.to_csv(file, index = False, mode = 'a+', header = False)
    header += 1
    end = time.clock()
    print(date_contract[0] + "写入成功" + '，用时: %s秒'%(end-start))
    




