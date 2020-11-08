import pandas as pd
import tushare as ts
import time
import datetime
import json
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow import keras
from tensorflow.keras.layers import Dense, LSTM, Dropout
import math
from sklearn.metrics import mean_squared_error

with open('./parameters.json', 'r') as f:
    p = json.load(f)
    token = p['TU_share_pro_taken']
ts.set_token(token)


class individual_stock_model():

    def __init__(self, ts_code, cutoff, predict_date, base_date, asset='E',
                 lookback=100, lookfwd=5, save_path='models/', features=6):
        # input
        self.ts_code = ts_code
        self.cutoff = cutoff
        self.asset = asset
        self.lookfwd = lookfwd
        self.lookback = lookback
        self.save_path = save_path
        self.predict_date = predict_date
        self.base_date = base_date
        self.features = features
        self.train = pd.DataFrame()  # 初始化训练集
        self.predict = pd.DataFrame()  # 初始化预测集
        self.base_price = 0  # 初始化预测日前一天的收盘价
        self.scaler = MinMaxScaler(feature_range=(0, 1))

        # 初始化model
        self.model = Sequential()
        self.model.add(LSTM(32, input_shape=(self.lookback, self.features), return_sequences=True))
        self.model.add(LSTM(16, return_sequences=False))
        self.model.add(Dense(1))
        self.model.compile(loss='mse', optimizer='adam')
        # output
        self.done_flag = False

    def get_stock_raw_data(self):
        df = ts.pro_bar(ts_code=self.ts_code,
                        asset=self.asset, adj='qfq', freq='D')
        df = df[['trade_date', 'open', 'high', 'low', 'close', 'amount']]
        df.drop(df[np.isnan(df['close'])].index, inplace=True)  # 删除收盘价为空的所有行。
        df.sort_values(by='trade_date', inplace=True)  # 按照时间排序
        moving_days_close = []
        for i in range(len(df) - self.lookfwd):
            ndays = df.iloc[i:i + self.lookfwd, 4]  # 注意确认第四列为收盘价
            moving_days_close.append(np.mean(ndays))
        df['moving_days_close'] = moving_days_close + list(np.zeros(self.lookfwd))
        df.fillna(method='ffill', axis=0, inplace=True)
        df.fillna(method='bfill', axis=0, inplace=True)

        self.train = df[df['trade_date'] <= self.cutoff]  # 取训练数据集
        predict = df[df['trade_date'] <= self.predict_date]
        self.predict = predict.tail(self.lookback)  # 取最后100个作为预测输入值
        # 计算预测日前一天的收盘价
        r = df[df['trade_date'] == self.base_date]
        self.base_price = r.iloc[0, 4]  # 第一行4列，注意确认是收盘价格
        print('预测日前一天{0}的收盘价为{1}'.format(self.base_date, self.base_price))

    def setup_train_data(self):
        self.train.drop('trade_date', axis=1, inplace=True)
        self.scaler.fit(self.train)  # 以训练集来设置scaler
        data = self.scaler.fit_transform(self.train)  # 将数据归一化
        X, Y = [], []
        for i in range(self.lookback, len(data)):  # 从 100 行循环到最后
            a = data[(i - self.lookback):i, :]  # 0- 99 行数据所有列
            b = data[i, -1]  # 100行 最后一列
            X.append(a)
            Y.append(b)
        return np.array(X), np.array(Y)

    def setup_input_x(self):
        self.predict.drop('trade_date', axis=1, inplace=True)
        print(self.predict)
        input_x = self.scaler.transform(self.predict)  # 将数据归一化
        print(input_x)
        input_x = np.reshape(input_x, (1, input_x.shape[0], input_x.shape[1]))  # 搭建符合输入格式的矩阵
        return input_x

    def build_model(self):
        self.get_stock_raw_data()
        x_train, y_train = self.setup_train_data()
        self.model.fit(x_train, y_train, epochs=100, batch_size=32, verbose=1)
        self.model.save('{0}model_{1}_{2}'.format(self.save_path, self.ts_code, self.cutoff))
        self.done_flag = True
        print(self.ts_code, ", done!")

    def predict_data(self):
        input_x = self.setup_input_x()  # 此处调用了setup_input_x, 注意不要重复调用
        loader = keras.models.load_model('{0}model_{1}_{2}'.format(self.save_path, self.ts_code, self.cutoff))
        output_y = loader.predict(input_x)
        # 将结果拼接成可以用scaler还原的形状
        print("预测后的shape",output_y.shape)
        print(output_y)
        zero_input = np.zeros((1, self.features - 1))
        output_y = np.column_stack((zero_input, output_y))
        print("重塑后的shape", output_y.shape)
        print( output_y)
        output_y = self.scaler.inverse_transform(output_y)
        print("还原后的shape", output_y.shape)
        print(output_y)
        y = output_y[0,5]
        print("预测的后n天平均价格为：", y)
        percentage = (y - self.base_price) / self.base_price
        print("预测价格变化为：", percentage)
        return y, percentage


aaa = individual_stock_model(ts_code='600678.SH', cutoff= '20201104',
                             predict_date='20201105', base_date='20201104')
aaa.get_stock_raw_data()
aaa.setup_train_data()
aaa.predict_data()

# aaa.build_model()
# aaa.predict_data()

#
# todolist=[
# '600678.SH',
# '000955.SZ',
# '600621.SH',
# '002318.SZ',
# '600695.SH',
# '600605.SH',
# '600769.SH',
# '002016.SZ',
# '000099.SZ',
# '600779.SH',
# '600371.SH',
# '600167.SH',
# '002049.SZ',
# '002222.SZ',
# '300033.SZ',
# '600897.SH',
# '000532.SZ',
# '000661.SZ',
# '600507.SH',
# '002164.SZ',
# '002039.SZ',
# '000655.SZ',
# '002107.SZ',
# '600106.SH',
# '600211.SH',
# '000672.SZ'
# ]


#
# for aaa in todolist:
#     print("start:", aaa)
#     aaa=build_stock_model(ts_code=aaa, cutoff='20201102')
#     aaa.build_models()
