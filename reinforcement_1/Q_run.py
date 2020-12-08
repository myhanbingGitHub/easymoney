import numpy as np
import pandas as pd
import time
import ccxt
import matplotlib.pyplot as plt
import logging

n_states = pow(3,7)  # 环境状态个数
actions = ['buy','sell','wait']
epsilon = 0.9  # 贪心率
alpha = 0.1  # 学习速率 越大学习越快 但有可能很容易记住噪音特征
gamma = 0.9  # 折扣因子  越大越信任己有经验，越小越看重每次奖励
max_epsilon = 3   # 最大学习回合数

api_key = 'c778848925934310a15db6755659c4af'
secret_key = 'd7c9a9a9ced84200afe4f51a8e1bdc01'

exchange = ccxt.fcoin({"apiKey":api_key,"secret":secret_key})
amount=0.01
fee=0
symbol='BTC/USDT'
period=60


# 创建Qtable
def build_q_table(n_states, actions):
    table = pd.DataFrame(
        np.zeros((n_states,len(actions))),
        columns=actions,
    )
    print(table)
    return table


# 选择action
def choose_action(state,q_table,step_counter,price_bid,price_ask,buy_num,sell_num):
    # 按照Qtable选择合适的action
    state_action = q_table.iloc[state,:]  # 选取Q table里面对应的行
    if (np.random.uniform()>epsilon) or ((state_action == 0).all()):   # 如果大于阈值或者这一行的值全部是零
        action_name = np.random.choice(actions)   # 就随意选择
    else:
        action_name = state_action.idxmax()  # 否则就选择这一行值最大的那个动作

    try:
        if action_name == 'buy':
            buy_num += 1
            # exchange.create_order(symbol, 'limit', 'buy', amount, price_ask)
        if action_name == 'sell':
            sell_num +=1
            # exchange.create_order(symbol, 'limit', 'sell', amount, price_bid)
    except:
        pass

    return action_name, buy_num, sell_num


# 更新环境
def update_env():
    time.sleep(1)
    result_pre = exchange.fetch_order_book(symbol, limit=20)  # 获取市场记录
    time.sleep(1)
    result = exchange.fetch_order_book(symbol, limit=20)  # 隔一个时间间隔，再次获取市场记录
    # 计算买卖双方的报价格
    price_bid = result['bids'][0][0]   # 获取卖方报价1
    price_ask = result['asks'][0][0]    # 获取买方报价1
    mid_price = (result['bids'][0][0] + result['bids'][1][0] + result['bids'][2][0]  # 获取前三个买卖的均价
                 + result['asks'][0][0] + result['asks'][1][0] + result['asks'][2][0]) / 6

    price_bid_pre = result_pre['bids'][0][0]
    price_ask_pre = result_pre['asks'][0][0]
    mid_price_pre = (result_pre['bids'][0][0] + result_pre['bids'][1][0] + result_pre['bids'][2][0]
                     + result_pre['asks'][0][0] + result_pre['asks'][1][0] + result_pre['asks'][2][0]) / 6

    # 获取买卖双方对应的量
    bid_size_4 = result['bids'][0][1] + result['bids'][1][1] + result['bids'][2][1] + result['bids'][3][1]
    ask_size_4 = result['asks'][0][1] + result['asks'][1][1] + result['asks'][2][1] + result['asks'][3][1]

    bid_size_4_pre = result_pre['bids'][0][1] + result_pre['bids'][1][1] + result_pre['bids'][2][1] + \
                     result_pre['bids'][3][1]
    ask_size_4_pre = result_pre['asks'][0][1] + result_pre['asks'][1][1] + result_pre['asks'][2][1] + \
                     result_pre['asks'][3][1]

    bid_size_8 = result['bids'][0][1] + result['bids'][1][1] + result['bids'][2][1] + result['bids'][3][1] \
                 + result['bids'][4][1] + result['bids'][5][1] + result['bids'][6][1] + result['bids'][7][1]
    ask_size_8 = result['asks'][0][1] + result['asks'][1][1] + result['asks'][2][1] + result['asks'][3][1] \
                 + result['asks'][4][1] + result['asks'][5][1] + result['asks'][6][1] + result['asks'][7][1]
    # 64类state
    S = 0
    if mid_price > mid_price_pre: S += 32  # 如果均价上涨 则状态值加32
    if price_bid > price_bid_pre: S += 16  # 如果卖价上涨 则状态值加16
    if price_ask > price_ask_pre: S += 8    # 如果买价上涨 则状态值加8
    if bid_size_4 > ask_size_4: S += 4    # 如果卖量 大于 买量 则状态值加4
    if bid_size_4_pre > ask_size_4_pre: S += 2   # ... 则状态值加2
    if bid_size_8 > ask_size_8: S += 1  # ... 则状态值加1
    # 上面的代码在尝试总结盘面可能出现的‘状态’，用数值来代表不同的状态。 为什么这么设计需要更深入的理解
    return S, price_bid, price_ask    # 返回状态S 以及买卖价格


# 获取环境反馈
def get_env_feedback(action, price_bid_pre, price_ask_pre, total):
    S_, price_bid, price_ask = update_env()
    R= 0
    # 用两个前后动作之间的价格差作为奖励R， 同时价格差也是本次动作的收益
    if action == 'buy':   # 如果前一个动作是买入了，那么价格上涨就是收益，否则亏损
        result = (price_bid-price_ask_pre) * amount
        cost = (price_bid + price_ask) * amount * fee
        R = result - cost
        # exchange.create_order(symbol,'limit','sell',amount,_[-1])
    if action == 'sell':  # 如果前一个动作是卖出了，那么价格上涨就是亏损，否则收益
        result = (price_bid-price_ask) * amount
        cost = (price_bid_pre + price_ask) * amount * fee
        R = result - cost
        # exchange.create_order(symbol,'limit','buy',amount,_[-1])
    total = np.append(total,total[-1]+R)  # 计算净值的发展曲线，类似于循环计算累计值
    return S_, R, total, price_bid, price_ask





# 主逻辑循环
def rl():
    # 保存净值
    total = np.array([])
    # 新建qtable
    q_table = build_q_table(n_states,actions)
    # 获取账户余额
    balance = exchange.fetch_balance()
    # 记录买入次数、卖出次数
    buy_num = 0
    sell_num = 0
    # 开始本次在线学习
    for episode in range(max_epsilon):
        # 计数
        step_counter = 0
        # 判断是否终止循环
        is_terminated = False
        # 更新环境
        S, price_bid, price_ask = update_env()
        # 更新净值
        total = np.append(total,0)
        # 开始循环
        while not is_terminated:
            # 根据当前状态选择行为
            A, buy_num,sell_num = choose_action(S, q_table,step_counter,price_bid,price_ask,buy_num,sell_num)
            # 获取环境反馈
            S_, R, total, price_bid, price_ask = get_env_feedback(A, price_bid,price_ask,total)
            打印当前状态
            print('回合',step_counter,'动作',A,'状态',S,'奖励',round(R,4),
                  '收益',round(total[-1],4))
            # 计算Q预测值
            q_predict = q_table.loc[S, A]
            # 计算Q目标值
            q_target = R + gamma * q_table.iloc[S_, :].max()
            # 更新Qtable
            q_table.loc[S,A]+= alpha * (q_target - q_predict)
            # 更新状态
            S = S_
            # 自增
            step_counter += 1
            # 当学习次数超过阈值时
            if step_counter > 10000:
                is_terminated = True
            return q_table, total


if  __name__ == "__main__":
    q_table, total = rl()
    print('\r\nQ-table:\n')
    print(q_table)
    np.save('Q_table.npy', q_table)
    np.save('total.npy',total)
    plt.plot(total)
    plt.show()