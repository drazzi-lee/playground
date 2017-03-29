#coding=utf-8
import sys
import os
import time
import numpy as np
from pyspark import SparkContext
from functools import partial

CUSTOMER_WAIMAI = 8
CUSTOMER_NUOMI = 4
CUSTOMER_DIANYING = 3
REFER_DAY = '20170101'

def remote_path():
    dir_input_ = '/path/to/input'
    dir_output_ = '/path/to/output'
    return [os.path.join(dir_input_, 'ods_pay_transaction'),\
            os.path.join(dir_output_, REFER_DAY)]
    pass
def local_path():
    dir_ = 'file:///path/to/testdata'
    return [os.path.join(dir_, 'ods_pay_transaction'),\
            os.path.join(dir_, 'result')]

def read_transaction(x):
    pos = x.find('\t')
    return x[:pos], [x[pos+1:]]

def keep_last_trans(trans):
    order_id_dict = {}
    ret = {}
    for tran in trans:
        _, order_id, _, _, _, _, _, create_time = tran.split('\t')
        create_time = int(create_time)
        if (order_id not in order_id_dict) or (create_time > order_id_dict[order_id]):
            order_id_dict[order_id] = create_time
            ret[order_id] = tran
    return ret.values()

# time defer by given reference time.
def refer_time():
    return time.mktime(time.strptime(REFER_DAY + ' 00:00:00', '%Y%m%d %H:%M:%S'))
def last_1(x):
    return x < refer_time() and x >= refer_time() - 86400 * 30
def last_3(x):
    return x < refer_time() and x >= refer_time() - 86400 * 90
def last_6(x):
    return x < refer_time() and x >= refer_time() - 86400 * 180

def addup_rates(stats_all, index, total_amount):
    stats_all[index][0] += 1
    stats_all[index][1] += total_amount

def safe_div(a, b):
    if b == 0:
        return 0
    return a * 1.0 / b

def map_with_time(time_func, x):
    pass_uid = x[0]
    trans = x[1]

    # stats_all:
    #   [0] cnt of trans
    #   [1] sum of total_amount
    stats_all = np.zeros((3, 2))

    # rates_all:
    #   [0] rate of cnt
    #   [1] rate of sum
    rates_all = np.zeros((3, 2))
    
    # group trans by order_id, each keep only last one(by create_time)
    trans = keep_last_trans(trans)
    sum_trans_cnt, sum_trans_amt = 0, 0
    for tran in trans:
        customer_id, order_id, device_type, pay_channel,\
                status, total_amount, refund_amount,\
                create_time = tran.split('\t')
        create_time = int(create_time)
        status = int(status)
        total_amount = int(total_amount)
        customer_id = int(customer_id)
        if not time_func(create_time): continue

        # addup trans for stats
        if customer_id == CUSTOMER_WAIMAI:
            addup_rates(stats_all, 0, total_amount)
        if customer_id == CUSTOMER_NUOMI:
            addup_rates(stats_all, 1, total_amount)
        if customer_id == CUSTOMER_DIANYING:
            addup_rates(stats_all, 2, total_amount)
        sum_trans_cnt += 1
        sum_trans_amt += total_amount

    for i in range(len(stats_all)):
        rates_all[i][0] = safe_div(stats_all[i][0], sum_trans_cnt)
        rates_all[i][1] = safe_div(stats_all[i][1], sum_trans_amt)
    
    return rates_all

def full_map(x):
    x1 = map_with_time(last_1, x)
    x3 = map_with_time(last_3, x)
    x6 = map_with_time(last_6, x)

    result = []
    result += x1.flatten().tolist()
    result += x3.flatten().tolist()
    result += x6.flatten().tolist()
    return x[0] + '\t' + '\t'.join(["{0:.4f}".format(i) for i in result])

def filter_trans(x):
    xs = x.split('\t')
    r_time = refer_time()
    r_time_180 = r_time - 86400 * 180
    status = int(xs[5])
    create_time = int(xs[8])
    # condition of succ status from BI status > 2
    return status > 2 and create_time <= r_time and create_time >= r_time_180

if __name__ == '__main__':
    sc = SparkContext(appName = 'ChannelRates By ods_pay_transaction ' + REFER_DAY)
    path = remote_path()

    transaction = sc.textFile(path[0])\
            .filter(filter_trans)\
            .map(read_transaction)\
            .reduceByKey(lambda x1, x2:  x1+x2)
    transaction.persist()

    transaction.map(full_map).saveAsTextFile(path[1])

    sc.stop()
