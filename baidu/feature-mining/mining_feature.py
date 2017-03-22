#coding=utf-8
import sys
import os
import time
import numpy as np
from pyspark import SparkContext
from datetime import datetime
from functools import partial

def remote_path():
    dir_ = '/feature_mining'
    return [os.path.join(dir_, 'ods_t_trans_app_df_simple_20161001'),\
            os.path.join(dir_, 'result')]
    pass
def local_path():
    dir_ = 'file:///home/input'
    return [os.path.join(dir_, 'ods_t_trans_app_df_simple_20161001'),\
    #return [os.path.join(dir_, 't'),\
            os.path.join(dir_, 'result')]
def read_transaction(x):
    pos = x.find('\t')
    return x[:pos], [x[pos+1:]]


def last_1(x):
    return x<=1475251200 and x>=1472659200
def last_3(x):
    return x<=1489334399 and x>=1467475200
def last_6(x):
    return x<=1475251200 and x>=1459699200

def update_a(a, channel, total_amount):
    a[channel][0] += 1
    a[channel][1] = (a[channel][1] * (a[channel][0] - 1) + total_amount)/a[channel][0]
    a[channel][2] = max(a[channel][2], total_amount)
    a[channel][3] += total_amount

def nuomi_channel(id):
    if id == '1501000004': 
        return True
    return False
def waimai_channel(id):
    if id in ['1000374961', '3300000108']:
        return True
    return False
def movie_channel(id):
    if id == '1500000001':
        return True
    return False

def keep_last_trans(trans):
    order_id_dict = {}
    ret = {}
    for tran in trans:
        #_, order_id, _, _, _, _, _, create_time = tran.split('\t')
        xs = tran.split('\t')
        if len(xs) < 7: continue 
        order_id = xs[0]
        create_time = int(time.mktime(time.strptime(xs[6],'%Y-%m-%d %H:%M:%S')))
        if (order_id not in order_id_dict) or (create_time > order_id_dict[order_id]):
            order_id_dict[order_id] = create_time
            ret[order_id] = tran
    return ret.values()

def safe_div(a,b):
    if b==0:
        return 0
    return a*1.0/b

def map_with_time(time_func, x):
    pass_uid = x[0]
    trans = x[1]
    
    a = np.zeros((3,4))
    #group trans by order_id, each keep only last one(by create_time)
    trans = keep_last_trans(trans)
    sum_trans, sum_amount = 0, 0
    for tran in trans:
        line = tran.split('\t')
        if len(line) < 7: 
            print 'pass'
            continue
        order_id, trans_id, f_enabled,\
                status, sp_id, total_amount,create_time = tran.split('\t')
        create_time = int(time.mktime(time.strptime(create_time,'%Y-%m-%d %H:%M:%S')))
        create_time = int(create_time)
        status = int(status)
        total_amount = int(total_amount)
        if not time_func(create_time): continue

        # a, a[channel][type]
        # channel = 0,1,2  waimai/nuomi/movie
        # type = 0,1,2,3 times/avg/max/total
        if status in [2, 3, 4, 6, 7] :
            if waimai_channel(sp_id):
                update_a(a, 0, total_amount)
            elif nuomi_channel(sp_id):
                update_a(a, 1, total_amount)
            elif movie_channel(sp_id):
                update_a(a, 2, total_amount)

    return a


def full_map(x):
    x1 = map_with_time(last_1, x)
    x3 = map_with_time(last_3, x)
    x6 = map_with_time(last_6, x)

    r = []
    for v1,v3,v6 in zip(x1,x3,x6):
        r += v1.flatten('F').tolist()
        r += v3.flatten('F').tolist()
        r += v6.flatten('F').tolist()
    return x[0] + '\t' + '\t'.join([str(i) for i in r])

if __name__ == '__main__':
    sc = SparkContext(appName = 'mining feature from pay_transaction')
    #path = remote_path()

    path = local_path()
    transaction = sc.textFile(path[0])\
            .map(read_transaction)\
            .reduceByKey(lambda x1, x2:  x1+x2)
    transaction.persist()
    print transaction.first() 
    transaction.map(full_map).saveAsTextFile(path[1])

    sc.stop()
