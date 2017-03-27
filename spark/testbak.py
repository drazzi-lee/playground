#coding=utf-8
import sys
import os
import time
from pyspark import SparkConf, SparkContext

def aaa(x):
	xs = x.split("\t", 1)
	return xs[0], xs[1].split("\t")

def bbb(x):
	uid = x[0]
	v1,v2,v3,v4,v5,v6,v7,v8 = None,None,None,None,None,None,None,None

	try:
		v1 = x[1][0][0]
	except: pass
	try: 
		v2 = x[1][0][1]
	except: pass
	try: 
		v3 = x[1][0][2]
	except: pass
	try: 
		v4 = x[1][0][3]
	except: pass
	try: 
		v5 = x[1][1][0]
	except: pass
	try: 
		v6 = x[1][1][1]
	except: pass
	try: 
		v7 = x[1][1][2]
	except: pass
	try: 
		v8 = x[1][1][3]
	except: pass

	if v1 is None: 
		v1 = '0' 
	if v2 is None: 
		v2 = '0' 
	if v3 is None: 
		v3 = '0' 
	if v4 is None:
		v4 = '0' 
	if v5 is None: 
		v5 = '0' 
	if v6 is None: 
		v6 = '0' 
	if v7 is None: 
		v7 = '0' 
	if v8 is None: 
		v8 = '0' 

	return '\t'.join([uid, v1, v2, v3, v4, v5, v6, v7, v8])
	
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

aaaDF = sc.textFile("./aaa").map(aaa)
bbbDF = sc.textFile("./bbb").map(aaa)

aRF = aaaDF.fullOuterJoin(bbbDF)
bRF = aRF.map(bbb)
bRF.saveAsTextFile("./ddd")

for line in bRF.take(10):
	print line
