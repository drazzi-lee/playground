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
	aaaV, bbbV = None, None

	try:
		aaaV = x[1][0] 
	except: pass
	try:
		bbbV = x[1][1] 
	except: pass

	if aaaV is None: 
		aaaV = ['0']*4 
	if bbbV is None: 
		bbbV = ['0']*4

	return '\t'.join([uid, '\t'.join(aaaV), '\t'.join(bbbV)])
	
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

aaaDF = sc.textFile("./aaa").map(aaa)
bbbDF = sc.textFile("./bbb").map(aaa)

aRF = aaaDF.fullOuterJoin(bbbDF)
bRF = aRF.map(bbb)
bRF.saveAsTextFile("./ddd")

for line in bRF.take(10):
	print line
