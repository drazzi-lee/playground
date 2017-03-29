#coding=utf-8
import sys
import os
import time
from pyspark import SparkConf, SparkContext

def splitFirstTab(x):
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

def ccc(x):
	print "DDDDDDD : " + x[0]
	print "DDDDDDD : " + x[1]
	
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

#aaaDF = sc.textFile("./data/aaa").map(aaa)
cccDF = sc.textFile("./data/ccc").map(splitFirstTab).reduceByKey(lambda x1, x2: x1 + x2)
cccDF.map(ccc)
print cccDF.first()
for line in cccDF.take(10):
	print line


#aRF = aaaDF.fullOuterJoin(bbbDF)
#bRF = aRF.map(bbb)
#bRF.saveAsTextFile("./Output/ddd")
#
#for line in bRF.take(10):
#	print line
