#coding=utf-8
import sys
import os
import time
from pyspark import SparkConf, SparkContext

def containsError(s):
	return "error" in s

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

inputRDD = sc.textFile("./log.txt")
#errorsRDD = inputRDD.filter(lambda x: "error" in x)
errorsRDD = inputRDD.filter(containsError)
warningsRDD = inputRDD.filter(lambda x: "warning" in x)
badLinesRDD = errorsRDD.union(warningsRDD)

nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect()
for num in spuared:
	print "%i " % (num)

print "Input had " + badLinesRDD.count() + " concerning lines"
print "Here are 10 examples: "
for line in badLinesRDD.take(10):
	print line

sc.stop()
