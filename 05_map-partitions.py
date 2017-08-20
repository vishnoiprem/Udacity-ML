
import os

import findspark
findspark.init('/Users/vishnoiprem/software/spark-2.2.0-bin-hadoop2.7')


from pyspark.sql import SparkSession

file_name="/Users/vishnoiprem/interview/spark_example/pyspark-tutorial/tutorial/wordcount/data.txt"
print( file_name)

spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()
sc=spark.sparkContext

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(numbers)

rdd = sc.parallelize(numbers, 3)
print(rdd.collect())
print(rdd.getNumPartitions())


def f(iterator):
 	for x in iterator:
 		print(x)
 	print("###########")

print(rdd.foreachPartition(f))

def adder(iterator):
	yield sum(iterator)

print(rdd.mapPartitions(adder).collect())


def minmax(iterator):
	firsttime = 0
	#min = 0;
	#max = 0;
	for x in iterator:
		if (firsttime == 0):
			min = x;
			max = x;
			firsttime = 1
		else:
			if x > max:
				max = x
			if x < min:
				min = x
		#
	return (min, max)
#

data = [10, 20, 3, 4, 5, 2, 2, 20, 20, 10]

print (minmax(data)	)


rdd = sc.parallelize(data, 3)

print( rdd.getNumPartitions())


minmaxlist = rdd.mapPartitions(minmax).collect()
print(minmaxlist)