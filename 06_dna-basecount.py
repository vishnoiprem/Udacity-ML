
import os

import findspark
findspark.init('/Users/vishnoiprem/software/spark-2.2.0-bin-hadoop2.7')


from pyspark.sql import SparkSession

file_name="/Users/vishnoiprem/interview/spark_example/pyspark-tutorial/tutorial/dna-basecount/dna_seq.txt"
print( file_name)

spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()
sc=spark.sparkContext


def mapper(seq):
	freq = dict()
	for x in list(seq):
		if x in freq:
			freq[x] +=1
		else:
			freq[x] = 1
#
	kv = [(x, freq[x]) for x in freq]
	return kv
#

recs = sc.textFile(file_name)
print(recs.collect())


ones = recs.flatMap(lambda x : [(c,1) for c in list(x)])
print(ones.collect())


baseCount = ones.reduceByKey(lambda x,y : x+y)
print(baseCount.collect())

rdd = recs.map(mapper)
print(rdd.collect())


rdd = recs.flatMap(mapper)
print(rdd.collect())

baseCount = rdd.reduceByKey(lambda x,y : x+y)

print(baseCount.collect())


 #basemapper = "/Users/mparsian/spark-1.6.1-bin-hadoop2.6/basemapper.py"
#import basemapper


#rdd = recs.flatMap(basemapper.mapper)






