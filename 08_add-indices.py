
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


a = [('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
a
print(a)

rdd = sc.parallelize(a)
print(rdd.collect())


sorted = rdd.sortByKey()

print(sorted)

sorted = rdd.sortByKey(False)
print(sorted.collect())

indices = sorted.zipWithIndex()
print(indices.collect())

#rdd2 = rdd.map(lambda x,y: (y,x))

#print(rdd2.collect())
