
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

data = ["abc,de", "abc,de,ze", "abc,de,ze,pe"]
rdd=sc.parallelize(data)

print(rdd.collect())
print(rdd.count())

rdd2 = rdd.flatMap(lambda x : x.split(","))
print(rdd2.collect())


data2 = ["abc,de", "xyz,deeee,ze", "abc,de,ze,pe", "xyz,bababa"]
rdd4 = sc.parallelize(data2)
print(rdd4.collect())
#print(rdd4.map(lambda x :(x.split(",")))


rdd5 = rdd4.map(lambda x : (x.split(",")[0], x.split(",")[1]))

print(rdd5.collect())
print(rdd4.map(lambda x : (x.split(",")[0])).collect())

