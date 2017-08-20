
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


num=[10,2,9,3,8,4,7,5,6,1]

rdd=sc.parallelize(num).takeOrdered(3)
print(rdd)

rdd=sc.parallelize(num).takeOrdered(3,key=lambda x: -x)
print(rdd)


kv = [(10,"z1"), (1,"z2"), (2,"z3"), (9,"z4"), (3,"z5"), (4,"z6"), (5,"z7"), (6,"z8"), (7,"z9")]

rdd=sc.parallelize(kv).takeOrdered(3)
print(rdd)



rdd=sc.parallelize(kv).takeOrdered(3,key=lambda x: -x[0])
print(rdd)