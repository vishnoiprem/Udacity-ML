
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


nums = sc.parallelize([1, 2, 3, 4, 5, 6, 7])

iltered1 = nums.filter(lambda x : x % 2 == 1)
print(iltered1.collect())
filtered2 = nums.filter(lambda x : x % 2 == 0)
print(filtered2)