
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


nums = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 20])


#umAndCount= nums.map(lambda x: (x,1).fold((0,0),(lambda x, y: (x[0]+y[0],x[1])+y[1])))
sumAndCount = nums.map(lambda x: (x, 1)).fold((0, 0), (lambda x, y: (x[0] + y[0], x[1] + y[1])))
sumAndCount
print(sumAndCount)