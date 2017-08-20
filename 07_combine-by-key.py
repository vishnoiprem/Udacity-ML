
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


input = [("k1", 1), ("k1", 2), ("k1", 3), ("k1", 4), ("k1", 5), 
             ("k2", 6), ("k2", 7), ("k2", 8), 
             ("k3", 10), ("k3", 12)]

print(input)


rdd = sc.parallelize(input)


sumCount = rdd.combineByKey( 
                                (lambda x: (x, 1)), 
                                (lambda x, y: (x[0] + y, x[1] + 1)), 
                                (lambda x, y: (x[0] + y[0], x[1] + y[1])) 
                               )

print(sumCount.collect())




