import os

import findspark
findspark.init('/Users/vishnoiprem/software/spark-2.2.0-bin-hadoop2.7')


from pyspark.sql import SparkSession

R="/Users/vishnoiprem/interview/spark_example/pyspark-tutorial/tutorial/basic-join/R.txt"
S="/Users/vishnoiprem/interview/spark_example/pyspark-tutorial/tutorial/basic-join/S.txt"


spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()
sc=spark.sparkContext

nums = sc.parallelize([1, 2, 3, 4, 5])

print(nums.collect())

bytwo = nums.map(lambda x: x + 2)
print(bytwo.collect())

squared = nums.map(lambda x: x * x)
print(squared.collect())


numbers = sc.parallelize([1, 2, 3, 4])
mult = numbers.fold(1, (lambda x, y: x * y))
print(mult)



