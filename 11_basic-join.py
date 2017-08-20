
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


R= sc.textFile(R)
S=sc.textFile(S)
r1 = R.map(lambda s: s.split(","))
print(r1.collect())

r2 = r1.flatMap(lambda s: [(s[0], s[1])])
print(r2.collect())


s1 = S.map(lambda s: s.split(","))


print(s1.collect())

s2 = s1.flatMap(lambda s: [(s[0], s[1])])

print(s2.collect())


RjoinedS = r2.join(s2)

print(RjoinedS.collect())