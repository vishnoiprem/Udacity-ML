
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

rdd=sc.textFile(file_name,1)

print(rdd.collect())
words=rdd.flatMap(lambda lines: lines.split(' '))

print(words.collect())

ones=words.map(lambda x : (x,1))

print(ones.collect())

counts=ones.reduceByKey(lambda x,y: x+y)

print(counts.collect())


frequencies=rdd.flatMap(lambda x : x.split(' ')).map(lambda x : (x,1)).reduceByKey(lambda x,y: x+y)
print(frequencies)
if [os.path.exists('/Users/vishnoiprem/interview/spark_example/pyspark-tutorial/tutorial/wordcount/counts')]:
	print('pem')
	os.popen('rm -rf /Users/vishnoiprem/interview/spark_example/pyspark-tutorial/tutorial/wordcount/counts/')


ones.saveAsTextFile('/Users/vishnoiprem/interview/spark_example/pyspark-tutorial/tutorial/wordcount/counts')



