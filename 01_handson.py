
import findspark
findspark.init('/Users/vishnoiprem/software/spark-2.2.0-bin-hadoop2.7')


from pyspark.sql import SparkSession

file_name="/Users/vishnoiprem/interview/spark_example/spark/examples/src/main/resources/test.txt"
print( file_name)

spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()

sc=spark.sparkContext

rdd=sc.textFile(file_name)

#print(rdd.collect())

print('rrd.first',rdd.first())
print('rdd.take',rdd.take(2))


print('*****random sample***************')
print(rdd.takeSample(False,11,2))

print(rdd.count())


print(rdd.map(lambda x: x.split(',')).take(5))
print(rdd.flatMap(lambda x: x.split(',')).take(5))
print(rdd.flatMap(lambda x: x.split(',')).collect())


print('************Filter***********')
print(rdd.filter(lambda line : ( 'Michael' in line)).collect())





sc.stop()

