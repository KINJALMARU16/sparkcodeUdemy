from pyspark import SparkConf , SparkContext
conf = SparkConf().setAppName("Assignment 1")
sc = SparkContext(conf=conf)


lines = sc.textFile("/home/kinjal/spark-2.1.0-bin-hadoop2.7/sparkcode/customer-orders.csv")
def total_field(lines):
	fields = lines.split(',')
	id = int(fields[0])
	time = float(fields[2])
	return (id,time)

total = lines.map(total_field).reduceByKey(lambda x,y : x+y).sortByKey()
final = total.collect()
for t in final:
	print(t )



