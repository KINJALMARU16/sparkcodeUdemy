from pyspark import SparkConf,SparkContext 

conf = SparkConf().setAppName("word count")
sc = SparkContext(conf=conf)


lines = sc.textFile("/home/kinjal/spark-2.1.0-bin-hadoop2.7/sparkcode/Book.txt")
upper = lines.map(lambda  x : x.upper() )
words = upper.flatMap(lambda x : x.split())
here  = words.map(lambda x: (x,1)).reduceByKey(lambda x,y : (x+y)).sortByKey()
he = here.collect()
for t in he:
	print(t) 
