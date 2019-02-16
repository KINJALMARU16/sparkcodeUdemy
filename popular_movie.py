from pyspark import SparkConf ,SparkContext 

conf = SparkConf().setAppName(" popular Movie ")
sc = SparkContext(conf=conf)


names  = sc.textFile("/home/kinjal/spark-2.1.0-bin-hadoop2.7/sparkcode/Marvel-Names.txt")
graph = sc.textFile("/home/kinjal/spark-2.1.0-bin-hadoop2.7/sparkcode/Marvel-Graph.txt")

def countoccur(line):
	fields = line.split()
	return (int(fields[0]),len(fields)-1)

pair = graph.map(countoccur)
print(pair)


