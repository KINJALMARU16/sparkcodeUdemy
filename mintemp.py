from pyspark import SparkConf,SparkContext

conf = SparkConf().setAppName("TempMin")
sc = SparkContext(conf=conf)

lines = sc.textFile("/home/kinjal/spark-2.1.0-bin-hadoop2.7/sparkcode/1800.csv")
#for t in parselines.take(10) :
#	print("HERE ")
#	print(t)
def parseline(lines):
	fields = lines.split(",")
	station = fields[0]
	types = fields[2]
	temp = fields[3]
	return (station ,types,temp)


parseLines = lines.map(parseline)
min_data = parseLines.filter(lambda x : "TMIN" in x[1])
#for t in min_data.take(10):
#	print(" FINAL : ") 
#	print(t)

avgdata = min_data.map(lambda x : (x[0], x[2] ))
data = avgdata.reduceByKey(lambda x, y  :min(x,y) )
t = data.collect()
for tst in t :
	print("HERE : ".format(tst))
