from pyspark import SparkContext,SparkConf

#sc = SparkContext(appName = "avgFrnds")
conf = SparkConf().setAppName("Read Text to RDD - Python")
sc = SparkContext(conf=conf)

def parselines(lines):
  fields = lines.split(",")
  age = int(fields[2])
  friends = int(fields[3])
  return (age,friends)
lines = sc.textFile("/home/kinjal/spark-2.1.0-bin-hadoop2.7/sparkcode/fakefriends.csv")
rdd = lines.map(parselines)
test = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x , y : (x[0]+y[0],x[1]+y[1]))

finalData = test.mapValues(lambda x : (x[0]/x[1]))

for k in finalData.take(10):
  print(k)
  print("FIRSTTTTTTC :::")






