import re
from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("CustomerAmount")
sc = SparkContext(conf = conf)

input = sc.textFile("/home/bjit-552/Downloads/Big Data/Course_Spark/Section2/Book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[0], x[1])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[1])
    word = result[0].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
