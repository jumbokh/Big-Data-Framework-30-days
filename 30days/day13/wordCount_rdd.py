# Create SparkContext
from pyspark import SparkContext
sc = SparkContext("local", "WordCountExample")

# Create SparkSession from SparkContext
from pyspark.sql import SparkSession
spark = SparkSession(sc)

# read as rdd
text_file = sc.textFile("test.txt")

# Map Reduce
word_counts = text_file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

# print results
result = word_counts.collect()
for (word, count) in result:
    print(f"{word}: {count}")

# close SparkContext
sc.stop()