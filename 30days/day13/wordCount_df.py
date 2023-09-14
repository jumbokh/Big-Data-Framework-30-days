# create SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# read text file as dataframe
text_df = spark.read.text("test.txt")

# word count program
from pyspark.sql.functions import split, explode

## split: split each line into list of words
## explode: expand list into single column
## alias: give the column a name "word" 
words_df = text_df.select(explode(split(text_df.value, " ")).alias("word"))

## count grouped words 
word_counts = words_df.groupBy("word").count()

# show results
word_counts.show()

# close SparkSession
spark.stop()