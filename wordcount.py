from pyspark.sql import SparkSession
import re

# Spark session & context
spark = SparkSession.builder.master('local').getOrCreate()
sc = spark.sparkContext

lines = sc.textFile("lai-eliduc.txt")
lines_split = lines.flatMap(lambda line: re.split(r"\W+", line))
words_in_line = lines_split.map(lambda word: (word, 1))
words_counter = words_in_line.reduceByKey(lambda a,b: a + b)

count = words_counter.sortBy(lambda x: x[1], ascending=False).collect()
result = sc.parallelize(count).saveAsTextFile("nbmots")