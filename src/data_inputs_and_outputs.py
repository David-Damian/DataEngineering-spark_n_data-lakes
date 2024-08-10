import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

# set a spark session and its config
spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

# print spark config

print(spark.SparkConfig)

# data path of log data
data_path = '../../data/sparkify_log_small.json'

# read data
user_log = spark.read.json(data_path)

# print schema
user_log.printSchema()

"""
OBSERVATRION: If a file is stored in cloud storage, you can use 
the same methods. When specifying the file path we just need to 
make sure that we are pointing to the cloud storage path that stores 
our target file.
"""
## show first rows

user_log.select(['artist', 'song']).show(n = 5)

## or using take methos

user_log.take(5)


# seeing some statistics of length variable
print(
    log_songs
    .describe(['length'])
    .show())

### WRITE DATA ON CSV

out_path = "../../data/sparkify_log_small.csv"

user_log.write.mode('overwrite').save(out_path)

## READING DATA FROM THE CSV

user_log2 = spark.read.option('header', True).csv(out_path)

user_log2.printSchema()
