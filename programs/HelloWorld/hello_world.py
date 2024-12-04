# Necessary imports
from pyspark.sql import SparkSession

# Spark object
spark = SparkSession.builder.master('local[*]').appName('HelloWorldRDD').getOrCreate()

# Converting text file to RDD
rdd = spark.sparkContext.textFile('./../../source/HelloWorld/sample.txt')

# Flattening the RDD
flat_mapped_rdd = rdd.flatMap(lambda x : x.split(" "))

# Filtering only alphanumeric words
filtered_rdd = flat_mapped_rdd.filter(lambda x : x.isalnum())

# Mapping word to tuple with starting value 1
mapped_rdd = flat_mapped_rdd.map(lambda x : (x, 1))

# Reducing by adding the count if key is same
reduced_rdd = mapped_rdd.reduceByKey(lambda x, y : x + y)

print(f"The partitions available : {reduced_rdd.getNumPartitions()}")

# Writing to a text file with only one partition
reduced_rdd.coalesce(1).saveAsTextFile("./../../outputs/HelloWorld/rdd_word_count.txt")

# Stopping the spark session
spark.stop()

##  Notes

# Local Mode : CPU cores = no.of.partitions (default)
# Cluster Mode : total number of executor cores across the cluster = no.of.partitions (default)
#
# repartition() : A full shuffle is triggered
# coalesce() : avoids the full shuffle (only supports reducing)

# Spark often defaults to 200 partitions for the shuffle
# For HDFS, the default block size is typically 128 MB