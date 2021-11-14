#preprocess.py WORKING FILES

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import sys
#sc = SparkContext(master, appName)
spark = SparkContext.getOrCreate()
ssc = StreamingContext(spark,2)
#lines = spark.readStream.format("socket").option("host","localhost").option("port", 6100).load()
#sqlContext = SQLContext(spark)
lines = ssc.socketTextStream("localhost",6100)
words = lines.flatMap(lambda line: line.split("},"))
words.pprint()
ssc.start() # Start the computation
ssc.awaitTermination() 
