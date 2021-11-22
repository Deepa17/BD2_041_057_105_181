
#from pyspark.sql.functions import from_json
from pyspark.sql.types import StructField, StructType, StringType,DoubleType
from pyspark.sql import DataFrame
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json

#schema of the json file(note: the field names have to be proper)
schema = StructType([
    StructField("feature0",StringType()),
    StructField("feature1",StringType()),
    StructField("feature2",StringType()),
    StructField("feature3",StringType()),
    StructField("feature4",StringType()),
    StructField("feature5",StringType()),
    StructField("feature6",StringType()),
    StructField("feature7",DoubleType()),
    StructField("feature8",DoubleType())
])


#function to read the stream
def readMyStream(rdd) :
  #rdd.pprint()
  line = rdd.collect()
  print("line:",line)
  df = spark.createDataFrame(data=json.loads(line[0]).values(), schema=schema)
  #df = spark.createDataFrame(data = spark.read.json(line[0]),schema=schema)
  columns = ['feature0','feature2','feature3','feature4','feature6','feature7','feature8']
  df = df.select([col for col in columns])
  #df1 = df.collect()
  return df


# split the data to test and train
def train_test(rdd):
  if not rdd.isEmpty():
    df = readMyStream(rdd)
    print("DataFrame:")
    df.show()
    x_col = ['feature0','feature3','feature4','feature6','feature7','feature8']
    X = data = df.select([col for col in x_col])
    y = df.select('feature2')
    print("Train and test are:")
    X.show()
    y.show()
    return(X,y)

#createing a spark context
sc = SparkContext(appName="crime")
ssc = StreamingContext(sc, batchDuration= 3)
spark = SparkSession.builder.getOrCreate()

#reading the stream
lines =  ssc.socketTextStream("localhost", 6100)

#divide into test_train 
lines.foreachRDD( lambda rdd: train_test(rdd) )
ssc.start()             

#wait till over
ssc.awaitTermination()
ssc.stop()


#,Category,Descript,DayOfWeek,PdDistrict,Resolution,Address,X,Y


