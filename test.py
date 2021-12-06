#from pyspark.sql.functions import from_json
from pyspark.sql.types import StructField, StructType, StringType,DoubleType,TimestampType
from pyspark.sql import DataFrame
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json
import numpy as np
import pickle

from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import hour, month, year

from pyspark.sql.functions import col , udf

import warnings

from sklearn import naive_bayes
warnings.filterwarnings('ignore')

schema = StructType([
    StructField("feature0",StringType()),
    StructField("feature1",StringType()),
    StructField("feature2",StringType()),
    StructField("feature3",StringType()),
    StructField("feature4",StringType()),
    StructField("feature5",DoubleType()),
    StructField("feature6",DoubleType())
])

district = {'MISSION':0,'BAYVIEW':1,'CENTRAL':2,'TARAVAL':3,
'TENDERLOIN':4,'INGLESIDE':5,'PARK':6,'SOUTHERN':7,
'RICHMOND':8,'NORTHERN':9}

days = {'Wednesday':0,'Tuesday':1,'Friday':2,'Thursday':3,
'Saturday':4,'Monday':5,'Sunday':6}

def predictions(filename,df):
    loaded_model = pickle.load(open(filename, 'rb'))
    result = loaded_model.predict(df)
    print(result)

def readMyStream(rdd) :
  #rdd.pprint()
  line = rdd.collect()
  #print("line:",line)
  #create a df
  df = spark.createDataFrame(data=json.loads(line[0]).values(), schema=schema)


  district_func = udf(lambda row : district.get(row,row))
  df = df.withColumn("feature3", district_func(col("feature3")))
  day_func = udf(lambda row : days.get(row,row))
  df = df.withColumn("feature2", day_func(col("feature2")))

  df = df.withColumn("timestamp",to_timestamp(df.feature1))
  df=df.withColumn("Hour",hour(df.timestamp)).withColumn("Month",month(df.timestamp)).withColumn("Year",year(df.timestamp))
  cols = ['feature2','feature3','feature5','feature6','Hour','Month','Year']
  data = df.select([col for col in cols])
  #df = df.select['feature2','feature3','feature4','feature5','Hour','Month','Year']
  return data

def test(rdd):
    if not rdd.isEmpty():
        df = readMyStream(rdd)
        df.show()
        df = np.asarray(df.collect())
        print("NaiveBayes:")
        predictions("naive_bayes.sav",df)
        print("SGD:")
        predictions("sgd.sav",df)

    

#creating a spark context
sc = SparkContext(appName="crime")
ssc = StreamingContext(sc, batchDuration= 3)
spark = SparkSession.builder.getOrCreate()

#reading the stream
lines =  ssc.socketTextStream("localhost", 6100)

#divide into test_train 
lines.foreachRDD( lambda rdd: test(rdd) )
ssc.start()             

#wait till over
ssc.awaitTermination(timeout=264000)

ssc.stop()