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

from pyspark.sql.functions import col , udf, monotonically_increasing_id

import warnings
warnings.filterwarnings('ignore')

from sklearn import naive_bayes

schema = StructType([
    StructField("feature0",StringType()),
    StructField("feature1",StringType()),
    StructField("feature2",StringType()),
    StructField("feature3",StringType()),
    StructField("feature4",StringType()),
    StructField("feature5",DoubleType()),
    StructField("feature6",DoubleType())
])

op_schema = StructType([
    StructField("id",StringType()),
    StructField("nb",StringType()),
    StructField("sgd",StringType())])

district = {'MISSION':0,'BAYVIEW':1,'CENTRAL':2,'TARAVAL':3,
'TENDERLOIN':4,'INGLESIDE':5,'PARK':6,'SOUTHERN':7,
'RICHMOND':8,'NORTHERN':9}

days = {'Wednesday':0,'Tuesday':1,'Friday':2,'Thursday':3,
'Saturday':4,'Monday':5,'Sunday':6}

def predictions(filename,df):
    loaded_model = pickle.load(open(filename, 'rb'))
    result = loaded_model.predict(df)
    #print(result)
    return result
    
def datatype(lis,dtype = int):
    return map(dtype,lis)

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
  lis = df.select('feature0').rdd.flatMap(lambda x: x).collect()
  return (data,lis)

def test(rdd):
    if not rdd.isEmpty():
        df,id = readMyStream(rdd)
        #data,id1 = readMyStream(rdd)
        #data.show()
        df = np.asarray(df.collect())
        #res = []
        print("NaiveBayes:")
        nb_res = (list(predictions("naive_bayes.sav",df)))
        print("SGD:")
        sgd_res = (list(predictions("sgd.sav",df)))
        #print("here2")
        nb_res = datatype(nb_res)
        sgd_res = datatype(sgd_res)
        res = list(zip(id,nb_res,sgd_res))
        data = res
        columns = ["id","NB","SGD"]
        #print(res)
        data1  = spark.createDataFrame(res,columns)
        data1.show()
        


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