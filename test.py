#import the required packages
from pyspark.sql.types import StructField, StructType, StringType,DoubleType,TimestampType
from pyspark.sql import DataFrame
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json
import numpy as np
import pickle

#import preprocessing function
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import hour, month, year
from pyspark.sql.functions import col , udf

#supress warnings
import warnings
warnings.filterwarnings('ignore')



#schema of the test dataset
schema = StructType([
    StructField("feature0",StringType()),
    StructField("feature1",StringType()),
    StructField("feature2",StringType()),
    StructField("feature3",StringType()),
    StructField("feature4",StringType()),
    StructField("feature5",DoubleType()),
    StructField("feature6",DoubleType())
])

#manual label encoding 
district = {'MISSION':0,'BAYVIEW':1,'CENTRAL':2,'TARAVAL':3,
'TENDERLOIN':4,'INGLESIDE':5,'PARK':6,'SOUTHERN':7,
'RICHMOND':8,'NORTHERN':9}

days = {'Wednesday':0,'Tuesday':1,'Friday':2,'Thursday':3,
'Saturday':4,'Monday':5,'Sunday':6}

#decoding the categories
categories = {0:'FRAUD',1:'SUICIDE',2:'SEX OFFENSES FORCIBLE',
3:'LIQUOR LAWS',4:'SECONDARY CODES',5:'FAMILY OFFENSES',6:'MISSING PERSON',
7:'OTHER OFFENSES',8:'DRIVING UNDER THE INFLUENCE',9:'WARRANTS',
10:'ARSON',11:'SEX OFFENSES NON FORCIBLE',12:'FORGERY/COUNTERFEITING',
13:'GAMBLING',14:'BRIBERY',15:'ASSAULT',16:'DRUNKENNESS',
17:'EXTORTION',18:'TREA',19:'WEAPON LAWS',20:'LOITERING',
21:'SUSPICIOUS OCC',22:'ROBBERY',23:'PROSTITUTION',
24:'EMBEZZLEMENT',25:'BAD CHECKS',26:'DISORDERLY CONDUCT',
27:'RUNAWAY',28:'RECOVERED VEHICLE',29:'VANDALISM',30:'DRUG/NARCOTIC',
31:'PORNOGRAPHY/OBSCENE MAT',32:'TRESPASS',33:'VEHICLE THEFT',
34:'NON-CRIMINAL',35:'STOLEN PROPERTY',36:'LARCENY/THEFT',37:'KIDNAPPING',
38:'BURGLARY'}


#performing the predictions
def predictions(filename,df):
    loaded_model = pickle.load(open(filename, 'rb'))
    result = loaded_model.predict(df)
    #print(result)
    return result

#changing the datatype of the predicted labels from str to int    
def datatype(lis,dtype = int):
    return map(dtype,lis)


#read the test data from the stream
def readStream(rdd) :
 
  line = rdd.collect()
  df = spark.createDataFrame(data=json.loads(line[0]).values(), schema=schema)

  #preprocessing
  district_func = udf(lambda row : district.get(row,row))
  df = df.withColumn("feature3", district_func(col("feature3")))
  day_func = udf(lambda row : days.get(row,row))
  df = df.withColumn("feature2", day_func(col("feature2")))
  
  #extracting the required numbers from the timestamp
  df = df.withColumn("timestamp",to_timestamp(df.feature1))
  df=df.withColumn("Hour",hour(df.timestamp)).withColumn("Month",month(df.timestamp)).withColumn("Year",year(df.timestamp))
  #getting the required columns
  cols = ['feature2','feature3','feature5','feature6','Hour','Month','Year']
  data = df.select([col for col in cols])
  
  #get the ids to make the final predictions
  lis = df.select('feature0').rdd.flatMap(lambda x: x).collect()
  return (data,lis)

#running the models for prediction
def test(rdd):
    if not rdd.isEmpty():
        df,id = readStream(rdd)
        df = np.asarray(df.collect())
       
        #nb model
        print("NaiveBayes:")
        nb_res = (list(predictions("naive_bayes.sav",df)))
        
        #SGD
        print("SGD:")
        sgd_res = (list(predictions("sgd.sav",df)))
        
        #pac 
        print("PAC:")
        pac_res = (list(predictions("pac.sav",df)))
        
        nb_res = datatype(nb_res)
        sgd_res = datatype(sgd_res)
        pac_res = datatype(pac_res)

        res = list(zip(id,nb_res,sgd_res,pac_res))
        data = res
        columns = ["id","NB","SGD","PAC"]
        

        data = spark.createDataFrame(res,columns)
        categ_func = udf(lambda row : categories.get(row,row))
        data = data.withColumn("NB", categ_func(col("NB")))
        data = data.withColumn("SGD", categ_func(col("SGD")))
        data = data.withColumn("PAC", categ_func(col("PAC")))
        data.show()
        


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
ssc.awaitTermination(timeout=300)

ssc.stop()