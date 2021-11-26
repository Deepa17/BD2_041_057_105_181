
#from pyspark.sql.functions import from_json
from pyspark.sql.types import StructField, StructType, StringType,DoubleType
from pyspark.sql import DataFrame
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json
import numpy as np

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import CountVectorizer

from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB

#from sklearn import preprocessing

from pyspark.ml.feature import StringIndexer

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
  #print("line:",line)
  #create a df
  df = spark.createDataFrame(data=json.loads(line[0]).values(), schema=schema)
  #df = spark.createDataFrame(data = spark.read.json(line[0]),schema=schema)--> this didnt work

  #select the required cols
  columns = ['feature0','feature1','feature3','feature4','feature6','feature7','feature8']
  df = df.select([col for col in columns])

  #encode the catergorical features
  categorical = ["feature1","feature3","feature4"]
  #renamed = ["category","day","district"]
  df = label_encode(df,"feature1","category")
  df = label_encode(df,"feature3","day")
  df = label_encode(df,"feature4","district")
  #drop the categorical cols
  df = df.drop(*categorical)


  df = tokenize(df,"feature6","Address")
  df = df.drop("feature6")

  return df


# split the data to test and train
def x_y(rdd):
  if not rdd.isEmpty():
    df = readMyStream(rdd)
    #print("DataFrame:")
    #df.show()

    x_col = ['feature7','feature8','day','district','Address']
    X = data = df.select([col for col in x_col])
    y = df.select('category')
    X = np.array(X.collect())
    y = np.array(y.collect())
    #print("Train and test are:")
    
    X_train, X_test, y_train, y_test=test_train(X,y)
    print(X_train)
    print(y_test)
    #return(X,y)

def test_train(X,y):
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.5, random_state=0)
  return(X_train, X_test, y_train, y_test)
#label encoding the categorical variables

def label_encode(df,feature,output_feature):
  encoder = StringIndexer(inputCol=feature,outputCol=output_feature)
  df = encoder.fit(df).transform(df)
  return df

def tokenize(df,feature,output_feature):
  tokenizer = Tokenizer(inputCol=feature, outputCol="words")
  df = tokenizer.transform(df)

  hashingTF = HashingTF(inputCol="words", outputCol=output_feature)
  df = hashingTF.transform(df)

  df = df.drop("words")
  return df


#def naive_bayes(X,y):
  

#createing a spark context
sc = SparkContext(appName="crime")
ssc = StreamingContext(sc, batchDuration= 3)
spark = SparkSession.builder.getOrCreate()

#reading the stream
lines =  ssc.socketTextStream("localhost", 6100)

#divide into test_train 
lines.foreachRDD( lambda rdd: x_y(rdd) )
ssc.start()             

#wait till over
ssc.awaitTermination()
ssc.stop()


#,Category,Descript,DayOfWeek,PdDistrict,Resolution,Address,X,Y


