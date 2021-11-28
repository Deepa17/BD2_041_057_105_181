
#from pyspark.sql.functions import from_json
from pyspark.sql.types import StructField, StructType, StringType,DoubleType,TimestampType
from pyspark.sql import DataFrame
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json
import numpy as np
#import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import hour, month, year

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

#splitting the data into test train set
def test_train(X,y):
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
  return(X_train, X_test, y_train, y_test)

#label encoding the categorical variables
def label_encode(df,feature,output_feature):
  encoder = StringIndexer(inputCol=feature,outputCol=output_feature)
  df = encoder.fit(df).transform(df)
  return df

#tokenizing the address
def tokenize(df,feature,output_feature):
  tokenizer = Tokenizer(inputCol=feature, outputCol="words")
  df = tokenizer.transform(df)

  hashingTF = HashingTF(inputCol="words", outputCol=output_feature)
  df = hashingTF.transform(df)

  df = df.drop("words")
  return df

#nb classifier
def naive_bayes(X_train, X_test, y_train, y_test,classes):
  from sklearn.naive_bayes import GaussianNB
  nb = GaussianNB()
  nb.partial_fit(X_train,y_train,classes)
  y_pred = nb.predict(X_test)
  metrics(y_pred,y_test)

#to return the metrics of the model 
def metrics(y_pred,y_true):
  from sklearn.metrics import accuracy_score
  from sklearn.metrics import classification_report

  target = list(np.unique(y_true))
  print(target)
  print("Accuracy: ",accuracy_score(y_pred,y_true))
  print("Classification_report:")
  print(classification_report(y_true,y_pred,labels=target))

#function to read the stream
def readMyStream(rdd) :
  #rdd.pprint()
  line = rdd.collect()
  #print("line:",line)
  #create a df
  df = spark.createDataFrame(data=json.loads(line[0]).values(), schema=schema)
  #df = spark.createDataFrame(data = spark.read.json(line[0]),schema=schema)--> this didnt work

  #select the required cols
  #columns = ['feature0','feature1','feature3','feature4','feature6','feature7','feature8']
  #df = df.select([col for col in columns])

  #encode the catergorical features
  categorical = ["feature1","feature3","feature4"]
  #renamed = ["category","day","district"]
  df = label_encode(df,"feature1","category")
  df = label_encode(df,"feature3","day")
  df = label_encode(df,"feature4","district")
  #drop the categorical cols
  df = df.drop(*categorical)

  
  #df = tokenize(df,"feature6","Address")
  #df = df.drop("feature6")
  #series = df.select(['Address']).rdd.map(lambda x : np.array(x.toArray())).as_matrix().reshape(-1,1)
  #op= np.appply_along_axis(lambda x:x[0],1,series)
  #vector_udf = udf(lambda vector: list(vector.toArray()))
  #df.select("Address").show()
  #vectors =  df.select("Address").rdd.map(lambda row: row.features.toArray())
  cols = df.columns
  #df.withColumn("Address",vector_udf((df['Address'])))
  #print(vectors)
  #df.rdd.mapValues(lambda x: [x.toArray()]).toDF(cols)
  #df.show()
  
  df = df.withColumn("timestamp",to_timestamp(df.feature0))
  df=df.withColumn("Hour",hour(df.timestamp)).withColumn("Month",month(df.timestamp)).withColumn("Year",year(df.timestamp))
  return df


# split the data to test and train
def x_y(rdd):
  
  df = readMyStream(rdd)
  #print("DataFrame:")
  df.show()

  x_col = ['feature7','feature8','day','district','Hour','Month','Year']
  X = data = df.select([col for col in x_col])
  y = df.select('category')
  X = np.asarray(X.collect())
  y = np.asarray(y.collect())
  print(X)
  print(y)
  return(X,y)

def model_train(rdd):
  if not rdd.isEmpty():
    X,y = x_y(rdd)
    classes = list(np.unique(y))
    print("Length:",len(classes))
    X_train, X_test, y_train, y_test=test_train(X,y)
    naive_bayes(X_train, X_test, y_train, y_test,classes)


#creating a spark context
sc = SparkContext(appName="crime")
ssc = StreamingContext(sc, batchDuration= 3)
spark = SparkSession.builder.getOrCreate()

#reading the stream
lines =  ssc.socketTextStream("localhost", 6100)

#divide into test_train 
lines.foreachRDD( lambda rdd: model_train(rdd) )
ssc.start()             

#wait till over
ssc.awaitTermination()
ssc.stop()


#,Category,Descript,DayOfWeek,PdDistrict,Resolution,Address,X,Y


