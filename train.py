#!/usr/bin/python3
#import the required packages
from pyspark.sql.types import StructField, StructType, StringType,DoubleType,TimestampType
from pyspark.sql import DataFrame
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json
import numpy as np
import pickle

#filter out the warnings
import warnings
warnings.filterwarnings('ignore')


#for preprocessing
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import hour, month, year

from pyspark.sql.functions import col , udf

#training the models
#classification
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn import linear_model

#clustering
from sklearn.cluster import MiniBatchKMeans, KMeans
from sklearn.metrics.pairwise import pairwise_distances_argmin


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

#for manual label encoding
categories = {'FRAUD':0,'SUICIDE':1,'SEX OFFENSES FORCIBLE':2,
'LIQUOR LAWS':3,'SECONDARY CODES':4,'FAMILY OFFENSES':5,'MISSING PERSON':6,
'OTHER OFFENSES':7,'DRIVING UNDER THE INFLUENCE':8,'WARRANTS':9,
'ARSON':10,'SEX OFFENSES NON FORCIBLE':11,'FORGERY/COUNTERFEITING':12,
'GAMBLING':13,'BRIBERY':14,'ASSAULT':15,'DRUNKENNESS':16,
'EXTORTION':17,'TREA':18,'WEAPON LAWS':19,'LOITERING':20,
'SUSPICIOUS OCC':21,'ROBBERY':22,'PROSTITUTION':23,
'EMBEZZLEMENT':24,'BAD CHECKS':25,'DISORDERLY CONDUCT':26,
'RUNAWAY':27,'RECOVERED VEHICLE':28,'VANDALISM':29,'DRUG/NARCOTIC':30,
'PORNOGRAPHY/OBSCENE MAT':31,'TRESPASS':32,'VEHICLE THEFT':33,
'NON-CRIMINAL':34,'STOLEN PROPERTY':35,'LARCENY/THEFT':36,'KIDNAPPING':37,
'BURGLARY':38}

district = {'MISSION':0,'BAYVIEW':1,'CENTRAL':2,'TARAVAL':3,
'TENDERLOIN':4,'INGLESIDE':5,'PARK':6,'SOUTHERN':7,
'RICHMOND':8,'NORTHERN':9}

days = {'Wednesday':0,'Tuesday':1,'Friday':2,'Thursday':3,
'Saturday':4,'Monday':5,'Sunday':6}

#classifier models
nb = GaussianNB(priors = None, var_smoothing = 1e-04)
sgd = linear_model.SGDClassifier()
pac = linear_model.PassiveAggressiveClassifier()

#clustering
iteration = 1
batch_size = 100
n_clusters = len(categories)

# perform the mini batch K-means
mbk = MiniBatchKMeans(init ='k-means++', n_clusters = n_clusters,
                      batch_size = batch_size)

#saving the final weights
def save_model():
  filename = 'naive_bayes.sav'
  pickle.dump(nb, open(filename, 'wb'))
  filename = 'sgd.sav'
  pickle.dump(sgd, open(filename, 'wb'))
  filename = 'pac.sav'
  pickle.dump(pac, open(filename, 'wb'))
  filename = 'mbk.sav'
  pickle.dump(mbk, open(filename, 'wb'))



#splitting the data into test train set
def test_train(X,y):
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
  return(X_train, X_test, y_train, y_test)


#nb classifier
def naive_bayes(X_train, X_test, y_train, y_test,classes,nb):
  nb.partial_fit(X_train,y_train,classes = classes)
  y_pred = nb.predict(X_test)
  print()
  print()
  print("-------------------- NAIVE BAYES --------------------")
  acc = metrics(y_pred,y_test,classes)
  return acc

#SGD classifier
def stgd(X_train, X_test, y_train, y_test,classes,sgd):
  sgd.partial_fit(X_train,y_train,classes = classes)
  y_pred = sgd.predict(X_test)
  print()
  print()
  print("-------------------- SGD CLASSIFIER --------------------")
  acc = metrics(y_pred,y_test,classes)
  return acc

#passive aggressive classifier 
def passive_agg(X_train, X_test, y_train, y_test, classes, pac):
  pac.partial_fit(X_train,y_train,classes = classes)
  y_pred = pac.predict(X_test)
  print()
  print()
  print("-------------------- PASSIVE AGGRESSIVE CLASSIFIER --------------------")
  acc = metrics(y_pred,y_test,classes)
  return acc

#clustering
def minibatch(X_train, X_test, y_train, y_test, classes, mbb):
  mbk.partial_fit(X_train,y_train)
  y_pred = pac.predict(X_test)
  print()
  print()
  print("-------------------- MINI BATCH KMEANS CLUSTERING --------------------")
  acc = metrics(y_pred,y_test,classes)
  return acc

#to return the metrics of the model 
def metrics(y_pred,y_true,classes):
  from sklearn.metrics import accuracy_score
  from sklearn.metrics import classification_report
  acc = accuracy_score(y_pred,y_true)
  print("Accuracy: ",acc)
  print("Classification_report:")
  print(classification_report(y_true,y_pred,labels = classes))
  return acc

#function to read the stream
def readStream(rdd) :
  #rdd.pprint()
  line = rdd.collect()
  df = spark.createDataFrame(data=json.loads(line[0]).values(), schema=schema)

  #encode the categorical variables
  categ_func = udf(lambda row : categories.get(row,row))
  df = df.withColumn("feature1", categ_func(col("feature1")))
  district_func = udf(lambda row : district.get(row,row))
  df = df.withColumn("feature4", district_func(col("feature4")))
  day_func = udf(lambda row : days.get(row,row))
  df = df.withColumn("feature3", day_func(col("feature3")))

  #extracting the month annd hour from the date column
  df = df.withColumn("timestamp",to_timestamp(df.feature0))
  df=df.withColumn("Hour",hour(df.timestamp)).withColumn("Month",month(df.timestamp)).withColumn("Year",year(df.timestamp))
  return df


# split the data to test and train
def x_y(rdd):
  df = readStream(rdd)
  print("DataFrame:")
  df.show()
  #getting the required columns
  x_col = ['feature3','feature4','feature7','feature8','Hour','Month','Year']
  X = data = df.select([col for col in x_col])
  y = df.select('feature1')
  X = np.asarray(X.collect())
  y = np.asarray(y.collect())
  return(X,y)

#training the model
def model_train(rdd):
  if not rdd.isEmpty():
    X,y = x_y(rdd)
    classes = [str(i) for i in range(39)]
    #print("classes: ",classes)
    X_train, X_test, y_train, y_test=test_train(X,y)
    
    #the models are trained and accuracy is obtained
    nb_acc = naive_bayes(X_train, X_test, y_train, y_test,classes,nb)
    stgd_acc = stgd(X_train, X_test, y_train, y_test,classes,sgd)
    pac_acc = passive_agg(X_train, X_test, y_train, y_test,classes,pac)
    mbk_acc = minibatch(X_train, X_test, y_train, y_test, classes,mbk)

    #writing the accuracies to a file
    file = open('acc.txt','a')
    file.write(str(nb_acc)+","+str(stgd_acc)+","+str(pac_acc)+","+str(mbk_acc)+"\n")
    file.close()
    #saving the weights 
    save_model()

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
ssc.awaitTermination(timeout=110)

ssc.stop()

#columns of train.csv
#Date,Category,Descript,DayOfWeek,PdDistrict,Resolution,Address,X,Y


