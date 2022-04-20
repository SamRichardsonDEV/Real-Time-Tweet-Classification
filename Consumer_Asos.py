from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from pyspark.ml import PipelineModel

import string
import re
from datetime import datetime
#from pyspark.streaming.kafka import KafkaUtils

kafka_topic = 'twitter_events'

spark = SparkSession \
    .builder \
    .appName('TwitterStream') \
    .config("spark.cassandra.connection.host", "localhost:9092") \
    .getOrCreate()
	
df = spark \
  .readStream \
  .format('kafka') \
  .option('kafka.bootstrap.servers', 'localhost:9092') \
  .option('subscribe', kafka_topic) \
  .option('startingOffsets', 'latest') \
  .option('failOnDataLoss', 'false') \
  .load()

df = df.selectExpr('CAST(value AS STRING)', 'timestamp')

schema = StructType().add('created_at', StringType()).add('text', StringType()).add('user',StructType().add('screen_name', StringType())) 

df = df.select(from_json(col('value'), schema).alias('tweet'), 'timestamp')
df = df.select('tweet.created_at', 'tweet.user.screen_name', 'tweet.text')

date_process = udf(
    lambda x: datetime.strftime(
        datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'
        )
    )
df = df.withColumn("created_at", date_process(df.created_at))

urlPattern = r"((http://)[^ ]*|(https://)[^ ]*|( www\.)[^ ]*)"

def preprocess_text(x):
    x = str(x)
    x = re.sub(urlPattern,'URL',x)
    for c in string.punctuation:
        x = x.replace(c,"")
    return x

pre_process = udf(lambda x: preprocess_text(x))
df = df.withColumn("cleaned_data", pre_process(df.text))

pipeline_model = PipelineModel.load('hdfs://localhost:9000/training_data/asos/models')
prediction = pipeline_model.transform(df)
prediction = prediction.select(col("created_at"), col("screen_name"), col("text"), col("prediction"))

random_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
predictionWithIDs = prediction.withColumn('uuid', random_udf())

def writeToCassandra(writeDF, epochId):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="asos", keyspace="stuff") \
    .mode("append") \
    .save()

query = predictionWithIDs.writeStream \
.trigger(processingTime="5 seconds") \
.outputMode("update") \
.foreachBatch(writeToCassandra) \
.start()

query.awaitTermination()