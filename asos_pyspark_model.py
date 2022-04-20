#Import Packages / Dependencies
import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col

import re
import string

from pyspark.ml.feature import RegexTokenizer, CountVectorizer, IDF
from pyspark.ml.feature import StopWordsRemover, StringIndexer
from pyspark.ml.classification import NaiveBayes, LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

#initialise spark-session and spark-context

spark = SparkSession.builder.appName('demo').master("local").enableHiveSupport().getOrCreate()
context = SQLContext(spark)

#Import File and Work Directory

df = spark.read.load('hdfs://localhost:9000/training_data/asos/training_data_nlp.csv', format="csv", sep=",", inferSchema="true", header="true")
df.show()

# Remove URLs
urlPattern = r"((http://)[^ ]*|(https://)[^ ]*|( www\.)[^ ]*)"

def preprocess_text(x):
    x = str(x)
    x = re.sub(urlPattern,'URL',x)
    for c in string.punctuation:
        x = x.replace(c,"")
    return x

pre_process = udf(lambda x: preprocess_text(x))
df = df.withColumn("cleaned_data", pre_process(df.text))

#Train/Test Split and Create Pipeline/Model.

train, test = df.randomSplit([0.8,0.2],seed = 100)

tokenize = RegexTokenizer(inputCol="cleaned_data", outputCol="dirty_words")
remover = StopWordsRemover(inputCol="dirty_words", outputCol="words")
vector_tf = CountVectorizer(inputCol="words", outputCol="tf")
idf = IDF(inputCol="tf", outputCol="features", minDocFreq=3)
label_indexer = StringIndexer(inputCol = "sentiment", outputCol = "label")
SVC = LinearSVC(maxIter=10, regParam=0.1)

pipeline = Pipeline(stages=[tokenize, remover, vector_tf, idf, label_indexer, SVC])

trained_model = pipeline.fit(train)

prediction_df = trained_model.transform(test)

#Evaluate Model

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(prediction_df)
print("Test set accuracy = ", str(accuracy))

#Export Model

trained_model.write().overwrite().save('hdfs://localhost:9000/training_data/asos/models')