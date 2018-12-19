import re
import math
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType, FloatType
from pyspark.sql import SparkSession

# Q1 - Computing TFIDF scores
# ---------------------------
# Build Spark session
spark = SparkSession.builder.getOrCreate()
# Omit all logs except errors
spark.sparkContext.setLogLevel('ERROR')
# Read each file in cricket folder as a separate record
rdd = spark.sparkContext.wholeTextFiles('/user/root/Final/cricket/')
# Suppress hortonworks path prefix from filename and create
# data frame with 2 columns ('doc' and 'text')
data = rdd.map(lambda x: (x[0].replace('hdfs://sandbox-hdp.hortonworks.com:8020', ''), x[1])).toDF(['doc', 'text'])
# Get total document count
total_docs = data.count()

# utility method for tokenizing a piece of text
def tokenize(text):
    return re.findall('\\w+', text.lower())
# Register the tokenize method as a udf
tokenize_udf = F.udf(tokenize, ArrayType(StringType()))
# tokenize all the text
data = data.select(['doc', tokenize_udf('text').alias('text')])
# make 1 separate row for each token
data_tokens = data.withColumn("token", F.explode('text'))

# calculate term frequency
tf = data_tokens.groupBy('doc', 'token').agg(F.count('text').alias('tf'))
# calculate document frequency
df = data_tokens.groupBy('token').agg(F.countDistinct('doc').alias('df'))

# utility method for calculating inverse document frequency
def inverse_doc_frequency(doc_frequency):
    return math.log((total_docs + 1) * 1.0 / (doc_frequency + 1))

# register inverse document frequency as a udf
inverse_doc_frequency_udf = F.udf(inverse_doc_frequency, FloatType())
# calculate the inverse document frequency
idf = df.withColumn('idf', inverse_doc_frequency_udf('df'))
# calculate tfidf
tfidf = tf.join(idf, 'token').withColumn('tfidf', F.col('tf') * F.col('idf'))
# show 10 rows from tfidf index
# tfidf.show(10, False)

# Q2 Retrieve the top N matching documents with a score
# -----------------------------------------------------
# 2a search
# utility method for searching query
def search(query, N):
    # tokenize query into terms
    terms = tokenize(query)
    # create a dataframe with each term as a separate row
    query_tokens = spark.createDataFrame(terms, StringType()).withColumnRenamed('value', 'token')
    # get aggregated score and count for each document for all the matched tokens
    result = query_tokens.join(tfidf, 'token').groupBy('doc').agg(F.sum('tfidf').alias('score_sum'), F.count('tfidf').alias('matched_terms'))
    # calculate document score
    result = result.withColumn('score', F.col('score_sum') * F.col('matched_terms') / len(terms))
    # show top N documents
    result.select('doc', 'score').sort(F.col('score').desc()).show(N, False)


# for searching
# search('Bangladesh', 10)

# 2b - real-time search
# for near real time
# persist the tfidf index into memory after the next search
tfidf.persist()

# and then search
search('Bangladesh wins', 1)
search('Bangladesh vs India', 1)

search('Bangladesh wins', 3)
search('Bangladesh vs India', 3)

search('Bangladesh wins', 5)
search('Bangladesh vs India', 5)
