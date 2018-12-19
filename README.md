# TFIDF Search With Spark

My system uses Apache Spark with HDFS for creating the TFIDF index and searching for queries.
I am using the cricket corpus for this project.
t first loads the documents as separate records and then tokenizes each record and calculates the count of each word per document (TF).
Then it calculates the number of distinct documents for each term (DF), IDF, and TFIDF index.

Once the TFIDF index is built, my system is able to take any query and tokenize the query the same way it would tokenize
any document in the corpus and then conduct the search.

