# TFIDF Search With Spark

My system uses Apache Spark with HDFS for creating the TFIDF index and searching for queries.
I am using the cricket corpus for this project.
t first loads the documents as separate records and then tokenizes each record and calculates the count of each word per document (TF).
Then it calculates the number of distinct documents for each term (DF), IDF, and TFIDF index.

Once the TFIDF index is built, my system is able to take any query and tokenize the query the same way it would tokenize
any document in the corpus and then conduct the search.

Part 1 (Computing the TFIDF score):

![image](https://user-images.githubusercontent.com/1936040/50247997-7b9fc200-03a7-11e9-9833-dc88dc6fd386.png)

Part 2 (Search) :

![image](https://user-images.githubusercontent.com/1936040/50248034-a853d980-03a7-11e9-8a0f-d14b8adce5b9.png)
