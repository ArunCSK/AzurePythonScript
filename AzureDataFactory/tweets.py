# Databricks notebook source
storage_account_name = "arunstorage12"
storage_account_access_key = "iFCTVZveS/XvhhHfL/Phpf/r3UM3CPwSBkEwiQWePdALeW9hamYc6mAEXQMeSjQVrAdCY19hfFlUBLmKbwsbog=="


spark.conf.set(
 "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
 storage_account_access_key)


file_location = "wasbs://aruncontainer@arunstorage12.blob.core.windows.net/train.csv"
file_location_test = "wasbs://aruncontainer@arunstorage12.blob.core.windows.net/test.csv"
#file_location = @input
print(file_location)
file_type = "csv"

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
df.show()

# COMMAND ----------

data = df.toPandas()
#print(data)
sentence = []
for i in range(len(data)):
    sentence.append((data['_c2'][i]))
print(sentence)        

# COMMAND ----------

import os
import re
import pandas as pd
from nltk.tokenize import sent_tokenize, word_tokenize, PunktSentenceTokenizer
from nltk.corpus import stopwords, state_union
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, accuracy_score


# COMMAND ----------

import nltk
nltk.download('punkt')
nltk.download('stopwords')

# COMMAND ----------

corpus = []
for i in range(0, len(data)):
    sentiment = re.sub('[^a-zA-Z]', ' ', data['_c2'][i])
    review = sentiment.split()
    review = word_tokenize(sentiment)
    ps = PorterStemmer()

    [ps.stem(word) for word in review
     if not word in set(stopwords.words('english'))]

    review = ' '.join(review)

    corpus.append(review)


# COMMAND ----------

cv = CountVectorizer(max_features = 10)
X = cv.fit_transform(corpus).toarray()

# COMMAND ----------

y = data.iloc[:, 2].values    
#print(y)

X_train = X
y_train = y

# COMMAND ----------

model = RandomForestClassifier(n_estimators= 5, criterion='entropy')

# COMMAND ----------

model.fit(X_train,y_train)

# COMMAND ----------

import pickle
filename = "tweet.pkl"
pklfile = open(filename, 'wb')
mdl = pickle.dump(model, pklfile)
pklfile.close()

# COMMAND ----------

display(dbutils.fs.ls("file:/databricks/driver/"))

# COMMAND ----------

spark.conf.set(
 "fs.azure.account.key.azurestorageusingscript.blob.core.windows.net",
 storage_account_access_key)



dbutils.fs.cp("file:/databricks/driver/tweet.pkl","/mnt/Tweetfile")

