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

# val = os.environ.get('SUBSCRIPTION_ID')
# val1 = os.getenv('SESSION_MANAGER')
# print(val, val1)

data = pd.read_csv("/home/admin1/Desktop/PythonVM/AzureDataFactory/train.csv",encoding="ISO-8859-1")
#print(data)
sentence = []
for i in range(len(data)):
    sentence.append((data['SentimentText'][i]))

#print(sentence)    

corpus = []
for i in range(0, len(data)):
    sentiment = re.sub('[^a-zA-Z]', ' ', data['SentimentText'][i])
    review = sentiment.split()
    review = word_tokenize(sentiment)
    ps = PorterStemmer()

    [ps.stem(word) for word in review
     if not word in set(stopwords.words('english'))]

    review = ' '.join(review)

    corpus.append(review)

cv = CountVectorizer(max_features = 1500)
X = cv.fit_transform(corpus).toarray()
y = data.iloc[:, 2].values    
#print(y)

X_train = X
y_train = y

model = RandomForestClassifier(n_estimators= 100, criterion='entropy')
model.fit(X_train,y_train)



test_data = pd.read_csv("/home/admin1/Desktop/PythonVM/AzureDataFactory/test.csv",encoding="ISO-8859-1")

test_sentence = []
for i in range(len(data)):
    test_sentence.append((test_data['SentimentText'][i]))

test_corpus = []
for i in range(0, len(data)):
    test_sentiment = re.sub('[^a-zA-Z]', ' ', data['SentimentText'][i])
    test_review = test_sentiment.split()
    test_review = word_tokenize(sentiment)
    test_ps = PorterStemmer()

    [test_ps.stem(word) for word in test_review
     if not word in set(stopwords.words('english'))]

    test_review = ' '.join(test_review)

    test_corpus.append(test_review)

test_cv = CountVectorizer(max_features = 1500)
X_test = test_cv.fit_transform(test_corpus).toarray()    

y_pred = model.predict(X_test)
print(y_pred)