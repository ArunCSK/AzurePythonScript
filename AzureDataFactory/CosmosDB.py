# Databricks notebook source
import pandas as pd
import pickle
from sklearn.linear_model import LinearRegression
import numpy as np

# COMMAND ----------

#dbutils.widgets.text("input", "","")
#dbutils.widgets.get("input")
#FilePath = getArgument("input")
#print ("Param -\'input':")
#print (FilePath)

storage_account_name = "ramtest12"
storage_account_access_key = "FkCgh5PffhWOEFuLxnKyyD5+lIqQMAVGuBcxKXW+KDjxclLxfk6206uhCvEOOLWotMFIsQSJLmvA/zecXsZe2Q=="

spark.conf.set(
 "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
 storage_account_access_key)

file_location = "wasbs://ramcontainer@ramtest12.blob.core.windows.net/7210_1.csv"
print(file_location)
file_type = "csv"

df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
df.head()

# COMMAND ----------

import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as error

# COMMAND ----------

HOST='https://ramcomsosdb.documents.azure.com:443/'
MASTER_KEY='qUWAuRlx9uX8YhmwPsuEgmo1AeqE8gpFWsK2O7LCpVAlV1JwpxPvfKi9dFSkW9DQO99sOPH1T117z8d6Tbxktg=='
DATABASE_ID='Iot'
COLLECTION_ID='cosdb'
#DOCUMENT_ID='Date'


# COMMAND ----------

client = cosmos_client.CosmosClient(HOST,{'masterKey': MASTER_KEY})
#client = document_client.DocumentClient(HOST,{'masterKey': MASTER_KEY})
database_link='dbs/'+DATABASE_ID
collection_link = database_link + '/colls/' + COLLECTION_ID
#client.CreateDatabase({"id":DATABASE_ID})
#client.CreateContainer(database_link, {"id": COLLECTION_ID})
#document = df.set_index("_c0").T.to_dict('list')

writeConfig = {
  "Endpoint": "https://ramcomsosdb.documents.azure.com:443/",
  "Masterkey": "qUWAuRlx9uX8YhmwPsuEgmo1AeqE8gpFWsK2O7LCpVAlV1JwpxPvfKi9dFSkW9DQO99sOPH1T117z8d6Tbxktg==",
  "Database": "Iot",
  "Collection": "cosdb",
  "query_custom": "SELECT * FROM c"
}
from pandas import DataFrame
#df.write.format('com.microsoft.azure.cosmosdb.spark').mode('overwrite').options(**writeConfig).save()
dt = df.toPandas().iloc[1:21,:]
#print(dt)
data = dt.set_index("_c0").T.to_dict('list')
print(data)


# COMMAND ----------


collection_link = database_link + '/colls/{0}'.format('cosdb')
collection = client.ReadContainer(collection_link)

client.CreateItem(collection_link, {
    "Data" : data
   }
)

