# Databricks notebook source
#import pandas as pd

dbutils.widgets.text("output", "","")
dbutils.widgets.get("output")
FilePath = getArgument("output")

dbutils.widgets.text("filename", "","")
dbutils.widgets.get("filename")
filename = getArgument("filename")
#print(FilePath)
storage_account_name = "arunstorage12"
storage_account_access_key = "iFCTVZveS/XvhhHfL/Phpf/r3UM3CPwSBkEwiQWePdALeW9hamYc6mAEXQMeSjQVrAdCY19hfFlUBLmKbwsbog=="

spark.conf.set(
 "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
 storage_account_access_key)


#file_location = "wasbs://example/location"+FilePath
file_location = "wasbs://aruncontainer@arunstorage12.blob.core.windows.net"+FilePath+"/"+filename
#file_location = @input
print(file_location)
file_type = "csv"


df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
df.show()
insertdata = df



# COMMAND ----------

import re
import os.path
import IPython
from pyspark.sql import SQLContext
import pickle
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import mlflow
import mlflow.sklearn



#import azureml.core
#from azureml.core import Workspace
data = df.toPandas()
#data_exp = data["_c0"].values
#data_salary = data["_c1"].values
data_exp = data.iloc[2:len(data),0:1].values
x_test = data
data_salary = data.iloc[2:len(data),1].values



m = len(data_exp)
data_exp = data_exp.reshape((m,1))
data_salary = data_salary.reshape((m,1))
reg = LinearRegression()
model = reg.fit(data_exp, data_salary)
Y_pred = reg.predict(data_exp)

r2_score = reg.score(data_exp , data_salary)

file_name = "SalaryModel.pkl"
pkl_file = open(file_name, 'wb')
model = pickle.dump(model, pkl_file)
pkl_file.close()

#modelpath = "/dbfs/DatabricksModel/"
#mlflow.sklearn.save_model(model, modelpath)

#val configMap = Map(
#  "Endpoint" -> aruncosmosdb1,
#  "Masterkey" -> KGqTRoVMYcDcsNw5cCLvVqKfQZVgsWENMQugL8786X0T2gL8EmcANoaW32Su7bqjdqFF8lBVqG67GqtiZU0T1w==,
#  "Database" -> aruncosmosdb,
#  "Collection" -> aruncosomosdb,
#  "preferredRegions" -> West India)
#val config = Config(configMap)

#print("Azure ML SDK Version: ", azureml.core.VERSION)

#ws= Workspace.getDetails()
#rint(ws.name, ws.location, ws.resource_group, ws.location, sep = '\t')

#display(dbutils.fs.ls("file:/databricks/driver/"))

#fullname = "fs.azure.account.key." +storage_account_name+ ".blob.core.windows.net"
#accountsource = "wasbs://aruncontainer@" +storage_account_name+ ".blob.core.windows.net"

#dbutils.fs.mount(
#source = storage_account_name,
#mount_point = "/mnt/Arun",
#extra_configs = {fullname : storage_account_access_key}
#)

dbutils.fs.mount(
  source = "wasbs://aruncontainer@arunstorage12.blob.core.windows.net",
  mount_point = "/mnt/Arun",
  extra_configs = {"fs.azure.account.key.arunstorage12.blob.core.windows.net": "iFCTVZveS/XvhhHfL/Phpf/r3UM3CPwSBkEwiQWePdALeW9hamYc6mAEXQMeSjQVrAdCY19hfFlUBLmKbwsbog=="})

#dbutils.fs.unmount("/mnt/Arun")

#dbutils.fs.ls("/mnt/Arun")

#dbutils.fs.put("file:/dbfs/mnt/Arun", pkl_file)
dbutils.fs.cp("file:/databricks/driver/SalaryModel.pkl","/mnt/Arun")
  
#dbutils.fs.put("/mnt/Arun", pkl_file)
#display(dbutils.fs.ls("dbfs:/mnt/temp/"))

#dbutils.fs.cp("file:/databricks/driver/SalaryModel.pkl","/mnt/Arun")



# COMMAND ----------

dbutils.fs.ls("/mnt/Arun")

# COMMAND ----------

file_loc = "/dbfs/mnt/Arun/SalaryModel.pkl"
with open("/dbfs/mnt/Arun/SalaryModel.pkl", mode='rb') as f:
  pkl_file = pickle.load(f)
  #model_pkl = pickle.load(pkl_file)
  y_pred = pkl_file.predict(data_exp)
  #print("prediction",y_pred)  
  

# COMMAND ----------

display(dbutils.fs.ls("file:/databricks/driver/"))

# COMMAND ----------

import azure.cosmos.cosmos_client as cosmos_client

config = {
    'ENDPOINT': 'https://aruncosmosdb1.documents.azure.com:443/',
    'PRIMARYKEY': 'KGqTRoVMYcDcsNw5cCLvVqKfQZVgsWENMQugL8786X0T2gL8EmcANoaW32Su7bqjdqFF8lBVqG67GqtiZU0T1w==',
    'DATABASE': 'test2',
    'CONTAINER': 'test2collection'
}

client = cosmos_client.CosmosClient(url_connection=config['ENDPOINT'], auth={'masterKey': config['PRIMARYKEY']})


# COMMAND ----------


container_definition = {
    'id': config['CONTAINER']
}
  
client = cosmos_client.CosmosClient(url_connection=config['ENDPOINT'], auth={'masterKey': config['PRIMARYKEY']})

db = client.CreateDatabase({'id': config['DATABASE']})
options = {
    'offerThroughput': 400
}
container = client.CreateContainer(db['_self'], container_definition, options)

item1 = client.CreateItem(container['_self'], {
    'id': '100',
    'Web Site': 0,
    'Cloud Service': 0,
    'Virtual Machine': 0,
    'message': "1233"
    }
)
#print(container['_self'])

# COMMAND ----------

item3 = client.CreateItem(container['_self'], {
    'id': '300',
    'Web Site': 0,
    'Cloud Service': 0,
    'Virtual Machine': 0,
    'message': "3000"
    }
)

# COMMAND ----------

writeConfig = {
   "Endpoint": "https://aruncosmosdb1.documents.azure.com:443/",
   "Masterkey": "KGqTRoVMYcDcsNw5cCLvVqKfQZVgsWENMQugL8786X0T2gL8EmcANoaW32Su7bqjdqFF8lBVqG67GqtiZU0T1w==",
   "Database": "test",
   "Collection": "testcollection",
   "Upsert": "true",
    "WritingBatchSize": "500",
    "CheckpointLocation": "/checkpointlocation_write1"
}

Data = pd.DataFrame(x_test)
df = spark.createDataFrame(Data)
df.collect()

#df.write.format("com.microsoft.azure.cosmosdb.spark").options(**writeConfig).save()


# COMMAND ----------



# COMMAND ----------


