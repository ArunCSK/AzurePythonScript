import pandas as pd

dataset = pd.read_csv("/home/admin1/Desktop/AzurePythonScript/AzureDataFactory/KaggleProjects/parking-citations.csv")

data = dataset.iloc[0:10,:]
print(data)
