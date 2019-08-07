import requests
import base64

DOMAIN = 'eastus.azuredatabricks.net'
TOKEN = b'dapie41f13876e143bd659a734628adc8b37'

response = requests.post(
  'https://%s/api/2.0/clusters/create' % (DOMAIN),
  headers={'Authorization': b"Basic " + base64.standard_b64encode(b"token:" + TOKEN)},
  json={
    "num_workers":1,
    "cluster_name": "AzurePythonScriptCluster",
    "spark_version": "4.2.x-scala2.11",
    "node_type_id": "Standard_D3_v2",
    'spark_env_vars': {
        'PYSPARK_PYTHON': '/databricks/python3/bin/python3',
    }
  }
)

if response.status_code == 200:
  print(response.json()['cluster_id'])
else:
  print("Error launching cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))