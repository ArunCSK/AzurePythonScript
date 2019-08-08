#dapie41f13876e143bd659a734628adc8b37
#not working
import json
import base64
import requests

DOMAIN = 'TweetDataBricks.cloud.databricks.com'
TOKEN = b'dapie41f13876e143bd659a734628adc8b37'
BASE_URL = 'https://%s/api/2.0/dbfs/' % (DOMAIN)

def dbfs_rpc(action, body):
  """ A helper function to make the DBFS API request, request/response is encoded/decoded as JSON """
  response = requests.post(
    BASE_URL + action,
    headers={"Authorization": b"Basic " + base64.standard_b64encode(b"token:" + TOKEN)},
    json=body
  )
  return response.json()

# Create a handle that will be used to add blocks
handle = dbfs_rpc("create", {"path": "/temp/upload_large_file", "overwrite": "true"})['handle']
with open('/a/local/file') as f:
  while True:
    # A block can be at most 1MB
    block = f.read(1 << 20)
    if not block:
        break
    data = base64.standard_b64encode(block)
    dbfs_rpc("add-block", {"handle": handle, "data": data})
# close the handle to finish uploading
dbfs_rpc("close", {"handle": handle})


