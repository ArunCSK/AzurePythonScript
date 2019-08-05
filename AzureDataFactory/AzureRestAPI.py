import json
import urllib.request
import adal
import requests
import os
from dotenv import load_dotenv

#json.load(urllib2.urlopen("https://management.azure.com/subscriptions/1c38b3b9-bf22-4c15-afba-fd39487f52cc/providers/Microsoft.DataFactory/locations/EastUS/configureFactoryRepo?api-version=2018-06-01"))

# url = "https://management.azure.com/subscriptions/1c38b3b9-bf22-4c15-afba-fd39487f52cc/providers/Microsoft.DataFactory/locations/East%20US/configureFactoryRepo?api-version=2018-06-01"
# request = urllib.request.Request(url)
# response = urllib.request.urlopen(request)
# print (response.read().decode('utf-8'))


# Select-AzSubscription -SubscriptionId "1c38b3b9-bf22-4c15-afba-fd39487f52cc"
# $tenantID = "524b0e96-35a3-46ef-ade3-a76c1936a890"
# $appId = "4ae60558-ccb0-4cda-a63a-44f668724c9b"
# $authKey = "xz0pPFs[jM*5.e5fmgVE0qI.Te6K_IVF"
# $subsId = "1c38b3b9-bf22-4c15-afba-fd39487f52cc"
# $resourceGroup = "AzureResourceusingAPI"
# $dataFactoryName = "AzureDFUsingAPI"
# $apiVersion = "2018-06-01"

project_folder = os.path.expanduser('/home/admin1/Desktop/AzurePythonScript') # adjust as appropriate
load_dotenv(os.path.join(project_folder, '.env'))

tenant = os.environ.get('AZURE_TENANT_ID')
authority_url = 'https://login.microsoftonline.com/' + tenant
client_id = os.environ.get('AZURE_CLIENT_ID')
client_secret = os.environ.get('AZURE_CLIENT_SECRET')
resource = 'https://management.azure.com/'

context = adal.AuthenticationContext(authority_url)
token = context.acquire_token_with_client_credentials(resource, client_id, client_secret)
headers = {'Authorization': 'Bearer ' + token['accessToken'], 'Content-Type': 'application/json'}
params = {'api-version': '2016-06-01'}
url = 'https://management.azure.com/' + 'subscriptions'
r = requests.get(url, headers=headers, params=params)
print(json.dumps(r.json(), indent=4, separators=(',', ': ')))
