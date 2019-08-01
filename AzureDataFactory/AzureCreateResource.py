import os
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from dotenv import load_dotenv

project_folder = os.path.expanduser('/home/admin1/Desktop/AzurePythonScript/AzureDataFactory') # adjust as appropriate
load_dotenv(os.path.join(project_folder, '.env'))

print(os.environ.get('AZURE_TENANT_ID'))

subscription_id = os.environ.get(
'AZURE_SUBSCRIPTION_ID',
os.environ.get('AZURE_SUBSCRIPTION_ID')) 
credentials = ServicePrincipalCredentials(
client_id=os.environ.get('AZURE_CLIENT_ID'),
secret=os.environ.get('AZURE_CLIENT_SECRET'),
tenant=os.environ.get('AZURE_TENANT_ID')
# client_id='4ae60558-ccb0-4cda-a63a-44f668724c9b',
# secret='W?.LTxfpU_eyqV:s3fm3wVovDnXaHZ81',
# tenant='524b0e96-35a3-46ef-ade3-a76c1936a890'
)

client = ResourceManagementClient(credentials, subscription_id)

def print_item(group):
    """Print a ResourceGroup instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    print("\tLocation: {}".format(group.location))
    print("\tTags: {}".format(group.tags))
    print_properties(group.properties)

def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
        print("\n\n")

# WEST_US = "westus"
# GROUP_NAME = "ArunPythonScriptResource"
# resource_group_params = {"location": "westus"}

# print("Create Resource Group")
# print_item(client.resource_groups.create_or_update(GROUP_NAME, resource_group_params))