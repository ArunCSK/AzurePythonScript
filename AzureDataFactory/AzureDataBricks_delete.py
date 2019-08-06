import os
import traceback
from azure.common.credentials import ServicePrincipalCredentials
from msrestazure.azure_exceptions import CloudError
from msrestazure.azure_configuration import AzureConfiguration
from msrest.service_client import ServiceClient
from msrest.polling import LROPoller
from msrestazure.polling.arm_polling import ARMPolling
from msrest.pipeline import ClientRawResponse
import uuid
from dotenv import load_dotenv

project_folder = os.path.expanduser('/home/admin1/Desktop/AzurePythonScript') # adjust as appropriate
load_dotenv(os.path.join(project_folder, '.env'))

SUBSCRIPTION_ID = os.environ.get('AZURE_SUBSCRIPTION_ID')
RESOURCE_GROUP = "AzureResourceGroupusingPythonScript"
MANAGED_RESOURCE_GROUP = "databricks-rg-AzureDatabricksforScripts-dsda2lochqn5y"
WORKSPACE_NAME = "AzureTwitterWorkspace"

BODY = {}

API_VERSION = '2018-04-01'

def get_credentials():
    credentials = ServicePrincipalCredentials(
        client_id=os.environ.get('AZURE_CLIENT_ID'),
        secret=os.environ.get('AZURE_CLIENT_SECRET'),
        tenant=os.environ.get('AZURE_TENANT_ID')
    )
    return credentials


def run_example():
    credentials = get_credentials()

    config = AzureConfiguration('https://management.azure.com')
    service_client = ServiceClient(credentials, config)

    query_parameters = {}
    query_parameters['api-version'] = API_VERSION

    header_parameters = {}
    header_parameters['x-ms-client-request-id'] = str(uuid.uuid1())

    operation_config = {}
    request = service_client.delete("/subscriptions/" + SUBSCRIPTION_ID + "/resourceGroups/" + RESOURCE_GROUP + "/providers/Microsoft.Databricks/workspaces/" + WORKSPACE_NAME, query_parameters)
    response = service_client.send(request, header_parameters, BODY, **operation_config)

    # if response.status_code == 202:
    #     response = wait_poller(service_client, operation_config, response)
    print(response.text)


if __name__ == "__main__":
    run_example()