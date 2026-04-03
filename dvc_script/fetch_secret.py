import os
from cfa.cloudops import CloudClient

cc = CloudClient(keyvault="cfa-predict")

client_id = cc.get_kv_secret("CFA-Function-Container-Deployer-SP-ClientID", "cfa-predict")
client_secret = cc.get_kv_secret("CFA-Function-Container-Deployer-SP-Secret", "cfa-predict")
tenant_id = cc.get_kv_secret("CFA-Function-Container-Deployer-SP-TenantID", "cfa-predict")

os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "cfadatalakedev"
os.environ["AZURE_CLIENT_ID"] = client_id
os.environ["AZURE_CLIENT_SECRET"] = client_secret
os.environ["AZURE_TENANT_ID"] = tenant_id

# Now call DVC
os.system("dvc push")