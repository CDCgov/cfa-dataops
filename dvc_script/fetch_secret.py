import os
import traceback
from cfa.cloudops import CloudClient

print("Starting Key Vault test...")

try:
    cc = CloudClient(keyvault="cfa-predict")
    print("CloudClient initialized")

    print("Fetching client id...")
    client_id = cc.get_kv_secret("CFA-Function-Container-Deployer-SP-ClientID", "cfa-predict")
    print("client_id fetched:", client_id)

    print("Fetching client secret...")
    client_secret = cc.get_kv_secret("CFA-Function-Container-Deployer-SP-Secret", "cfa-predict")
    print("client_secret fetched:", "***hidden***" if client_secret else "<empty>")

    print("Fetching tenant id...")
    tenant_id = cc.get_kv_secret("CFA-Function-Container-Deployer-SP-TenantID", "cfa-predict")
    print("tenant_id fetched:", tenant_id)

except Exception as e:
    print("ERROR:", e)
    traceback.print_exc()