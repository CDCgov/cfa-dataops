import os
import subprocess
import sys
from cfa.cloudops import CloudClient

KEYVAULT_NAME = "cfa-predict"
STORAGE_ACCOUNT = "cfadatalakedev"

def must_get_secret(cc: CloudClient, secret_name: str) -> str:
    value = cc.get_kv_secret(secret_name, KEYVAULT_NAME)
    if not value:
        raise RuntimeError(f"Missing or empty secret: {secret_name}")
    return value

def main() -> int:
    try:
        cc = CloudClient(keyvault=KEYVAULT_NAME)

        client_id = must_get_secret(cc, "CFA-Function-Container-Deployer-SP-ClientID")
        client_secret = must_get_secret(cc, "CFA-Function-Container-Deployer-SP-Secret")
        tenant_id = must_get_secret(cc, "CFA-Function-Container-Deployer-SP-TenantID")

        os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = STORAGE_ACCOUNT
        os.environ["AZURE_CLIENT_ID"] = client_id
        os.environ["AZURE_CLIENT_SECRET"] = client_secret
        os.environ["AZURE_TENANT_ID"] = tenant_id

        print(f"Using storage account: {STORAGE_ACCOUNT}")
        print(f"Using client id: {client_id}")

        result = subprocess.run(["dvc", "push", "-v"], check=False)
        return result.returncode

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())