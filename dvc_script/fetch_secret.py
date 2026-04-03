import os
import subprocess
import sys
from cfa.cloudops import CloudClient

KEYVAULT_NAME = "cfa-predict"
STORAGE_ACCOUNT = "cfadatalakedev"

def main():
    try:
        client_id = os.getenv("AZURE_CLIENT_ID")
        tenant_id = os.getenv("AZURE_TENANT_ID")

        if not client_id or not tenant_id:
            raise RuntimeError("Missing AZURE_CLIENT_ID or AZURE_TENANT_ID in environment")

        cc = CloudClient(keyvault=KEYVAULT_NAME)

        client_secret = cc.get_kv_secret(
            "CFA-Function-Container-Deployer-SP-Secret",
            KEYVAULT_NAME
        )

        os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = STORAGE_ACCOUNT
        os.environ["AZURE_CLIENT_SECRET"] = client_secret

        print(f"Using client id: {client_id}")
        print(f"Using tenant id: {tenant_id}")
        print("Fetched client secret from Key Vault")

        subprocess.run(["dvc", "push", "-v"], check=False)

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())

# python -c "from cfa.cloudops import CloudClient; cc=CloudClient(keyvault='cfa-predict'); print(cc.get_kv_secret('CFA-Function-Container-Deployer-SP-Secret','cfa-predict'))"