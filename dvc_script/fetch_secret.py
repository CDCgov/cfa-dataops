import os
import subprocess
import sys

from cfa.cloudops import CloudClient

KEYVAULT_NAME = "cfa-predict"
STORAGE_ACCOUNT = "cfadatalakedev"
SECRET_NAME = "CFA-Function-Container-Deployer-SP-Secret"


def main() -> int:
    try:
        client_id = os.getenv("AZURE_CLIENT_ID")
        tenant_id = os.getenv("AZURE_TENANT_ID")
        subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")

        if not client_id:
            raise RuntimeError("Missing AZURE_CLIENT_ID in environment")
        if not tenant_id:
            raise RuntimeError("Missing AZURE_TENANT_ID in environment")
        if not subscription_id:
            raise RuntimeError("Missing AZURE_SUBSCRIPTION_ID in environment")

        cc = CloudClient(
            keyvault=KEYVAULT_NAME,
            use_federated=True,
        )

        client_secret = cc.get_kv_secret(SECRET_NAME, KEYVAULT_NAME)
        if not client_secret:
            raise RuntimeError(f"Failed to retrieve secret '{SECRET_NAME}' from Key Vault")

        os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = STORAGE_ACCOUNT
        os.environ["AZURE_CLIENT_SECRET"] = client_secret

        print(f"Using storage account: {STORAGE_ACCOUNT}")
        print(f"Using client id: {client_id}")
        print(f"Using tenant id: {tenant_id}")
        print(f"Using subscription id: {subscription_id}")
        print(f"Fetched secret '{SECRET_NAME}' from Key Vault")

        result = subprocess.run(["dvc", "push", "-v"], check=False)
        return result.returncode

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())