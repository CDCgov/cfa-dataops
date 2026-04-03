import os
import subprocess
import sys

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


KEYVAULT_NAME = "cfa-predict"
VAULT_URL = f"https://{KEYVAULT_NAME}.vault.azure.net/"
STORAGE_ACCOUNT = "cfadatalakedev"


def get_secret(secret_client: SecretClient, name: str) -> str:
    value = secret_client.get_secret(name).value
    if not value:
        raise RuntimeError(f"Secret '{name}' is empty or missing")
    return value


def main() -> int:
    try:
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)

        client_id = get_secret(secret_client, "CFA-Function-Container-Deployer-SP-ClientID")
        client_secret = get_secret(secret_client, "CFA-Function-Container-Deployer-SP-Secret")
        tenant_id = get_secret(secret_client, "CFA-Function-Container-Deployer-SP-TenantID")

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

# python -c "from azure.identity import ClientSecretCredential; c=ClientSecretCredential(tenant_id='$env:AZURE_TENANT_ID', client_id='$env:AZURE_CLIENT_ID', client_secret='$env:AZURE_CLIENT_SECRET'); print('credential created')"