import os
import subprocess
import sys

from cfa.cloudops import CloudClient


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> int:
    try:
        keyvault_name = get_required_env("AZURE_KEYVAULT_NAME")
        storage_account = get_required_env("AZURE_STORAGE_ACCOUNT_NAME")
        secret_name = get_required_env("AZURE_KEYVAULT_SP_SECRET_ID")

        client_id = get_required_env("AZURE_CLIENT_ID")
        tenant_id = get_required_env("AZURE_TENANT_ID")
        subscription_id = get_required_env("AZURE_SUBSCRIPTION_ID")

        cc = CloudClient(
            keyvault=keyvault_name,
            use_federated=True,
        )

        client_secret = cc.get_kv_secret(secret_name, keyvault_name)
        if not client_secret:
            raise RuntimeError(
                f"Failed to retrieve secret '{secret_name}' from Key Vault '{keyvault_name}'"
            )

        os.environ["AZURE_CLIENT_SECRET"] = client_secret

        print(f"Using Key Vault: {keyvault_name}")
        print(f"Using storage account: {storage_account}")
        print(f"Using secret name: {secret_name}")
        print(f"Using client id: {client_id}")
        print(f"Using tenant id: {tenant_id}")
        print(f"Using subscription id: {subscription_id}")
        print("Fetched client secret from Key Vault")

        result = subprocess.run(["dvc", "push", "-v"], check=False)
        return result.returncode

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())