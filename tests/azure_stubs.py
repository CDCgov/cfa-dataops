import sys
from collections.abc import Callable
from types import ModuleType
from typing import Any


def install_azure_stubs(
    ensure_module: Callable[..., ModuleType],
) -> None:
    """Install minimal Azure SDK module stubs used by tests.

    The stubs provide package/module structure and just enough client/exception
    behavior for catalog access checks to exercise success and error paths.
    """

    class _ClientAuthenticationError(Exception):
        """Stub for azure.core.exceptions.ClientAuthenticationError."""

        pass

    class _ResourceNotFoundError(Exception):
        """Stub for azure.core.exceptions.ResourceNotFoundError."""

        pass

    class _HttpResponseError(Exception):
        """Stub for azure.core.exceptions.HttpResponseError."""

        def __init__(self, message: str = "", status_code: int | None = None):
            super().__init__(message)
            self.status_code = status_code

    class _ContainerClientStub:
        """Container client stub that forwards checks to patched blob helper."""

        def __init__(self, account_url: str, container: str):
            self._account_url = account_url
            self._container = container

        def get_container_properties(self) -> dict[str, str]:
            # Delegate to the catalog helper so tests can patch it to simulate auth/access failures.
            from cfa.dataops import catalog as _catalog

            try:
                _catalog.walk_blobs_in_container(
                    account_name=self._account_url.split("https://", 1)[-1].split(
                        ".", 1
                    )[0],
                    container_name=self._container,
                    prefix="",
                )
            except Exception as e:
                status_code = getattr(e, "status_code", None)
                error_text = str(e)
                if status_code == 404:
                    raise _ResourceNotFoundError(error_text) from e
                if status_code in {401, 403}:
                    raise _HttpResponseError(error_text, status_code=status_code) from e
                if "credential" in error_text.lower():
                    raise _ClientAuthenticationError(error_text) from e
                raise
            return {"name": self._container}

    class _BlobServiceClientStub:
        """Blob service client stub compatible with constructor usage in catalog."""

        def __init__(
            self,
            account_url: str,
            credential: Any = None,
            **kwargs: Any,
        ):
            self._account_url = account_url
            self._credential = credential

        def get_container_client(self, container: str) -> _ContainerClientStub:
            return _ContainerClientStub(
                account_url=self._account_url, container=container
            )

    ensure_module(
        "azure.identity",
        ManagedIdentityCredential=type("ManagedIdentityCredential", (), {}),
    )
    ensure_module("azure", __path__=[])
    sys.modules["azure"].identity = sys.modules["azure.identity"]

    ensure_module("azure.core", __path__=[])
    ensure_module(
        "azure.core.exceptions",
        ClientAuthenticationError=_ClientAuthenticationError,
        HttpResponseError=_HttpResponseError,
        ResourceNotFoundError=_ResourceNotFoundError,
    )
    sys.modules["azure"].core = sys.modules["azure.core"]
    sys.modules["azure.core"].exceptions = sys.modules["azure.core.exceptions"]

    ensure_module("azure.storage", __path__=[])
    ensure_module("azure.storage.blob", BlobServiceClient=_BlobServiceClientStub)
    sys.modules["azure"].storage = sys.modules["azure.storage"]
    sys.modules["azure.storage"].blob = sys.modules["azure.storage.blob"]
