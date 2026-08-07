"""Tests for BlobEndpoint blob access checks."""

from unittest.mock import patch

import pytest

from cfa.dataops.catalog import BlobEndpoint


@pytest.fixture
def blob_endpoint():
    """Create a BlobEndpoint instance for testing."""
    ledger_location = {
        "account": "account_test",
        "container": "container_test",
        "prefix": "_access/test/ledger/",
    }
    return BlobEndpoint(
        account="myaccount",
        container="mycontainer",
        prefix="test/prefix",
        ledger_location=ledger_location,
        ns="test.endpoint",
    )


class TestCheckBlobAccess:
    """Tests for the check_blob_access method."""

    def test_check_blob_access_success(self, blob_endpoint):
        with patch("cfa.dataops.catalog.BlobServiceClient") as mock_bsc:
            mock_container = mock_bsc.return_value.get_container_client.return_value
            mock_container.get_container_properties.return_value = {}
            has_access, message = blob_endpoint.check_blob_access()
        assert has_access is True
        assert "Access confirmed" in message
        assert "myaccount" in message
        assert "mycontainer" in message

    def test_check_blob_access_401_unauthorized(self, blob_endpoint):
        class Err401(Exception):
            status_code = 401

        with patch(
            "cfa.dataops.catalog.walk_blobs_in_container",
            side_effect=Err401("Unauthorized"),
        ):
            has_access, message = blob_endpoint.check_blob_access()

        assert has_access is False
        assert "Unauthorized" in message
        assert "credentials" in message.lower()

    def test_check_blob_access_403_forbidden(self, blob_endpoint):
        class Err403(Exception):
            status_code = 403

        with patch(
            "cfa.dataops.catalog.walk_blobs_in_container",
            side_effect=Err403("Access denied"),
        ):
            has_access, message = blob_endpoint.check_blob_access()

        assert has_access is False
        assert "Access denied" in message
        assert "permissions" in message.lower()

    def test_check_blob_access_resource_not_found(self, blob_endpoint):
        class Err404(Exception):
            status_code = 404

        with patch(
            "cfa.dataops.catalog.walk_blobs_in_container",
            side_effect=Err404("Container not found"),
        ):
            has_access, message = blob_endpoint.check_blob_access()

        assert has_access is False
        assert "not found" in message.lower()
        assert "mycontainer" in message

    def test_check_blob_access_authentication_error(self, blob_endpoint):
        with patch(
            "cfa.dataops.catalog.walk_blobs_in_container",
            side_effect=Exception("credential unavailable"),
        ):
            has_access, message = blob_endpoint.check_blob_access()

        assert has_access is False
        assert "Authentication failed" in message

    def test_check_blob_access_generic_exception(self, blob_endpoint):
        with patch(
            "cfa.dataops.catalog.walk_blobs_in_container",
            side_effect=Exception("Something went wrong"),
        ):
            has_access, message = blob_endpoint.check_blob_access()

        assert has_access is False
        assert "Failed to verify access" in message


class TestVerifyBlobAccess:
    """Tests for the verify_blob_access method."""

    def test_verify_blob_access_success(self, blob_endpoint):
        with patch.object(
            blob_endpoint, "check_blob_access", return_value=(True, "Access OK")
        ):
            blob_endpoint.verify_blob_access()

    def test_verify_blob_access_denied(self, blob_endpoint):
        with patch.object(
            blob_endpoint,
            "check_blob_access",
            return_value=(False, "Access denied"),
        ):
            with pytest.raises(RuntimeError) as exc_info:
                blob_endpoint.verify_blob_access()

        error_msg = str(exc_info.value)
        assert "Cannot access Blob storage" in error_msg
        assert "Access denied" in error_msg
        assert "test.endpoint" in error_msg

    def test_verify_blob_access_caches_ext_access_check(self, blob_endpoint):
        with (
            patch("cfa.dataops.catalog.check_ext_env", return_value=True) as mock_ext,
            patch.object(
                blob_endpoint, "check_blob_access", return_value=(True, "Access OK")
            ),
        ):
            blob_endpoint.verify_blob_access()
            blob_endpoint.verify_blob_access()

        assert mock_ext.call_count == 1


class TestWriteBlobWithAccessCheck:
    """Tests for write_blob with access checks."""

    def test_write_blob_fails_without_access(self, blob_endpoint, mocker):
        mocker.patch.object(
            blob_endpoint,
            "verify_blob_access",
            side_effect=RuntimeError("Cannot access Blob storage. Access denied"),
        )

        with pytest.raises(RuntimeError) as exc_info:
            blob_endpoint.write_blob(
                file_buffer=b"test data",
                path_after_prefix="test.txt",
            )

        assert "Cannot access Blob storage" in str(exc_info.value)

    def test_write_blob_proceeds_with_access(self, blob_endpoint, mocker):
        mocker.patch.object(blob_endpoint, "verify_blob_access")
        mocker.patch("cfa.dataops.catalog.write_blob_stream")

        blob_endpoint.write_blob(
            file_buffer=b"test data",
            path_after_prefix="test.txt",
        )


class TestReadBlobsWithAccessCheck:
    """Tests for read_blobs with access checks."""

    def test_read_blobs_fails_without_access(self, blob_endpoint, mocker):
        mocker.patch.object(
            blob_endpoint,
            "verify_blob_access",
            side_effect=RuntimeError("Cannot access Blob storage. Access denied"),
        )

        with pytest.raises(RuntimeError) as exc_info:
            blob_endpoint.read_blobs()

        assert "Cannot access Blob storage" in str(exc_info.value)


class TestReadCsvWithAccessCheck:
    """Tests for read_csv with access checks."""

    def test_read_csv_fails_without_access(self, blob_endpoint, mocker):
        mocker.patch.object(
            blob_endpoint,
            "verify_blob_access",
            side_effect=RuntimeError("Cannot access Blob storage. Access denied"),
        )

        with pytest.raises(RuntimeError) as exc_info:
            blob_endpoint.read_csv("test.csv")

        assert "Cannot access Blob storage" in str(exc_info.value)


class TestGetDataframeWithAccessCheck:
    """Tests for get_dataframe with access checks."""

    def test_get_dataframe_fails_without_access(self, blob_endpoint, mocker):
        mocker.patch.object(
            blob_endpoint,
            "verify_blob_access",
            side_effect=RuntimeError("Cannot access Blob storage. Access denied"),
        )

        with pytest.raises(RuntimeError) as exc_info:
            blob_endpoint.get_dataframe(output="pandas")

        assert "Cannot access Blob storage" in str(exc_info.value)


class TestGetVersionBlobsWithAccessCheck:
    """Tests for _get_version_blobs with access checks."""

    def test_get_version_blobs_ledger_fails_without_access(self, mocker):
        ledger_location = {
            "account": "account_test",
            "container": "container_test",
            "prefix": "_access/test/ledger/",
        }
        ledger_endpoint = BlobEndpoint(
            account="myaccount",
            container="mycontainer",
            prefix="_access/test/ledger",
            ledger_location=ledger_location,
            ns="ledger_endpoint",
        )
        mocker.patch.object(
            ledger_endpoint,
            "verify_blob_access",
            side_effect=RuntimeError("Cannot access Blob storage. Access denied"),
        )
        mock_walk = mocker.patch("cfa.dataops.catalog.walk_blobs_in_container")

        with pytest.raises(RuntimeError) as exc_info:
            ledger_endpoint._get_version_blobs()

        assert "Cannot access Blob storage" in str(exc_info.value)
        mock_walk.assert_not_called()


class TestGetVersionsWithAccessCheck:
    """Tests for get_versions with access checks."""

    def test_get_versions_fails_without_access(self, blob_endpoint, mocker):
        mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=True)
        mocker.patch.object(
            blob_endpoint,
            "verify_blob_access",
            side_effect=RuntimeError("Cannot access Blob storage. Access denied"),
        )

        with pytest.raises(RuntimeError) as exc_info:
            blob_endpoint.get_versions()

        assert "Cannot access Blob storage" in str(exc_info.value)

    def test_get_versions_checks_ext_env_once_per_instance(self, blob_endpoint, mocker):
        mock_ext = mocker.patch("cfa.dataops.catalog.check_ext_env", return_value=True)
        mocker.patch.object(
            blob_endpoint, "check_blob_access", return_value=(True, "Access OK")
        )
        mocker.patch(
            "cfa.dataops.catalog.walk_blobs_in_container",
            return_value=[{"name": "test/prefix/2026-01-01T00-00-00/"}],
        )

        blob_endpoint.get_versions()

        assert mock_ext.call_count == 1
