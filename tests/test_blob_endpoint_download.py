"""Tests for BlobEndpoint.download_version_to_local method"""

import os
import tempfile

import pytest

from cfa.dataops.catalog import BlobEndpoint


@pytest.fixture
def blob_endpoint(mocker, mock_write_blob_stream):
    """Create a BlobEndpoint instance for testing"""
    mocker.patch(
        "cfa.dataops.catalog.write_blob_stream",
        mock_write_blob_stream,
    )
    ledger_location = {
        "account": "account_test",
        "container": "container_test",
        "prefix": "_access/test/ledger/",
    }
    return BlobEndpoint(
        account="account_test",
        container="container_test",
        prefix="test/prefix",
        ledger_location=ledger_location,
        ns="test.endpoint",
    )


class TestDownloadVersionToLocal:
    """Tests for the download_version_to_local method"""

    def test_download_version_to_local_single_file(
        self, mocker, blob_endpoint
    ):
        """Test downloading a single file version to local path"""
        # Mock the blob reading
        test_content = b"test file content"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            return test_content

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        # Mock the version blobs
        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-01T12-00-00/data.csv",
                    "creation_time": "2025-01-01T12:00:00",
                }
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="2025-01-01T12-00-00",
                force=False,
            )

            assert result is True

            # Verify file was written
            expected_file = os.path.join(
                tmpdir, "2025-01-01T12-00-00", "data.csv"
            )
            assert os.path.exists(expected_file)

            # Verify content
            with open(expected_file, "rb") as f:
                assert f.read() == test_content

    def test_download_version_to_local_multiple_files(
        self, mocker, blob_endpoint
    ):
        """Test downloading multiple files in a version to local path"""
        test_content_1 = b"test file 1"
        test_content_2 = b"test file 2"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            if "data_0.parquet" in blob_url:
                return test_content_1
            elif "data_1.parquet" in blob_url:
                return test_content_2

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-01T12-00-00/data_0.parquet",
                    "creation_time": "2025-01-01T12:00:00",
                },
                {
                    "name": "test/prefix/2025-01-01T12-00-00/data_1.parquet",
                    "creation_time": "2025-01-01T12:00:01",
                },
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="2025-01-01T12-00-00",
                force=False,
            )

            assert result is True

            # Verify both files were written
            file1 = os.path.join(
                tmpdir, "2025-01-01T12-00-00", "data_0.parquet"
            )
            file2 = os.path.join(
                tmpdir, "2025-01-01T12-00-00", "data_1.parquet"
            )
            assert os.path.exists(file1)
            assert os.path.exists(file2)

            with open(file1, "rb") as f:
                assert f.read() == test_content_1
            with open(file2, "rb") as f:
                assert f.read() == test_content_2

    def test_download_version_to_local_nested_structure(
        self, mocker, blob_endpoint
    ):
        """Test downloading files with nested directory structure"""
        test_content = b"nested file content"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            return test_content

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-01T12-00-00/subdir/nested/file.txt",
                    "creation_time": "2025-01-01T12:00:00",
                }
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="2025-01-01T12-00-00",
                force=False,
            )

            assert result is True

            # Verify nested directory was created
            expected_file = os.path.join(
                tmpdir, "2025-01-01T12-00-00", "subdir", "nested", "file.txt"
            )
            assert os.path.exists(expected_file)

            with open(expected_file, "rb") as f:
                assert f.read() == test_content

    def test_download_version_to_local_latest(self, mocker, blob_endpoint):
        """Test downloading the latest version"""
        test_content = b"latest version content"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            return test_content

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        mocker.patch.object(
            blob_endpoint,
            "get_versions",
            return_value=["2025-01-02T12-00-00", "2025-01-01T12-00-00"],
        )

        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-02T12-00-00/data.csv",
                    "creation_time": "2025-01-02T12:00:00",
                }
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="latest",
                force=False,
            )

            assert result is True

            # Verify the latest version was downloaded
            expected_file = os.path.join(
                tmpdir, "2025-01-02T12-00-00", "data.csv"
            )
            assert os.path.exists(expected_file)

    def test_download_version_to_local_no_redownload_without_force(
        self, mocker, blob_endpoint
    ):
        """Test that existing files are not re-downloaded without force flag"""
        test_content = b"test content"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            return test_content

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-01T12-00-00/data.csv",
                    "creation_time": "2025-01-01T12:00:00",
                }
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create the file beforehand
            expected_file = os.path.join(
                tmpdir, "2025-01-01T12-00-00", "data.csv"
            )
            os.makedirs(os.path.dirname(expected_file), exist_ok=True)
            existing_content = b"existing content"
            with open(expected_file, "wb") as f:
                f.write(existing_content)

            result = blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="2025-01-01T12-00-00",
                force=False,
            )

            # Should return False since no files were written
            assert result is False

            # Verify file content is unchanged
            with open(expected_file, "rb") as f:
                assert f.read() == existing_content

    def test_download_version_to_local_force_redownload(
        self, mocker, blob_endpoint
    ):
        """Test that existing files are re-downloaded with force flag"""
        test_content = b"new test content"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            return test_content

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-01T12-00-00/data.csv",
                    "creation_time": "2025-01-01T12:00:00",
                }
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create the file beforehand with different content
            expected_file = os.path.join(
                tmpdir, "2025-01-01T12-00-00", "data.csv"
            )
            os.makedirs(os.path.dirname(expected_file), exist_ok=True)
            with open(expected_file, "wb") as f:
                f.write(b"old content")

            result = blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="2025-01-01T12-00-00",
                force=True,
            )

            assert result is True

            # Verify file content was updated
            with open(expected_file, "rb") as f:
                assert f.read() == test_content

    def test_download_version_to_local_creates_directories(
        self, mocker, blob_endpoint
    ):
        """Test that download creates necessary directories"""
        test_content = b"test content"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            return test_content

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-01T12-00-00/deep/nested/path/data.csv",
                    "creation_time": "2025-01-01T12:00:00",
                }
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="2025-01-01T12-00-00",
                force=False,
            )

            assert result is True

            # Verify nested directories were created
            expected_file = os.path.join(
                tmpdir,
                "2025-01-01T12-00-00",
                "deep",
                "nested",
                "path",
                "data.csv",
            )
            assert os.path.exists(expected_file)
            assert os.path.isfile(expected_file)

    def test_download_version_to_local_ledger_entry(
        self, mocker, blob_endpoint
    ):
        """Test that ledger entry is created when files are written"""
        test_content = b"test content"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            return test_content

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-01T12-00-00/data.csv",
                    "creation_time": "2025-01-01T12:00:00",
                }
            ],
        )

        # Mock ledger_entry to track if it was called
        mock_ledger = mocker.patch.object(blob_endpoint, "ledger_entry")

        with tempfile.TemporaryDirectory() as tmpdir:
            blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="2025-01-01T12-00-00",
                force=False,
            )

            # Verify ledger entry was called with 'read' action
            mock_ledger.assert_called_once_with(action="read")

    def test_download_version_to_local_no_ledger_entry_when_not_written(
        self, mocker, blob_endpoint
    ):
        """Test that no ledger entry is created when no files are written"""
        test_content = b"test content"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            return test_content

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-01T12-00-00/data.csv",
                    "creation_time": "2025-01-01T12:00:00",
                }
            ],
        )

        # Mock ledger_entry to track if it was called
        mock_ledger = mocker.patch.object(blob_endpoint, "ledger_entry")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create the file beforehand
            expected_file = os.path.join(
                tmpdir, "2025-01-01T12-00-00", "data.csv"
            )
            os.makedirs(os.path.dirname(expected_file), exist_ok=True)
            with open(expected_file, "wb") as f:
                f.write(b"existing content")

            blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="2025-01-01T12-00-00",
                force=False,
            )

            # Verify ledger entry was NOT called
            mock_ledger.assert_not_called()

    def test_download_version_to_local_mixed_new_and_existing_files(
        self, mocker, blob_endpoint
    ):
        """Test downloading when some files exist and some don't"""
        test_content_1 = b"content 1"
        test_content_2 = b"content 2"

        def mock_read_blob_stream(blob_url, account_name, container_name):
            if "file1.txt" in blob_url:
                return test_content_1
            elif "file2.txt" in blob_url:
                return test_content_2

        mocker.patch(
            "cfa.dataops.catalog.read_blob_stream",
            side_effect=mock_read_blob_stream,
        )

        mocker.patch.object(
            blob_endpoint,
            "_get_version_blobs",
            return_value=[
                {
                    "name": "test/prefix/2025-01-01T12-00-00/file1.txt",
                    "creation_time": "2025-01-01T12:00:00",
                },
                {
                    "name": "test/prefix/2025-01-01T12-00-00/file2.txt",
                    "creation_time": "2025-01-01T12:00:01",
                },
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create file1 beforehand
            file1 = os.path.join(tmpdir, "2025-01-01T12-00-00", "file1.txt")
            os.makedirs(os.path.dirname(file1), exist_ok=True)
            with open(file1, "wb") as f:
                f.write(b"existing")

            result = blob_endpoint.download_version_to_local(
                local_path=tmpdir,
                version="2025-01-01T12-00-00",
                force=False,
            )

            # Should return True because file2 was written
            assert result is True

            # Verify file1 wasn't overwritten
            with open(file1, "rb") as f:
                assert f.read() == b"existing"

            # Verify file2 was created
            file2 = os.path.join(tmpdir, "2025-01-01T12-00-00", "file2.txt")
            assert os.path.exists(file2)
            with open(file2, "rb") as f:
                assert f.read() == test_content_2
