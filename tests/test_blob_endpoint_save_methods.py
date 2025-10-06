"""Tests for BlobEndpoint save methods"""

import os
import tempfile
from io import BytesIO

import pandas as pd
import polars as pl
import pytest

from cfa.dataops.catalog import BlobEndpoint, dict_to_sn


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


@pytest.fixture
def sample_pandas_df():
    """Create a sample pandas DataFrame for testing"""
    return pd.DataFrame(
        {
            "col1": [1, 2, 3, 4, 5],
            "col2": ["a", "b", "c", "d", "e"],
            "col3": [1.1, 2.2, 3.3, 4.4, 5.5],
        }
    )


@pytest.fixture
def sample_polars_df():
    """Create a sample polars DataFrame for testing"""
    return pl.DataFrame(
        {
            "col1": [1, 2, 3, 4, 5],
            "col2": ["a", "b", "c", "d", "e"],
            "col3": [1.1, 2.2, 3.3, 4.4, 5.5],
        }
    )


class TestSaveDataframe:
    """Tests for the save_dataframe method"""

    def test_save_pandas_dataframe_parquet(
        self, mocker, blob_endpoint, sample_pandas_df
    ):
        """Test saving a pandas DataFrame as parquet"""
        mock_write = mocker.patch.object(blob_endpoint, "write_blob")

        blob_endpoint.save_dataframe(
            df=sample_pandas_df,
            path_after_prefix="data/output.parquet",
            file_format="parquet",
            auto_version=False,
        )

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[1]["path_after_prefix"] == "data/output.parquet"
        assert call_args[1]["auto_version"] is False
        assert isinstance(call_args[1]["file_buffer"], bytes)

    def test_save_pandas_dataframe_parquet_auto_extension(
        self, mocker, blob_endpoint, sample_pandas_df
    ):
        """Test saving a pandas DataFrame as parquet with automatic extension"""
        mock_write = mocker.patch.object(blob_endpoint, "write_blob")

        blob_endpoint.save_dataframe(
            df=sample_pandas_df,
            path_after_prefix="data/output",
            file_format="parquet",
            auto_version=False,
        )

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[1]["path_after_prefix"] == "data/output.parquet"

    def test_save_pandas_dataframe_csv(
        self, mocker, blob_endpoint, sample_pandas_df
    ):
        """Test saving a pandas DataFrame as CSV"""
        mock_write = mocker.patch.object(blob_endpoint, "write_blob")

        blob_endpoint.save_dataframe(
            df=sample_pandas_df,
            path_after_prefix="data/output.csv",
            file_format="csv",
            auto_version=False,
        )

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[1]["path_after_prefix"] == "data/output.csv"
        assert isinstance(call_args[1]["file_buffer"], bytes)

    def test_save_pandas_dataframe_json(
        self, mocker, blob_endpoint, sample_pandas_df
    ):
        """Test saving a pandas DataFrame as JSON"""
        mock_write = mocker.patch.object(blob_endpoint, "write_blob")

        blob_endpoint.save_dataframe(
            df=sample_pandas_df,
            path_after_prefix="data/output.jsonl",
            file_format="jsonl",
            auto_version=False,
        )

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[1]["path_after_prefix"] == "data/output.jsonl"
        assert isinstance(call_args[1]["file_buffer"], bytes)

    def test_save_pandas_dataframe_json_auto_extension_fix(
        self, mocker, blob_endpoint, sample_pandas_df, capsys
    ):
        """Test that .json extension is automatically changed to .jsonl"""
        mock_write = mocker.patch.object(blob_endpoint, "write_blob")

        blob_endpoint.save_dataframe(
            df=sample_pandas_df,
            path_after_prefix="data/output.json",
            file_format="json",
            auto_version=False,
        )

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[1]["path_after_prefix"] == "data/output.jsonl"

        # Check that warning message was printed
        captured = capsys.readouterr()
        assert "Changing file extension to .jsonl" in captured.out

    def test_save_polars_dataframe_parquet(
        self, mocker, blob_endpoint, sample_polars_df
    ):
        """Test saving a polars DataFrame as parquet"""
        mock_write = mocker.patch.object(blob_endpoint, "write_blob")

        blob_endpoint.save_dataframe(
            df=sample_polars_df,
            path_after_prefix="data/output.parquet",
            file_format="parquet",
            auto_version=False,
        )

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[1]["path_after_prefix"] == "data/output.parquet"
        assert isinstance(call_args[1]["file_buffer"], bytes)

    def test_save_polars_dataframe_csv(
        self, mocker, blob_endpoint, sample_polars_df
    ):
        """Test saving a polars DataFrame as CSV"""
        mock_write = mocker.patch.object(blob_endpoint, "write_blob")

        blob_endpoint.save_dataframe(
            df=sample_polars_df,
            path_after_prefix="data/output.csv",
            file_format="csv",
            auto_version=False,
        )

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[1]["path_after_prefix"] == "data/output.csv"
        assert isinstance(call_args[1]["file_buffer"], bytes)

    def test_save_polars_dataframe_jsonl(
        self, mocker, blob_endpoint, sample_polars_df
    ):
        """Test saving a polars DataFrame as JSONL"""
        mock_write = mocker.patch.object(blob_endpoint, "write_blob")

        blob_endpoint.save_dataframe(
            df=sample_polars_df,
            path_after_prefix="data/output.jsonl",
            file_format="jsonl",
            auto_version=False,
        )

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[1]["path_after_prefix"] == "data/output.jsonl"
        assert isinstance(call_args[1]["file_buffer"], bytes)

    def test_save_dataframe_with_auto_version(
        self, mocker, blob_endpoint, sample_pandas_df
    ):
        """Test saving a DataFrame with auto-versioning enabled"""
        mock_write = mocker.patch.object(blob_endpoint, "write_blob")

        blob_endpoint.save_dataframe(
            df=sample_pandas_df,
            path_after_prefix="data/output.parquet",
            file_format="parquet",
            auto_version=True,
        )

        mock_write.assert_called_once()
        call_args = mock_write.call_args
        assert call_args[1]["auto_version"] is True

    def test_save_dataframe_invalid_format(
        self, blob_endpoint, sample_pandas_df
    ):
        """Test that invalid file format raises ValueError"""
        with pytest.raises(ValueError, match="File format .* not supported"):
            blob_endpoint.save_dataframe(
                df=sample_pandas_df,
                path_after_prefix="data/output.txt",
                file_format="txt",
                auto_version=False,
            )

    def test_save_dataframe_parquet_content_roundtrip(
        self, mocker, blob_endpoint, sample_pandas_df
    ):
        """Test that saved parquet content can be read back correctly"""
        captured_buffer = None

        def capture_write_blob(file_buffer, path_after_prefix, auto_version):
            nonlocal captured_buffer
            captured_buffer = file_buffer

        mocker.patch.object(
            blob_endpoint, "write_blob", side_effect=capture_write_blob
        )

        blob_endpoint.save_dataframe(
            df=sample_pandas_df,
            path_after_prefix="data/output.parquet",
            file_format="parquet",
            auto_version=False,
        )

        # Verify we can read back the data
        df_read = pd.read_parquet(BytesIO(captured_buffer))
        pd.testing.assert_frame_equal(df_read, sample_pandas_df)


class TestSaveFilesToBlob:
    """Tests for the save_file_to_blob method"""

    def test_save_file_to_blob(self, mocker, blob_endpoint):
        """Test saving a local file to blob storage"""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as f:
            f.write("test content\nline 2\nline 3")
            temp_file_path = f.name

        try:
            mock_write = mocker.patch.object(blob_endpoint, "write_blob")

            blob_endpoint.save_file_to_blob(
                file_path=temp_file_path,
                path_after_prefix="data/uploaded_file.txt",
                auto_version=False,
            )

            mock_write.assert_called_once()
            call_args = mock_write.call_args
            assert (
                call_args[1]["path_after_prefix"] == "data/uploaded_file.txt"
            )
            assert call_args[1]["auto_version"] is False
            assert isinstance(call_args[1]["file_buffer"], bytes)
        finally:
            os.unlink(temp_file_path)

    def test_save_file_to_blob_with_auto_version(self, mocker, blob_endpoint):
        """Test saving a file with auto-versioning enabled"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as f:
            f.write("test content")
            temp_file_path = f.name

        try:
            mock_write = mocker.patch.object(blob_endpoint, "write_blob")

            blob_endpoint.save_file_to_blob(
                file_path=temp_file_path,
                path_after_prefix="data/uploaded_file.txt",
                auto_version=True,
            )

            mock_write.assert_called_once()
            call_args = mock_write.call_args
            assert call_args[1]["auto_version"] is True
        finally:
            os.unlink(temp_file_path)

    def test_save_file_to_blob_nonexistent_file(self, blob_endpoint):
        """Test that saving a non-existent file raises ValueError"""
        with pytest.raises(ValueError, match="File .* does not exist"):
            blob_endpoint.save_file_to_blob(
                file_path="/nonexistent/path/to/file.txt",
                path_after_prefix="data/output.txt",
                auto_version=False,
            )

    def test_save_file_content_preserved(self, mocker, blob_endpoint):
        """Test that file content is preserved when saving"""
        test_content = b"test binary content\x00\x01\x02"

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(test_content)
            temp_file_path = f.name

        try:
            captured_buffer = None

            def capture_write_blob(
                file_buffer, path_after_prefix, auto_version
            ):
                nonlocal captured_buffer
                captured_buffer = file_buffer

            mocker.patch.object(
                blob_endpoint, "write_blob", side_effect=capture_write_blob
            )

            blob_endpoint.save_file_to_blob(
                file_path=temp_file_path,
                path_after_prefix="data/binary_file.bin",
                auto_version=False,
            )

            assert captured_buffer == test_content
        finally:
            os.unlink(temp_file_path)


class TestSaveDirToBlob:
    """Tests for the save_dir_to_blob method"""

    def test_save_dir_to_blob(self, mocker, blob_endpoint):
        """Test saving a directory to blob storage"""
        # Create a temporary directory with files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create some test files
            with open(os.path.join(temp_dir, "file1.txt"), "w") as f:
                f.write("content 1")
            with open(os.path.join(temp_dir, "file2.txt"), "w") as f:
                f.write("content 2")

            # Create a subdirectory with a file
            subdir = os.path.join(temp_dir, "subdir")
            os.makedirs(subdir)
            with open(os.path.join(subdir, "file3.txt"), "w") as f:
                f.write("content 3")

            mock_write = mocker.patch.object(blob_endpoint, "write_blob")

            blob_endpoint.save_dir_to_blob(
                dir_path=temp_dir,
                path_after_prefix="data/uploaded_dir",
                auto_version=False,
            )

            # Should call write_blob once per file (3 files total)
            assert mock_write.call_count == 3

            # Verify auto_version is False for all calls
            for call in mock_write.call_args_list:
                assert call[1]["auto_version"] is False
                assert isinstance(call[1]["file_buffer"], bytes)

    def test_save_dir_to_blob_with_auto_version(self, mocker, blob_endpoint):
        """Test saving a directory with auto-versioning enabled"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with open(os.path.join(temp_dir, "file1.txt"), "w") as f:
                f.write("content 1")

            mock_write = mocker.patch.object(blob_endpoint, "write_blob")

            blob_endpoint.save_dir_to_blob(
                dir_path=temp_dir,
                path_after_prefix="data/uploaded_dir",
                auto_version=True,
            )

            # Should call write_blob once per file (1 file)
            assert mock_write.call_count == 1
            call_args = mock_write.call_args
            assert call_args[1]["auto_version"] is True

    def test_save_dir_to_blob_nonexistent_dir(self, blob_endpoint):
        """Test that saving a non-existent directory raises ValueError"""
        with pytest.raises(ValueError, match="Directory .* does not exist"):
            blob_endpoint.save_dir_to_blob(
                dir_path="/nonexistent/path/to/dir",
                path_after_prefix="data/output",
                auto_version=False,
            )

    def test_save_empty_dir_to_blob(self, mocker, blob_endpoint):
        """Test saving an empty directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_write = mocker.patch.object(blob_endpoint, "write_blob")

            blob_endpoint.save_dir_to_blob(
                dir_path=temp_dir,
                path_after_prefix="data/empty_dir",
                auto_version=False,
            )

            # Empty directory should result in no write_blob calls
            mock_write.assert_not_called()

    def test_save_dir_preserves_file_count(self, mocker, blob_endpoint):
        """Test that all files in a directory are captured"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple files to ensure count is correct
            for i in range(5):
                with open(os.path.join(temp_dir, f"file{i}.txt"), "w") as f:
                    f.write(f"content {i}")

            mock_write = mocker.patch.object(blob_endpoint, "write_blob")

            blob_endpoint.save_dir_to_blob(
                dir_path=temp_dir,
                path_after_prefix="data/multi_files",
                auto_version=False,
            )

            # Should call write_blob once per file (5 files)
            assert mock_write.call_count == 5
            # Verify all buffers are bytes
            for call in mock_write.call_args_list:
                assert isinstance(call[1]["file_buffer"], bytes)


class TestSaveMethodsIntegration:
    """Integration tests using the full catalog structure"""

    def test_save_dataframe_through_catalog(
        self, mocker, mock_write_blob_stream, dataset_ns_map, dataset_defaults
    ):
        """Test save_dataframe works through the catalog namespace"""
        datacat = dict_to_sn(dataset_ns_map, dataset_defaults)

        mocker.patch(
            "cfa.dataops.catalog.write_blob_stream",
            mock_write_blob_stream,
        )

        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        # This should not raise any errors
        datacat.tests.etl_test.load.save_dataframe(
            df=df,
            path_after_prefix="test_output.parquet",
            file_format="parquet",
            auto_version=False,
        )

    def test_save_file_through_catalog(
        self, mocker, mock_write_blob_stream, dataset_ns_map, dataset_defaults
    ):
        """Test save_file_to_blob works through the catalog namespace"""
        datacat = dict_to_sn(dataset_ns_map, dataset_defaults)

        mocker.patch(
            "cfa.dataops.catalog.write_blob_stream",
            mock_write_blob_stream,
        )

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt"
        ) as f:
            f.write("test content")
            temp_file_path = f.name

        try:
            # This should not raise any errors
            datacat.tests.etl_test.load.save_file_to_blob(
                file_path=temp_file_path,
                path_after_prefix="test_file.txt",
                auto_version=False,
            )
        finally:
            os.unlink(temp_file_path)
