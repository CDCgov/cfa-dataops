"""Tests for utils.tree function"""

import os
import tempfile
from pathlib import Path

from cfa.dataops.utils import tree


class TestTreeFunction:
    """Tests for the tree function"""

    def test_tree_simple_directory(self):
        """Test tree output for a simple directory structure"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a simple structure
            Path(tmpdir, "file1.txt").touch()
            Path(tmpdir, "file2.txt").touch()

            result = tree(tmpdir, level=-1)

            # Should contain the directory name and files
            assert os.path.basename(tmpdir) in result
            assert "file1.txt" in result
            assert "file2.txt" in result
            assert "2 files" in result

    def test_tree_nested_directories(self):
        """Test tree output for nested directory structure"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create nested structure
            subdir = Path(tmpdir, "subdir")
            subdir.mkdir()
            Path(tmpdir, "file1.txt").touch()
            Path(subdir, "file2.txt").touch()

            result = tree(tmpdir, level=-1)

            assert "subdir" in result
            assert "file1.txt" in result
            assert "file2.txt" in result
            assert "1 directories" in result or "1 directory" in result

    def test_tree_level_limit(self):
        """Test tree respects level parameter"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create deep nested structure
            level1 = Path(tmpdir, "level1")
            level2 = Path(level1, "level2")
            level3 = Path(level2, "level3")

            level1.mkdir()
            level2.mkdir()
            level3.mkdir()

            Path(tmpdir, "file0.txt").touch()
            Path(level1, "file1.txt").touch()
            Path(level2, "file2.txt").touch()
            Path(level3, "file3.txt").touch()

            # Level 1 should only show first level (directories and files at root)
            result = tree(tmpdir, level=1)
            assert "level1" in result
            assert "file0.txt" in result
            # Should not show deeper levels (level 1 means one level of recursion into level1)
            assert "level2" not in result
            assert "file2.txt" not in result
            assert "file3.txt" not in result

    def test_tree_limit_to_directories(self):
        """Test tree with limit_to_directories option"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create structure with both files and directories
            subdir = Path(tmpdir, "subdir")
            subdir.mkdir()
            Path(tmpdir, "file1.txt").touch()
            Path(tmpdir, "file2.txt").touch()
            Path(subdir, "file3.txt").touch()

            result = tree(tmpdir, limit_to_directories=True)

            # Should show directory
            assert "subdir" in result
            # Should not show files
            assert "file1.txt" not in result
            assert "file2.txt" not in result
            assert "file3.txt" not in result
            # Should only count directories, not files
            assert "1 directories" in result or "1 directory" in result
            assert "files" not in result or "0 files" in result

    def test_tree_show_hidden_false(self):
        """Test tree hides hidden files by default"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create normal and hidden files
            Path(tmpdir, "visible.txt").touch()
            Path(tmpdir, ".hidden.txt").touch()
            hidden_dir = Path(tmpdir, ".hidden_dir")
            hidden_dir.mkdir()
            Path(hidden_dir, "file.txt").touch()

            result = tree(tmpdir, show_hidden=False)

            # Should show visible file
            assert "visible.txt" in result
            # Should not show hidden files/directories
            assert ".hidden.txt" not in result
            assert ".hidden_dir" not in result

    def test_tree_show_hidden_true(self):
        """Test tree shows hidden files when requested"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create normal and hidden files
            Path(tmpdir, "visible.txt").touch()
            Path(tmpdir, ".hidden.txt").touch()
            hidden_dir = Path(tmpdir, ".hidden_dir")
            hidden_dir.mkdir()

            result = tree(tmpdir, show_hidden=True)

            # Should show all files
            assert "visible.txt" in result
            assert ".hidden.txt" in result
            assert ".hidden_dir" in result

    def test_tree_length_limit(self):
        """Test tree respects length_limit parameter"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create many files
            for i in range(20):
                Path(tmpdir, f"file{i:02d}.txt").touch()

            result = tree(tmpdir, length_limit=5)

            # Should contain the limit message
            assert "length_limit" in result
            assert "5, reached" in result

    def test_tree_empty_directory(self):
        """Test tree output for an empty directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = tree(tmpdir)

            # Should contain directory name and count
            assert os.path.basename(tmpdir) in result
            assert "0 directories" in result

    def test_tree_directory_counts(self):
        """Test tree correctly counts directories and files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create known structure
            Path(tmpdir, "dir1").mkdir()
            Path(tmpdir, "dir2").mkdir()
            Path(tmpdir, "dir3").mkdir()
            Path(tmpdir, "file1.txt").touch()
            Path(tmpdir, "file2.txt").touch()

            result = tree(tmpdir)

            assert "3 directories" in result
            assert "2 files" in result

    def test_tree_visual_structure(self):
        """Test tree produces correct visual structure with branches"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create specific structure to test visual elements
            Path(tmpdir, "file1.txt").touch()
            Path(tmpdir, "file2.txt").touch()

            result = tree(tmpdir)

            # Should contain tree drawing characters
            assert "├──" in result or "└──" in result

    def test_tree_accepts_string_path(self):
        """Test tree accepts string paths in addition to Path objects"""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "test.txt").touch()

            # Should work with string path
            result = tree(str(tmpdir))

            assert "test.txt" in result

    def test_tree_accepts_path_object(self):
        """Test tree accepts Path objects"""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "test.txt").touch()

            # Should work with Path object
            result = tree(Path(tmpdir))

            assert "test.txt" in result

    def test_tree_deep_nesting(self):
        """Test tree handles deep directory nesting"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a deeply nested structure
            current = Path(tmpdir)
            for i in range(5):
                current = current / f"level{i}"
                current.mkdir(parents=True)
                Path(current, f"file{i}.txt").touch()

            result = tree(tmpdir, level=-1)

            # Should show all levels
            for i in range(5):
                assert f"level{i}" in result
                assert f"file{i}.txt" in result

    def test_tree_mixed_files_and_dirs(self):
        """Test tree with mixed files and directories at various levels"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Root level
            Path(tmpdir, "root_file.txt").touch()

            # First level
            dir1 = Path(tmpdir, "dir1")
            dir1.mkdir()
            Path(dir1, "file_in_dir1.txt").touch()

            # Second level
            dir2 = Path(dir1, "dir2")
            dir2.mkdir()
            Path(dir2, "file_in_dir2.txt").touch()

            result = tree(tmpdir, level=-1)

            assert "root_file.txt" in result
            assert "dir1" in result
            assert "file_in_dir1.txt" in result
            assert "dir2" in result
            assert "file_in_dir2.txt" in result

    def test_tree_level_zero(self):
        """Test tree with level=0 shows nothing beyond root"""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "file.txt").touch()
            subdir = Path(tmpdir, "subdir")
            subdir.mkdir()

            result = tree(tmpdir, level=0)

            # Should only show the root directory name
            assert os.path.basename(tmpdir) in result
            # Should not traverse into contents
            assert "file.txt" not in result
            assert "subdir" not in result

    def test_tree_multiple_subdirectories(self):
        """Test tree displays multiple subdirectories correctly"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple subdirectories
            for i in range(4):
                subdir = Path(tmpdir, f"subdir_{i}")
                subdir.mkdir()
                Path(subdir, f"file_{i}.txt").touch()

            result = tree(tmpdir)

            # All subdirs should be present
            for i in range(4):
                assert f"subdir_{i}" in result
                assert f"file_{i}.txt" in result

    def test_tree_special_characters_in_names(self):
        """Test tree handles files/directories with special characters"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create files with special characters (safe for filesystem)
            Path(tmpdir, "file-with-dash.txt").touch()
            Path(tmpdir, "file_with_underscore.txt").touch()
            Path(tmpdir, "file with spaces.txt").touch()

            result = tree(tmpdir)

            assert "file-with-dash.txt" in result
            assert "file_with_underscore.txt" in result
            assert "file with spaces.txt" in result

    def test_tree_returns_string(self):
        """Test that tree returns a string"""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = tree(tmpdir)
            assert isinstance(result, str)

    def test_tree_single_file_no_directories(self):
        """Test tree with only a single file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "single_file.txt").touch()

            result = tree(tmpdir)

            assert "single_file.txt" in result
            assert "0 directories" in result
            assert "1 files" in result or "1 file" in result

    def test_tree_only_directories_no_files(self):
        """Test tree with only directories, no files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "dir1").mkdir()
            Path(tmpdir, "dir2").mkdir()
            Path(tmpdir, "dir3").mkdir()

            result = tree(tmpdir)

            assert "dir1" in result
            assert "dir2" in result
            assert "dir3" in result
            assert "3 directories" in result
