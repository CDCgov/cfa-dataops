from cfa.scenarios.dataops.datasets.config_validator import validate_dataset_config, verify_no_repeats
import unittest

good_config = {
    "properties":
        {"name": "test_dataset_name", 
         "automate": False,
        "transform_template": "test_tf.sql",
        "schema": "datasets/schemas/tests.py"},
    "source": {
        "url": "https://data.csv",
        "pagination": {"limit": 1000}
    },
    "extract":{
        "account": "test_account_raw",
        "container": "test_container_raw",
        "prefix": "dataops/scenarios/raw/test"
    },      
    "load": {"account": "test_account_tf",
        "container": "test_container_tf",
        "prefix": "dataops/scenarios/transformed/test"
        },
    "_metadata": {"filename": 'test_filename'}
}
bad_config = {
    "properties":
        {"name": "test_dataset_name", 
         "automate": False,
        "transform_template": "test_tf.sql",
        "schema": "datasets/schemas/tests.py"},
    "source": {
        "url": "https://data.csv",
        "pagination": {"limit": 1000}
    },
    "extract":{
        "account": "test_account_raw",
        "container": "test_container_raw",
    },      
    "load": {"account": "test_account_tf",
        "container": "test_container_tf",
        },
    "_metadata": {"filename": 'test_filename'}
}

class TestConfigVal(unittest.TestCase):
    def test_validate_dataset_config(self):
        self.assertEqual(validate_dataset_config(good_config), None)
    def test_bad_validate_dataset_config(self):
        with self.assertRaises(KeyError):
            validate_dataset_config(bad_config)
            
class TestVerifyNoRepeats(unittest.TestCase):
    def test_verify_no_repeats_no_duplicates(self):
        configs = [
        {
            "properties": {"name": "dataset_one"},
            "_metadata": {"filename": "file1.toml"}
        },
        {
            "properties": {"name": "dataset_two"},
            "_metadata": {"filename": "file2.toml"}
        }
        ]
        # Should not raise
        self.assertIsNone(verify_no_repeats(configs))

    def test_verify_no_repeats_with_duplicates(self):
        configs = [
        {
            "properties": {"name": "dataset_one"},
            "_metadata": {"filename": "file1.toml"}
        },
        {
            "properties": {"name": "dataset_one"},
            "_metadata": {"filename": "file2.toml"}
        }
        ]
        with self.assertRaises(AttributeError) as exc:
            verify_no_repeats(configs)
        assert "dataset_one" in str(exc.exception)
        assert "file1.toml" in str(exc.exception)
        assert "file2.toml" in str(exc.exception)