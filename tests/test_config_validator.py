import unittest

from pydantic import ValidationError

from cfa.dataops.config_validator import ConfigValidator


class TestConfigValidator(unittest.TestCase):
    def setUp(self):
        self.valid_storage_endpoint = {
            "account": "test_account",
            "container": "test_container",
            "prefix": "dataops/scenarios/test",
        }

        self.good_config = {
            "properties": {
                "name": "test_dataset_name",
                "type": "etl",
                "automate": False,
                "transform_templates": ["test_tf.sql"],
                "schema": "datasets/schemas/tests.py",
            },
            "source": {"url": "https://data.csv", "uid": "test_uid"},
            "extract": self.valid_storage_endpoint.copy(),
            "load": {
                **self.valid_storage_endpoint,
                "prefix": "dataops/scenarios/transformed/test",
            },
            "stage_00": {
                **self.valid_storage_endpoint,
                "prefix": "dataops/scenarios/stage_00/test",
            },
            "stage_01": {
                **self.valid_storage_endpoint,
                "prefix": "dataops/scenarios/stage_01/test",
            },
        }

    def test_valid_config(self):
        """Test that a valid configuration passes validation."""
        config = ConfigValidator(**self.good_config)
        self.assertIsInstance(config, ConfigValidator)

    def test_invalid_storage_endpoint(self):
        """Test that invalid storage endpoints are caught."""
        bad_config = self.good_config.copy()
        bad_config["stage_00"] = {
            "account": "test_account"
        }  # Missing required fields

        with self.assertRaises(ValidationError):
            ConfigValidator(**bad_config)

    def test_stage_field_validation(self):
        """Test that stage fields are properly validated."""
        # Test valid stage field
        config = self.good_config.copy()
        config["stage_02"] = self.valid_storage_endpoint.copy()
        validated = ConfigValidator(**config)
        self.assertIsNotNone(validated.stage_02)

        # Test invalid stage field
        config["stage_03"] = {"invalid": "endpoint"}
        with self.assertRaises(ValidationError):
            ConfigValidator(**config)

    def test_optional_stages(self):
        """Test that stages are optional."""
        config = self.good_config.copy()
        del config["stage_00"]
        del config["stage_01"]

        validated = ConfigValidator(**config)
        self.assertIsInstance(validated, ConfigValidator)
