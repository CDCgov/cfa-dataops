"""The dataset config validator."""

from enum import Enum
from typing import List, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)


class DatasetType(str, Enum):
    """Dataset types."""

    etl = "etl"
    experiment = "experiment"
    multistage = "multistage"
    reference = "reference"


class PropertiesValidation(BaseModel):
    """Validate the properties field."""

    model_config = ConfigDict(extra="allow")
    name: str = Field(
        ...,
        description="The name of the dataset.",
        min_length=1,
    )
    type: DatasetType = Field(
        ...,
        description="The type of the dataset.",
    )
    schemas: Optional[str] = Field(
        None,
        description="the relative path to where the data schema(s) are defined.",
        min_length=1,
    )
    transform_templates: Optional[List[str]] = Field(
        None,
        description="A list of transform templates to applied to raw data.",
        min_items=1,
    )
    automate: Optional[bool] = Field(
        False,
        description="Whether to automate workflows that cache raw data and transform it.",
    )


class SourceValidation(BaseModel):
    """Validate the source field. This field is meant to be very open ended."""

    model_config = ConfigDict(extra="allow")


class StorageEndpointValidation(BaseModel):
    """Validate the storage endpoint field. This field is meant to be very open ended."""

    model_config = ConfigDict(extra="forbid")
    account: str = Field(
        ...,
        description="the account name for the storage endpoint.",
        min_length=1,
    )
    container: str = Field(
        ...,
        description="the container name for the storage endpoint.",
        min_length=1,
    )
    prefix: str = Field(
        ...,
        description="the prefix (folder path) for the storage endpoint. A sub-directory within the this path will be created for each version/timestamped dataset.",
        min_length=1,
    )


class ConfigValidator(BaseModel):
    """Validate the dataset configuration."""

    model_config = ConfigDict(extra="allow")
    properties: PropertiesValidation
    load: Optional[StorageEndpointValidation] = None
    source: Optional[SourceValidation] = None
    extract: Optional[StorageEndpointValidation] = None

    # Any stage_xx field will be validated dynamically; no need for explicit field or nesting.
    @classmethod
    def validate_stage_fields(cls, values):
        # ValidationError already imported at top
        for key, value in values.items():
            if key.startswith("stage_") and value is not None:
                if not isinstance(value, StorageEndpointValidation):
                    try:
                        values[key] = StorageEndpointValidation(**value)
                    except Exception as e:
                        raise ValidationError(
                            [e], StorageEndpointValidation
                        ) from e
        return values

    _validate_stages = model_validator(mode="before")(validate_stage_fields)
