from pydantic import BaseModel, Field
from typing import List, Optional, Union
from pathlib import Path

class DatabaseConfig(BaseModel):
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    name: str = Field(..., description="Database name")
    user: str = Field(..., description="Database user")
    password: str = Field(..., description="Database password")


class ColumnConfig(BaseModel):
    name: str = Field(..., description="Column name")
    min_value: Optional[Union[int, float]] = Field(None, description="Minimum value")
    max_value: Optional[Union[int, float]] = Field(None, description="Maximum value")
    values: Optional[List[Union[str, int, float, bool]]] = Field(None, description="List of allowed values")

class TableConfig(BaseModel):
    name: str = Field(..., description="Table name")
    row_count: int = Field(default=50, gt=0, description="Number of rows to generate")
    columns: List[ColumnConfig] = Field(default_factory=list, description="Column-specific configurations")

class AutofillConfig(BaseModel):
    database: DatabaseConfig
    tables: List[TableConfig] = Field(default_factory=list)

def load_config(config_path: str | Path) -> AutofillConfig:
    """Load configuration from JSON file"""
    import json
    with open(config_path) as f:
        data = json.load(f)
    return AutofillConfig(**data)
