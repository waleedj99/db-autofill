from faker import Faker
from typing import Dict, Any, List, Optional
import random
import datetime
import collections

class DataGenerator:
    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.faker = Faker()
        # Track generated values for unique columns: {table: {col: set()}}
        self.unique_tracker = collections.defaultdict(lambda: collections.defaultdict(set))
        
    def generate_row(self, table_name: str, valid_foreign_keys: Dict[str, List[Any]] = None, column_configs: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Generate a single row of data for the given table.
        valid_foreign_keys: Dict mapping referenced_table_name -> list of available IDs
        column_configs: Dict mapping column_name -> ColumnConfig object (or dict with min_value/max_value)
        """
        if valid_foreign_keys is None:
            valid_foreign_keys = {}
        if column_configs is None:
            column_configs = {}
            
        table_schema = self.schema[table_name]
        columns = table_schema.get('columns', {})
        fks = table_schema.get('foreign_keys', {})
        unique_cols = table_schema.get('unique_columns', [])
        
        row = {}
        
        for col_name, col_info in columns.items():
            # Skip identity/serial columns that are auto-generated
            # col_info.get('default') might be None if key exists but value is None
            default_val = col_info.get('default')
            if col_info.get('is_identity') or (default_val and default_val.startswith('nextval')):
                continue
                
            # Handle Foreign Keys
            if col_name in fks:
                ref_table = fks[col_name]['references_table']
                possible_ids = valid_foreign_keys.get(ref_table, [])
                
                if not possible_ids:
                    # If no parent records exist
                    error_msg = f"Cannot generate data for {table_name}.{col_name}: No records found in parent table {ref_table}"
                    if col_info.get('is_nullable', True):
                        val = None
                    else:
                        raise ValueError(error_msg)
                else:
                    val = random.choice(possible_ids)
                row[col_name] = val
                continue
                
            # Generate value with uniqueness check
            is_unique = col_name in unique_cols or col_info.get('is_unique')
            
            # Simple retry mechanism for uniqueness
            for attempt in range(10):
                # Check for column specific config
                col_cfg = column_configs.get(col_name)
                val = self._generate_value(col_info, col_cfg)
                if is_unique and val is not None:
                    if val in self.unique_tracker[table_name][col_name]:
                        continue # Collision, retry
                    self.unique_tracker[table_name][col_name].add(val)
                row[col_name] = val
                break
            else:
                # Failed to generate unique value after retries
                if is_unique:
                     # Fallback to appending random string if text
                     if 'text' in col_info['type'].lower() or 'char' in col_info['type'].lower():
                         val = f"{val}_{random.randint(1000,9999)}"
                         row[col_name] = val
                         self.unique_tracker[table_name][col_name].add(val)
                     else:
                         raise ValueError(f"Could not generate unique value for {table_name}.{col_name} after 10 attempts")

        return row

    def _generate_value(self, col_info: Dict[str, Any], col_cfg: Any = None) -> Any:
        dtype = col_info['type'].lower()
        is_nullable = col_info.get('is_nullable', False)
        
        # 10% chance of NULL if nullable
        if is_nullable and random.random() < 0.1:
            return None

        # Custom Values Check
        if col_cfg and col_cfg.values:
            # Validate types on first usage (or every time, fine for this scale)
            allowed_values = col_cfg.values
            val = random.choice(allowed_values)
            
            # Simple Type Checking
            # This is not exhaustive but catches obvious mismatches like string in int column
            val_type = type(val)
            if 'int' in dtype and not isinstance(val, int):
                raise ValueError(f"Type Mismatch: Column '{col_info.get('name')}' is type '{dtype}' but config provided value '{val}' of type '{val_type.__name__}'")
            if ('numeric' in dtype or 'decimal' in dtype or 'float' in dtype) and not isinstance(val, (int, float)):
                 raise ValueError(f"Type Mismatch: Column '{col_info.get('name')}' is type '{dtype}' but config provided value '{val}' of type '{val_type.__name__}'")
            # For text/char, almost anything can be cast to string, so less strict.
            
            return val

        # Determine limits for Range Checks
        min_val = None
        max_val = None
        if col_cfg:
            min_val = col_cfg.min_value
            max_val = col_cfg.max_value
            
            # Validate that range is being applied to correct type
            if (min_val is not None or max_val is not None):
                 if 'int' not in dtype and 'numeric' not in dtype and 'decimal' not in dtype and 'double' not in dtype and 'float' not in dtype:
                      # Range provided for non-numeric column
                      raise ValueError(f"Configuration Error: min_value/max_value provided for column '{col_info.get('name')}' which is of type '{dtype}'. Ranges only supported for numeric types.")

        if 'json' in dtype:
            return "{}"

        if 'int' in dtype:
            # Defaults
            effective_min = int(min_val) if min_val is not None else 1
            effective_max = int(max_val) if max_val is not None else 10000
            if effective_min > effective_max:
                effective_max = effective_min  # gracefully handle error config
            return self.faker.random_int(min=effective_min, max=effective_max)

        elif 'char' in dtype or 'text' in dtype:
            if 'email' in col_info.get('name', '').lower():
                return self.faker.email()
            if 'name' in col_info.get('name', '').lower():
                return self.faker.name()
            # Handle max length if possible (not parsed yet), usually safe to default
            return self.faker.text(max_nb_chars=50)
        elif 'bool' in dtype:
            return self.faker.boolean()
        elif 'date' in dtype or 'time' in dtype:
            return self.faker.date_time_between(start_date='-1y', end_date='now')
        elif 'numeric' in dtype or 'decimal' in dtype or 'double' in dtype:
            # Defaults
            effective_min = float(min_val) if min_val is not None else 0.0
            effective_max = float(max_val) if max_val is not None else 1000.0
            if effective_min > effective_max:
                 effective_max = effective_min
            return self.faker.pyfloat(right_digits=2, min_value=effective_min, max_value=effective_max)
        
        # Fallback
        return self.faker.word()
