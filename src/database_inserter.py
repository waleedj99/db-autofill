import psycopg2
from typing import List, Dict, Any, Union
try:
    from .config import DatabaseConfig
except ImportError:
    from config import DatabaseConfig

class DatabaseInserter:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.conn = None
        
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.name,
            user=self.config.user,
            password=self.config.password
        )

    def close(self):
        if self.conn:
            self.conn.close()

    def fetch_ids(self, table_name: str, pk_column: str = 'id') -> List[Any]:
        """Fetch all primary keys from a table to use as foreign keys"""
        with self.conn.cursor() as cur:
            # Safe because table_name comes from internal schema, but good practice to quote
            # Escape double quotes in identifiers to prevent SQL injection via table/column names if they were malicious
            safe_table = table_name.replace('"', '""')
            safe_pk = pk_column.replace('"', '""')
            q = f'SELECT "{safe_pk}" FROM "{safe_table}"'
            cur.execute(q)
            return [row[0] for row in cur.fetchall()]

    def insert_batch(self, table_name: str, data: List[Dict[str, Any]]):
        if not data:
            return
            
        columns = list(data[0].keys())
        values_template = ', '.join(['%s'] * len(columns))
        columns_quoted = ', '.join([f'"{c.replace('"', '""')}"' for c in columns])
        safe_table = table_name.replace('"', '""')
        
        query = f'INSERT INTO "{safe_table}" ({columns_quoted}) VALUES ({values_template})'
        
        with self.conn.cursor() as cur:
            values = [[row[c] for c in columns] for row in data]
            cur.executemany(query, values)

            print(f"Inserted {len(values)} rows into {table_name}")

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()
