import psycopg2
from typing import Dict, Any
try:
    from .config import DatabaseConfig
except ImportError:
    from config import DatabaseConfig

def extract_schema(db_config: DatabaseConfig) -> Dict[str, Any]:
    """
    Extract complete schema from PostgreSQL database.
    
    Returns:
        {
            'customers': {
                'columns': {
                    'id': {'type': 'integer', 'is_pk': True, 'is_nullable': False, ...},
                    'email': {...}
                },
                'foreign_keys': {
                    'customer_id': {'references_table': 'customers', 'references_column': 'id'}
                },
                'unique_columns': ['email']
            }
        }
    """
    
    conn = psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.name,
        user=db_config.user,
        password=db_config.password
    )
    
    schema = {}
    
    try:
        cursor = conn.cursor()
        
        # 1. Get all tables in public schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        # 2. For each table, get columns
        for table_name in tables:
            schema[table_name] = {
                'columns': {},
                'foreign_keys': {},
                'unique_columns': []
            }
            
            # Get columns
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default, is_identity
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table_name,))
            
            for row in cursor.fetchall():
                col_name, col_type, is_nullable, col_default, is_identity = row
                schema[table_name]['columns'][col_name] = {
                    'type': col_type,
                    'is_nullable': is_nullable == 'YES',
                    'default': col_default,
                    'is_identity': is_identity == 'YES'
                }
        
        # 3. Get primary keys
        cursor.execute("""
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
        """)
        for row in cursor.fetchall():
            table_name, col_name = row
            if table_name in schema:
                schema[table_name]['columns'][col_name]['is_pk'] = True
        
        # 4. Get foreign keys
        cursor.execute("""
            SELECT 
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu 
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu 
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
        """)
        for row in cursor.fetchall():
            table_name, col_name, ref_table, ref_col = row
            if table_name in schema:
                schema[table_name]['foreign_keys'][col_name] = {
                    'references_table': ref_table,
                    'references_column': ref_col
                }
        
        # 5. Get unique constraints
        cursor.execute("""
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'UNIQUE' AND tc.table_schema = 'public'
        """)
        for row in cursor.fetchall():
            table_name, col_name = row
            if table_name in schema:
                schema[table_name]['unique_columns'].append(col_name)
                schema[table_name]['columns'][col_name]['is_unique'] = True
        
        cursor.close()
        return schema
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Test the parser
    # DatabaseConfig is already imported at the top level
    db_config = DatabaseConfig(
        host="localhost",
        port=5432,
        name="your_test_db",
        user="postgres",
        password="your_password"
    )
    
    schema = extract_schema(db_config)
    print(f"Found {len(schema)} tables:")
    for table in schema:
        print(f"  - {table}: {len(schema[table]['columns'])} columns")
        if schema[table]['foreign_keys']:
            print(f"    FKs: {list(schema[table]['foreign_keys'].keys())}")
