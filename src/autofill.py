import click
import json
try:
    from .config import load_config
    from .schema_parser import extract_schema
except ImportError:
    from config import load_config
    from schema_parser import extract_schema

@click.command()
@click.option('--config', default='examples/config.json', help='Path to configuration file')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def main(config, yes):
    """Database Autofill Tool"""
    try:
        from .dependency_analyzer import get_insertion_order
        from .data_generator import DataGenerator
        from .database_inserter import DatabaseInserter
    except ImportError:
        # Fallback for running as script
        from dependency_analyzer import get_insertion_order
        from data_generator import DataGenerator
        from database_inserter import DatabaseInserter

    try:
        click.echo(f"Loading configuration from {config}...")
        cfg = load_config(config)
        
        # Security Guard: Warn if running on non-local DB
        if cfg.database.host.lower() not in ['localhost', '127.0.0.1', '::1'] and not yes:
             click.echo(click.style(f"WARNING: You are about to run autofill on a remote host: {cfg.database.host}", fg='red', bold=True))
             click.echo("This will insert fake data into the target database.")
             if not click.confirm("Are you sure you want to continue?"):
                 click.echo("Aborted.")
                 return
        
        click.echo("Connecting to database and extracting schema...")
        schema = extract_schema(cfg.database)
        
        click.echo("Analyzing dependencies...")
        insertion_order = get_insertion_order(schema)
        click.echo(f"Insertion Order: {' -> '.join(insertion_order)}")
        
        generator = DataGenerator(schema)
        
        with DatabaseInserter(cfg.database) as inserter:
            try:
                for table_name in insertion_order:
                    # Determine how many rows to generate
                    target_rows = 50 # Default
                    column_configs_map = {}
                    
                    for t_conf in cfg.tables:
                        if t_conf.name == table_name:
                            target_rows = t_conf.row_count
                            # Build column config lookup map
                            for col_conf in t_conf.columns:
                                column_configs_map[col_conf.name] = col_conf
                            break
                    
                    click.echo(f"Processing {table_name} (Target: {target_rows} rows)...")
                    
                    # Fetch valid FKs for this table
                    # We need to know which tables this table references
                    fks = schema[table_name].get('foreign_keys', {})
                    valid_fks = {}
                    
                    for _, fk_info in fks.items():
                        ref_table = fk_info['references_table']
                        # ref_pk = fk_info['references_column'] 
                        # Optimization: We usually want the PK of the referenced table. 
                        # schema parser returns target column, which is usually PK.
                        ref_pk = fk_info['references_column']

                        # TODO: caching optimization?
                        ids = inserter.fetch_ids(ref_table, ref_pk)
                        valid_fks[ref_table] = ids
                    
                    # Generate Data
                    batch = []
                    for _ in range(target_rows):
                        try:
                            row = generator.generate_row(table_name, valid_fks, column_configs_map)
                            batch.append(row)
                        except ValueError as ve:
                            click.echo(f"Skipping row for {table_name}: {ve}", err=True)
                            break
                            
                    # Insert Data
                    if batch:
                        inserter.insert_batch(table_name, batch)
                    else:
                        click.echo(f"No data generated for {table_name}")

                inserter.commit()
                click.echo("\nAutofill completed successfully!")
                
            except Exception as e:
                click.echo(f"An error occurred during processing. Rolling back changes...", err=True)
                inserter.rollback()
                raise e
            
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        # import traceback
        # traceback.print_exc()

if __name__ == "__main__":
    main()
