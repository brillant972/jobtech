"""
PostgreSQL Database Creator with Parquet Data Loader
This script creates a new PostgreSQL database and loads data from parquet files.
"""

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import numpy as np
import json
import logging
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgreSQLParquetLoader:
    def __init__(self, host: str = 'localhost', port: int = 5432, 
                 user: str = 'postgres', password: str = 'password'):
        """
        Initialize the PostgreSQL connection parameters
        
        Args:
            host: PostgreSQL server host
            port: PostgreSQL server port
            user: PostgreSQL username
            password: PostgreSQL password
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None
    
    def connect_to_postgres(self, database: str = 'postgres') -> bool:
        """
        Connect to PostgreSQL server
        
        Args:
            database: Database name to connect to
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=database
            )
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to PostgreSQL database: {database}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def create_database(self, database_name: str) -> bool:
        """
        Create a new PostgreSQL database
        
        Args:
            database_name: Name of the database to create
            
        Returns:
            bool: True if database created successfully, False otherwise
        """
        try:
            # Connect to the default postgres database first
            if not self.connect_to_postgres('postgres'):
                return False
            
            # Set autocommit mode to create database
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            # Check if database already exists
            self.cursor.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                (database_name,)
            )
            
            if self.cursor.fetchone():
                logger.info(f"Database '{database_name}' already exists")
                return True
            
            # Create the database
            self.cursor.execute(f'CREATE DATABASE "{database_name}"')
            logger.info(f"Database '{database_name}' created successfully")
            
            # Close connection to postgres database
            self.close_connection()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create database '{database_name}': {e}")
            return False
    
    def get_parquet_files(self, directory: str) -> List[str]:
        """
        Get all parquet files from a directory
        
        Args:
            directory: Directory path to search for parquet files
            
        Returns:
            List of parquet file paths
        """
        parquet_files = []
        
        if not os.path.exists(directory):
            logger.error(f"Directory '{directory}' does not exist")
            return parquet_files
        
        for file in os.listdir(directory):
            if file.lower().endswith('.parquet'):
                parquet_files.append(os.path.join(directory, file))
        
        logger.info(f"Found {len(parquet_files)} parquet files in '{directory}'")
        return parquet_files
    
    def infer_postgresql_type(self, pandas_dtype: str, sample_data=None) -> str:
        """
        Map pandas data types to PostgreSQL data types
        
        Args:
            pandas_dtype: Pandas data type
            sample_data: Sample data to inspect for complex types
            
        Returns:
            PostgreSQL data type
        """
        # Handle array/list columns
        if sample_data is not None and len(sample_data) > 0:
            # Check if the column contains arrays/lists
            first_valid = None
            for item in sample_data:
                if pd.notna(item):
                    first_valid = item
                    break
            
            if first_valid is not None:
                # Check if it's a list or array
                if isinstance(first_valid, (list, tuple, np.ndarray)):
                    # Determine the array element type
                    if len(first_valid) > 0:
                        element_type = type(first_valid[0])
                        if element_type in (int, np.integer):
                            return 'INTEGER[]'
                        elif element_type in (float, np.floating):
                            return 'DOUBLE PRECISION[]'
                        elif element_type in (str, np.str_):
                            return 'TEXT[]'
                        elif element_type in (bool, np.bool_):
                            return 'BOOLEAN[]'
                    return 'TEXT[]'  # Default array type
                
                # Check if it's a nested structure (dict, etc.)
                elif isinstance(first_valid, dict):
                    return 'JSONB'
        
        # Standard type mapping
        type_mapping = {
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'int16': 'SMALLINT',
            'int8': 'SMALLINT',
            'float64': 'DOUBLE PRECISION',
            'float32': 'REAL',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'TEXT',
            'category': 'TEXT'
        }
        
        # Handle string dtypes
        if 'string' in str(pandas_dtype).lower():
            return 'TEXT'
        
        return type_mapping.get(str(pandas_dtype), 'TEXT')
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        Create a PostgreSQL table based on DataFrame structure
        
        Args:
            df: Pandas DataFrame
            table_name: Name of the table to create
            
        Returns:
            bool: True if table created successfully, False otherwise
        """
        try:
            # Generate CREATE TABLE statement
            columns = []
            for col, dtype in df.dtypes.items():
                # Get sample data for complex type detection
                sample_data = df[col].dropna().head(10) if len(df) > 0 else None
                pg_type = self.infer_postgresql_type(dtype, sample_data)
                
                # Clean column name (replace spaces and special chars)
                clean_col = col.replace(' ', '_').replace('-', '_').replace('.', '_')
                columns.append(f'"{clean_col}" {pg_type}')
            
            create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {', '.join(columns)}
            )
            '''
            
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            logger.info(f"Table '{table_name}' created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}")
            return False
    
    def load_parquet_to_table(self, parquet_file: str, table_name: str = None) -> bool:
        """
        Load data from parquet file to PostgreSQL table
        
        Args:
            parquet_file: Path to the parquet file
            table_name: Name of the table (if None, uses filename without extension)
            
        Returns:
            bool: True if data loaded successfully, False otherwise
        """
        try:
            # Read parquet file
            df = pd.read_parquet(parquet_file)
            logger.info(f"Loaded parquet file: {parquet_file} ({len(df)} rows)")
            
            # Generate table name if not provided
            if table_name is None:
                table_name = os.path.splitext(os.path.basename(parquet_file))[0]
            
            # Clean table name
            table_name = table_name.replace(' ', '_').replace('-', '_').replace('.', '_')
            
            # Create table
            if not self.create_table_from_dataframe(df, table_name):
                return False
            
            # Clean column names in DataFrame
            df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') 
                         for col in df.columns]
            
            # Insert data using pandas to_sql (requires SQLAlchemy)
            try:
                from sqlalchemy import create_engine
                
                # Create SQLAlchemy engine
                engine = create_engine(
                    f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database_name}'
                )
                
                # Prepare data for insertion (handle arrays and complex types)
                prepared_df = self.prepare_data_for_insertion(df)
                
                # Insert data
                prepared_df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
                logger.info(f"Data inserted into table '{table_name}' successfully")
                
            except ImportError:
                # Fallback method without SQLAlchemy
                logger.warning("SQLAlchemy not available, using manual insertion method")
                self._insert_data_manually(df, table_name)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load parquet file '{parquet_file}': {e}")
            return False
    
    def prepare_data_for_insertion(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame data for PostgreSQL insertion, handling arrays and complex types
        
        Args:
            df: Original DataFrame
            
        Returns:
            DataFrame with data formatted for PostgreSQL
        """
        df_copy = df.copy()
        
        for col in df_copy.columns:
            # Check if column contains arrays/lists
            if len(df_copy) > 0 and df_copy[col].notna().any():
                first_valid = df_copy[col].dropna().iloc[0]
                
                if isinstance(first_valid, (list, tuple, np.ndarray)):
                    # Convert arrays to PostgreSQL array format
                    df_copy[col] = df_copy[col].apply(
                        lambda x: self._format_array_for_postgres(x) if pd.notna(x) else None
                    )
                elif isinstance(first_valid, dict):
                    # Convert dict to JSON string
                    df_copy[col] = df_copy[col].apply(
                        lambda x: json.dumps(x) if pd.notna(x) else None
                    )
        
        return df_copy
    
    def _format_array_for_postgres(self, arr):
        """
        Format array/list for PostgreSQL insertion
        
        Args:
            arr: Array, list, or tuple
            
        Returns:
            Formatted array for PostgreSQL
        """
        if pd.isna(arr) or arr is None:
            return None
        
        try:
            # Convert to list if it's a numpy array
            if isinstance(arr, np.ndarray):
                arr = arr.tolist()
            
            # Handle nested arrays (convert to string representation)
            if isinstance(arr, (list, tuple)):
                # Format as PostgreSQL array literal
                formatted_items = []
                for item in arr:
                    if isinstance(item, str):
                        # Escape quotes and wrap in quotes
                        formatted_items.append(f'"{item.replace('"', '""')}"')
                    elif pd.isna(item) or item is None:
                        formatted_items.append('NULL')
                    else:
                        formatted_items.append(str(item))
                
                return '{' + ','.join(formatted_items) + '}'
            
            return str(arr)
            
        except Exception as e:
            logger.warning(f"Failed to format array {arr}: {e}")
            return str(arr)
    def _insert_data_manually(self, df: pd.DataFrame, table_name: str):
        """
        Manually insert data without SQLAlchemy
        
        Args:
            df: Pandas DataFrame
            table_name: Name of the table
        """
        try:
            # Prepare data for insertion (handle arrays and complex types)
            prepared_df = self.prepare_data_for_insertion(df)
            
            # Prepare column names
            columns = ', '.join([f'"{col}"' for col in prepared_df.columns])
            placeholders = ', '.join(['%s'] * len(prepared_df.columns))
            
            insert_sql = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'
            
            # Convert DataFrame to list of tuples
            data = []
            for _, row in prepared_df.iterrows():
                row_data = []
                for val in row.values:
                    if pd.isna(val):
                        row_data.append(None)
                    else:
                        row_data.append(val)
                data.append(tuple(row_data))
            
            # Execute batch insert
            self.cursor.executemany(insert_sql, data)
            self.conn.commit()
            logger.info(f"Manually inserted {len(data)} rows into '{table_name}'")
            
        except Exception as e:
            logger.error(f"Failed to manually insert data: {e}")
            raise
    
    def load_all_parquet_files(self, directory: str, database_name: str) -> bool:
        """
        Load all parquet files from a directory into the database
        
        Args:
            directory: Directory containing parquet files
            database_name: Name of the database
            
        Returns:
            bool: True if all files loaded successfully, False otherwise
        """
        try:
            # Create database
            if not self.create_database(database_name):
                return False
            
            # Connect to the new database
            if not self.connect_to_postgres(database_name):
                return False
            
            # Store database name for later use
            self.database_name = database_name
            
            # Get all parquet files
            parquet_files = self.get_parquet_files(directory)
            
            if not parquet_files:
                logger.warning(f"No parquet files found in '{directory}'")
                return False
            
            # Load each parquet file
            success_count = 0
            for parquet_file in parquet_files:
                if self.load_parquet_to_table(parquet_file):
                    success_count += 1
            
            logger.info(f"Successfully loaded {success_count}/{len(parquet_files)} parquet files")
            return success_count == len(parquet_files)
            
        except Exception as e:
            logger.error(f"Failed to load parquet files: {e}")
            return False
    
    def get_all_tables(self) -> List[str]:
        """
        Get list of all user tables in the current database
        
        Returns:
            List of table names
        """
        try:
            self.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
            """)
            
            tables = [row[0] for row in self.cursor.fetchall()]
            logger.info(f"Found {len(tables)} tables in database")
            return tables
            
        except Exception as e:
            logger.error(f"Failed to get table list: {e}")
            return []
    
    def drop_all_tables(self) -> bool:
        """
        Drop all user tables in the current database
        
        Returns:
            bool: True if all tables dropped successfully, False otherwise
        """
        try:
            tables = self.get_all_tables()
            
            if not tables:
                logger.info("No tables to drop")
                return True
            
            # Drop tables with CASCADE to handle dependencies
            for table in tables:
                try:
                    self.cursor.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                    logger.info(f"Dropped table: {table}")
                except Exception as e:
                    logger.error(f"Failed to drop table {table}: {e}")
                    return False
            
            self.conn.commit()
            logger.info(f"Successfully dropped {len(tables)} tables")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop tables: {e}")
            return False
    
    def clear_database(self) -> bool:
        """
        Clear all data from all tables without dropping them
        
        Returns:
            bool: True if all tables cleared successfully, False otherwise
        """
        try:
            tables = self.get_all_tables()
            
            if not tables:
                logger.info("No tables to clear")
                return True
            
            # Truncate all tables
            for table in tables:
                try:
                    self.cursor.execute(f'TRUNCATE TABLE "{table}" CASCADE')
                    logger.info(f"Cleared table: {table}")
                except Exception as e:
                    logger.error(f"Failed to clear table {table}: {e}")
                    return False
            
            self.conn.commit()
            logger.info(f"Successfully cleared {len(tables)} tables")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear tables: {e}")
            return False
    
    def reset_database(self, method: str = 'drop') -> bool:
        """
        Reset the database by either dropping or clearing all tables
        
        Args:
            method: 'drop' to drop tables, 'clear' to truncate data only
            
        Returns:
            bool: True if reset successful, False otherwise
        """
        try:
            if method.lower() == 'drop':
                return self.drop_all_tables()
            elif method.lower() == 'clear':
                return self.clear_database()
            else:
                logger.error(f"Invalid reset method: {method}. Use 'drop' or 'clear'")
                return False
                
        except Exception as e:
            logger.error(f"Failed to reset database: {e}")
            return False
    
    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

def main():
    """Main function to demonstrate usage"""
    
    # Configuration for your Docker PostgreSQL (container: local-postgres)
    CONFIG = {
        'host': 'localhost',     # Connect from host machine
        'port': 54876,           # Your mapped port (5432 -> 54876)
        'user': 'postgres',      # From container env: POSTGRES_USER=postgres
        'password': 'postgres',  # From container env: POSTGRES_PASSWORD=postgres
        'database_name': 'jobtech',
        'parquet_directory': 'data/clean/'  # Change this to your directory
    }
    
    # Options for handling existing tables
    RESET_OPTIONS = {
        'skip': 'Skip reset - keep existing tables and data',
        'drop': 'Drop all tables and recreate from parquet files',
        'clear': 'Keep table structure but clear all data before loading',
        'interactive': 'Ask for confirmation before resetting'
    }
    
    # Set your preferred option here
    RESET_MODE = 'interactive'  # Change this to 'skip', 'drop', 'clear', or 'interactive'
    
    # Create loader instance
    loader = PostgreSQLParquetLoader(
        host=CONFIG['host'],
        port=CONFIG['port'],
        user=CONFIG['user'],
        password=CONFIG['password']
    )
    
    try:
        # Create database if it doesn't exist
        if not loader.create_database(CONFIG['database_name']):
            logger.error("Failed to create database")
            return
        
        # Connect to the database
        if not loader.connect_to_postgres(CONFIG['database_name']):
            logger.error("Failed to connect to database")
            return
        
        # Store database name for later use
        loader.database_name = CONFIG['database_name']
        
        # Handle existing tables based on reset mode
        existing_tables = loader.get_all_tables()
        
        if existing_tables:
            logger.info(f"Found {len(existing_tables)} existing tables: {existing_tables}")
            
            if RESET_MODE == 'interactive':
                print("\nExisting tables found in database:")
                for i, table in enumerate(existing_tables, 1):
                    print(f"  {i}. {table}")
                
                print("\nOptions:")
                print("1. Skip reset - keep existing tables and data")
                print("2. Drop all tables and recreate from parquet files")
                print("3. Keep table structure but clear all data before loading")
                print("4. Cancel operation")
                
                while True:
                    choice = input("\nEnter your choice (1-4): ").strip()
                    if choice == '1':
                        reset_action = 'skip'
                        break
                    elif choice == '2':
                        reset_action = 'drop'
                        break
                    elif choice == '3':
                        reset_action = 'clear'
                        break
                    elif choice == '4':
                        logger.info("Operation cancelled by user")
                        return
                    else:
                        print("Invalid choice. Please enter 1, 2, 3, or 4.")
            else:
                reset_action = RESET_MODE
            
            # Perform the reset action
            if reset_action == 'drop':
                logger.info("Dropping all existing tables...")
                if not loader.reset_database('drop'):
                    logger.error("Failed to drop tables")
                    return
            elif reset_action == 'clear':
                logger.info("Clearing all existing tables...")
                if not loader.reset_database('clear'):
                    logger.error("Failed to clear tables")
                    return
            elif reset_action == 'skip':
                logger.info("Skipping reset - existing tables will be kept")
            
        # Load parquet files
        parquet_files = loader.get_parquet_files(CONFIG['parquet_directory'])
        
        if not parquet_files:
            logger.warning(f"No parquet files found in '{CONFIG['parquet_directory']}'")
            return
        
        # Load each parquet file
        success_count = 0
        for parquet_file in parquet_files:
            if loader.load_parquet_to_table(parquet_file):
                success_count += 1
        
        logger.info(f"Successfully loaded {success_count}/{len(parquet_files)} parquet files")
        
        # Show final table count
        final_tables = loader.get_all_tables()
        logger.info(f"Database now contains {len(final_tables)} tables")
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        
    finally:
        # Clean up
        loader.close_connection()

if __name__ == "__main__":
    main()