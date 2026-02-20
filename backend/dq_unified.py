# dq_unified.py - COMPLETE FIXED VERSION
import pandas as pd
import os
import logging
import sys
import psutil
import warnings
import json
import re
warnings.filterwarnings('ignore')
from db_config import MYSQL_CONFIG, POSTGRESQL_CONFIG, SQLSERVER_CONFIG, ORACLE_CONFIG, PERFORMANCE_CONFIG
from app_config import APP_SETTINGS, QUALITY_THRESHOLDS
from datetime import datetime
# Add this line in the imports section (around line 20-30):
from database_navigator import navigate_database

# Global storage for single source sessions (for pagination)
single_sessions = {}

# Import the dual-mode input handler
try:
    from input_handler import init_input_handler, get_input, get_choice, get_multiple_choice, get_input_handler
    HAS_INPUT_HANDLER = True
except ImportError:
    HAS_INPUT_HANDLER = False
    logging.warning("Input handler not found. Running in CLI-only mode.")

# Setup logger
logger = logging.getLogger(__name__)

def normalize_db_schema(db_config):
    """
    Normalize schema field for different database types
    This prevents schema-related errors in API mode
    """
    if not db_config or 'type' not in db_config:
        return db_config
    
    db_type = db_config['type'].lower()
    
    # Ensure schema field exists
    if 'schema' not in db_config:
        if db_type == 'postgresql':
            db_config['schema'] = 'public'
        elif db_type == 'mysql':
            # MySQL uses database name as schema
            db_config['schema'] = db_config.get('database', '')
        elif db_type == 'oracle':
            # Oracle uses username as schema
            db_config['schema'] = db_config.get('user', db_config.get('database', ''))
        elif db_type == 'sqlserver':
            db_config['schema'] = 'dbo'
        elif db_type == 'sqlite':
            db_config['schema'] = 'main'
    
    return db_config

class LargeDatasetHandler:
    """Handler for large datasets with memory optimization"""
    
    @staticmethod
    def get_memory_usage():
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    @staticmethod
    def optimize_dataframe_dtypes(df):
        if df is None or df.empty:
            return df
        
        original_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
        
        for col in df.select_dtypes(include=['int64']).columns:
            if df[col].min() >= 0:
                if df[col].max() < 255:
                    df[col] = df[col].astype('uint8')
                elif df[col].max() < 65535:
                    df[col] = df[col].astype('uint16')
                elif df[col].max() < 4294967295:
                    df[col] = df[col].astype('uint32')
            else:
                if df[col].min() > -128 and df[col].max() < 127:
                    df[col] = df[col].astype('int8')
                elif df[col].min() > -32768 and df[col].max() < 32767:
                    df[col] = df[col].astype('int16')
                elif df[col].min() > -2147483648 and df[col].max() < 2147483647:
                    df[col] = df[col].astype('int32')
        
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = df[col].astype('float32')
        
        for col in df.select_dtypes(include=['object']).columns:
            if len(df[col]) > 0 and df[col].nunique() / len(df) < 0.5:
                df[col] = df[col].astype('category')
        
        optimized_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
        memory_saved = original_memory - optimized_memory
        
        if memory_saved > 0:
            logger.info(f"Memory optimization: {original_memory:.1f}MB -> {optimized_memory:.1f}MB (Saved: {memory_saved:.1f}MB)")
        
        return df
    
    @staticmethod
    def load_large_csv(file_path, max_rows=None):
        logger.info(f"Loading large CSV: {file_path}")
        
        if isinstance(file_path, dict):
            actual_path = file_path.get('value', '')
            if not actual_path:
                actual_path = file_path.get('filepath') or file_path.get('full_path') or ''
            
            if actual_path:
                file_path = actual_path
                logger.info(f"Loading CSV file: {os.path.basename(file_path)}")
            else:
                for key, value in file_path.items():
                    if isinstance(value, str) and os.path.exists(value):
                        file_path = value
                        logger.info(f"Loading CSV file: {os.path.basename(file_path)}")
                        break
        
        try:
            total_rows = 0
            with open(file_path, 'r', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f) - 1
            
            logger.info(f"CSV file has {total_rows:,} rows")
            
            if max_rows and total_rows > max_rows:
                logger.info(f"Dataset large ({total_rows:,} > {max_rows:,}), loading in chunks")
                chunks = []
                rows_loaded = 0
                chunk_size = min(50000, max_rows)
                
                for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
                    chunks.append(chunk)
                    rows_loaded += len(chunk)
                    
                    if rows_loaded % 100000 == 0:
                        logger.info(f"Loaded {rows_loaded:,} rows")
                    
                    if rows_loaded >= max_rows:
                        if rows_loaded > max_rows:
                            excess = rows_loaded - max_rows
                            chunks[-1] = chunk.iloc[:-excess]
                        break
                
                df = pd.concat(chunks, ignore_index=True)
                logger.info(f"Loaded {len(df):,} rows from CSV (chunked)")
            else:
                logger.info(f"Loading {total_rows:,} rows from CSV...")
                df = pd.read_csv(file_path, low_memory=False)
                logger.info(f"Loaded {len(df):,} rows from CSV")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading large CSV: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    def load_large_excel(file_path, sheet_name=None, max_rows=None):
        logger.info(f"Loading large Excel: {file_path}")
        
        try:
            excel_file = pd.ExcelFile(file_path)
            
            if sheet_name is None:
                if len(excel_file.sheet_names) > 1:
                    logger.info(f"Available sheets: {excel_file.sheet_names}")
                    if HAS_INPUT_HANDLER:
                        sheet_name = get_input(
                            prompt="Enter sheet name (or press Enter for first sheet)",
                            field_name="sheet_name",
                            default=excel_file.sheet_names[0],
                            required=False
                        ).strip()
                        if not sheet_name:
                            sheet_name = excel_file.sheet_names[0]
                    else:
                        sheet_name = input("Enter sheet name (or press Enter for first sheet): ").strip()
                        if not sheet_name:
                            sheet_name = excel_file.sheet_names[0]
                else:
                    sheet_name = excel_file.sheet_names[0]
            
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if file_size_mb > 50:
                logger.warning(f"Large Excel file: {file_size_mb:.1f}MB")
            
            logger.info(f"Loading Excel file...")
            df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
            
            logger.info(f"Loaded {len(df):,} rows from Excel")
            
            if max_rows and len(df) > max_rows:
                logger.warning(f"Dataset too large ({len(df):,} > {max_rows:,}), sampling...")
                df = df.head(max_rows)
            
            return df, sheet_name
            
        except Exception as e:
            logger.error(f"Error loading large Excel: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    def load_large_database(db_config, max_rows=None):
        logger.info(f"Loading large database table: {db_config['table']}")
        
        try:
            # NORMALIZE SCHEMA FIRST - CRITICAL FIX!
            db_config = normalize_db_schema(db_config.copy())
            logger.info(f"Normalized DB config with schema: {db_config.get('schema')}")
            if db_config['type'] == 'postgresql':
                return LargeDatasetHandler._load_large_postgresql(db_config, max_rows)
            elif db_config['type'] == 'mysql':
                return LargeDatasetHandler._load_large_mysql(db_config, max_rows)
            elif db_config['type'] == 'oracle':
                return LargeDatasetHandler._load_large_oracle(db_config, max_rows)
            elif db_config['type'] == 'sqlserver':
                return LargeDatasetHandler._load_large_sqlserver(db_config, max_rows)
            elif db_config['type'] == 'sqlite':
                return LargeDatasetHandler._load_large_sqlite(db_config, max_rows)
            else:
                raise ValueError(f"Unsupported database type: {db_config['type']}")
                
        except Exception as e:
            logger.error(f"Error loading large database: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    def _load_large_postgresql(db_config, max_rows):
        import psycopg2
        
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        
        try:
            # ==================== CHANGED HERE ====================
            # Use schema from db_config, default to 'public'
            schema = db_config.get('schema', 'public')
            table = db_config['table']
            
            # Build full table name with schema
            full_table_name = f"{schema}.{table}"
            # ==================== END CHANGE ====================
            
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            total_rows = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"PostgreSQL table {full_table_name} has {total_rows:,} rows")
            
            if total_rows == 0:
                logger.warning("Table is empty")
                return pd.DataFrame()
            
            query = f"SELECT * FROM {full_table_name}"
            if max_rows and total_rows > max_rows:
                logger.info(f"Table large ({total_rows:,} > {max_rows:,}), limiting query")
                query += f" LIMIT {max_rows}"
            
            rows_to_load = min(total_rows, max_rows) if max_rows else total_rows
            logger.info(f"Loading {rows_to_load:,} rows from PostgreSQL...")
            
            if rows_to_load > 50000:
                cursor = conn.cursor(name='large_cursor')
                cursor.execute(query)
                
                chunk_size = 50000
                chunks = []
                
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    
                    if not chunks:
                        colnames = [desc[0] for desc in cursor.description]
                    
                    chunk_df = pd.DataFrame(rows, columns=colnames)
                    chunks.append(chunk_df)
                    
                    rows_loaded = sum(len(chunk) for chunk in chunks)
                    if rows_loaded % 100000 == 0:
                        logger.info(f"Loaded {rows_loaded:,} rows")
                
                cursor.close()
                
                if chunks:
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    df = pd.DataFrame(columns=colnames)
            else:
                df = pd.read_sql_query(query, conn)
            
            logger.info(f"Loaded {len(df):,} rows from PostgreSQL")
            return df
            
        finally:
            conn.close()
    
    @staticmethod
    def _load_large_mysql(db_config, max_rows):
        import mysql.connector
        
        conn = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        
        try:
            cursor = conn.cursor()
            # ==================== CHANGED HERE ====================
            # MySQL doesn't use schemas, but we have schema field
            # Just use table name directly
            table = db_config['table']
            # ==================== END CHANGE ====================
            
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"MySQL table {table} has {total_rows:,} rows")
            
            if total_rows == 0:
                logger.warning("Table is empty")
                return pd.DataFrame()
            
            query = f"SELECT * FROM {table}"
            if max_rows and total_rows > max_rows:
                logger.info(f"Table large ({total_rows:,} > {max_rows:,}), limiting query")
                query += f" LIMIT {max_rows}"
            
            rows_to_load = min(total_rows, max_rows) if max_rows else total_rows
            logger.info(f"Loading {rows_to_load:,} rows from MySQL...")
            
            df = pd.read_sql_query(query, conn)
            logger.info(f"Loaded {len(df):,} rows from MySQL")
            return df
            
        finally:
            conn.close()
    
    @staticmethod
    def _load_large_oracle(db_config, max_rows):
        try:
            import cx_Oracle
        except ImportError:
            try:
                import oracledb as cx_Oracle
            except ImportError:
                raise ImportError("Oracle client not installed")
        
        dsn = cx_Oracle.makedsn(
            db_config['host'], 
            db_config['port'], 
            service_name=db_config['service_name']
        )
        
        conn = cx_Oracle.connect(
            user=db_config['user'],
            password=db_config['password'],
            dsn=dsn,
            encoding=db_config.get('encoding', 'UTF-8')
        )
        
        try:
            # ==================== CHANGED HERE ====================
            # Oracle uses schema (owner) from db_config
            schema = db_config.get('schema', db_config.get('user', ''))
            table = db_config['table']
            
            # Oracle needs quoted identifiers with schema
            full_table_name = f'"{schema}"."{table}"'
            # ==================== END CHANGE ====================
            
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            total_rows = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"Oracle table {full_table_name} has {total_rows:,} rows")
            
            if total_rows == 0:
                logger.warning("Table is empty")
                return pd.DataFrame()
            
            query = f"SELECT * FROM {full_table_name}"
            if max_rows and total_rows > max_rows:
                logger.info(f"Table large ({total_rows:,} > {max_rows:,}), limiting query")
                query = f"SELECT * FROM (SELECT * FROM {full_table_name}) WHERE ROWNUM <= {max_rows}"
            
            rows_to_load = min(total_rows, max_rows) if max_rows else total_rows
            logger.info(f"Loading {rows_to_load:,} rows from Oracle...")
            
            if rows_to_load > 50000:
                cursor = conn.cursor()
                cursor.arraysize = 10000
                cursor.execute(query)
                
                chunk_size = 50000
                chunks = []
                
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    
                    if not chunks:
                        colnames = [desc[0] for desc in cursor.description]
                    
                    chunk_df = pd.DataFrame(rows, columns=colnames)
                    chunks.append(chunk_df)
                    
                    rows_loaded = sum(len(chunk) for chunk in chunks)
                    if rows_loaded % 100000 == 0:
                        logger.info(f"Loaded {rows_loaded:,} rows")
                
                cursor.close()
                
                if chunks:
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    df = pd.DataFrame(columns=colnames)
            else:
                df = pd.read_sql_query(query, conn)
            
            logger.info(f"Loaded {len(df):,} rows from Oracle")
            return df
            
        finally:
            conn.close()
    
    @staticmethod
    def _load_large_sqlserver(db_config, max_rows):
        try:
            import pyodbc
        except ImportError:
            raise ImportError("pyodbc not installed")
        
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_config['host']},{db_config['port']};DATABASE={db_config['database']};UID={db_config['user']};PWD={db_config['password']}"
        conn = pyodbc.connect(conn_str)
        
        try:
            # ==================== CHANGED HERE ====================
            # SQL Server uses schema from db_config, default to 'dbo'
            schema = db_config.get('schema', 'dbo')
            table = db_config['table']
            
            # SQL Server uses [schema].[table] format
            full_table_name = f"[{schema}].[{table}]"
            # ==================== END CHANGE ====================
            
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            total_rows = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"SQL Server table {full_table_name} has {total_rows:,} rows")
            
            if total_rows == 0:
                logger.warning("Table is empty")
                return pd.DataFrame()
            
            if max_rows and total_rows > max_rows:
                logger.info(f"Table large ({total_rows:,} > {max_rows:,}), limiting query")
                query = f"SELECT TOP {max_rows} * FROM {full_table_name}"
            else:
                query = f"SELECT * FROM {full_table_name}"
            
            rows_to_load = min(total_rows, max_rows) if max_rows else total_rows
            logger.info(f"Loading {rows_to_load:,} rows from SQL Server...")
            
            df = pd.read_sql_query(query, conn)
            logger.info(f"Loaded {len(df):,} rows from SQL Server")
            return df
            
        finally:
            conn.close()
    
    @staticmethod
    def _load_large_sqlite(db_config, max_rows):
        import sqlite3
        
        if not os.path.exists(db_config['file_path']):
            raise FileNotFoundError(f"SQLite file not found: {db_config['file_path']}")
        
        conn = sqlite3.connect(db_config['file_path'])
        
        try:
            cursor = conn.cursor()
            # ==================== CHANGED HERE ====================
            # SQLite doesn't use schemas, but we have schema field
            # Just use table name directly
            table = db_config['table']
            # ==================== END CHANGE ====================
            
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"SQLite table {table} has {total_rows:,} rows")
            
            if total_rows == 0:
                logger.warning("Table is empty")
                return pd.DataFrame()
            
            query = f"SELECT * FROM {table}"
            if max_rows and total_rows > max_rows:
                logger.info(f"Table large ({total_rows:,} > {max_rows:,}), limiting query")
                query += f" LIMIT {max_rows}"
            
            rows_to_load = min(total_rows, max_rows) if max_rows else total_rows
            logger.info(f"Loading {rows_to_load:,} rows from SQLite...")
            
            df = pd.read_sql_query(query, conn)
            logger.info(f"Loaded {len(df):,} rows from SQLite")
            return df
            
        finally:
            conn.close()

def select_data_source(ui_data=None):
    logger.info("Prompting user for data source type")
    
    logger.info(f"UI Data source_type: {ui_data.get('source_type') if ui_data else 'No UI data'}")
    
    if ui_data and 'source_type' in ui_data:
        source_type = ui_data['source_type']
        logger.info(f"Using source_type from UI data: {source_type}")
        
        if source_type == 'excel':
            return "excel", ui_data.get('file_path')
        elif source_type == 'csv':
            return "csv", ui_data.get('file_path')
        elif source_type == 'database':
            return "database", ui_data.get('db_config')

    if HAS_INPUT_HANDLER and ui_data is not None:
        init_input_handler(mode='ui', data=ui_data)
    
    if HAS_INPUT_HANDLER:
        source_choice = get_choice(
            prompt="\n📋 SELECT DATA SOURCE TYPE:",
            options={
                '1': '📁 CSV File',
                '2': '📊 Excel File', 
                '3': '🗄️ Database'
            },
            field_name='source_type'
        )
    else:
        print("\n" + "="*50)
        print("📋 SELECT DATA SOURCE TYPE:")
        print("="*50)
        print("1. 📁 CSV File")
        print("2. 📊 Excel File")
        print("3. 🗄️ Database")
        print("="*50)
        source_choice = input("Enter source type (1-3): ").strip()
    
    logger.info(f"User selected source type: {source_choice}")
    
    if source_choice == "1":
        if HAS_INPUT_HANDLER:
            file_path = get_input(
                prompt="Enter CSV file path",
                field_name='file_path',
                required=True
            )
            return "csv", file_path
        else:
            return "csv", get_file_path("CSV")
    elif source_choice == "2":
        if HAS_INPUT_HANDLER:
            file_path = get_input(
                prompt="Enter Excel file path",
                field_name='file_path',
                required=True
            )
            return "excel", file_path
        else:
            return "excel", get_file_path("Excel")
    elif source_choice == "3":
        if HAS_INPUT_HANDLER:
            db_config = ui_data.get('db_config') if ui_data else None
            if db_config:
                return "database", db_config
            else:
                return "database", get_database_config(ui_data)
        else:
            return "database", get_database_config()
    else:
        logger.warning(f"Invalid source type selected: {source_choice}")
        return "csv", get_file_path("CSV")

def get_file_path(file_type):
    logger.info(f"Getting {file_type} file path from user")
    file_path = input(f"Enter {file_type} file path: ").strip()
    
    if not file_path:
        logger.warning("No file path provided by user")
        return None
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None
    
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if file_size_mb > 100:
        logger.warning(f"Large file detected: {file_size_mb:.1f}MB")
    
    logger.info(f"File path validated: {file_path} ({file_size_mb:.1f}MB)")
    return file_path

def get_database_config(ui_data=None):
    logger.info("Getting database configuration - Dual mode")
    
    if ui_data and 'db_config' in ui_data:
        logger.info("Using database config from UI data")
        db_config = ui_data['db_config']
        
        if 'type' not in db_config:
            db_config['type'] = 'mysql'
        
        # Check if it already has hierarchical selection
        if 'selection_mode' in db_config and db_config['selection_mode'] == 'hierarchical':
            # Already has database, schema, table from navigation
            logger.info(f"Using hierarchical selection: {db_config.get('database')}.{db_config.get('schema')}.{db_config.get('table')}")
            return db_config
        
        # Otherwise ask for table name (old way)
        if 'table' not in db_config:
            if HAS_INPUT_HANDLER:
                db_config['table'] = get_input(
                    prompt="Enter table name",
                    field_name='table',
                    required=True
                )
            else:
                db_config['table'] = input("Enter table name: ").strip()
        
        return db_config
    
    use_config_mode = os.environ.get('DQ_USE_CONFIG_FILE', '').lower() == 'true' or APP_SETTINGS.get('use_config_file', False)
    
    if use_config_mode:
        logger.info("Using config file mode")
        return get_config_file_database_config()
    
    skip_connection_prompt = os.environ.get('DQ_SKIP_CONNECTION_PROMPT', '').lower() == 'true'
    
    if skip_connection_prompt:
        logger.info("Auto-selecting Dynamic Input mode")
        return get_database_type_selection()
    
    # NEW: Show navigation option
    if HAS_INPUT_HANDLER:
        connection_choice = get_choice(
            prompt="\n💡 CONNECTION METHOD:",
            options={
                '1': 'Use default configuration from config file',
                '2': 'Enter connection details manually',
                '3': 'Browse and select database/schema/table'  # NEW OPTION
            },
            field_name='connection_method'
        )
    else:
        print("\n" + "="*50)
        print("💡 CONNECTION METHOD:")
        print("="*50)
        print("   1. Use default configuration from config file")
        print("   2. Enter connection details manually")
        print("   3. Browse and select database/schema/table")  # NEW OPTION
        print("="*50)
        connection_choice = input("Enter choice (1-3): ").strip()
    
    if connection_choice == "1":
        return get_config_file_database_config()
    elif connection_choice == "3":  # NEW: Browse and select
        logger.info("User selected Browse and Select mode")
        
        # Use the new navigator
        print("\n" + "="*60)
        print("🌐 DATABASE NAVIGATOR")
        print("="*60)
        print("You will now browse the database hierarchy:")
        print("  1. Select database type")
        print("  2. Enter connection details")
        print("  3. Browse and select database")
        print("  4. Browse and select schema")
        print("  5. Browse and select table")
        print("="*60)
        
        result = navigate_database(mode='cli')
        
        if not result:
            logger.warning("Database navigation cancelled")
            return None
        
        # Convert navigator result to db_config format
        db_config = {
            'type': result['type'],
            'host': result.get('host', 'localhost'),
            'port': result.get('port', 3306),
            'database': result['database'],
            'schema': result['schema'],
            'table': result['table'],
            'user': result.get('user', ''),
            'password': result.get('password', ''),  # Password handled internally by navigator
            'selection_mode': 'hierarchical'  # Mark as hierarchical selection
        }
        
        # Add specific fields for Oracle
        if result['type'] == 'oracle':
            if 'service_name' in result:
                db_config['service_name'] = result['service_name']
            if 'encoding' in result:
                db_config['encoding'] = result['encoding']
        
        # Add specific field for SQLite
        if result['type'] == 'sqlite' and 'file_path' in result:
            db_config['file_path'] = result['file_path']
        
        logger.info(f"Selected via navigation: {db_config['database']}.{db_config['schema']}.{db_config['table']}")
        return db_config
    else:
        return get_database_type_selection()

def get_database_type_selection():
    if HAS_INPUT_HANDLER:
        db_type = get_choice(
            prompt="\n🗄️ SELECT DATABASE TYPE:",
            options={
                '1': 'PostgreSQL',
                '2': 'MySQL',
                '3': 'Oracle',
                '4': 'SQL Server',
                '5': 'SQLite'
            },
            field_name='db_type'
        )
    else:
        print("\n" + "="*50)
        print("🗄️ SELECT DATABASE TYPE:")
        print("="*50)
        print("1. PostgreSQL")
        print("2. MySQL")
        print("3. Oracle")
        print("4. SQL Server")
        print("5. SQLite")
        print("="*50)
        db_type = input("Enter database type (1-5): ").strip()
    
    logger.info(f"User selected database type: {db_type}")
    
    if db_type == "1":
        return get_dynamic_database_config('1')
    elif db_type == "2":
        return get_dynamic_database_config('2')
    elif db_type == "3":
        return get_dynamic_database_config('3')
    elif db_type == "4":
        return get_dynamic_database_config('4')
    elif db_type == "5":
        return get_dynamic_database_config('5')
    else:
        logger.warning(f"Invalid database type selected: {db_type}")
        return get_dynamic_database_config('1')

def get_config_file_database_config(db_type_choice=None):
    logger.info("Getting database config from config file")
    
    db_config = {}
    
    if not db_type_choice:
        if HAS_INPUT_HANDLER:
            db_type_choice = get_choice(
                prompt="\n📋 CONFIG FILE DATABASE SELECTION:",
                options={
                    '1': 'PostgreSQL',
                    '2': 'MySQL',
                    '3': 'Oracle',
                    '4': 'SQL Server',
                    '5': 'SQLite'
                },
                field_name='db_type'
            )
        else:
            print("\n" + "="*50)
            print("📋 CONFIG FILE DATABASE SELECTION:")
            print("="*50)
            print("1. PostgreSQL")
            print("2. MySQL")
            print("3. Oracle")
            print("4. SQL Server")
            print("5. SQLite")
            print("="*50)
            db_type_choice = input("Enter database type (1-5): ").strip()
    
    db_type_map = {
        '1': 'postgresql',
        '2': 'mysql',
        '3': 'oracle',
        '4': 'sqlserver',
        '5': 'sqlite'
    }
    
    if db_type_choice not in db_type_map:
        logger.warning(f"Invalid database type choice: {db_type_choice}")
        return None
    
    db_type = db_type_map[db_type_choice]
    db_config['type'] = db_type
    
    try:
        if db_type == 'postgresql':
            db_config.update(POSTGRESQL_CONFIG)
        elif db_type == 'mysql':
            db_config.update(MYSQL_CONFIG)
        elif db_type == 'oracle':
            db_config.update(ORACLE_CONFIG)
        elif db_type == 'sqlserver':
            db_config.update(SQLSERVER_CONFIG)
        elif db_type == 'sqlite':
            if HAS_INPUT_HANDLER:
                db_config['file_path'] = get_input(
                    prompt="Enter SQLite file path",
                    field_name='file_path',
                    required=True
                )
            else:
                db_config['file_path'] = input("Enter SQLite file path: ").strip()
            
            if not os.path.exists(db_config['file_path']):
                logger.error(f"SQLite file not found: {db_config['file_path']}")
                return None
        
        logger.info(f"Using {db_type.upper()} configuration from config file")
        
    except Exception as e:
        logger.error(f"Error loading config for {db_type}: {e}")
        return None
    
    if HAS_INPUT_HANDLER:
        db_config['table'] = get_input(
            prompt="Enter table name",
            field_name='table',
            required=True
        )
    else:
        print("\n" + "="*50)
        print("📋 TABLE SELECTION:")
        print("="*50)
        db_config['table'] = input("Enter table name: ").strip()
    
    if not db_config['table']:
        logger.error("No table name provided")
        return None
    
    if HAS_INPUT_HANDLER:
        sampling_choice = get_choice(
            prompt="\n💡 LARGE DATASET OPTIONS:",
            options={
                '1': 'Load full table (recommended for <500k rows)',
                '2': 'Load sample of rows',
                '3': 'Cancel and choose different source'
            },
            field_name='sampling_choice'
        )
    else:
        print("\n" + "="*50)
        print("💡 LARGE DATASET OPTIONS:")
        print("="*50)
        print("   1. Load full table (recommended for <500k rows)")
        print("   2. Load sample of rows")
        print("   3. Cancel and choose different source")
        print("="*50)
        sampling_choice = input("Enter choice (1-3): ").strip()
    
    if sampling_choice == "2":
        if HAS_INPUT_HANDLER:
            sample_size = get_input(
                prompt="Enter sample size (e.g., 50000)",
                field_name='sample_size',
                default='50000'
            )
        else:
            sample_size = input("Enter sample size (e.g., 50000): ").strip()
        
        try:
            db_config['sample_size'] = int(sample_size)
            logger.info(f"Will load sample of {db_config['sample_size']:,} rows")
        except:
            logger.warning("Invalid sample size, loading full table")
            db_config['sample_size'] = None
    elif sampling_choice == "3":
        return None
    else:
        db_config['sample_size'] = None
    
    logger.info(f"Database configuration from config file: {db_config['type']}, Table: {db_config['table']}")
    return db_config

def get_dynamic_database_config(db_type_choice=None):
    logger.info("Getting dynamic database config from user input")
    
    if not db_type_choice:
        return get_database_type_selection()
    
    db_type_map = {
        '1': 'postgresql',
        '2': 'mysql',
        '3': 'oracle',
        '4': 'sqlserver',
        '5': 'sqlite'
    }
    
    if db_type_choice not in db_type_map:
        logger.warning(f"Invalid database type choice: {db_type_choice}")
        return None
    
    db_type = db_type_map[db_type_choice]
    db_config = {'type': db_type}
    
    if HAS_INPUT_HANDLER:
        db_config['host'] = get_input(
            prompt="Enter host",
            field_name='host',
            default='localhost'
        )
        
        if db_type == 'postgresql':
            default_port = '5432'
        elif db_type == 'mysql':
            default_port = '3306'
        elif db_type == 'oracle':
            default_port = '1521'
        elif db_type == 'sqlserver':
            default_port = '1433'
        else:
            default_port = ''
        
        port_input = get_input(
            prompt=f"Enter port",
            field_name='port',
            default=default_port
        )
        db_config['port'] = int(port_input) if port_input else int(default_port)
        
        db_config['database'] = get_input(
            prompt="Enter database name",
            field_name='database',
            required=True
        )
        
        db_config['user'] = get_input(
            prompt="Enter username",
            field_name='user',
            required=True
        )
        
        if not HAS_INPUT_HANDLER or get_input_handler().mode == 'cli':
            import getpass
            password = getpass.getpass("Enter password: ")
            db_config['password'] = password
        else:
            db_config['password'] = get_input(
                prompt="Enter password",
                field_name='password',
                required=True
            )
    else:
        db_config['host'] = input("Enter host [default: localhost]: ").strip() or 'localhost'
        
        if db_type == 'postgresql':
            default_port = '5432'
        elif db_type == 'mysql':
            default_port = '3306'
        elif db_type == 'oracle':
            default_port = '1521'
        elif db_type == 'sqlserver':
            default_port = '1433'
        else:
            default_port = ''
        
        port_input = input(f"Enter port [default: {default_port}]: ").strip()
        db_config['port'] = int(port_input) if port_input else int(default_port)
        
        db_config['database'] = input("Enter database name: ").strip()
        db_config['user'] = input("Enter username: ").strip()
        
        import getpass
        password = getpass.getpass("Enter password: ")
        db_config['password'] = password
    
    if db_type == 'oracle':
        if HAS_INPUT_HANDLER:
            service_name = get_input(
                prompt="Enter service name",
                field_name='service_name',
                default='XE'
            )
        else:
            service_name = input("Enter service name [default: XE]: ").strip()
        
        db_config['service_name'] = service_name or 'XE'
        db_config['encoding'] = 'UTF-8'
    
    # Add schema field to db_config for ALL databases
    if HAS_INPUT_HANDLER:
        # For databases that use schemas - ASK USER
        if db_type in ['postgresql', 'oracle', 'sqlserver']:
            # Set default schema
            if db_type == 'postgresql':
                default_schema = 'public'
            elif db_type == 'oracle':
                default_schema = db_config.get('user', db_config['database'])
            elif db_type == 'sqlserver':
                default_schema = 'dbo'
            else:
                default_schema = ''
            
            schema = get_input(
                prompt=f"Enter schema name",
                field_name='schema',
                default=default_schema
            )
            db_config['schema'] = schema if schema else default_schema
        else:
            # For MySQL and SQLite - DON'T ASK, just set schema = database
            db_config['schema'] = db_config['database']
            logger.info(f"Schema automatically set to database name: {db_config['schema']}")
    else:
        # CLI mode
        if db_type in ['postgresql', 'oracle', 'sqlserver']:
            # Set default schema
            if db_type == 'postgresql':
                default_schema = 'public'
            elif db_type == 'oracle':
                default_schema = db_config.get('user', db_config['database'])
            elif db_type == 'sqlserver':
                default_schema = 'dbo'
            else:
                default_schema = ''
            
            schema_input = input(f"Enter schema name [default: {default_schema}]: ").strip()
            db_config['schema'] = schema_input if schema_input else default_schema
        else:
            # For MySQL and SQLite - DON'T ASK, just set schema = database
            db_config['schema'] = db_config['database']
            print(f"Note: Schema automatically set to database name: {db_config['schema']}")
    # ==================== END OF SCHEMA CODE ====================

    if db_type != 'sqlite' and (not HAS_INPUT_HANDLER or get_input_handler().mode == 'cli'):
        logger.info(f"Testing {db_type} connection...")
        if test_database_connection(db_config):
            logger.info(f"{db_type.upper()} connection successful")
        else:
            logger.error(f"{db_type.upper()} connection failed")
            
            if HAS_INPUT_HANDLER:
                retry = get_input(
                    prompt="Do you want to retry? (y/n)",
                    field_name='retry_connection',
                    default='n'
                ).strip().lower()
            else:
                retry = input("Do you want to retry? (y/n): ").strip().lower()
            
            if retry == 'y':
                return get_dynamic_database_config(db_type_choice)
            else:
                return None
    
    if HAS_INPUT_HANDLER:
        db_config['table'] = get_input(
            prompt="Enter table name",
            field_name='table',
            required=True
        )
    else:
        print("\n" + "="*50)
        print("📋 TABLE SELECTION:")
        print("="*50)
        db_config['table'] = input("Enter table name: ").strip()
    
    if not db_config['table']:
        logger.error("No table name provided")
        return None
    
    if HAS_INPUT_HANDLER and get_input_handler().mode == 'cli':
        sampling_choice = get_choice(
            prompt="\n💡 LARGE DATASET OPTIONS:",
            options={
                '1': 'Load full table (recommended for <500k rows)',
                '2': 'Load sample of rows',
                '3': 'Cancel and choose different source'
            },
            field_name='sampling_choice'
        )
    elif not HAS_INPUT_HANDLER:
        print("\n" + "="*50)
        print("💡 LARGE DATASET OPTIONS:")
        print("="*50)
        print("   1. Load full table (recommended for <500k rows)")
        print("   2. Load sample of rows")
        print("   3. Cancel and choose different source")
        print("="*50)
        sampling_choice = input("Enter choice (1-3): ").strip()
    else:
        sampling_choice = '1'
    
    if sampling_choice == "2":
        if HAS_INPUT_HANDLER:
            sample_size = get_input(
                prompt="Enter sample size (e.g., 50000)",
                field_name='sample_size',
                default='50000'
            )
        else:
            sample_size = input("Enter sample size (e.g., 50000): ").strip()
        
        try:
            db_config['sample_size'] = int(sample_size)
            logger.info(f"Will load sample of {db_config['sample_size']:,} rows")
        except:
            logger.warning("Invalid sample size, loading full table")
            db_config['sample_size'] = None
    elif sampling_choice == "3":
        return None
    else:
        db_config['sample_size'] = None
    
    logger.info(f"Dynamic database configuration collected: {db_config['type']}, Table: {db_config['table']}")
    return db_config

def test_database_connection(db_config):
    try:
        if db_config['type'] == 'postgresql':
            import psycopg2
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            conn.close()
            return True
            
        elif db_config['type'] == 'mysql':
            import mysql.connector
            conn = mysql.connector.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            conn.close()
            return True
            
        elif db_config['type'] == 'oracle':
            try:
                import cx_Oracle
            except ImportError:
                import oracledb as cx_Oracle
            dsn = cx_Oracle.makedsn(
                db_config['host'], 
                db_config['port'], 
                service_name=db_config['service_name']
            )
            conn = cx_Oracle.connect(
                user=db_config['user'],
                password=db_config['password'],
                dsn=dsn,
                encoding=db_config.get('encoding', 'UTF-8')
            )
            conn.close()
            return True
            
        elif db_config['type'] == 'sqlserver':
            import pyodbc
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_config['host']},{db_config['port']};DATABASE={db_config['database']};UID={db_config['user']};PWD={db_config['password']}"
            conn = pyodbc.connect(conn_str)
            conn.close()
            return True
            
        elif db_config['type'] == 'sqlite':
            import sqlite3
            if os.path.exists(db_config['file_path']):
                conn = sqlite3.connect(db_config['file_path'])
                conn.close()
                return True
            else:
                return False
                
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False

def load_data_from_source(source_type, source_config):
    logger.info(f"Loading data from {source_type.upper()}")
    
    try:
        max_rows = APP_SETTINGS.get('max_rows_in_memory', 200000)
        
        if source_type == "csv":
            df = LargeDatasetHandler.load_large_csv(source_config, max_rows=max_rows)
        elif source_type == "excel":
            result = LargeDatasetHandler.load_large_excel(source_config, max_rows=max_rows)
            df = result[0] if isinstance(result, tuple) else result
        elif source_type == "database":
            sample_size = source_config.get('sample_size')
            if sample_size:
                max_rows = min(sample_size, max_rows)
            df = LargeDatasetHandler.load_large_database(source_config, max_rows=max_rows)
        else:
            logger.error(f"Unknown source type: {source_type}")
            return None
        
        if df is None or df.empty:
            logger.error(f"No data loaded from {source_type}")
            return None
        
        df = LargeDatasetHandler.optimize_dataframe_dtypes(df)
        
        #df = standardize_data_types(df)  # Standardize data types for consistency

        memory_mb = LargeDatasetHandler.get_memory_usage()
        df_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
        
        logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns. Memory: {memory_mb:.1f}MB total, {df_memory:.1f}MB for dataframe")
        
        return df
        
    except ImportError as e:
        logger.error(f"Required library not installed: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error loading data from {source_type}: {str(e)}", exc_info=True)
        return None

def standardize_data_types(df):
    """
    Standardize data types across all data sources
    Ensures consistent results for CSV, Excel, and databases
    """
    logger.info("Standardizing data types...")
    
    if df is None or df.empty:
        return df
    
    df_standardized = df.copy()
    
    for column in df_standardized.columns:
        # Try to convert numeric columns
        try:
            numeric_series = pd.to_numeric(df_standardized[column], errors='coerce')
            if numeric_series.notna().sum() / len(df_standardized) > 0.7:
                df_standardized[column] = numeric_series
        except:
            pass
    
    logger.info(f"Data type standardization complete")
    
    return df_standardized

def detect_missing_values_fast(series):
    """
    UNIVERSAL missing value detection - FAST vectorized version
    Works for ALL data sources: CSV, Excel, MySQL, PostgreSQL, Oracle, SQL Server
    """
    # Start with pandas isnull (catches None, NaN, NaT)
    mask = series.isnull()
    
    # Convert ALL to string for consistent checking (handles both object and non-object dtypes)
    str_series = series.astype('string')
    
    # Trim whitespace for comparison
    str_stripped = str_series.str.strip()
    
    # 1. Check for empty strings (AFTER stripping)
    mask = mask | (str_stripped == '')
    
    # 2. Check ALL case variations WITHOUT quotes first
    missing_patterns = [
        # Lowercase
        'nan', 'null', 'none', 'na', 'n/a', 'undefined', 'missing',
        # Uppercase  
        'NAN', 'NULL', 'NONE', 'NA', 'N/A', 'UNDEFINED', 'MISSING',
        # Mixed case
        'NaN', 'Null', 'None', 'Na', 'N/A', 'Undefined', 'Missing',
        # Special cases
        '\\N', '\\\\N',  # PostgreSQL COPY NULL
        '-', '--', '---', '....', '...',
        '#N/A', '#VALUE!', '#REF!', '#DIV/0!', '#NAME?', '#NUM!', '#NULL!',
    ]
    
    for pattern in missing_patterns:
        mask = mask | (str_stripped == pattern)
    
    # 3. Check QUOTED versions (strip quotes THEN check)
    # Handle both single and double quotes
    str_no_quotes = str_stripped.str.strip("'").str.strip('"')
    
    quoted_patterns = ['nan', 'null', 'none', 'na', 'n/a', 'undefined', 'missing']
    for pattern in quoted_patterns:
        # Check both lowercase and uppercase
        mask = mask | (str_no_quotes.str.lower() == pattern)
        mask = mask | (str_no_quotes == pattern.upper())
        mask = mask | (str_no_quotes == pattern.capitalize())
    
    # 4. Check for whitespace-only strings (tabs, newlines, spaces)
    mask = mask | str_series.str.isspace()
    
    # 5. Check for quoted empty strings
    mask = mask | (str_stripped.isin(["''", '""']))
    
    # 6. For numeric-like columns, also check if string representation is a missing pattern
    # This catches cases where numbers are stored as strings with null values
    if series.dtype in ['int64', 'int32', 'float64', 'float32']:
        # Convert to string and check patterns
        num_as_str = series.astype(str).str.strip()
        mask = mask | num_as_str.isin(['nan', 'NaN', 'NAN', 'null', 'NULL', 'None', 'NONE'])
    
    # DEBUG: Log if we found PostgreSQL pattern
    if mask.sum() > 0 and '\\N' in str_series.astype(str).values:
        logger.debug(f"Found PostgreSQL \\N pattern in data")
        
    return mask
# ============ FIXED QUALITY CHECK FUNCTIONS ============

def check_nulls(df, error_logger=None, session_id=None, source_info=""):
    """Check for null/empty values - UNIVERSAL FIX"""
    logger.info("Checking for null/empty values")
    
    null_results = {}
    total_null_cells = 0
    null_issue_rows = {}
    
    logger.info(f"DataFrame shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")
    
    # DEBUG: Show data source info
    logger.info(f"Data source: {source_info}")
    logger.info(f"DataFrame dtypes: {df.dtypes.to_dict()}")
        
    # DEBUG: Show raw data samples
    if len(df) > 0:
        logger.info("First 2 rows (first 3 columns only):")
        for i in range(min(2, len(df))):
            row_data = {}
            for col in df.columns[:3]:  # Only first 3 columns
                val = df.iloc[i][col]
                row_data[col] = repr(val)[:50]  # Limit length
            logger.info(f"  Row {i}: {row_data}")

    for column in df.columns:
        # USE THE UNIVERSAL FUNCTION
        missing_mask = detect_missing_values_fast(df[column])
        total_empty = missing_mask.sum()
        total_null_cells += total_empty
        
        null_percentage = (total_empty / len(df)) * 100 if len(df) > 0 else 0
        null_results[column] = {
            'null_count': total_empty,
            'percentage': null_percentage
        }
        
        if total_empty > 0:
            # DEBUG: Show sample of what's being detected
            # sample_values = df.loc[missing_mask, column].head(3)
            # sample_repr = [repr(val) for val in sample_values]
            # logger.info(f"  Sample missing in '{column}': {sample_repr}")
            
            # DEBUG: Show what's detected as missing vs not missing
            missing_samples = df.loc[missing_mask, column].head(3).tolist()
            not_missing_samples = df.loc[~missing_mask, column].head(3).tolist()
            
            logger.info(f"  Missing samples: {[repr(v) for v in missing_samples]}")
            logger.info(f"  Not missing samples: {[repr(v) for v in not_missing_samples]}")
            
            logger.warning(f"Column '{column}': {total_empty:,} missing cells ({null_percentage:.1f}%)")
            
            logger.warning(f"Column '{column}': {total_empty:,} missing cells ({null_percentage:.1f}%)")
            
            missing_indices = df[missing_mask].index.tolist()
            
            column_null_details = []
            for idx in missing_indices:
                db_row = idx + 1
                excel_row = idx + 2
                
                actual_value = df.loc[idx, column]
                
                identifier = f"Row {db_row}"
                # Use same detection for identifier
                if 'customerid' in df.columns:
                    cust_val = df.loc[idx, 'customerid']
                    if not detect_missing_values_fast(pd.Series([cust_val])).iloc[0]:
                        identifier = f"CustomerID={cust_val}"
                elif 'customer_id' in df.columns:
                    cust_val = df.loc[idx, 'customer_id']
                    if not detect_missing_values_fast(pd.Series([cust_val])).iloc[0]:
                        identifier = f"CustomerID={cust_val}"
                elif 'id' in df.columns:
                    id_val = df.loc[idx, 'id']
                    if not detect_missing_values_fast(pd.Series([id_val])).iloc[0]:
                        identifier = f"ID={id_val}"
                
                column_null_details.append({
                    'db_row': db_row,
                    'excel_row': excel_row,
                    'row_index': idx,
                    'identifier': identifier,
                    'value': str(actual_value) if not pd.isna(actual_value) else 'NULL/NaN'
                })
            
            null_issue_rows[column] = column_null_details

            if error_logger and session_id:
                batch_errors = []
                for detail in column_null_details:
                    batch_errors.append({
                        'session_id': session_id,
                        'check_type': 'single_source',
                        'source_name': source_info,
                        'column_name': column,
                        'row_index': detail['row_index'],
                        'excel_row': detail['excel_row'],
                        'db_row': detail['db_row'],
                        'actual_value': 'EMPTY/NULL',
                        'expected_value': 'Non-empty value',
                        'error_type': 'empty_cell',
                        'error_description': f'Empty cell in {column} (Row {detail["db_row"]})',
                        'severity': 'medium'
                    })
                if batch_errors:
                    error_logger.log_batch_errors(session_id, 'single_source', source_info, batch_errors)
        else:
            logger.info(f"Column '{column}': No missing cells")
            null_issue_rows[column] = []
    
    logger.info(f"Total missing cells found: {total_null_cells:,}")
    
        # DEBUG: Log database type for comparison
    db_type = "unknown"
    if 'postgresql' in source_info.lower():
        db_type = "PostgreSQL"
    elif 'mysql' in source_info.lower():
        db_type = "MySQL" 
    elif 'oracle' in source_info.lower():
        db_type = "Oracle"
    
    if db_type != "unknown":
        logger.info(f"=== Analyzing {db_type} database ===")
        # Show one column sample
        if len(df.columns) > 0:
            first_col = df.columns[0]
            sample = df[first_col].dropna().head(3).tolist()
            logger.info(f"First column '{first_col}' samples: {[repr(v) for v in sample]}")
            
    # DEBUG SUMMARY
    logger.info("="*60)
    logger.info("MISSING VALUE SUMMARY")
    logger.info("="*60)
    for col, result in null_results.items():
        if result['null_count'] > 0:
            logger.info(f"{col}: {result['null_count']:,} missing ({result['percentage']:.1f}%)")
    logger.info("="*60)
    
    return total_null_cells, null_results, null_issue_rows

#def check_duplicates(df):
def check_duplicates(df, error_logger=None, session_id=None, source_info=""):
    """Check for duplicate rows"""
    logger.info("Checking for duplicate rows")
    
    # ========== STEP 1: CLEAN THE DATA ==========
    logger.info("Cleaning data (removing whitespace)...")
    df_clean = df.copy()
    
    # Clean column names
    df_clean.columns = [str(col).strip() for col in df_clean.columns]
    logger.info(f"Columns after cleaning: {list(df_clean.columns)}")
    
    # Clean ALL columns - remove whitespace from strings
    for col in df_clean.columns:
        # Convert to string and strip whitespace
        df_clean[col] = df_clean[col].astype(str).str.strip()
        # Also collapse multiple spaces inside strings
        df_clean[col] = df_clean[col].str.replace(r'\s+', ' ', regex=True)
    
    # Show sample of cleaned data
    logger.info(f"Sample of first column after cleaning: {df_clean.iloc[0, 0]!r}")
    logger.info(f"Sample of row 6 first column: {df_clean.iloc[5, 0]!r}")
    
    # Use the cleaned dataframe
    df = df_clean
    total_rows = len(df)
    logger.info(f"Total rows to check: {total_rows}")
    
    # ========== STEP 2: Check for serial number in first column ==========
    ignore_first_col = False
    
    if len(df.columns) > 1 and len(df) >= 3:
        first_col = df.columns[0]
        first_col_values = df.iloc[:5, 0]  # Check first 5 rows
        
        logger.info(f"Checking first column '{first_col}' for serial numbers")
        logger.info(f"First 5 values: {list(first_col_values)}")
        
        # Simple check: are first few values numbers like 1, 2, 3?
        try:
            # Try to convert first 3 values to numbers
            nums = []
            for i, val in enumerate(first_col_values.head(3)):
                try:
                    num = float(str(val).strip())
                    nums.append(num)
                    logger.info(f"  Row {i+1}: '{val}' → {num} (numeric)")
                except:
                    logger.info(f"  Row {i+1}: '{val}' → Not numeric")
            
            # If we have at least 2 numbers and they're sequential
            if len(nums) >= 2:
                is_sequential = True
                for i in range(1, len(nums)):
                    if abs(nums[i] - nums[i-1] - 1) > 0.1:
                        is_sequential = False
                        break
                
                if is_sequential:
                    ignore_first_col = True
                    logger.info(f"First column appears to be serial numbers. Ignoring it.")
                else:
                    logger.info(f"First column values not sequential: {nums}")
            else:
                logger.info(f"Not enough numeric values to be serial: {nums}")
        except Exception as e:
            logger.info(f"Error checking serial: {e}")
    
    # ========== STEP 3: Check for duplicates ==========
    if ignore_first_col:
        columns_to_check = df.columns[1:]
        logger.info(f"Checking duplicates on columns: {list(columns_to_check)}")
        duplicate_mask = df.duplicated(subset=columns_to_check, keep=False)
    else:
        logger.info(f"Checking duplicates on ALL columns")
        # First, let's see what we're comparing
        logger.info("First few rows for comparison:")
        for i in range(min(3, len(df))):
            logger.info(f"  Row {i}: {dict(df.iloc[i].head(3))}")
        
        duplicate_mask = df.duplicated(keep=False)
    
    # Get duplicate rows
    duplicate_rows = df[duplicate_mask]
    logger.info(f"Duplicate mask found {duplicate_mask.sum()} rows")
    
    # ========== STEP 4: Count duplicates ==========
    if len(duplicate_rows) > 0:
        logger.info(f"Found {len(duplicate_rows)} potential duplicate rows")
        
        if ignore_first_col:
            unique_duplicates = duplicate_rows.drop_duplicates(subset=columns_to_check)
        else:
            unique_duplicates = duplicate_rows.drop_duplicates()
        
        logger.info(f"Unique duplicate groups: {len(unique_duplicates)}")
        # duplicate_count = len(duplicate_rows) - len(unique_duplicates) #2-1==1
        duplicate_count = len(duplicate_rows) 
        
        # Show what duplicates were found
        logger.info("Duplicate groups found:")
        if ignore_first_col:
            for group in duplicate_rows.drop_duplicates(subset=columns_to_check).head(3).itertuples():
                logger.info(f"  Group sample: {group}")
        else:
            for group in unique_duplicates.head(3).itertuples():
                logger.info(f"  Group sample: {group}")
    else:
        duplicate_count = 0
        logger.info("No duplicate rows found")
    
    # ========== STEP 5: Get duplicate details ==========
    duplicate_details = []
    if duplicate_count > 0:
        logger.info(f"Processing {len(duplicate_rows.index)} duplicate indices")
        for idx in duplicate_rows.index:
            duplicate_details.append({
                'db_row': idx + 1,
                'excel_row': idx + 2,
                'row_index': idx,
                'data_sample': df.iloc[idx].to_dict()
            })
    
    logger.info(f"FINAL RESULT: Found {duplicate_count} duplicate groups")
    
    # Add error logging if error_logger is provided
    batch_errors = []  # Initialize here
    if duplicate_count > 0 and error_logger and session_id:
        batch_errors = []
        for detail in duplicate_details:
            batch_errors.append({
                'session_id': session_id,
                'check_type': 'single_source',
                'source_name': source_info,
                'error_type': 'duplicate_row',
                'error_description': f'Duplicate row found (DB Row: {detail["db_row"]})',
                'excel_row': detail['excel_row'],
                'db_row': detail['db_row'],
                'severity': 'low'
            })
        
    if error_logger and batch_errors:
        error_logger.log_batch_errors(session_id, 'single_source', source_info, batch_errors)

    return duplicate_count, duplicate_details

#def check_data_formats(df):
def check_data_formats(df, error_logger=None, session_id=None, source_info=""):
    """Check data formats by analyzing content patterns"""
    logger.info("Checking data formats")
    
    format_issues = 0
    format_issue_details = []
    
    if df is None or df.empty:
        logger.info("Dataframe is empty, skipping format check")
        return 0, []
    
    def detect_column_type(column_data):
        """Detect the most likely data type based on content analysis"""
        non_null_data = column_data.dropna()
        if len(non_null_data) == 0:
            return None
        
        # Take sample for analysis
        sample_size = min(100, len(non_null_data))
        # sample = non_null_data.head(sample_size).astype(str).str.strip()
        sample = non_null_data.head(sample_size).astype('string').str.strip()
        
        # Patterns for detection
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        phone_patterns = [
            r'^(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$',
            r'^\d{10}$',
            r'^\d{3}-\d{3}-\d{4}$',
            r'^\(\d{3}\) \d{3}-\d{4}$'
        ]
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            r'^\d{2}/\d{2}/\d{4}$',
            r'^\d{2}-\d{2}-\d{4}$',
            r'^\d{2}\.\d{2}\.\d{4}$',
            r'^\d{4}/\d{2}/\d{2}$'
        ]
        numeric_pattern = r'^-?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^-?\d+(\.\d+)?$'
        
        # Count matches for each type
        email_count = sum(sample.str.match(email_pattern, case=False).fillna(False))
        phone_count = sum(any(re.match(p, val) for p in phone_patterns) for val in sample)
        date_count = sum(any(re.match(p, val) for p in date_patterns) for val in sample)
        numeric_count = sum(sample.str.match(numeric_pattern).fillna(False))
        
        # Calculate percentages
        total = len(sample)
        email_pct = email_count / total if total > 0 else 0
        phone_pct = phone_count / total if total > 0 else 0
        date_pct = date_count / total if total > 0 else 0
        numeric_pct = numeric_count / total if total > 0 else 0
        
        # Determine type based on highest percentage (threshold: 70%)
        if email_pct >= 0.7:
            return 'email'
        elif phone_pct >= 0.7:
            return 'phone'
        elif date_pct >= 0.7:
            return 'date'
        elif numeric_pct >= 0.7:
            return 'numeric'
        
        return None
    
    def validate_email(value):
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(value), re.IGNORECASE))
    
    def validate_phone(value):
        """Validate phone format"""
        value_str = str(value).strip()
        patterns = [
            r'^(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$',
            r'^\d{10}$',
            r'^\d{3}-\d{3}-\d{4}$',
            r'^\(\d{3}\) \d{3}-\d{4}$',
            r'^\+\d{1,3} \d{3} \d{3} \d{4}$'
        ]
        return any(re.match(p, value_str) for p in patterns)
    
    def validate_numeric(value):
        """Validate numeric format (no leading zeros, no alphabets)"""
        value_str = str(value).strip()
        
        # Remove thousand separators
        value_str = value_str.replace(',', '')
        
        # Check if it's a valid number
        try:
            # Try to convert to float
            num = float(value_str)
            
            # Check for leading zeros in integers
            if num.is_integer() and not (-1 < num < 1):
                # Check original string for leading zeros
                clean_str = value_str.lstrip('-+').lstrip()
                if clean_str.startswith('0') and len(clean_str) > 1 and not clean_str.startswith('0.'):
                    return False
            
            # Check for alphabets in the middle
            if re.search(r'[a-zA-Z]', value_str):
                return False
                
            return True
        except:
            return False
    
    def validate_date(value):
        """Validate date format"""
        value_str = str(value).strip()
        patterns = [
            r'^\d{4}-\d{2}-\d{2}$',
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            r'^\d{2}/\d{2}/\d{4}$',
            r'^\d{2}-\d{2}-\d{4}$',
            r'^\d{2}\.\d{2}\.\d{4}$',
            r'^\d{4}/\d{2}/\d{2}$'
        ]
        return any(re.match(p, value_str) for p in patterns)
    
    def check_date_consistency(column_data):
        """Check if all dates in column follow same format"""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',
            r'^\d{2}/\d{2}/\d{4}$',
            r'^\d{2}-\d{2}-\d{4}$',
            r'^\d{2}\.\d{2}\.\d{4}$',
            r'^\d{4}/\d{2}/\d{2}$'
        ]
        
        def validate_date_format(value_str):
            """Validate date format"""
            return any(re.match(p, value_str) for p in date_patterns)
        
        def get_date_format(value_str):
            """Identify the specific date format"""
            if re.match(r'^\d{4}-\d{2}-\d{2}$', value_str):
                return 'YYYY-MM-DD'
            elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', value_str):  # ← ADD THIS!
                return 'YYYY-MM-DD HH:MM:SS'  # Excel datetime format
            elif re.match(r'^\d{4}/\d{2}/\d{2}$', value_str):
                return 'YYYY/MM/DD'
            elif re.match(r'^\d{2}/\d{2}/\d{4}$', value_str):
                # Try to determine if it's MM/DD or DD/MM
                try:
                    parts = value_str.split('/')
                    if len(parts) == 3:
                        month = int(parts[0])
                        day = int(parts[1])
                        if month > 12 and day <= 12:
                            return 'DD/MM/YYYY'
                        elif month <= 12 and day > 12:
                            return 'MM/DD/YYYY'
                        else:
                            return 'MM/DD/YYYY or DD/MM/YYYY'
                except:
                    pass
                return 'MM/DD/YYYY or DD/MM/YYYY'
            elif re.match(r'^\d{2}-\d{2}-\d{4}$', value_str):
                return 'DD-MM-YYYY or MM-DD-YYYY'
            elif re.match(r'^\d{2}\.\d{2}\.\d{4}$', value_str):
                return 'DD.MM.YYYY or MM.DD.YYYY'
            elif validate_date_format(value_str):
                return 'valid_date'
            else:
                return 'invalid_date'
        
        # Collect non-null dates
        non_null_dates = []
        for val in column_data:
            if pd.isna(val):
                continue
            val_str = str(val).strip()
            if val_str and val_str.lower() not in ['nan', 'null', 'none', '']:
                non_null_dates.append(val_str)
        
        if len(non_null_dates) < 2:
            return True, "Only one date or empty", []
        
        formats_found = set()
        invalid_dates = []
        
        for date_str in non_null_dates:
            date_format = get_date_format(date_str)
            formats_found.add(date_format)
            if date_format == 'invalid_date':
                invalid_dates.append(date_str)
        
        if len(formats_found) == 1 and 'invalid_date' not in formats_found:
            return True, list(formats_found)[0], invalid_dates
        else:
            return False, list(formats_found), invalid_dates
    
    # Analyze each column
    for column in df.columns:
        if df[column].dropna().empty:
            continue
            
        # Detect column type from data
        detected_type = detect_column_type(df[column])
        
        if not detected_type:
            continue
            
        logger.info(f"Detected {column} as {detected_type} type")
        
        # Select validator based on detected type
        if detected_type == 'email':
            validator = validate_email
            expected_format = "Valid email format (user@domain.com)"
        elif detected_type == 'phone':
            validator = validate_phone
            expected_format = "Valid phone number format"
        elif detected_type == 'date':
            validator = validate_date
            expected_format = "Valid date format"
        elif detected_type == 'numeric':
            validator = validate_numeric
            expected_format = "Numeric value (no leading zeros, no alphabets)"
        else:
            continue
        
        # Check for date consistency
        if detected_type == 'date':
            is_consistent, format_info, invalid_dates = check_date_consistency(df[column])
            
            # Log what format was detected
            logger.info(f"  Date format check for {column}: Consistent={is_consistent}, Format={format_info}")
            
            if not is_consistent:
                format_issues += 1
                issue_detail = {
                    'column': column,
                    'db_row': -1,
                    'excel_row': -1,
                    'row_index': -1,
                    'identifier': 'Column Level',
                    'actual_value': f'Multiple date formats found: {format_info}',
                    'expected_format': 'All dates should follow the same format (e.g., all YYYY-MM-DD or all DD/MM/YYYY)',
                    'pattern_type': 'date',
                    'error_type': 'format_inconsistency'
                }
                format_issue_details.append(issue_detail)
                logger.warning(f"  {column}: Inconsistent date formats: {format_info}")
            
            # Also check for invalid dates
            if invalid_dates:
                for date_str in invalid_dates[:3]:  # Show first 3 invalid dates
                    logger.warning(f"    Invalid date format: {date_str}")
                    format_issues += 1
                    issue_detail = {
                        'column': column,
                        'db_row': -1,
                        'excel_row': -1,
                        'row_index': -1,
                        'identifier': 'Invalid date',
                        'actual_value': date_str[:100],
                        'expected_format': 'Valid date format',
                        'pattern_type': 'date',
                        'error_type': 'invalid_date_format'
                    }
                    format_issue_details.append(issue_detail)
        
        # Validate each value
        for idx, value in df[column].items():
            if pd.isna(value):
                continue
                
            value_str = str(value).strip()
            
            if not value_str or value_str.lower() in ['nan', 'null', 'none', '']:
                continue
                
            if not validator(value_str):
                format_issues += 1
                
                db_row = idx + 1
                excel_row = idx + 2
                
                # Find identifier
                identifier = f"Row {db_row}"
                identifier_columns = ['customerid', 'id', 'employeeid', 'userid', 'studentid']
                for id_col in identifier_columns:
                    if id_col in df.columns and not pd.isna(df.loc[idx, id_col]):
                        identifier = f"{id_col.upper()}={df.loc[idx, id_col]}"
                        break
                
                issue_detail = {
                    'column': column,
                    'db_row': db_row,
                    'excel_row': excel_row,
                    'row_index': idx,
                    'identifier': identifier,
                    'actual_value': value_str[:100],
                    'expected_format': expected_format,
                    'pattern_type': detected_type,
                    'error_type': 'format_issue'
                }
                
                format_issue_details.append(issue_detail)
                
                if format_issues <= 5:
                    logger.warning(f"  Row {db_row}: '{value_str[:50]}...' doesn't match {expected_format}")
    
    # Also check for columns that look numeric but have format issues
    for column in df.columns:
        # Skip if already processed
        if any(detail['column'] == column for detail in format_issue_details):
            continue
            
        # Check for numeric-like columns
        sample = df[column].dropna().head(20)
        if len(sample) > 0:
            numeric_like = 0
            for val in sample:
                val_str = str(val).strip()
                if re.match(r'^-?\d+(\.\d+)?$', val_str):
                    numeric_like += 1
            
            # If 50% or more look numeric, check for leading zeros
            if numeric_like / len(sample) >= 0.5:
                for idx, value in df[column].items():
                    if pd.isna(value):
                        continue
                        
                    value_str = str(value).strip()
                    
                    # Check for leading zeros in what looks like integers
                    if re.match(r'^0[0-9]+$', value_str.lstrip('-+')):
                        format_issues += 1
                        
                        db_row = idx + 1
                        excel_row = idx + 2
                        
                        identifier = f"Row {db_row}"
                        identifier_columns = ['customerid', 'id', 'employeeid', 'userid', 'studentid']
                        for id_col in identifier_columns:
                            if id_col in df.columns and not pd.isna(df.loc[idx, id_col]):
                                identifier = f"{id_col.upper()}={df.loc[idx, id_col]}"
                                break
                        
                        issue_detail = {
                            'column': column,
                            'db_row': db_row,
                            'excel_row': excel_row,
                            'row_index': idx,
                            'identifier': identifier,
                            'actual_value': value_str[:100],
                            'expected_format': "Numeric value should not have leading zeros",
                            'pattern_type': 'numeric_format',
                            'error_type': 'leading_zeros'
                        }
                        
                        format_issue_details.append(issue_detail)
    
    logger.info(f"Total format issues found: {format_issues:,}")

    # Add error logging if error_logger is provided

    if format_issues > 0 and error_logger and session_id:
        batch_errors = []
        for detail in format_issue_details:
            batch_errors.append({
                'session_id': session_id,
                'check_type': 'single_source',
                'source_name': source_info,
                'column_name': detail['column'],
                'row_index': detail['row_index'],
                'excel_row': detail['excel_row'],
                'db_row': detail['db_row'],
                'actual_value': detail['actual_value'],
                'expected_value': detail['expected_format'],
                'error_type': 'format_issue',
                'error_description': f'Format issue in {detail["column"]}: "{detail["actual_value"]}"',
                'severity': 'medium'
            })
        
        if batch_errors and error_logger:
            error_logger.log_batch_errors(session_id, 'single_source', source_info, batch_errors)

    return format_issues, format_issue_details

#def check_mandatory_fields(df, ui_data=None):
def check_mandatory_fields(df, ui_data=None, error_logger=None, session_id=None, source_info=""):
    """Check mandatory columns"""
    logger.info("Checking mandatory fields")
    
    if HAS_INPUT_HANDLER and get_input_handler().mode == 'ui' and ui_data is not None:
        get_input_handler().set_current_data(ui_data)
        
        logger.info(f"Available columns: {list(df.columns)}")
        
        if 'mandatory_fields' in ui_data:
            mandatory_input = ui_data['mandatory_fields']
        else:
            mandatory_input = get_input(
                prompt="Enter mandatory column names (comma-separated, or 'all' for all columns)",
                field_name='mandatory_fields',
                default=''
            )
        
        get_input_handler().clear_current_data()
    
    elif HAS_INPUT_HANDLER and get_input_handler().mode == 'cli':
        logger.info(f"Available columns: {list(df.columns)}")
        mandatory_input = get_input(
            prompt="Enter mandatory column names (comma-separated, or 'all' for all columns)",
            field_name='mandatory_fields',
            default=''
        )
    
    else:
        logger.info(f"Available columns: {list(df.columns)}")
        mandatory_input = input(" Enter mandatory column names (comma-separated, or 'all' for all columns): ").strip()
    
    if mandatory_input.lower() == 'all':
        mandatory_fields = list(df.columns)
        logger.info(f"Checking ALL columns as mandatory: {mandatory_fields}")
    else:
        mandatory_fields = [field.strip() for field in mandatory_input.split(',')] if mandatory_input else []
    
    mandatory_issues = 0
    mandatory_issue_details = {}
    total_missing_values = 0
    rows_with_missing_mandatory = set()
    
    for field in mandatory_fields:
        if field in df.columns:
            missing_mask = detect_missing_values_fast(df[field])
            total_empty = missing_mask.sum()
            
            fill_percentage = ((len(df) - total_empty) / len(df)) * 100 if len(df) > 0 else 0
            
            if total_empty > 0:
                logger.warning(f"Mandatory field '{field}': {total_empty:,} missing values ({fill_percentage:.1f}% filled)")
                missing_indices = df[missing_mask].index.tolist()
                for idx in missing_indices:
                    rows_with_missing_mandatory.add(idx)
                
                column_missing_details = []
                for idx in missing_indices:
                    db_row = idx + 1
                    excel_row = idx + 2
                    
                    identifier = ""
                    if 'customerid' in df.columns:
                        customer_val = df.loc[idx, 'customerid']
                        if not (pd.isna(customer_val) or 
                                (isinstance(customer_val, str) and 
                                customer_val.strip().lower() in ['nan', 'null', 'none', ''])):
                            identifier = f"customerid={customer_val}"
                    elif 'id' in df.columns:
                        identifier = f"id={df.loc[idx, 'id']}"
                    else:
                        identifier = f"Row {db_row}"
                    
                    column_missing_details.append({
                        'db_row': db_row,
                        'excel_row': excel_row,
                        'row_index': idx,
                        'identifier': identifier
                    })
                
                max_rows_to_store = min(len(column_missing_details), APP_SETTINGS['max_rows_to_display'])
                mandatory_issue_details[field] = column_missing_details[:max_rows_to_store]
                
                mandatory_issues += 1
            else:
                logger.info(f"Mandatory field '{field}': {fill_percentage:.1f}% filled")
                mandatory_issue_details[field] = []
        else:
            logger.warning(f"Mandatory field '{field}' not found in data")
    
    # CORRECT INDENTATION: This is OUTSIDE the for loop!
    # Add error logging if error_logger is provided
    if mandatory_issues > 0 and error_logger and session_id:
        batch_errors = []
        for field in mandatory_fields:
            if field in df.columns:
                missing_mask = detect_missing_values_fast(df[field])
                missing_indices = df[missing_mask].index.tolist()
                
                for idx in missing_indices:
                    db_row = idx + 1
                    excel_row = idx + 2
                    
                    identifier = ""
                    if 'customerid' in df.columns:
                        customer_val = df.loc[idx, 'customerid']
                        if not (pd.isna(customer_val) or 
                                (isinstance(customer_val, str) and 
                                customer_val.strip().lower() in ['nan', 'null', 'none', ''])):
                            identifier = f"customerid={customer_val}"
                    elif 'id' in df.columns:
                        identifier = f"id={df.loc[idx, 'id']}"
                    else:
                        identifier = f"Row {db_row}"
                    
                    batch_errors.append({
                        'session_id': session_id,
                        'check_type': 'single_source',
                        'source_name': source_info,
                        'column_name': field,
                        'row_index': idx,
                        'excel_row': excel_row,
                        'db_row': db_row,
                        'actual_value': 'MISSING',
                        'expected_value': 'Mandatory value required',
                        'error_type': 'mandatory_field_missing',
                        'error_description': f'Mandatory field {field} is missing',
                        'severity': 'high'
                    })
        
        if batch_errors and error_logger:
            error_logger.log_batch_errors(session_id, 'single_source', source_info, batch_errors)

    if mandatory_input.lower() == 'all':
        total_missing_values = len(rows_with_missing_mandatory)
        logger.info(f"All columns mandatory: {total_missing_values} unique rows with missing values")
    else:
        total_missing_values = sum(len(details) for details in mandatory_issue_details.values())
        logger.info(f"Mandatory field check completed: {mandatory_issues} columns with issues, {total_missing_values} total missing values")
    
    return mandatory_issues, mandatory_issue_details, total_missing_values

def calculate_quality_score(df, null_issue_rows, duplicate_rows, format_issue_details, mandatory_issue_details):
    """Calculate quality score based on ACTUAL row counts - FIXED"""
    logger.info("Calculating quality score")
    
    total_rows = len(df)
    if total_rows == 0:
        logger.warning("Empty dataset, quality score is 0")
        return 0.0
    
    rows_with_issues = set()
    
    if null_issue_rows:
        for column, rows in null_issue_rows.items():
            for row in rows:
                if isinstance(row, dict) and 'row_index' in row:
                    rows_with_issues.add(row['row_index'])
    
    if duplicate_rows:
        for row in duplicate_rows:
            if isinstance(row, dict) and 'row_index' in row:
                rows_with_issues.add(row['row_index'])
    
    if format_issue_details:
        for detail in format_issue_details:
            if isinstance(detail, dict) and 'row_index' in detail:
                rows_with_issues.add(detail['row_index'])
    
    if mandatory_issue_details:
        mandatory_rows_set = set()
        for column, rows in mandatory_issue_details.items():
            for row in rows:
                if isinstance(row, dict) and 'row_index' in row:
                    mandatory_rows_set.add(row['row_index'])
        rows_with_issues.update(mandatory_rows_set)
    
    total_issue_rows = len(rows_with_issues)
    good_rows = total_rows - total_issue_rows
    
    quality_score = (good_rows / total_rows) * 100 if total_rows > 0 else 0
    quality_score = round(quality_score, 2)
    
    logger.info(f"Quality Score Calculation:")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(f"  Bad Rows: {total_issue_rows:,}")
    logger.info(f"  Good rows (no issues): {good_rows:,}")
    logger.info(f"  Quality Score: {quality_score}%")
    
    return quality_score

# def generate_detailed_report(df, source_info, null_issue_rows, duplicate_rows, format_issue_details, 
#                             mandatory_issue_details, quality_score, mode='cli', error_logger=None, session_id=None):
def generate_detailed_report(df, source_info, null_issue_rows, duplicate_rows, format_issue_details, 
                            mandatory_issue_details, quality_score, mode='cli', error_logger=None, 
                            session_id=None, total_mandatory_missing=0):
    """Generate detailed quality report"""
    
    logger.info("Generating detailed data quality report")
    
    if mode == 'cli':
        logger.info(f"Report for: {source_info}, Records: {len(df):,}, Columns: {len(df.columns)}")
        logger.info(f"Overall quality score: {quality_score:.1f}%")
        
        if not error_logger:
            try:
                from dq_error_log import ErrorLogger
                error_logger = ErrorLogger()
                session_id = f"SINGLE_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                logger.info(f"Error Logger Session ID: {session_id}")
            except Exception as e:
                logger.warning(f"Error logger initialization failed: {e}")
                error_logger = None
                session_id = "UNKNOWN"
    
    total_null_issues = sum(len(rows) for rows in null_issue_rows.values())
    duplicate_count = len(duplicate_rows) if isinstance(duplicate_rows, list) else 0
    # OLD: total_mandatory_issues = 0
    # NEW: Get it from check_mandatory_fields return value
    total_mandatory_issues = total_mandatory_missing  # This is the 6012 value!

    # OLD: total_mandatory_issues = len(mandatory_rows_set)
    # Remove or comment out this line - don't recalculate!
    # total_mandatory_issues = len(mandatory_rows_set)
    total_format_issues = len(format_issue_details)
    
    json_response = {
        'session_id': session_id if session_id else f"SINGLE_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'source_type': 'csv' if 'CSV' in source_info.upper() else 'excel' if 'EXCEL' in source_info.upper() else 'database',
        'source_info': source_info,
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns': list(df.columns),
        'data_preview': df.head(5).to_dict('records'),
        'checks_performed': ['nulls', 'duplicates', 'mandatory', 'formats'],
        'results': {
            'nulls': {
                'total_null_cells': int(total_null_issues),
                'null_percentage': (total_null_issues/(len(df)*len(df.columns))*100) if len(df)>0 else 0,
                'issue_rows': null_issue_rows
            },
            'duplicates': {
                'total_duplicates': int(duplicate_count),
                'duplicate_percentage': (duplicate_count/len(df)*100) if len(df)>0 else 0,
                'duplicate_details': duplicate_rows,
            },
            'formats': {
                'total_format_issues': int(total_format_issues),
                'format_issue_details': format_issue_details
            },
            'mandatory_fields': {
                'columns_with_issues': len(mandatory_issue_details),
                'total_missing_values': total_mandatory_issues,
                'issue_details': mandatory_issue_details
            }
        },
        'quality_metrics': {
            'quality_score': float(quality_score),
            'assessment_category': 'EXCELLENT' if quality_score >= 90 else 'GOOD' if quality_score >= 70 else 'FAIR' if quality_score >= 50 else 'POOR'
        },
        'summary': {
            'issues_found': 1 if (total_null_issues > 0 or duplicate_count > 0 or total_format_issues > 0 or total_mandatory_issues > 0) else 0,
            'status': 'ISSUES_FOUND' if (total_null_issues > 0 or duplicate_count > 0 or total_format_issues > 0 or total_mandatory_issues > 0) else 'PASS'
        }
    }
    
    if mode == 'api' and error_logger and session_id:
        try:
            error_logs = error_logger.get_error_logs_for_session(session_id, limit=200)
            error_summary = error_logger.get_error_summary_for_session(session_id)
            json_response['error_logs'] = error_logs
            json_response['error_summary'] = error_summary
            logger.info(f"Added {len(error_logs)} error logs to API response")
        except Exception as e:
            logger.error(f"Error getting error logs: {str(e)}")
            json_response['error_logs'] = []
            json_response['error_summary'] = {'total_errors': 0, 'errors_by_type': [], 'errors_by_column': []}
    
    if mode == 'cli':
        print("\n" + "="*70)
        print("📊 DETAILED DATA QUALITY REPORT")
        print("="*70)
        print(f"Source: {source_info}")
        print(f"Session ID: {json_response['session_id']}")
        print(f"Total Records: {len(df):,} | Total Columns: {len(df.columns)}")
        print(f"DQ Score: {quality_score:.2f}%")
        print("="*70)
        
        if total_null_issues > 0:
            print(f"\n🔍 EMPTY/INULL CELLS FOUND ({total_null_issues:,} total):")
            print("-"*60)
            for column, row_details in null_issue_rows.items():
                if row_details:
                    print(f"\n📋 Column: {column} ({len(row_details)} empty cells)")
                    print("-"*40)
                    for detail in row_details[:3]:
                        # print(f"  • DB Row {detail['db_row']} (Excel Row {detail['excel_row']}) - {detail['identifier']}")
                        print(f"  • DB Row {detail['db_row']} (Excel Row {detail['excel_row']})" + 
                                  (f" - {detail['identifier']}" if 'identifier' in detail else ""))
                    if len(row_details) > 3:
                        print(f"  ... and {len(row_details) - 3} more rows")
        
        if duplicate_count > 0:
            print(f"\n🔍 DUPLICATE ROWS FOUND ({duplicate_count:,} total):")
            print("-"*60)
            for detail in duplicate_rows[:3]:
                #print(f"  • DB Row {detail['db_row']} (Excel Row {detail['excel_row']}) - {detail['identifier']}")
                print(f"  • DB Row {detail['db_row']} (Excel Row {detail['excel_row']})" + 
                                   (f" - {detail['identifier']}" if 'identifier' in detail else ""))
            if len(duplicate_rows) > 3:
                print(f"  ... and {len(duplicate_rows) - 3} more duplicate rows")
        
        if total_format_issues > 0:
            print(f"\n🔍 FORMAT ISSUES FOUND ({total_format_issues:,} total):")
            print("-"*60)
            for detail in format_issue_details[:3]:
                print(f"  • {detail['column']} Row {detail['db_row']}: '{detail['actual_value']}' (Expected: {detail['expected_format']})")
            if len(format_issue_details) > 3:
                print(f"  ... and {len(format_issue_details) - 3} more format issues")
        
        if total_mandatory_issues > 0:
            print(f"\n🔍 MISSING MANDATORY FIELDS ({total_mandatory_issues:,} total):")
            print("-"*60)
            for column, row_details in mandatory_issue_details.items():
                if row_details:
                    print(f"\n📋 Column: {column} ({len(row_details)} missing values)")
                    print("-"*40)
                    for detail in row_details[:3]:
                        #print(f"  • DB Row {detail['db_row']} (Excel Row {detail['excel_row']}) - {detail['identifier']}")
                        print(f"  • DB Row {detail['db_row']} (Excel Row {detail['excel_row']})" + 
                                          (f" - {detail['identifier']}" if 'identifier' in detail else ""))
                    if len(row_details) > 3:
                        print(f"  ... and {len(row_details) - 3} more rows")
        
        print("\n💡 RECOMMENDATIONS:")
        print("-"*60)
        
        recommendations = []
        if total_null_issues > 0:
            recommendations.append(f"Fill in {total_null_issues:,} empty/null cells")
        if duplicate_count > 0:
            recommendations.append(f"Remove {duplicate_count:,} duplicate rows")
        if total_format_issues > 0:
            recommendations.append(f"Fix {total_format_issues:,} format issues")
        if total_mandatory_issues > 0:
            recommendations.append(f"Populate {total_mandatory_issues:,} missing mandatory fields")
        
        if total_null_issues == 0 and duplicate_count == 0 and total_format_issues == 0 and total_mandatory_issues == 0:
            recommendations.append("✅ No data quality issues found!")
        
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
        
        # Error logging for CLI
        if error_logger:
            # Log null issues
            for column, row_details in null_issue_rows.items():
                if row_details:
                    for detail in row_details[:20]:
                        error_logger.log_error({
                            'session_id': session_id,
                            'check_type': 'single_source',
                            'source_name': source_info,
                            'column_name': column,
                            'row_index': detail['row_index'],
                            'excel_row': detail['excel_row'],
                            'db_row': detail['db_row'],
                            'actual_value': 'EMPTY/NULL',
                            'expected_value': 'Non-empty value',
                            'error_type': 'empty_cell',
                            'error_description': f'Empty cell found in column {column} (DB Row: {detail["db_row"]}, Excel Row: {detail["excel_row"]})',
                            'severity': 'medium'
                        })
            
            # Log duplicate issues
            if duplicate_count > 0:
                for detail in duplicate_rows[:20]:
                    error_logger.log_error({
                        'session_id': session_id,
                        'check_type': 'single_source',
                        'source_name': source_info,
                        'error_type': 'duplicate_row',
                        'error_description': f'Duplicate row found (DB Row: {detail["db_row"]}, Excel Row: {detail["excel_row"]})',
                        'excel_row': detail['excel_row'],
                        'db_row': detail['db_row'],
                        'severity': 'low'
                    })
            
            # Log format issues
            if total_format_issues > 0:
                for detail in format_issue_details[:20]:
                    error_logger.log_error({
                        'session_id': session_id,
                        'check_type': 'single_source',
                        'source_name': source_info,
                        'column_name': detail['column'],
                        'row_index': detail['row_index'],
                        'excel_row': detail['excel_row'],
                        'db_row': detail['db_row'],
                        'actual_value': detail['actual_value'],
                        'expected_value': detail['expected_format'],
                        'error_type': 'format_issue',
                        'error_description': f'Format issue in {detail["column"]}: "{detail["actual_value"]}" (Expected: {detail["expected_format"]})',
                        'severity': 'medium'
                    })
            
            # Log mandatory field issues
            for column, row_details in mandatory_issue_details.items():
                if row_details:
                    for detail in row_details[:20]:
                        error_logger.log_error({
                            'session_id': session_id,
                            'check_type': 'single_source',
                            'source_name': source_info,
                            'column_name': column,
                            'row_index': detail['row_index'],
                            'excel_row': detail['excel_row'],
                            'db_row': detail['db_row'],
                            'actual_value': 'MISSING',
                            'expected_value': 'Mandatory value required',
                            'error_type': 'mandatory_field_missing',
                            'error_description': f'Mandatory field {column} is missing (DB Row: {detail["db_row"]}, Excel Row: {detail["excel_row"]})',
                            'severity': 'high'
                        })
            total_errors = total_null_issues + duplicate_count + total_format_issues + total_mandatory_issues
            print(f"\n📊 ERROR LOGGING SUMMARY:")
            print(f"   Session ID: {session_id}")
            print(f"   Total errors logged: {total_errors}")
            print(f"   Check dq_error_logs table for detailed error information")
        
        # AUDIT LOGGING FOR CLI MODE 
        try:
            from dq_audit import DataQualityAudit
            audit = DataQualityAudit()
            
            audit_data = {
                'check_type': 'single_source',
                'source_type': json_response['source_type'],
                'source_name': source_info,
                'source_row_count': len(df),
                'total_null_count': total_null_issues,
                'duplicate_row_count': duplicate_count,
                'formatting_issues': total_format_issues,
                'mandatory_field_issues': total_mandatory_issues,
                'quality_score': quality_score,
                'overall_score': quality_score,
                'assessment_category': 'EXCELLENT' if quality_score >= 90 else 'GOOD' if quality_score >= 70 else 'FAIR' if quality_score >= 50 else 'POOR',
                'issues_summary': f"Null cells: {total_null_issues:,}, Duplicates: {duplicate_count:,}, Format issues: {total_format_issues:,}, Mandatory issues: {total_mandatory_issues:,}"
            }
            
            if APP_SETTINGS['audit_enabled']:
                audit.log_audit_record(audit_data)
            
            logger.info(f"Audit logged for session: {audit.session_id}")
            
        except ImportError as e:
            logger.warning(f"Audit module not available for single source analysis: {e}")
        except Exception as e:
            logger.warning(f"Single source audit logging failed: {e}")

        print("\n" + "="*70)
        print("✅ ANALYSIS COMPLETED SUCCESSFULLY!")
        print("="*70)
        
        return json_response
    
    elif mode == 'api':
        logger.info(f"Returning JSON report for API, Session: {json_response['session_id']}")
        return json_response

def run_single_source_analysis_ui(ui_data=None):
    """Run single source analysis in UI mode - COMPLETE FIXED VERSION"""
    try:
        logger.info("=" * 50)
        logger.info("RUN_SINGLE_SOURCE_ANALYSIS_UI - START")
        
        if not ui_data:
            logger.error("ERROR: ui_data is None!")
            return {'error': 'No UI data provided'}
        
        if HAS_INPUT_HANDLER:
            init_input_handler(mode='ui', data=ui_data)
        
        source_type, source_config = select_data_source(ui_data)
        
        if source_config is None:
            logger.error("No valid source configuration provided")
            return {'error': 'No valid source configuration provided'}
        
        logger.info(f"Loading data from {source_type.upper()}...")
        df = load_data_from_source(source_type, source_config)
        
        if df is None or df.empty:
            logger.error("No data loaded or dataset is empty")
            return {'error': 'No data loaded or dataset is empty'}
        
        if source_type in ['csv', 'excel']:
            source_info = f"{source_type.upper()}: {os.path.basename(source_config)}"
        else:
            source_info = f"Database: {source_config['type']} - Table: {source_config['table']}"
        
        logger.info(f"Data loaded: {len(df):,} rows, {len(df.columns)} columns")

        from dq_error_log import ErrorLogger
        error_logger = ErrorLogger()
        session_id = f"SINGLE_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Session ID: {session_id}")

        # Collect column data types for all sources
        column_data_types = {}
        for column in df.columns:
            dtype = str(df[column].dtype)
            # Convert pandas dtypes to simpler representations
            if dtype == 'object':
                column_data_types[column] = 'string'
            elif dtype.startswith('int'):
                column_data_types[column] = 'integer'
            elif dtype.startswith('float'):
                column_data_types[column] = 'float'
            elif dtype == 'bool':
                column_data_types[column] = 'boolean'
            elif 'datetime' in dtype:
                column_data_types[column] = 'datetime'
            elif dtype == 'category':
                column_data_types[column] = 'category'
            else:
                column_data_types[column] = dtype
        
        logger.info(f"Column data types collected: {len(column_data_types)} columns")
        
        logger.info("=" * 50)
        logger.info("Starting quality checks...")
        logger.info("=" * 50)
        
        logger.info("1. Checking for null/empty values...")
        #null_count, null_results, null_issue_rows = check_nulls(df, error_logger, session_id, source_info)
        null_count, null_results, null_issue_rows = check_nulls(df, error_logger, session_id, source_info)
        logger.info(f"   Found {null_count:,} empty cells")
        
        logger.info("2. Checking for duplicate rows...")
        #duplicate_count, duplicate_rows = check_duplicates(df)
        duplicate_count, duplicate_rows = check_duplicates(df, error_logger, session_id, source_info)
        logger.info(f"   Found {duplicate_count:,} duplicate rows")
        
        logger.info("3. Checking data formats...")
        #format_issues, format_issue_details = check_data_formats(df)
        format_issues, format_issue_details = check_data_formats(df, error_logger, session_id, source_info)
        logger.info(f"   Found {format_issues:,} format issues")
        
        logger.info("4. Checking mandatory fields...")
        #mandatory_issues, mandatory_issue_details, total_mandatory_missing = check_mandatory_fields(df, ui_data)
        mandatory_issues, mandatory_issue_details, total_mandatory_missing = check_mandatory_fields(df, ui_data, error_logger, session_id, source_info)
        logger.info(f"   Found {total_mandatory_missing:,} missing mandatory values")
        
        logger.info("5. Calculating quality score...")
        quality_score = calculate_quality_score(
            df=df,
            null_issue_rows=null_issue_rows,
            duplicate_rows=duplicate_rows,
            format_issue_details=format_issue_details,
            mandatory_issue_details=mandatory_issue_details
        )
        
        # Store session data for pagination
        single_sessions[session_id] = {
            'df': df,
            'source_info': source_info,
            'source_type': source_type,
            'timestamp': datetime.now().isoformat(),
            'column_data_types': column_data_types,
            'results': None  # Will be filled later
        }

        results = {
            'session_id': session_id,
            'source_type': source_type,
            'source_info': source_info,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'column_data_types': column_data_types,
            'checks_performed': ['nulls', 'duplicates', 'mandatory', 'formats'],
            'results': {
                'nulls': {
                    'total_null_cells': int(null_count),
                    'null_percentage': (null_count/(len(df)*len(df.columns))*100) if len(df)>0 else 0,
                    'issue_rows': null_issue_rows
                },
                'duplicates': {
                    'total_duplicates': int(duplicate_count),
                    'duplicate_percentage': (duplicate_count/len(df)*100) if len(df)>0 else 0,
                    'duplicate_details': duplicate_rows
                },
                'formats': {
                    'total_format_issues': int(format_issues),
                    'format_issue_details': format_issue_details
                },
                'mandatory_fields': {
                    'columns_with_issues': len(mandatory_issue_details),
                    'total_missing_values': total_mandatory_missing,
                    'issue_details': mandatory_issue_details
                }
            },
            'quality_metrics': {
                'quality_score': float(quality_score),
                'assessment_category': 'EXCELLENT' if quality_score >= 90 else 'GOOD' if quality_score >= 70 else 'FAIR' if quality_score >= 50 else 'POOR'
            },
            'summary': {
                'issues_found': 1 if (null_count > 0 or duplicate_count > 0 or format_issues > 0 or total_mandatory_missing > 0) else 0,
                'status': 'ISSUES_FOUND' if (null_count > 0 or duplicate_count > 0 or format_issues > 0 or total_mandatory_missing > 0) else 'PASS'
            },
            'pagination_info': {
                'total_rows': len(df),
                'page_size': 50,  # Default page size
                'total_pages': max(1, (len(df) + 49) // 50),
                'api_endpoints': {
                    'data_rows': f'/api/single/{session_id}/data?page=1&page_size=50',
                    'error_details': f'/api/single/{session_id}/errors?page=1&page_size=50'
                }
            }
        }
        
        # Store results in session for pagination access
        single_sessions[session_id]['results'] = results
        
        try:
            error_logs = error_logger.get_error_logs_for_session(session_id, limit=200)
            error_summary = error_logger.get_error_summary_for_session(session_id)
            results['error_logs'] = error_logs
            results['error_summary'] = error_summary
            logger.info(f"Added {len(error_logs)} error logs to results")
        except Exception as e:
            logger.warning(f"Could not get error logs: {e}")
            results['error_logs'] = []
            results['error_summary'] = {'total_errors': 0, 'errors_by_type': [], 'errors_by_column': []}
        
        try:
            from dq_audit import DataQualityAudit
            audit = DataQualityAudit()
            
            audit_data = {
                'session_id': session_id,
                'check_type': 'single_source',
                'source_type': source_type,
                'source_name': source_info,
                'source_row_count': len(df),
                'total_null_count': null_count,
                'duplicate_row_count': duplicate_count,
                'formatting_issues': format_issues,
                'mandatory_field_issues': total_mandatory_missing,
                'quality_score': quality_score,
                'overall_score': quality_score,
                'assessment_category': 'EXCELLENT' if quality_score >= 90 else 'GOOD' if quality_score >= 70 else 'FAIR' if quality_score >= 50 else 'POOR',
                'issues_summary': f"Null cells: {null_count:,}, Format issues: {format_issues:,}, Duplicates: {duplicate_count:,}, Mandatory issues: {total_mandatory_missing:,}"
            }
            
            if APP_SETTINGS.get('audit_enabled', True):
                audit.log_audit_record(audit_data)
                audit_logs = audit.get_audit_logs_for_session(session_id)
                results['audit_logs'] = audit_logs
                logger.info(f"Added {len(audit_logs)} audit logs to results")
            else:
                results['audit_logs'] = []
                
        except Exception as e:
            logger.error(f"Audit logging failed: {e}")
            results['audit_logs'] = []
        
        logger.info(f"Analysis completed successfully for {source_info}")
        logger.info(f"Final quality score: {quality_score}%")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in unified analysis: {str(e)}", exc_info=True)
        return {'error': str(e)}

def main(ui_data=None):
    """Main function for unified data quality analysis"""
    logger.info("Starting unified data quality analysis")
    
    try:
        if ui_data and HAS_INPUT_HANDLER:
            init_input_handler(mode='ui', data=ui_data)
            return run_single_source_analysis_ui(ui_data)
        
        source_type, source_config = select_data_source()
        
        if source_config is None:
            logger.error("No valid source configuration provided")
            return
        
        logger.info(f"Loading data from {source_type.upper()}")
        
        df = load_data_from_source(source_type, source_config)
        
        if df is None or df.empty:
            logger.error("No data loaded or dataset is empty")
            return
        
        logger.info(f"DATA PREVIEW:")
        logger.info(f"   Total rows: {len(df):,}")
        logger.info(f"   Total columns: {len(df.columns)}")
        logger.info(f"   Columns: {list(df.columns)}")
        
        logger.info(f"Running quality checks")
        
        # null_count, null_results, null_issue_rows = check_nulls(df)
        
        # duplicate_count, duplicate_rows = check_duplicates(df)
        
        # format_issues, format_issue_details = check_data_formats(df)
        
        # mandatory_issues, mandatory_issue_details, total_mandatory_missing = check_mandatory_fields(df, None)

        null_count, null_results, null_issue_rows = check_nulls(df)
        duplicate_count, duplicate_rows = check_duplicates(df, None, None, "")
        format_issues, format_issue_details = check_data_formats(df, None, None, "")
        mandatory_issues, mandatory_issue_details, total_mandatory_missing = check_mandatory_fields(df, None, None, None, "")
        
        quality_score = calculate_quality_score(df, null_issue_rows, duplicate_rows, format_issue_details, mandatory_issue_details)
        
        # Create source info string
        if source_type in ['csv', 'excel']:
            source_info = f"{source_type.upper()}: {os.path.basename(source_config)}"
        else:
            source_info = f"Database: {source_config['type']} - Table: {source_config['table']}"
        
        # generate_detailed_report(df, source_info, null_issue_rows, duplicate_rows, format_issue_details, 
        #                 mandatory_issue_details, quality_score, mode='cli')
        
        generate_detailed_report(df, source_info, null_issue_rows, duplicate_rows, format_issue_details, 
                        mandatory_issue_details, quality_score, mode='cli', 
                        total_mandatory_missing=total_mandatory_missing)
        
        logger.info(f"Analysis completed successfully for {source_info}")
        
    except Exception as e:
        logger.error(f"Error in unified analysis: {str(e)}", exc_info=True)

def cleanup_single_sessions(max_age_hours=24, max_sessions=50):
    """Clean up old single source sessions from memory"""
    try:
        current_time = datetime.now()
        sessions_to_delete = []
        
        for session_id, session_info in list(single_sessions.items()):
            try:
                session_time = datetime.fromisoformat(session_info.get('timestamp', current_time.isoformat()))
                age_hours = (current_time - session_time).total_seconds() / 3600
                
                if age_hours > max_age_hours:
                    sessions_to_delete.append(session_id)
            except:
                pass
        
        # Clean by count if still too many
        if len(single_sessions) > max_sessions:
            # Sort sessions by timestamp (oldest first)
            sorted_sessions = sorted(
                single_sessions.items(),
                key=lambda x: x[1].get('timestamp', '')
            )
            
            # Add oldest sessions to delete list
            for session_id, _ in sorted_sessions[:len(single_sessions) - max_sessions]:
                if session_id not in sessions_to_delete:
                    sessions_to_delete.append(session_id)
        
        # Delete sessions
        deleted_count = 0
        for session_id in sessions_to_delete:
            if session_id in single_sessions:
                single_sessions.pop(session_id)
                deleted_count += 1
        
        logger.info(f"Single session cleanup: Deleted {deleted_count} sessions from memory")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Error cleaning up single sessions: {e}")
        return 0
# ========== END ADDITION ==========

if __name__ == "__main__":
    main()