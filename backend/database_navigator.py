# database_navigator.py
"""
Database Navigator for Data Quality Framework
Reuses components from database_dropdown.py for hierarchical selection
"""

import os
import sys
import sqlite3
import logging

# Setup logger
logger = logging.getLogger(__name__)

# Database connectors with error handling
DB_CONNECTORS = {
    'mysql': False,
    'postgres': False,
    'sqlserver': False,
    'oracle': False
}

try:
    import mysql.connector
    DB_CONNECTORS['mysql'] = True
except ImportError:
    logger.warning("mysql-connector-python not installed. MySQL support disabled.")

try:
    import psycopg2
    from psycopg2 import sql
    DB_CONNECTORS['postgres'] = True
except ImportError:
    logger.warning("psycopg2-binary not installed. PostgreSQL/Redshift support disabled.")

try:
    import pyodbc
    DB_CONNECTORS['sqlserver'] = True
except ImportError:
    logger.warning("pyodbc not installed. SQL Server support disabled.")

try:
    import cx_Oracle
    DB_CONNECTORS['oracle'] = True
except ImportError:
    logger.warning("cx_Oracle not installed. Oracle support disabled.")

# Import configuration from your file
try:
    from db_config import (
        MYSQL_CONFIG, 
        POSTGRESQL_CONFIG, 
        SQLSERVER_CONFIG, 
        ORACLE_CONFIG
    )
    CONFIG_LOADED = True
except ImportError:
    CONFIG_LOADED = False
    logger.warning("db_config.py not found. Using default configurations.")


class DatabaseNavigator:
    """
    Database navigator for hierarchical selection (Database → Schema → Table)
    Returns selection instead of displaying data
    """
    
    def __init__(self, mode='cli', ui_data=None):
        """
        Initialize database navigator
        
        Args:
            mode: 'cli' or 'ui'
            ui_data: Pre-provided data for UI mode
        """
        self.connection = None
        self.db_type = None
        self.selected_database = None
        self.selected_schema = None
        self.selected_table = None
        self.current_config = {}  # Store current connection config
        self.mode = mode
        self.ui_data = ui_data or {}
        
        # Default configurations if config file is not loaded
        if not CONFIG_LOADED:
            self.MYSQL_CONFIG = {
                'host': 'localhost',
                'user': 'root', 
                'password': 'root',
                'database': '',
                'port': '3306'
            }
            self.POSTGRESQL_CONFIG = {
                'host': 'localhost',
                'port': '5432',
                'database': 'postgres',
                'user': 'postgres',
                'password': 'postgres'
            }
            self.SQLSERVER_CONFIG = {
                'host': 'localhost',
                'port': '1433',
                'database': 'master',
                'user': 'sa',
                'password': 'password'
            }
            self.ORACLE_CONFIG = {
                'host': 'localhost',
                'port': '1521',
                'service_name': 'XE',
                'user': 'system',
                'password': 'manager',
                'encoding': 'UTF-8'
            }
        else:
            self.MYSQL_CONFIG = MYSQL_CONFIG.copy()
            self.POSTGRESQL_CONFIG = POSTGRESQL_CONFIG.copy()
            self.SQLSERVER_CONFIG = SQLSERVER_CONFIG.copy()
            self.ORACLE_CONFIG = ORACLE_CONFIG.copy()
            # Ensure PostgreSQL has a default database for initial connection
            if 'database' not in self.POSTGRESQL_CONFIG or not self.POSTGRESQL_CONFIG['database']:
                self.POSTGRESQL_CONFIG['database'] = 'postgres'
        
        # Get available databases
        self.available_dbs = self.get_available_databases()
        logger.info("DatabaseNavigator initialized")
    
    def get_available_databases(self):
        """Return list of available database types based on installed connectors"""
        available = []
        
        if DB_CONNECTORS['mysql']:
            available.append(('MySQL', 'mysql'))
        if DB_CONNECTORS['postgres']:
            available.append(('PostgreSQL', 'postgres'))
            available.append(('Redshift', 'redshift'))  # Redshift uses psycopg2
        if DB_CONNECTORS['sqlserver']:
            available.append(('SQL Server', 'sqlserver'))
        if DB_CONNECTORS['oracle']:
            available.append(('Oracle', 'oracle'))
        
        # SQLite is always available (built-in)
        available.append(('SQLite', 'sqlite'))
            
        return available
    
    def display_menu(self, title, items, back_option=True):
        """Display a numbered menu and get user selection"""
        if self.mode == 'ui':
            # In UI mode, selection comes from ui_data
            return self._get_ui_selection(title, items)
        
        # CLI mode - show interactive menu
        print(f"\n{'='*60}")
        print(f"{title.upper()}")
        print('='*60)
        
        for idx, item in enumerate(items, 1):
            if isinstance(item, tuple):
                print(f"{idx}. {item[0]}")
            else:
                print(f"{idx}. {item}")
        
        print('='*60)
        
        while True:
            if back_option:
                choice = input(f"\nSelect option (1-{len(items)}), or 'b' to go back: ")
            else:
                choice = input(f"\nSelect option (1-{len(items)}): ")
            
            if back_option and choice.lower() == 'b':
                return None
            
            try:
                choice_num = int(choice)
                if 1 <= choice_num <= len(items):
                    if isinstance(items[choice_num-1], tuple):
                        return items[choice_num-1][1]
                    else:
                        return items[choice_num-1]
                else:
                    print(f"Please enter a number between 1 and {len(items)}")
            except ValueError:
                print("Please enter a valid number" + (" or 'b' to go back" if back_option else ""))
    
    def _get_ui_selection(self, title, items):
        """Get selection from UI data"""
        # Try to match title to find selection in ui_data
        key_map = {
            'Select Database Type': 'db_type',
            'Select Database': 'database',
            'Select Schema': 'schema',
            'Select Table': 'table'
        }
        
        if title in key_map:
            key = key_map[title]
            if key in self.ui_data:
                selection = self.ui_data[key]
                logger.info(f"UI selection for {key}: {selection}")
                return selection
        
        # If not found in UI data, use first item
        if items:
            if isinstance(items[0], tuple):
                return items[0][1]
            else:
                return items[0]
        
        return None
    
    def select_database_type(self):
        """Prompt user to select database type"""
        return self.display_menu("Select Database Type", self.available_dbs, back_option=False)
    
    def get_custom_config(self, default_config, db_type):
        """Get custom configuration from user"""
        if self.mode == 'ui':
            # In UI mode, config comes from ui_data
            config = {}
            config_fields = ['host', 'port', 'database', 'user', 'password', 'service_name', 'encoding']
            
            for field in config_fields:
                if field in self.ui_data:
                    config[field] = self.ui_data[field]
                elif field in default_config:
                    config[field] = default_config[field]
            
            return config
        
        # CLI mode - interactive input
        config = default_config.copy()
        
        print(f"\n{'='*60}")
        print(f"{db_type.upper()} CONFIGURATION")
        print('='*60)
        print("Press Enter to use default values from config file")
        
        for key in config.keys():
            if key == 'database' and db_type == 'postgres':
                # For PostgreSQL, we need a database to connect initially
                value = input(f"Initial database [{config[key]}]: ").strip()
                if value:
                    config[key] = value
            elif key == 'database' and db_type != 'postgres':
                # For other databases, we'll connect without specific database first
                continue
            elif key == 'port':
                port = input(f"Port [{config[key]}]: ").strip()
                if port:
                    config[key] = int(port) if port.isdigit() else port
            elif key == 'service_name':
                value = input(f"Service Name [{config[key]}]: ").strip()
                if value:
                    config[key] = value
            elif key == 'encoding':
                value = input(f"Encoding [{config[key]}]: ").strip()
                if value:
                    config[key] = value
            else:
                value = input(f"{key} [{config[key]}]: ").strip()
                if value:
                    config[key] = value
        
        return config
    
    def connect_to_database(self, db_type, config=None, specific_database=None):
        """Establish connection based on selected database type"""
        try:
            logger.info(f"Connecting to {db_type.upper()}...")
            
            if db_type == 'mysql':
                if config is None:
                    config = self.MYSQL_CONFIG
                self.current_config = config.copy()
                
                connection_config = {
                    'host': config['host'],
                    'user': config['user'],
                    'password': config['password'],
                    'port': config.get('port', 3306)
                }
                
                # Connect without specific database first
                self.connection = mysql.connector.connect(**connection_config)
                
            elif db_type in ['postgres', 'postgresql']:
                logger.info("Connecting to PostgreSQL...")
                
                if config is None:
                    config = self.POSTGRESQL_CONFIG
                self.current_config = config.copy()
                
                # Use 'postgres' as default database for initial connection
                db_to_connect = specific_database if specific_database else 'postgres'
                
                try:
                    # Connect
                    logger.info(f"Connecting to PostgreSQL: host={config['host']}, port={config['port']}, database={db_to_connect}, user={config['user']}")
                    conn = psycopg2.connect(
                        host=config['host'],
                        port=config['port'],
                        database=db_to_connect,
                        user=config['user'],
                        password=config['password']
                    )
                    
                    logger.info(f"Connection object created: {conn}")
                    
                    conn.autocommit = True

                    # TEST THE CONNECTION IMMEDIATELY
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    
                    self.connection = conn  # Only assign after successful test
                    
                    logger.info(f"✅ PostgreSQL connection successful to database: {db_to_connect}")
                    return True
                    
                except Exception as e:
                    logger.error(f"❌ PostgreSQL connection failed: {str(e)}", exc_info=True)
                    self.connection = None
                    return False
                
            elif db_type == 'redshift':
                if self.mode == 'ui':
                    redshift_config = {
                        'host': self.ui_data.get('host', ''),
                        'port': self.ui_data.get('port', '5439'),
                        'database': self.ui_data.get('database', 'dev'),
                        'user': self.ui_data.get('user', ''),
                        'password': self.ui_data.get('password', '')
                    }
                else:
                    print(f"\n{'='*60}")
                    print("REDSHIFT CONFIGURATION")
                    print('='*60)
                    
                    redshift_config = {
                        'host': input("Host/Endpoint: "),
                        'port': input("Port [5439]: ") or '5439',
                        'database': input("Database [dev]: ") or 'dev',
                        'user': input("Username: "),
                        'password': input("Password: ")
                    }
                
                self.current_config = redshift_config.copy()
                
                self.connection = psycopg2.connect(
                    host=redshift_config['host'],
                    port=redshift_config['port'],
                    database=redshift_config['database'],
                    user=redshift_config['user'],
                    password=redshift_config['password']
                )
                self.connection.autocommit = True
                
            elif db_type == 'sqlserver':
                if config is None:
                    config = self.SQLSERVER_CONFIG
                self.current_config = config.copy()
                
                # Try to connect to master database first
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={config['host']},{config['port']};"
                    f"DATABASE=master;"
                    f"UID={config['user']};"
                    f"PWD={config['password']};"
                    "TrustServerCertificate=yes;"
                )
                self.connection = pyodbc.connect(connection_string)
                
            elif db_type == 'oracle':
                if config is None:
                    config = self.ORACLE_CONFIG
                self.current_config = config.copy()
                
                dsn = cx_Oracle.makedsn(
                    config['host'], 
                    config['port'], 
                    service_name=config['service_name']
                )
                self.connection = cx_Oracle.connect(
                    user=config['user'],
                    password=config['password'],
                    dsn=dsn,
                    encoding=config.get('encoding', 'UTF-8')
                )
                
            elif db_type == 'sqlite':
                if self.mode == 'ui':
                    db_path = self.ui_data.get('file_path', '')
                else:
                    db_path = input("\nEnter SQLite database file path: ").strip()
                
                if not os.path.exists(db_path):
                    if self.mode == 'cli':
                        create_new = input(f"File '{db_path}' does not exist. Create new database? (y/n): ")
                        if create_new.lower() != 'y':
                            logger.warning("Connection cancelled.")
                            return False
                    else:
                        logger.error(f"SQLite file not found: {db_path}")
                        return False
                
                self.connection = sqlite3.connect(db_path)
                self.selected_database = 'main'  # SQLite has only one database
                self.current_config = {'database': db_path}
            
            logger.info(f"Successfully connected to {db_type.upper()}!")
            
            return True
            
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}", exc_info=True)
            return False
    
    def reconnect_to_database(self, specific_database=None):
        """Reconnect to the database, optionally to a specific database"""
        if not self.db_type or not self.current_config:
            return False
        
        try:
            # Close existing connection if open
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
            
            # Reconnect
            if self.db_type in ['mysql', 'sqlserver', 'oracle', 'sqlite']:
                return self.connect_to_database(self.db_type, self.current_config)
            elif self.db_type in ['postgres', 'postgresql', 'redshift']:
                # ========== FIX: Always reconnect with specific database ==========
                reconnect_config = self.current_config.copy()
                
                if specific_database:
                    reconnect_config['database'] = specific_database
                    logger.info(f"Reconnecting to PostgreSQL database: {specific_database}")
                elif 'database' in reconnect_config:
                    logger.info(f"Reconnecting to PostgreSQL database: {reconnect_config['database']}")
                else:
                    logger.warning("No database specified for PostgreSQL reconnection")
                    reconnect_config['database'] = 'postgres'
                
                return self.connect_to_database(self.db_type, reconnect_config, reconnect_config['database'])
            
        except Exception as e:
            logger.error(f"Error reconnecting: {str(e)}")
            return False
    
    def get_databases(self):
        """Get list of databases"""
        try:
            logger.info(f"get_databases() called. db_type: {self.db_type}, connection exists: {self.connection is not None}")
            databases = []

            # If no connection but we have config, try to connect
            if not self.connection and self.db_type and self.current_config:
                logger.info(f"No connection found. Attempting to connect to {self.db_type}...")
                if self.db_type in ['postgres', 'postgresql', 'redshift']:
                    # For PostgreSQL, connect to default 'postgres' database
                    if not self.connect_to_database(self.db_type, self.current_config, 'postgres'):
                        logger.error("Failed to reconnect")
                        return []
                else:
                    if not self.connect_to_database(self.db_type, self.current_config):
                        logger.error("Failed to reconnect")
                        return []

            if self.connection and hasattr(self.connection, 'closed') and self.connection.closed:
                logger.error("Connection was closed, reconnecting...")
                self.reconnect_to_database()
                
            if not self.connection:
                logger.error("No database connection")
                return []
            
            cursor = self.connection.cursor()
            
            try:
                if self.db_type == 'mysql':
                    cursor.execute("SHOW DATABASES")
                    databases = [row[0] for row in cursor.fetchall()]
                    
                elif self.db_type in ['postgres','postgresql','redshift']:
                    # cursor = self.connection.cursor()
                    # cursor.execute("""
                    #     SELECT datname 
                    #     FROM pg_database 
                    #     ORDER BY datname
                    # """)
                    # cursor.execute("SELECT datname FROM pg_database ORDER BY datname")
                    cursor = self.connection.cursor()
                    cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
                    rows = cursor.fetchall()
                    logger.info(f"Raw rows: {rows}")
                    databases = [row[0] for row in rows]
                    
                elif self.db_type == 'sqlserver':
                    cursor.execute("""
                        SELECT name 
                        FROM sys.databases 
                        WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
                        ORDER BY name
                    """)
                    databases = [row[0] for row in cursor.fetchall()]
                    
                elif self.db_type == 'oracle':
                    # In Oracle, databases are equivalent to schemas/users
                    cursor.execute("""
                        SELECT username 
                        FROM all_users 
                        --WHERE username NOT IN ('SYS', 'SYSTEM')
                        ORDER BY username
                    """)
                    databases = [row[0] for row in cursor.fetchall()]
                    
                elif self.db_type == 'sqlite':
                    # SQLite has only one database
                    databases = ['main']
                
                logger.info(f"Found {len(databases)} databases")
                return databases
                
            except Exception as e:
                logger.error(f"Error fetching databases: {str(e)}", exc_info=True)
                databases = []
                return []
        except Exception as e:
            logger.error(f"Error in get_databases: {e}", exc_info=True)
            return []   
        finally:
            cursor.close()
        
    def select_database(self):
        """Select a specific database"""
        databases = self.get_databases()
        if not databases:
            logger.warning("No databases found.")
            return None
        
        selected_db = self.display_menu(f"Select Database ({len(databases)} found)", databases)
        
        if selected_db:
            self.selected_database = selected_db
            logger.info(f"Selected database: {selected_db}")
            
            # For MySQL, switch to the selected database
            if self.db_type == 'mysql':
                try:
                    cursor = self.connection.cursor()
                    cursor.execute(f"USE {selected_db}")
                    cursor.close()
                except Exception as e:
                    logger.error(f"Error switching to database: {str(e)}")
            # For PostgreSQL/Redshift, reconnect to the selected database
            elif self.db_type in ['postgres', 'postgresql','redshift']:
                if not self.reconnect_to_database(selected_db):
                    logger.error(f"Failed to connect to database '{selected_db}'")
                    return None
        
        return selected_db
    
    def get_schemas(self):
        """Get list of schemas for the selected database"""
        if not self.connection:
            logger.error("No database connection")
            return []
        
        cursor = self.connection.cursor()
        
        try:
            if self.db_type == 'mysql':
                # In MySQL, schemas are the same as databases
                schemas = [self.selected_database]
                
            elif self.db_type in ['postgres', 'postgresql', 'redshift']:
                # For PostgreSQL/Redshift, we're already connected to the selected database
                cursor.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    --WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ORDER BY schema_name
                """)
                schemas = [row[0] for row in cursor.fetchall()]
                
            elif self.db_type == 'sqlserver':
                # Switch to selected database
                try:
                    cursor.execute(f"USE {self.selected_database}")
                except:
                    # If we can't switch, try reconnecting
                    self.reconnect_to_database(self.selected_database)
                    cursor = self.connection.cursor()
                    cursor.execute(f"USE {self.selected_database}")
                
                cursor.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name NOT IN ('db_owner', 'db_accessadmin', 'db_securityadmin', 
                                             'db_ddladmin', 'db_backupoperator', 'db_datareader', 
                                             'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
                    ORDER BY schema_name
                """)
                schemas = [row[0] for row in cursor.fetchall()]
                
            elif self.db_type == 'oracle':
                # In Oracle, schemas are users
                schemas = [self.selected_database]
                
            elif self.db_type == 'sqlite':
                # SQLite doesn't have schemas
                schemas = ['main']
            
            logger.info(f"Found {len(schemas)} schemas")
            return schemas
            
        except Exception as e:
            logger.error(f"Error fetching schemas: {str(e)}")
            return []
        finally:
            cursor.close()
    
    def select_schema(self):
        """Select a specific schema"""
        schemas = self.get_schemas()
        if not schemas:
            logger.warning("No schemas found.")
            return None
        
        selected_schema = self.display_menu(f"Select Schema for database '{self.selected_database}'", schemas)
        
        if selected_schema:
            self.selected_schema = selected_schema
            logger.info(f"Selected schema: {selected_schema}")
        
        return selected_schema
    
    def get_tables(self):
        """Get list of tables in the selected schema"""
        if not self.connection:
            logger.error("No database connection")
            return []
        
        cursor = self.connection.cursor()
        
        try:
            if self.db_type in ['postgres', 'postgresql', 'redshift']:
                # Always reconnect to ensure we're in the correct database
                if self.selected_database:
                    logger.info(f"Ensuring connection to database: {self.selected_database} for table list")
                    # Store current connection state
                    was_connected = self.connection is not None
                    
                    # Reconnect to the selected database
                    if not self.reconnect_to_database(self.selected_database):
                        logger.error(f"Failed to connect to database: {self.selected_database}")
                        return []
                    
                    # Re-create cursor after reconnection
                    cursor = self.connection.cursor()
                    
                    # Debug: Verify we're in the right database
                    cursor.execute("SELECT current_database()")
                    current_db = cursor.fetchone()[0]
                    logger.info(f"Connected to database: {current_db}, schema: {self.selected_schema}")
                    
                    if current_db != self.selected_database:
                        logger.error(f"Still connected to wrong database: {current_db}, expected: {self.selected_database}")
                        return []
                else:
                    cursor = self.connection.cursor()
            else:
                cursor = self.connection.cursor()
            if self.db_type == 'mysql':
                cursor.execute(f"USE {self.selected_schema}")
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
                
            elif self.db_type in ['postgres', 'postgresql', 'redshift']:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """, (self.selected_schema,))
                tables = [row[0] for row in cursor.fetchall()]
                logger.info(f"Found {len(tables)} tables in {self.selected_database}.{self.selected_schema}: {tables}")

            elif self.db_type == 'sqlserver':
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """, (self.selected_schema,))
                tables = [row[0] for row in cursor.fetchall()]
                
            elif self.db_type == 'oracle':
                cursor.execute("""
                    SELECT table_name 
                    FROM all_tables 
                    WHERE owner = :owner 
                    ORDER BY table_name
                """, owner=self.selected_schema)
                tables = [row[0] for row in cursor.fetchall()]
                
            elif self.db_type == 'sqlite':
                cursor.execute("""
                    SELECT name 
                    FROM sqlite_master 
                    WHERE type='table' 
                    ORDER BY name
                """)
                tables = [row[0] for row in cursor.fetchall()]
            
            logger.info(f"Found {len(tables)} tables in schema '{self.selected_schema}'")
            return tables
            
        except Exception as e:
            logger.error(f"Error fetching tables: {str(e)}")
            return []
        finally:
            cursor.close()
    
    def select_table(self):
        """Select a specific table"""
        tables = self.get_tables()
        if not tables:
            logger.warning(f"No tables found in schema '{self.selected_schema}'.")
            return None
        
        selected_table = self.display_menu(f"Select Table from schema '{self.selected_schema}'", tables)
        
        if selected_table:
            self.selected_table = selected_table
            logger.info(f"Selected table: {selected_table}")
        
        return selected_table
    
    def browse_and_select(self, db_type=None, db_config=None):
        """
        Main method: Browse database hierarchy and return selection
        
        Args:
            db_type: Optional, pre-selected database type
            db_config: Optional, pre-provided connection configuration
            
        Returns:
            dict: {
                'type': 'postgresql',
                'host': 'localhost',
                'port': 5432,
                'database': 'selected_db',
                'schema': 'selected_schema',
                'table': 'selected_table',
                'user': 'postgres'
                # Note: Password not included for security
            }
            OR None if cancelled
        """
        try:
            logger.info("Starting database navigation...")
            
            # Step 1: If db_type not provided, ask for it
            if not db_type:
                if self.mode == 'cli':
                    print("\n" + "="*60)
                    print("DATABASE TYPE SELECTION")
                    print("="*60)
                self.db_type = self.select_database_type()
                if not self.db_type:
                    logger.warning("No database type selected")
                    return None
            else:
                self.db_type = db_type
            
            logger.info(f"Database type: {self.db_type}")
            
            # Step 2: Get connection configuration
            config = None
            if db_config:
                config = db_config
                logger.info("Using provided database configuration")
            else:
                logger.info("Getting database configuration...")
                if self.db_type == 'mysql':
                    config = self.get_custom_config(self.MYSQL_CONFIG, 'mysql')
                elif self.db_type == 'postgres':
                    config = self.get_custom_config(self.POSTGRESQL_CONFIG, 'postgres')
                elif self.db_type == 'redshift':
                    # Redshift uses PostgreSQL driver
                    config = {'type': 'postgres'}
                    if self.mode == 'ui':
                        # Copy redshift config from ui_data
                        config.update({k: v for k, v in self.ui_data.items() 
                                     if k in ['host', 'port', 'database', 'user', 'password']})
                elif self.db_type == 'sqlserver':
                    config = self.get_custom_config(self.SQLSERVER_CONFIG, 'sqlserver')
                elif self.db_type == 'oracle':
                    config = self.get_custom_config(self.ORACLE_CONFIG, 'oracle')
                elif self.db_type == 'sqlite':
                    config = {}
            
            if not config:
                logger.error("No database configuration obtained")
                return None
            
            # Step 3: Connect to database
            if not self.connect_to_database(self.db_type, config):
                logger.error("Failed to connect to database")
                return None
            
            # Step 4: Browse hierarchy
            if self.mode == 'cli':
                print(f"\n{'='*60}")
                print(f"BROWSING: {self.db_type.upper()}")
                print('='*60)
            
            # Reset selections
            self.selected_database = None
            self.selected_schema = None
            self.selected_table = None
            
            # Step 5: Select database
            if self.mode == 'cli':
                print("\n📁 STEP 1: Select Database")
            if not self.select_database():
                logger.warning("Database selection cancelled")
                return None
            
            # Step 6: Select schema
            if self.mode == 'cli':
                print("\n📁 STEP 2: Select Schema")
            if not self.select_schema():
                logger.warning("Schema selection cancelled")
                return None
            
            # Step 7: Select table
            if self.mode == 'cli':
                print("\n📁 STEP 3: Select Table")
            if not self.select_table():
                logger.warning("Table selection cancelled")
                return None
            
            # Step 8: Prepare result
            result = {
                'type': 'postgresql' if self.db_type == 'postgres' else self.db_type,
                #'type': self.db_type,
                'database': self.selected_database,
                'schema': self.selected_schema,
                'table': self.selected_table,
                'selection_mode': 'hierarchical'  # Mark that this was selected via hierarchy
            }
            
            # Add connection details (but not password for security)
            connection_fields = ['host', 'port', 'user', 'password', 'service_name', 'encoding', 'file_path']
            for field in connection_fields:
                if field in self.current_config:
                    result[field] = self.current_config[field]
            
            if self.mode == 'cli':
                print(f"\n{'='*60}")
                print("✅ SELECTION COMPLETE!")
                print("="*60)
                print(f"   Database: {self.selected_database}")
                print(f"   Schema: {self.selected_schema}")
                print(f"   Table: {self.selected_table}")
                print("="*60)
            
            logger.info(f"Selection complete: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error during navigation: {str(e)}", exc_info=True)
            return None
        finally:
            # Clean up connection
            if self.connection:
                try:
                    self.connection.close()
                    self.connection = None
                except:
                    pass


# Helper functions for easy integration
def navigate_database(mode='cli', ui_data=None, db_type=None, db_config=None):
    """
    Simple function to start database navigation
    
    Args:
        mode: 'cli' or 'ui'
        ui_data: Pre-provided data for UI mode
        db_type: Optional pre-selected database type
        db_config: Optional pre-provided connection config
        
    Returns:
        dict: Database selection configuration or None
    """
    navigator = DatabaseNavigator(mode=mode, ui_data=ui_data)
    return navigator.browse_and_select(db_type=db_type, db_config=db_config)


def get_database_hierarchy(db_type, db_config):
    """
    Get database hierarchy without interactive selection
    Useful for UI to populate dropdowns
    
    Args:
        db_type: Database type
        db_config: Connection configuration
        
    Returns:
        dict: {'databases': [], 'schemas': [], 'tables': []}
    """
    try:
        navigator = DatabaseNavigator(mode='ui', ui_data=db_config)
        navigator.db_type = db_type
        
        # Connect
        if not navigator.connect_to_database(db_type, db_config):
            return {'error': 'Connection failed'}
        
        # Get databases
        databases = navigator.get_databases()
        
        # Get schemas for first database (or default)
        schemas = []
        if databases:
            navigator.selected_database = databases[0]
            schemas = navigator.get_schemas()
        
        # Get tables for first schema (or default)
        tables = []
        if schemas:
            navigator.selected_schema = schemas[0]
            tables = navigator.get_tables()
        
        # Clean up
        if navigator.connection:
            navigator.connection.close()
        
        return {
            'databases': databases,
            'schemas': schemas,
            'tables': tables
        }
        
    except Exception as e:
        logger.error(f"Error getting database hierarchy: {str(e)}")
        return {'error': str(e)}


if __name__ == "__main__":
    # Test the navigator
    import logging
    logging.basicConfig(level=logging.INFO)
    
    print("\n" + "="*60)
    print("DATABASE NAVIGATOR TEST")
    print("="*60)
    
    result = navigate_database(mode='cli')
    
    if result:
        print("\n" + "="*60)
        print("SELECTION RESULT:")
        print("="*60)
        for key, value in result.items():
            if key != 'password':  # Don't print password
                print(f"{key}: {value}")
            else:
                print(f"{key}: ********")  # Mask password
    else:
        print("\n❌ Navigation cancelled or failed")