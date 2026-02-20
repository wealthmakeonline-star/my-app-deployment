# db_config.py
"""
Database configuration file for Data Quality Framework
Update these credentials according to your environment
"""

# MySQL Database Configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root', 
    'password': 'root',
    'database': 'dq_checks',
    'port':'3306'
}

# PostgreSQL Database Configuration (if needed)
POSTGRESQL_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'dq_checks',
    'user': 'postgres',
    'password': 'postgres'
}

# SQL Server Database Configuration (if needed)
SQLSERVER_CONFIG = {
    'host': 'localhost',
    'port': '1433',
    'database': 'dq_checks',
    'user': 'sa',
    'password': 'password'
}

# Oracle Database Configuration (if needed)
ORACLE_CONFIG = {
    'host': 'localhost',
    'port': '1521',
    'service_name': 'XE',
    'user': 'system',
    'password': 'manager',
    'encoding': 'UTF-8'
}

# Performance settings for large datasets
PERFORMANCE_CONFIG = {
    'default_batch_size': 50000,  # Process in batches of 50k rows
    'max_memory_mb': 1024,  # Try to stay under 1GB memory usage
    'sample_size_large_dataset': 10000,  # Sample size for preview
    'enable_sampling': True  # Enable sampling for very large datasets
}