# app_config.py 
"""
Application configuration file for Data Quality Framework
"""

import os  

# For your app_config.py or at the top of dq_error_log.py
BATCH_SETTINGS = {
    'max_batch_size': 500,           # Safe for MySQL default config
    'max_value_length': 500,         # Actual/expected values
    'max_description_length': 1000,  # Error descriptions
    'enable_fallback': True,         # Fallback to individual inserts
    'log_progress_interval': 10      # Log progress every 10 batches
}

# For 1 million+ row datasets:
LARGE_DATASET_SETTINGS = {
    'max_batch_size': 100,           # Smaller batches
    'use_chunked_processing': True,  # Process in chunks from disk
    'temp_file_path': 'temp_errors.json',  # Save to file first
}

# Application Settings
APP_SETTINGS = {
    'default_sample_size': 5,
    'max_rows_to_display': 10,
    'log_retention_days': 30,
    'audit_enabled': True,
    'fallback_logging': True,
    'large_dataset_threshold': 50000,  # Consider dataset large above this
    'streaming_batch_size': 10000,  # Process in batches of 10k
    'enable_memory_monitoring': True,
    'max_rows_in_memory': 200000,  # Maximum rows to load in memory
    'use_config_file': os.environ.get('DQ_USE_CONFIG_FILE', 'false').lower() == 'true' 
    }

# Data Quality Thresholds
QUALITY_THRESHOLDS = {
    'excellent_score': 90,
    'good_score': 70,
    'fair_score': 50,
    'null_threshold_percentage': 5,
    'duplicate_threshold_percentage': 2,
    'growth_rate_threshold': 20
}

# File Paths
FILE_PATHS = {
    'log_directory': 'logs',
    'fallback_audit_file': 'dq_audit_fallback.csv',
    'temp_directory': 'temp',
    'large_file_cache': 'temp/large_datasets'
}

# Pattern Definitions
DATA_PATTERNS = {
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'phone': r'^\+?1?\d{9,15}$',
    'date_iso': r'^\d{4}-\d{2}-\d{2}$',
    'date_us': r'^\d{2}/\d{2}/\d{4}$'
}