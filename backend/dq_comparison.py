# dq_comparison.py - UPDATED FOR DUAL MODE (CLI & UI) - NO PRINT STATEMENTS
import pandas as pd
import os,time
import logging
import re
import hashlib
from datetime import datetime
import sys
from dq_unified import select_data_source, load_data_from_source, get_config_file_database_config
from app_config import APP_SETTINGS, QUALITY_THRESHOLDS
from dq_error_log import ErrorLogger
# Add this import
from database_navigator import navigate_database, get_database_hierarchy

# Try to import app_config with better error handling
try:
    from app_config import APP_SETTINGS, QUALITY_THRESHOLDS
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Could not import app_config: {e}")
    logger.warning("Using default settings...")
    # Provide default settings if import fails
    APP_SETTINGS = {
        'audit_enabled': True,
        'fallback_logging': True,
        'use_config_file': False
    }
    QUALITY_THRESHOLDS = {
        'excellent_score': 90,
        'good_score': 70,
        'fair_score': 50
    }

# Import the dual-mode input handler
try:
    from input_handler import init_input_handler, get_input, get_choice, get_multiple_choice, get_input_handler
    HAS_INPUT_HANDLER = True
except ImportError:
    # Fallback for compatibility
    HAS_INPUT_HANDLER = False
    logger = logging.getLogger(__name__)
    logger.warning("Input handler not found. Running in CLI-only mode.")

# Setup logger
logger = logging.getLogger(__name__)

import numpy as np

# ========== ADD THIS NEW FUNCTION AFTER IMPORTS ==========
def load_data_in_chunks(source_type, source_config, max_rows=None, chunk_size=50000):
    """
    Load data in chunks for memory efficiency with large datasets
    Supports CSV, Excel, and Database sources
    """
    logger.info(f"Loading {source_type.upper()} data in chunks of {chunk_size:,} rows")
    
    if source_type == "csv":
        chunks = []
        total_rows = 0
        
        for chunk in pd.read_csv(source_config, chunksize=chunk_size, low_memory=False):
            chunks.append(chunk)
            total_rows += len(chunk)
            
            if max_rows and total_rows >= max_rows:
                # Trim last chunk if needed
                if total_rows > max_rows:
                    excess = total_rows - max_rows
                    chunks[-1] = chunk.iloc[:-excess]
                break
            
            # Log progress for large files
            if total_rows % 200000 == 0:
                logger.info(f"Loaded {total_rows:,} rows...")
        
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
            logger.info(f"Total loaded: {len(df):,} rows in {len(chunks)} chunks")
            return df
        else:
            return pd.DataFrame()
    
    elif source_type == "excel":
        # Excel chunking - use existing handler with limits
        from dq_unified import LargeDatasetHandler
        
        # Get file size
        file_size_mb = os.path.getsize(source_config) / (1024 * 1024)
        
        if file_size_mb > 100:  # Very large Excel file
            logger.warning(f"Large Excel file ({file_size_mb:.1f}MB), loading first {max_rows or 100000} rows")
        
        # Use existing handler with max_rows limit
        result = LargeDatasetHandler.load_large_excel(source_config, max_rows=max_rows or 100000)
        
        if isinstance(result, tuple):
            df, sheet_name = result
        else:
            df = result
        
        return df
    
    elif source_type == "database":
        # Database chunking - use existing handler
        from dq_unified import LargeDatasetHandler
        
        # Set sample size for large tables
        if max_rows and max_rows > 100000:
            logger.info(f"Large database table, limiting to {max_rows:,} rows")
            if 'sample_size' not in source_config:
                source_config['sample_size'] = max_rows
        
        return LargeDatasetHandler.load_large_database(source_config, max_rows=max_rows)
    
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


def filter_trivial_mismatches(mismatch_details):
    """
    Filter out trivial mismatches that don't need to be logged
    """
    filtered = []
    
    for mismatch in mismatch_details:
        skip_mismatch = False
        
        # Check if differences are just NULL vs empty string variations
        for diff in mismatch.get('differences', []):
            source_val = str(diff.get('source_normalized', '')).lower()
            target_val = str(diff.get('target_normalized', '')).lower()
            
            # Consider these as equivalent for error logging
            null_equivalents = ['null', 'nan', 'none', '', 'empty']
            if source_val in null_equivalents and target_val in null_equivalents:
                skip_mismatch = True
                break
            
            # Skip whitespace-only differences
            if source_val.strip() == target_val.strip():
                skip_mismatch = True
                break
            
            # Skip case-only differences
            if source_val.lower() == target_val.lower():
                skip_mismatch = True
                break
        
        if not skip_mismatch:
            filtered.append(mismatch)
    
    logger.info(f"Filtered {len(mismatch_details) - len(filtered)} trivial mismatches")
    return filtered


def calculate_mismatch_significance(difference):
    """Calculate how significant a mismatch is (1-10 scale)"""
    source_val = str(difference.get('source', '')).lower()
    target_val = str(difference.get('target', '')).lower()
    
    # Null/empty variations are low significance
    null_patterns = ['null', 'nan', 'none', '', 'empty']
    if source_val in null_patterns and target_val in null_patterns:
        return 1
    
    # Whitespace/case only differences
    if source_val.strip() == target_val.strip():
        return 2
    
    # Numeric differences (more significant)
    try:
        src_num = float(source_val) if source_val not in null_patterns else 0
        tgt_num = float(target_val) if target_val not in null_patterns else 0
        diff_pct = abs(src_num - tgt_num) / max(abs(src_num), 1) * 100
        
        if diff_pct > 10:  # >10% difference
            return 8
        elif diff_pct > 1:  # >1% difference
            return 5
        else:
            return 3
    except:
        pass
    
    # String differences
    if source_val and target_val:
        # Check if it's a meaningful difference
        if len(source_val) > 10 and len(target_val) > 10:
            # Longer strings with differences are more significant
            return 7
        else:
            return 4
    
    return 3  # Default significance

def get_dynamic_labels(source_info, target_info):
    """
    Get dynamic labels for source and target based on their info
    Returns: (source_label, target_label)
    """
    def extract_label(info_str, default_label="Unknown"):
        if not info_str:
            return default_label
        
        info_str = str(info_str)
        info_lower = info_str.lower()
        
        # Check for CSV
        if 'csv:' in info_lower:
            # Try to extract filename
            if ':' in info_str:
                filename = info_str.split(':', 1)[1].strip()
                if filename:
                    return f"CSV ({filename})"
            return 'CSV'
        
        # Check for Excel
        elif 'excel:' in info_lower:
            # Try to extract filename
            if ':' in info_str:
                parts = info_str.split(':', 1)[1].strip()
                if '(' in parts and ')' in parts:
                    # Has sheet name: "data.xlsx (Sheet1)"
                    return f"Excel ({parts})"
                elif parts:
                    return f"Excel ({parts})"
            return 'Excel'
        
        # Check for Database - ENHANCED!
        elif 'database:' in info_lower:
            # Extract DB type and details
            if ':' in info_str:
                details = info_str.split(':', 1)[1].strip()
                if ' - ' in details:
                    # Format: "mysql - dq_checks.customers"
                    db_type, db_details = details.split(' - ', 1)
                    db_type = db_type.strip().title()  # "mysql" → "MySQL"
                    return f"{db_type} Database"
                elif details:
                    return f"Database ({details})"
            return 'Database'
        
        # Try to extract from string patterns
        else:
            if 'mysql' in info_lower:
                return 'MySQL Database'
            elif 'postgresql' in info_lower or 'postgres' in info_lower:
                return 'PostgreSQL Database'
            elif 'oracle' in info_lower:
                return 'Oracle Database'
            elif 'sqlserver' in info_lower or 'sql server' in info_lower:
                return 'SQL Server Database'
            elif 'sqlite' in info_lower:
                return 'SQLite Database'
            elif 'csv' in info_lower:
                return 'CSV'
            elif 'excel' in info_lower:
                return 'Excel'
            elif 'database' in info_lower or 'db' in info_lower:
                return 'Database'
            else:
                # Can't identify, use default
                return default_label
    
    source_label = extract_label(source_info, default_label="Source")
    target_label = extract_label(target_info, default_label="Target")
    
    # If both labels are the same, add Source/Target prefix
    if source_label == target_label:
        source_label = f"Source {source_label}"
        target_label = f"Target {target_label}"
    
    return source_label, target_label
# ========== SAFE BOOLEAN CHECK FUNCTION ==========
def safe_bool_check(value):
    """
    Safely check if value is truthy, handling numpy arrays and pandas objects
    Use this INSTEAD of regular 'if value:' checks
    """
    # Handle None
    if value is None:
        return False
    
    # Handle numpy arrays
    if hasattr(value, '__len__') and not isinstance(value, (str, bytes)):
        try:
            # For numpy arrays
            if hasattr(value, 'shape') and hasattr(value, 'dtype'):
                return value.size > 0
            # For other array-like objects
            return len(value) > 0
        except:
            return False
    
    # Handle pandas Series/DataFrame
    if hasattr(value, 'empty'):
        return not value.empty
    
    # Handle regular booleans
    try:
        return bool(value)
    except ValueError as e:
        if "The truth value of an array" in str(e):
            return False
        raise

# Update the load_single_comparison_source function (around line 40)
def load_single_comparison_source(source_type_label, ui_data=None):
    """Load a single source for comparison with enhanced UI - DUAL MODE"""
    
    # If UI data provided, use it
    if ui_data and 'source_type' in ui_data:
        source_choice = ui_data['source_type']
        
        # MAP STRING TYPE TO NUMERIC CHOICE FOR COMPATIBILITY
        type_map = {
            'csv': '1',
            'excel': '2',
            'database': '3'
        }
        
        if isinstance(source_choice, str) and source_choice.lower() in type_map:
            source_choice = type_map[source_choice.lower()]
        elif isinstance(source_choice, str) and source_choice.isdigit():
            # Already numeric
            pass
        else:
            # Try to infer from source_file_path
            if 'file_path' in ui_data:
                file_path = ui_data['file_path']
                if isinstance(file_path, str):
                    if file_path.lower().endswith('.csv'):
                        source_choice = '1'
                    elif file_path.lower().endswith(('.xlsx', '.xls')):
                        source_choice = '2'
                    else:
                        source_choice = '1'  # Default to CSV
                else:
                    source_choice = '1'  # Default to CSV
            else:
                source_choice = '1'  # Default to CSV
                
    elif ui_data and source_type_label.lower() + '_source_type' in ui_data:
        # Support dynamic field names
        field_name = source_type_label.lower() + '_source_type'
        source_choice = ui_data[field_name]
        
        # MAP STRING TYPE TO NUMERIC CHOICE
        type_map = {
            'csv': '1',
            'excel': '2',
            'database': '3'
        }
        
        if isinstance(source_choice, str) and source_choice.lower() in type_map:
            source_choice = type_map[source_choice.lower()]
    else:
        # Use input handler for dual mode
        if HAS_INPUT_HANDLER:
            source_choice = get_choice(
                prompt=f"\n📋 SELECT {source_type_label} DATA TYPE:",
                options={
                    '1': '📁 CSV File',
                    '2': '📊 Excel File',
                    '3': '🗄️ Database'
                },
                field_name=f'{source_type_label.lower()}_source_type'
            )
        else:
            # Original CLI code
            logger.info(f"\n📋 SELECT {source_type_label} DATA TYPE:")
            logger.info("-"*40)
            logger.info("1. 📁 CSV File")
            logger.info("2. 📊 Excel File")
            logger.info("3. 🗄️ Database")
            logger.info("-"*40)
            
            source_choice = input(f"Enter {source_type_label} type (1-3): ").strip()
    
    if source_choice == "1":
        return load_csv_source(source_type_label, ui_data)
    elif source_choice == "2":
        return load_excel_source(source_type_label, ui_data)
    elif source_choice == "3":
        return load_database_source(source_type_label, ui_data)
    else:
        logger.error(f"Invalid selection for {source_type_label}: {source_choice}")
        return None, None, None
    
def load_csv_source(source_type_label, ui_data=None):
    """Load CSV source with chunking for large files"""
    
    logger.info(f"DEBUG load_csv_source called for {source_type_label}")
    
    # Get file path - check multiple possible field names
    file_path = None
    
    # Check these field names in order
    possible_fields = [
        'file_path',
        'source_file_path' if source_type_label.upper() == 'SOURCE' else 'target_file_path',
        'target_file_path' if source_type_label.upper() == 'TARGET' else 'source_file_path',
        f'{source_type_label.lower()}_file_path',
        'value'
    ]
    
    for field in possible_fields:
        if ui_data and field in ui_data and ui_data[field]:
            file_path = ui_data[field]
            logger.info(f"DEBUG: Using file_path from '{field}': {file_path}")
            break
    
    # If still no file path, use input handler
    if not file_path and HAS_INPUT_HANDLER:
        file_path = get_input(
            prompt=f"Enter {source_type_label} CSV file path",
            field_name=f'{source_type_label.lower()}_file_path',
            required=True
        )
    elif not file_path:
        file_path = input(f"Enter {source_type_label} CSV file path: ").strip()
    
    if not file_path or not os.path.exists(file_path):
        logger.error(f"CSV file not found: {file_path}")
        return None, None, None
    
    try:
        # Check file size
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        if file_size_mb > 50:  # For files > 50MB, use chunking
            logger.info(f"Large CSV file detected ({file_size_mb:.1f}MB), loading in chunks...")
            
            # Estimate row count
            with open(file_path, 'r', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f) - 1  # Subtract header
            
            logger.info(f"CSV has approximately {total_rows:,} rows")
            
            # Use chunking for large files
            max_rows_to_load = min(total_rows, 500000)  # Max 5 lakh rows
            df = load_data_in_chunks("csv", file_path, max_rows=max_rows_to_load)
        else:
            # Small file, load normally
            df = pd.read_csv(file_path)
        
        source_info = f"CSV: {os.path.basename(file_path)}"
        source_file = os.path.basename(file_path)
        
        logger.info(f"{source_type_label} CSV loaded: {len(df):,} rows, {len(df.columns)} columns")
        return df, source_info, source_file
        
    except Exception as e:
        logger.error(f"Error loading {source_type_label} CSV: {e}")
        return None, None, None

def load_excel_source(source_type_label, ui_data=None):
    """Load Excel source with chunking for large files"""
    
    logger.info(f"DEBUG load_excel_source called for {source_type_label}")
    logger.info(f"DEBUG ui_data: {ui_data}")
    logger.info(f"DEBUG ui_data type: {type(ui_data)}")

    # Get file path from UI data or input
    if ui_data and 'file_path' in ui_data:
        file_path = ui_data['file_path']
        logger.info(f"DEBUG Found file_path in ui_data: {file_path}")
    elif ui_data and 'source_file_path' in ui_data:
        # Support both naming conventions
        file_path = ui_data['source_file_path']
        logger.info(f"DEBUG Found source_file_path in ui_data: {file_path}")
    elif ui_data and source_type_label.lower() + '_file_path' in ui_data:
        # Support dynamic field names
        field_name = source_type_label.lower() + '_file_path'
        file_path = ui_data[field_name]
        logger.info(f"DEBUG Found {field_name} in ui_data: {file_path}")
    elif HAS_INPUT_HANDLER:
        file_path = get_input(
            prompt=f"Enter {source_type_label} Excel file path",
            field_name=f'{source_type_label.lower()}_file_path',
            required=True
        )
    else:
        file_path = input(f"Enter {source_type_label} Excel file path: ").strip()
    
    # DEBUG: Log the file path being used
    logger.debug(f"Loading Excel file from path: {file_path}")
    
    if not file_path or not os.path.exists(file_path):
        logger.error(f"Excel file not found: {file_path}")
        logger.debug(f"Current working directory: {os.getcwd()}")
        logger.debug(f"File exists: {os.path.exists(file_path) if file_path else 'No path provided'}")
        return None, None, None
    
    try:
        # Check file size
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        # Get sheet name from UI data or input
        sheet_name = None
        if ui_data and 'sheet_name' in ui_data:
            sheet_name = ui_data['sheet_name']
            logger.debug(f"Using sheet name from UI: {sheet_name}")
        elif ui_data and 'source_sheet_name' in ui_data:
            sheet_name = ui_data['source_sheet_name']
            logger.debug(f"Using source_sheet_name from UI: {sheet_name}")
        elif ui_data and source_type_label.lower() + '_sheet_name' in ui_data:
            field_name = source_type_label.lower() + '_sheet_name'
            sheet_name = ui_data[field_name]
            logger.debug(f"Using dynamic sheet name from UI: {sheet_name}")
        
        if file_size_mb > 20:  # For Excel files > 20MB, use optimized loading
            logger.info(f"Large Excel file detected ({file_size_mb:.1f}MB), loading with optimizations...")
            
            # Load with chunking
            df = load_data_in_chunks("excel", file_path, max_rows=300000)  # Max 3 lakh rows
            
            if sheet_name:
                logger.info(f"Note: Sheet '{sheet_name}' selected but chunked loading may use different sheet")
        else:
            # Small file, load normally
            excel_file = pd.ExcelFile(file_path)
            
            if sheet_name is None:
                if len(excel_file.sheet_names) > 1:
                    logger.info(f"📑 Available sheets: {excel_file.sheet_names}")
                    
                    if HAS_INPUT_HANDLER:
                        sheet_name = get_input(
                            prompt=f"Enter sheet name for {source_type_label} (or press Enter for first sheet)",
                            field_name=f'{source_type_label.lower()}_sheet_name',
                            default=excel_file.sheet_names[0],
                            required=False
                        ).strip()
                        if not sheet_name:
                            sheet_name = excel_file.sheet_names[0] if excel_file.sheet_names else 'Sheet1'
                            logger.debug(f"Using first sheet: {sheet_name}")
                    else:
                        sheet_name = input(f"Enter sheet name for {source_type_label} (or press Enter for first sheet): ").strip()
                        if not sheet_name:
                            sheet_name = excel_file.sheet_names[0] if excel_file.sheet_names else 'Sheet1'
                            logger.debug(f"Using first sheet: {sheet_name}")
                else:
                    sheet_name = excel_file.sheet_names[0] if excel_file.sheet_names else 'Sheet1'
                    logger.debug(f"Using first sheet: {sheet_name}")
            
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        source_info = f"Excel: {os.path.basename(file_path)} ({sheet_name})"
        source_file = os.path.basename(file_path)
        
        logger.info(f"{source_type_label} Excel loaded: {len(df):,} rows, {len(df.columns)} columns")
        return df, source_info, source_file
        
    except Exception as e:
        logger.error(f"Error loading {source_type_label} Excel: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None, None, None

def load_database_source(source_type_label, ui_data=None):
    """Load database source with dynamic input - DUAL MODE"""
    logger.info(f"\n🗄️ {source_type_label} DATABASE CONFIGURATION")
    logger.info("-"*40)
    
    # ===== FIX: Handle nested UI data structure =====
    # If ui_data is from comparison (nested source/target), extract relevant part
    actual_ui_data = ui_data
    
    # For comparison mode, check if we have nested structure
    if ui_data and isinstance(ui_data, dict):
        # Check if this is the main comparison UI data (has source/target keys)
        if 'source' in ui_data or 'target' in ui_data:
            # We're in comparison mode, need to extract source/target specific data
            if source_type_label.upper() == 'SOURCE' and 'source' in ui_data:
                actual_ui_data = ui_data['source']
                logger.debug(f"Extracted source-specific UI data")
            elif source_type_label.upper() == 'TARGET' and 'target' in ui_data:
                actual_ui_data = ui_data['target']
                logger.debug(f"Extracted target-specific UI data")
    
    # Now use actual_ui_data for processing
    db_config = None
    
    if actual_ui_data and HAS_INPUT_HANDLER:
        # UI mode - check what information we have
        init_input_handler(mode='ui', data=actual_ui_data)
        
        # Check if UI provides complete db_config (skip navigation)
        if 'db_config' in actual_ui_data and actual_ui_data['db_config']:
            db_config = actual_ui_data['db_config']
            logger.info(f"UI mode: Using provided database configuration for {source_type_label}")
        
        # Check if UI wants to use navigator
        elif actual_ui_data.get('use_navigator') == True or actual_ui_data.get('navigation_mode') == True:
            logger.info(f"UI mode: Using database navigator for {source_type_label}")
            db_config = get_database_config_for_comparison(source_type_label, actual_ui_data)
            
        # Check if UI provides individual connection fields
        elif any(key.endswith('_db_type') for key in actual_ui_data.keys()):
            logger.info(f"UI mode: Processing individual fields for {source_type_label}")
            db_config = get_database_config_for_comparison(source_type_label, actual_ui_data)
            
        else:
            # Fallback: Use normal flow
            db_config = get_database_config_for_comparison(source_type_label, actual_ui_data)
    
    else:
        # CLI mode - use existing flow
        db_config = get_database_config_for_comparison(source_type_label, actual_ui_data)
    
    if not db_config:
        logger.error(f"No database configuration obtained for {source_type_label}")
        return None, None, None
    
    try:
        # Load data using the existing load_data_from_source function
        from dq_unified import load_data_from_source
        
        # NO CONVERSION NEEDED - db_config already has 'postgresql' not 'postgres'
        df = load_data_from_source("database", db_config)
        
        if df is None or df.empty:
            logger.error(f"No data loaded from {source_type_label} database")
            return None, None, None
        
        # If database has CustomerID column, we should note it but it won't be used for comparison
        if 'CustomerID' in df.columns or 'customerid' in [col.lower() for col in df.columns]:
            logger.info(f"⚠️  Note: Database has CustomerID column which will be excluded from comparison")
        
        source_info = f"Database: {db_config['type']} - {db_config['database']}.{db_config['table']}"
        source_file = f"{db_config['type']}_{db_config['table']}"
        
        logger.info(f"{source_type_label} database loaded: {len(df):,} rows, {len(df.columns)} columns")
        return df, source_info, source_file
    except Exception as e:
        logger.error(f"Error loading {source_type_label} database: {e}")
        return None, None, None

def get_database_config_for_comparison(source_type_label, ui_data=None):
    """Get database configuration for comparison - DUAL MODE"""
    
    # ===== FIX: Handle both flat and nested UI data =====
    actual_ui_data = ui_data
    
    # If ui_data is None or empty, just proceed
    if ui_data and isinstance(ui_data, dict):
        # Check if we should use source/target specific data
        if source_type_label.upper() == 'SOURCE' and 'source' in ui_data:
            actual_ui_data = ui_data['source']
            logger.debug(f"Using source-specific UI data")
        elif source_type_label.upper() == 'TARGET' and 'target' in ui_data:
            actual_ui_data = ui_data['target']
            logger.debug(f"Using target-specific UI data")
    
    # Get database type from UI data or input
    if actual_ui_data and 'db_type' in actual_ui_data:
        db_type = actual_ui_data['db_type']
        
        # FIX: Convert string db_type to numeric code
        db_type_map = {
            'postgresql': '1',
            'postgres': '1',
            'mysql': '2',
            'oracle': '3',
            'sqlserver': '4',
            'sql server': '4',
            'sqlite': '5'
        }
        
        # If it's a string, convert to numeric
        if isinstance(db_type, str) and db_type.lower() in db_type_map:
            db_type = db_type_map[db_type.lower()]
            logger.info(f"Converted db_type to numeric code: {db_type}")
    elif HAS_INPUT_HANDLER:
        db_type = get_choice(
            prompt=f"\n📊 SELECT {source_type_label} DATABASE TYPE:",
            options={
                '1': 'PostgreSQL',
                '2': 'MySQL',
                '3': 'Oracle',
                '4': 'SQL Server',
                '5': 'SQLite'
            },
            field_name=f'{source_type_label.lower()}_db_type'
        )
    else:
        # Original CLI code
        logger.info(f"\n📊 SELECT {source_type_label} DATABASE TYPE:")
        logger.info("1. PostgreSQL")
        logger.info("2. MySQL")
        logger.info("3. Oracle")
        logger.info("4. SQL Server")
        logger.info("5. SQLite")
        
        db_type = input(f"Enter {source_type_label} database type (1-5): ").strip()
    
    # Map choice to database type - KEEP 'postgresql' for dq_unified compatibility
    db_type_map = {
        '1': 'postgresql',  # KEEP as 'postgresql'
        '2': 'mysql',
        '3': 'oracle',
        '4': 'sqlserver',
        '5': 'sqlite'
    }
    
    if db_type not in db_type_map:
        logger.error(f"Invalid database type for {source_type_label}: {db_type}")
        return None
    
    db_type_code = db_type_map[db_type]
    logger.info(f"Selected database type code: {db_type_code}")
    
    # CHECK ENVIRONMENT VARIABLE FIRST
    use_config_mode = os.environ.get('DQ_USE_CONFIG_FILE', '').lower() == 'true' or APP_SETTINGS.get('use_config_file', False)
    skip_connection_prompt = os.environ.get('DQ_SKIP_CONNECTION_PROMPT', '').lower() == 'true'
    
    if use_config_mode:
        logger.info(f"\n🔧 USING CONFIG FILE MODE")
        return get_config_file_database_config(db_type)
    
    # If connection prompt is skipped, go directly to dynamic input
    if skip_connection_prompt:
        logger.info(f"\n🔧 {source_type_label} DATABASE CONNECTION DETAILS:")
        from dq_unified import get_dynamic_database_config
        return get_dynamic_database_config(db_type)
    
    # ===== NEW: ADD DATABASE NAVIGATOR OPTION =====
    # Otherwise, ask for connection method
    if HAS_INPUT_HANDLER:
        connection_choice = get_choice(
            prompt=f"\n🔧 {source_type_label} CONNECTION OPTIONS:",
            options={
                '1': 'Use default configuration from config file',
                '2': 'Enter connection details manually',
                '3': 'Browse and select database/schema/table'  # NEW OPTION
            },
            field_name=f'{source_type_label.lower()}_connection_choice'
        )
    else:
        logger.info(f"\n🔧 {source_type_label} CONNECTION OPTIONS:")
        logger.info("1. Use default configuration from config file")
        logger.info("2. Enter connection details manually")
        logger.info("3. Browse and select database/schema/table")  # NEW OPTION
        
        connection_choice = input(f"Enter choice for {source_type_label} (1-3): ").strip()
    
    if connection_choice == "1":
        return get_config_file_database_config(db_type)
    elif connection_choice == "3":  # NEW: Browse and select
        logger.info(f"User selected Browse and Select mode for {source_type_label}")
        
        # Import database navigator
        from database_navigator import navigate_database
        
        # CLI mode display
        if not actual_ui_data or not HAS_INPUT_HANDLER or get_input_handler().mode == 'cli':
            print(f"\n{'='*60}")
            print(f"🌐 {source_type_label} DATABASE NAVIGATOR")
            print("="*60)
            print("You will now browse the database hierarchy:")
            print("  1. Select database type")
            print("  2. Enter connection details")
            print("  3. Browse and select database")
            print("  4. Browse and select schema")
            print("  5. Browse and select table")
            print("="*60)
        
        # Determine mode
        mode = 'ui' if actual_ui_data and HAS_INPUT_HANDLER else 'cli'
        
        # Prepare UI data for navigator if in UI mode
        navigator_ui_data = None
        if mode == 'ui' and actual_ui_data:
            navigator_ui_data = actual_ui_data.copy()  # Pass all UI data to navigator
            
            # Ensure db_type is in correct format for navigator
            if f'{source_type_label.lower()}_db_type' in navigator_ui_data:
                # Convert numeric choice to string type
                ui_db_choice = navigator_ui_data[f'{source_type_label.lower()}_db_type']
                if ui_db_choice in ['1', '2', '3', '4', '5']:
                    navigator_ui_data['db_type'] = db_type_map.get(ui_db_choice, 'mysql')
        
        # FIX: Convert 'postgresql' to 'postgres' for database_navigator.py
        navigator_db_type = db_type_code
        if navigator_db_type == 'postgresql':
            navigator_db_type = 'postgres'  # Convert for database_navigator.py
            logger.info(f"Converted 'postgresql' to 'postgres' for database_navigator compatibility")
        
        # Use the database navigator
        result = navigate_database(
            mode=mode,
            ui_data=navigator_ui_data,
            db_type=navigator_db_type  # Pass the CONVERTED database type
        )
        
        if not result:
            logger.warning(f"Database navigation cancelled for {source_type_label}")
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
            'password': result.get('password', ''),
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
        
        # FIX: Convert back to 'postgresql' for dq_unified compatibility
        if db_config['type'] == 'postgres':
            db_config['type'] = 'postgresql'
            logger.info(f"Converted back to 'postgresql' for dq_unified compatibility")
        
        logger.info(f"Selected via navigation for {source_type_label}: {db_config['database']}.{db_config['schema']}.{db_config['table']}")
        return db_config
    else:
        # Call the dynamic config function from dq_unified
        from dq_unified import get_dynamic_database_config
        return get_dynamic_database_config(db_type)

def load_comparison_sources(ui_data=None):
    """Load source and target from any combination - DUAL MODE"""
    logger.info("LOADING SOURCE AND TARGET DATA")
    
    # Initialize input handler if UI data provided
    if ui_data and HAS_INPUT_HANDLER:
        init_input_handler(mode='ui', data=ui_data)
    
    logger.info("\n" + "="*50)
    logger.info("🔗 SOURCE DATA CONFIGURATION")
    logger.info("="*50)
    
    # Get source data from UI or interactive
    source_ui_data = None
    if ui_data and 'source' in ui_data:
        source_ui_data = ui_data['source']
    
    source_df, source_info, source_file = load_single_comparison_source("SOURCE", source_ui_data)
    
    logger.info("\n" + "="*50)
    logger.info("🔗 TARGET DATA CONFIGURATION")
    logger.info("="*50)
    
    # Get target data from UI or interactive
    target_ui_data = None
    if ui_data and 'target' in ui_data:
        target_ui_data = ui_data['target']
    
    target_df, target_info, target_file = load_single_comparison_source("TARGET", target_ui_data)
    
    return source_df, target_df, source_info, target_info, source_file, target_file

def select_columns_for_comparison(source_df, target_df, ui_data=None):
    """Let user select specific columns to compare with case-insensitive matching - DUAL MODE"""
    logger.info("SELECTING COLUMNS FOR COMPARISON")
    
    # Convert all column names to lowercase for case-insensitive comparison
    source_cols_lower = [col.lower() for col in source_df.columns]
    target_cols_lower = [col.lower() for col in target_df.columns]
    
    # Find common columns (case-insensitive)
    common_cols_lower = list(set(source_cols_lower).intersection(set(target_cols_lower)))
    
    # DEBUG: Show what columns are being considered
    logger.info(f"Source columns (original): {list(source_df.columns)}")
    logger.info(f"Target columns (original): {list(target_df.columns)}")
    logger.info(f"Common columns lowercased: {common_cols_lower}")
    
    # Map back to original column names for display
    common_cols = []
    for col_lower in common_cols_lower:
        # Find original column name from source
        source_original = None
        for col in source_df.columns:
            if col.lower() == col_lower:
                source_original = col
                break
        
        # Find original column name from target  
        target_original = None
        for col in target_df.columns:
            if col.lower() == col_lower:
                target_original = col
                break
        
        if source_original and target_original:
            common_cols.append({
                'lowercase': col_lower,
                'source_original': source_original,
                'target_original': target_original
            })
    
    # logger.info(f"Found {len(common_cols)} common columns between source and target (case-insensitive, excluding CustomerID)")
    logger.info(f"Found {len(common_cols)} common columns between source and target (case-insensitive)")
    
    if common_cols is None or not safe_bool_check(common_cols):
        logger.warning("No common columns found between source and target datasets")
        return [], []
    
    # Get column selection from UI data or interactive
    if ui_data and 'selected_columns' in ui_data:
        selection = ui_data['selected_columns']
        if isinstance(selection, list):
            # Convert list to comma-separated string for processing
            selection = ','.join([str(s) for s in selection])
    elif HAS_INPUT_HANDLER:
        # Display columns with numbers
        logger.info("\n📋 Common columns available for comparison:")
        for i, col_info in enumerate(common_cols, 1):
            logger.info(f"  {i}. {col_info['source_original']} (Source) ↔ {col_info['target_original']} (Target)")
        
        selection = get_input(
            prompt=f"\nSelect columns to compare (e.g.: 1,3,5 or 'all' for all {len(common_cols)} columns)",
            field_name='selected_columns',
            default='all'
        )
    else:
        # Original CLI code
        logger.info("\n📋 Common columns available for comparison:")
        for i, col_info in enumerate(common_cols, 1):
            logger.info(f"  {i}. {col_info['source_original']} (Source) ↔ {col_info['target_original']} (Target)")
        
        selection = input(f"\nSelect columns to compare (e.g.: 1,3,5 or 'all' for all {len(common_cols)} columns): ").strip()
    
    logger.info(f"User column selection input: {selection}")
    
    if selection.lower() == 'all':
        logger.info("User selected ALL common columns for comparison")
        selected_source_cols = [col_info['source_original'] for col_info in common_cols]
        return selected_source_cols, common_cols
    else:
        try:
            selected_indices = [int(x.strip()) - 1 for x in selection.split(',')]
            selected_source_cols = []
            selected_common_cols = []
            
            for i in selected_indices:
                if 0 <= i < len(common_cols):
                    selected_source_cols.append(common_cols[i]['source_original'])
                    selected_common_cols.append(common_cols[i])
            
            if selected_source_cols is None or not safe_bool_check(selected_source_cols):
                logger.warning("No valid columns selected by user, using all common columns")
                selected_source_cols = [col_info['source_original'] for col_info in common_cols]
                selected_common_cols = common_cols
            
            logger.info(f"User selected {len(selected_source_cols)} columns: {selected_source_cols}")
            return selected_source_cols, selected_common_cols
            
        except Exception as e:
            logger.warning(f"Invalid column selection input, using all common columns. Error: {e}")
            selected_source_cols = [col_info['source_original'] for col_info in common_cols]
            return selected_source_cols, common_cols

def get_target_column_name(source_column, common_cols):
    """Get the corresponding target column name for a source column"""
    for col_info in common_cols:
        if col_info['source_original'] == source_column:
            return col_info['target_original']
    return source_column  # Fallback to same name

# def normalize_date(value, column_name=""):
    """Normalize date values to YYYY-MM-DD format with PROPER timestamp handling"""
    # Handle NULL/NaN consistently
    if pd.isna(value) or value is None:
        return "NULL"
    
    try:
        # Handle pandas Timestamp from database
        if isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d')
        
        # Handle Python datetime from database
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d')
        
        # Handle numpy datetime64 from database
        if hasattr(value, 'dtype') and 'datetime64' in str(value.dtype):
            try:
                return pd.Timestamp(value).strftime('%Y-%m-%d')
            except:
                pass
        
        # ===== FIX: Handle ALL integer/numeric timestamps =====
        # Check if it's a numeric value (from database timestamp)
        if isinstance(value, (int, float, np.integer, np.int64, np.int32, np.float64)):
            # Convert to float for easier handling
            num_value = float(value)
            
            # 1. Try nanoseconds conversion first (1.6e18 range)
            if num_value > 1e17:  # Nanoseconds
                try:
                    # Convert nanoseconds to seconds
                    seconds = num_value / 1_000_000_000
                    dt = datetime.fromtimestamp(seconds)
                    return dt.strftime('%Y-%m-%d')
                except:
                    pass
            
            # 2. Try milliseconds conversion (1.6e12 range)
            elif num_value > 1e11:  # Milliseconds
                try:
                    seconds = num_value / 1000
                    dt = datetime.fromtimestamp(seconds)
                    return dt.strftime('%Y-%m-%d')
                except:
                    pass
            
            # 3. Try seconds conversion (1.6e9 range)
            elif num_value > 1e8:  # Seconds
                try:
                    dt = datetime.fromtimestamp(num_value)
                    return dt.strftime('%Y-%m-%d')
                except:
                    pass
        
        # If not a numeric timestamp, treat as string
        str_value = str(value).strip()
        
        if not str_value or str_value.lower() in ['nan', 'none', 'null', 'nat', '']:
            return "NULL"
        
        # If it's already a datetime string in ISO format
        if re.match(r'^\d{4}-\d{2}-\d{2}$', str_value):
            return str_value
        
        # Remove time portion if present
        if ' ' in str_value:
            str_value = str_value.split(' ')[0]
        if 'T' in str_value:  # Handle ISO format with T
            str_value = str_value.split('T')[0]
        
        # Try various date formats
        date_formats = [
            '%Y-%m-%d',          # 2023-01-15
            '%d-%m-%Y',          # 15-01-2023
            '%m/%d/%Y',          # 01/15/2023
            '%d/%m/%Y',          # 15/01/2023
            '%d.%m.%Y',          # 15.01.2023
            '%Y/%m/%d',          # 2023/01/15
            '%d %b %Y',          # 15 Jan 2023
            '%d %B %Y',          # 15 January 2023
            '%b %d, %Y',         # Jan 15, 2023
            '%B %d, %Y',         # January 15, 2023
            '%Y%m%d',            # 20230115
            '%d%m%Y',            # 15012023
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(str_value, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        try:
            parsed = pd.to_datetime(str_value, errors='coerce', dayfirst=True)
            if not pd.isna(parsed):
                return parsed.strftime('%Y-%m-%d')
        except:
            pass
        # If all parsing fails, return the original value
        return str_value
    except Exception as e:
        logger.debug(f"Error normalizing date {value}: {e}")
        return str(value) if not pd.isna(value) else "NULL"

def normalize_date(value, column_name=""):
    """
    PERFECT DATE NORMALIZATION - Handles ALL database vs file scenarios
    - Database timestamps (milliseconds, nanoseconds, seconds)
    - Database datetime objects, pandas Timestamps
    - Excel serial dates, CSV string dates
    - MySQL, PostgreSQL, Oracle, SQL Server, SQLite
    """
    # Handle NULL/NaN consistently
    if pd.isna(value) or value is None:
        return "NULL"
    
    try:
        # ===== UNIVERSAL SOLUTION: pandas handles EVERYTHING =====
        ts = pd.to_datetime(value, errors='coerce')
        
        if not pd.isna(ts):
            return ts.strftime('%Y-%m-%d')
        
        # ===== FALLBACK =====
        str_value = str(value).strip()
        if not str_value or str_value.lower() in ['nan', 'none', 'null', 'nat', '']:
            return "NULL"
        
        # Extract date portion if it contains time
        if ' ' in str_value:
            str_value = str_value.split(' ')[0]
        if 'T' in str_value:
            str_value = str_value.split('T')[0]
            
        return str_value
        
    except Exception as e:
        logger.debug(f"Date normalization error: {e}")
        return str(value) if not pd.isna(value) else "NULL"    
def normalize_numeric(value, column_name=""):
    """Normalize numeric values with proper NULL handling"""
    if pd.isna(value) or value is None:
        return "NULL"
    
    try:
        # Handle numpy/pandas numeric types from database
        if hasattr(value, 'dtype'):
            # Convert to Python native type
            value = float(value) if hasattr(value, '__float__') else str(value)
        
        str_value = str(value).strip()
        
        if not str_value or str_value.lower() in ['nan', 'none', 'null', '']:
            return "NULL"
        
        # Handle numpy float/integer types
        if 'numpy' in str(type(value)):
            try:
                # Convert numpy types to standard Python types
                import numpy as np
                if isinstance(value, (np.integer, np.floating)):
                    value = value.item()  # Convert to Python int/float
                    str_value = str(value)
            except:
                pass
        
        # Remove currency symbols, commas, and spaces
        cleaned = re.sub(r'[$,₹€£¥,\s]', '', str_value)
        
        # Handle scientific notation
        if 'e' in cleaned.lower() or 'E' in cleaned:
            try:
                float_val = float(cleaned)
                # Format without scientific notation
                return f"{float_val:.10f}".rstrip('0').rstrip('.')
            except:
                return str_value
        
        # Try to convert to float
        try:
            float_val = float(cleaned)
            
            # For integers, remove decimal part if .0
            if float_val.is_integer():
                return str(int(float_val))
            else:
                # Keep reasonable decimal places
                formatted = f"{float_val:.10f}".rstrip('0').rstrip('.')
                return formatted if formatted else "0"
        except (ValueError, TypeError):
            # If not numeric, check if it's a string representation of number
            if re.match(r'^-?\d+$', cleaned):  # Integer
                return cleaned
            elif re.match(r'^-?\d+\.\d+$', cleaned):  # Decimal
                return cleaned
            else:
                return str_value
    except Exception as e:
        logger.debug(f"Error normalizing numeric {value}: {e}")
        return str(value) if not pd.isna(value) else "NULL"

def normalize_phone_number(value, column_name=""):
    """SPECIALIZED phone number normalization with zero-padding handling"""
    if pd.isna(value) or value is None:
        return "NULL"
    
    try:
        # Handle numeric phone numbers from database
        if isinstance(value, (int, float)):
            value = str(int(value)) if isinstance(value, float) and value.is_integer() else str(value)
            
        str_value = str(value).strip()
        
        if not str_value or str_value.lower() in ['nan', 'none', 'null', '']:
            return "NULL"
        
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', str_value)
        
        if not digits_only:
            return "NULL"
        
        # Store original for comparison debugging
        original = digits_only
        
        # For COMPARISON: Remove ALL leading zeros
        normalized = digits_only.lstrip('0')
        if not normalized:  # If after stripping zeros we get empty string
            normalized = "0"
        
        # DEBUG: Log if we removed zeros
        if original != normalized and len(original) > 1 and original[0] == '0':
            logger.debug(f"Phone number zero-padding detected: '{original}' → '{normalized}'")
        
        return normalized
    except Exception as e:
        logger.debug(f"Error normalizing phone {value}: {e}")
        return str(value) if not pd.isna(value) else "NULL"
    
def normalize_string(value):
    """Normalize string values with PROPER NULL handling"""
    if pd.isna(value) or value is None:
        return "NULL"
    
    try:
        str_value = str(value).strip()
        
        if not str_value or str_value.lower() in ['nan', 'none', 'null']:
            return "NULL"
        
        # Remove extra whitespace and convert to lowercase for comparison
        normalized = re.sub(r'\s+', ' ', str_value).strip()
        
        # Return empty string as "EMPTY" to distinguish from NULL
        if normalized == '':
            return "EMPTY"
        
        return normalized.lower()
    except Exception as e:
        logger.debug(f"Error normalizing string {value}: {e}")
        return "NULL"

# def smart_normalize_value(value, column_name=""):
#     """
#     SMART normalization that handles various data formats with IMPROVED NULL handling
#     """
#     # Handle NULL/NaN consistently
#     if pd.isna(value) or value is None:
#         return "NULL"
    
#     try:
#         str_value = str(value).strip()

#         # Check for empty/null strings
#         if not str_value or str_value.lower() in ['nan', 'none', 'null', 'nat', '']:
#             return "NULL"  # Consistent
        
#         # Convert column name to lowercase for pattern matching (ALREADY IN YOUR CODE)
#         column_lower = column_name.lower() if column_name else ""
        
#         # Debug logging for first few rows (temporary)
#         global _debug_counter
#         if '_debug_counter' not in globals():
#             _debug_counter = 0
        
#         if _debug_counter < 5 and column_lower and any(x in column_lower for x in ['phone', 'date', 'salary']):
#             logger.debug(f"DEBUG NORM: Col={column_name}, Value={value}, Type={type(value)}")
#             _debug_counter += 1

#         str_value = str(value).strip()
        
#         if not str_value or str_value.lower() in ['nan', 'none', 'null', 'nat', '']:
#             return "NULL"
        
#         column_lower = column_name.lower() if column_name else ""
        
#         # 1. DATE HANDLING - FIXED
#         if any(date_keyword in column_lower for date_keyword in ['date', 'time', 'dob', 'joined', 'created', 'updated']):
#             normalized_date = normalize_date(value, column_name)
#             return normalized_date
        
#         # 2. PHONE NUMBER HANDLING - FIXED WITH ZERO-PADDING NORMALIZATION
#         if any(phone_keyword in column_lower for phone_keyword in ['phone', 'mobile', 'tel', 'contact']):
#             normalized_phone = normalize_phone_number(str_value, column_name)
#             return normalized_phone
        
#         # 3. NUMERIC HANDLING - FIXED (including salary)
#         if any(num_keyword in column_lower for num_keyword in ['salary', 'amount', 'price', 'cost', 'value', 'number', 'id', 'code', 'customerid']):
#             normalized_num = normalize_numeric(str_value, column_name)
#             return normalized_num
        
#         # 4. EMAIL HANDLING
#         if 'email' in column_lower or 'mail' in column_lower:
#             # Normalize email - lowercase and trim
#             email_parts = str_value.lower().strip().split('@')
#             if len(email_parts) == 2:
#                 return f"{email_parts[0].strip()}@{email_parts[1].strip()}"
        
#         # 5. DEFAULT STRING NORMALIZATION - FIXED
#         return normalize_string(str_value)
        
#     except Exception as e:
#         logger.debug(f"Error in smart_normalize_value for {value} in column {column_name}: {e}")
#         return str(value) if not pd.isna(value) else "NULL"

def smart_normalize_value(value, column_name=""):
    """
    TOTALLY GENERIC normalization - NO industry assumptions
    Works for ANY data type from ANY industry (finance, healthcare, retail, etc.)
    Detects data type based on CONTENT only, not column names
    """
    # Handle NULL/NaN consistently
    if pd.isna(value) or value is None:
        return "NULL"
    
    try:
        # Convert to string for processing
        str_value = str(value).strip()
        
        # Check for empty/null strings
        if not str_value or str_value.lower() in ['nan', 'none', 'null', 'nat', '']:
            return "NULL"
        
        # ===== 1. FIRST: Check if it's EMPTY after stripping =====
        if str_value == '':
            return "EMPTY"
        
        # ===== 2. Try to detect DATE/TIME patterns (based on format only) =====
        # Common date patterns (YYYY-MM-DD, DD/MM/YYYY, etc.)
        date_patterns = [
            r'^\d{4}-\d{1,2}-\d{1,2}$',                    # 2023-12-31
            r'^\d{4}-\d{1,2}-\d{1,2}[ T]\d{1,2}:\d{2}',    # 2023-12-31 14:30
            r'^\d{4}/\d{1,2}/\d{1,2}$',                    # 2023/12/31
            r'^\d{1,2}-\d{1,2}-\d{4}$',                    # 31-12-2023
            r'^\d{1,2}/\d{1,2}/\d{4}$',                    # 31/12/2023
            r'^\d{4}\d{2}\d{2}$',                          # 20231231
            r'^\d{1,2}[./]\d{1,2}[./]\d{4}$',              # 31.12.2023
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, str_value, re.IGNORECASE):
                try:
                    parsed = pd.to_datetime(str_value, errors='coerce', dayfirst=True)
                    
                    if not pd.isna(parsed):
                        return parsed.strftime('%Y-%m-%d')
                except:
                    pass
        
        # ===== 3. Try NUMERIC conversion =====
        try:
            # Remove common non-numeric characters (commas, currency symbols, spaces)
            clean_num = re.sub(r'[$,₹€£¥\s]', '', str_value)
            
            # Handle percentages
            if clean_num.endswith('%'):
                clean_num = clean_num[:-1]
                is_percent = True
            else:
                is_percent = False
            
            # Try to convert to number
            num_val = pd.to_numeric(clean_num, errors='coerce')
            
            if not pd.isna(num_val):
                # Handle leading zeros for integer-like numbers
                if num_val.is_integer() and not (-1 < num_val < 1):
                    # Check if original had leading zeros
                    if re.match(r'^[+-]?0[0-9]+$', str_value):
                        # Remove leading zeros for comparison
                        return str(int(num_val))
                
                # Format number consistently
                if is_percent:
                    return f"{num_val}%"
                elif num_val.is_integer():
                    return str(int(num_val))
                else:
                    # Keep reasonable precision
                    return f"{num_val:.10f}".rstrip('0').rstrip('.')
        except:
            pass
        
        # ===== 4. Check for BOOLEAN values =====
        bool_map = {
            'true': 'TRUE', 'false': 'FALSE',
            'yes': 'TRUE', 'no': 'FALSE',
            '1': 'TRUE', '0': 'FALSE',
            't': 'TRUE', 'f': 'FALSE',
            'y': 'TRUE', 'n': 'FALSE'
        }
        
        lower_val = str_value.lower()
        if lower_val in bool_map:
            return bool_map[lower_val]
        
        # ===== 5. Check for EMAIL pattern (generic, not column-specific) =====
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(email_pattern, str_value, re.IGNORECASE):
            return str_value.lower().strip()
        
        # ===== 6. Check for pure digits (could be IDs, codes, phones - generic) =====
        if re.match(r'^[\d\s\-+()]+$', str_value):
            # Remove all non-digit characters for comparison
            digits_only = re.sub(r'\D', '', str_value)
            if digits_only:
                # Remove leading zeros for better comparison (generic for all IDs/codes)
                normalized = digits_only.lstrip('0')
                return normalized if normalized else '0'
        
        # ===== 7. DEFAULT: String normalization =====
        # Remove extra whitespace, normalize case
        normalized = re.sub(r'\s+', ' ', str_value).strip()
        
        # Convert to lowercase for case-insensitive comparison
        normalized = normalized.lower()
        
        # Return normalized string
        return normalized
        
    except Exception as e:
        logger.debug(f"Error normalizing value '{value}' (type: {type(value).__name__}): {e}")
        # Fallback: return original as string
        return str(value) if not pd.isna(value) else "NULL"

def compare_row_counts(source_df, target_df, error_logger, session_id, source_info, target_info):
    """FIRST CHECK: Compare row counts"""
    logger.info("FIRST CHECK: ROW COUNT COMPARISON")
    
    source_label, target_label = get_dynamic_labels(source_info, target_info)

    source_count = len(source_df)
    target_count = len(target_df)
    
    logger.info(f"Source rows: {source_count:,}, Target rows: {target_count:,}")
    
    if source_count == target_count:
        logger.info("✅ Row counts match perfectly")
        return True, 0, 0
    else:
        diff = abs(source_count - target_count)
        percentage_diff = (diff / max(source_count, target_count)) * 100
        logger.warning(f"Row count difference: {diff:,} rows ({percentage_diff:.2f}%)")
        
        if source_count > target_count:
            logger.warning(f"Source has {diff:,} more rows than target")
            missing_in_target = diff
            extra_in_target = 0
        else:
            logger.warning(f"Target has {diff:,} more rows than source")
            missing_in_target = 0
            extra_in_target = diff
        
        # Log this error
        error_logger.log_error({
            'session_id': session_id,
            'check_type': 'comparison',
            'source_name': source_info,
            'target_name': target_info,
            'actual_value': f"{source_label} has {source_count:,} rows",
            'expected_value': f"{target_label} has {target_count:,} rows",
            'error_type': 'row_count_mismatch',
            'error_description': f"Row count mismatch. Source({source_label})={source_count}, Target({target_label})={target_count}, Difference={diff}",
            'severity': 'high'
        })
        
        return False, missing_in_target, extra_in_target

def compare_column_structures(source_df, target_df,source_info, target_info, ui_data=None):
    """
    Compare column structures between source and target
    
    Returns:
        dict: {
            'column_match': bool,
            'total_columns_match': bool,
            'column_names_match': bool,
            'column_order_match': bool,
            'data_types_match': bool,
            'details': {
                'missing_in_target': list,
                'missing_in_source': list,
                'type_mismatches': list,
                'order_mismatches': list
            }
        }
    """
    logger.info("Comparing column structures...")
    
    results = {
        'column_match': True,
        'total_columns_match': True,
        'column_names_match': True,
        'column_order_match': True,
        'data_types_match': True,
        'details': {
            'missing_in_target': [],
            'missing_in_source': [],
            'type_mismatches': [],
            'order_mismatches': []
        }
    }
    
    # Get column names
    source_columns = list(source_df.columns)
    target_columns = list(target_df.columns)
    
    # Convert to lowercase for case-insensitive comparison if requested
    if ui_data and ui_data.get('case_insensitive', True):
        source_columns_lower = [col.lower() for col in source_columns]
        target_columns_lower = [col.lower() for col in target_columns]
        
        # Create mapping for original names
        source_col_map = {col.lower(): col for col in source_columns}
        target_col_map = {col.lower(): col for col in target_columns}
        
        # Check total column count
        if len(source_columns) != len(target_columns):
            results['total_columns_match'] = False
            results['column_match'] = False
            logger.warning(f"Column count mismatch: Source={len(source_columns)}, Target={len(target_columns)}")
        
        # Find missing columns (case-insensitive)
        missing_in_target = [source_col_map[col] for col in source_columns_lower 
                            if col not in target_columns_lower]
        missing_in_source = [target_col_map[col] for col in target_columns_lower 
                            if col not in source_columns_lower]
        
        results['details']['missing_in_target'] = missing_in_target
        results['details']['missing_in_source'] = missing_in_source
        
        if missing_in_target or missing_in_source:
            results['column_names_match'] = False
            results['column_match'] = False
            
            if missing_in_target:
                logger.warning(f"Columns missing in target: {missing_in_target}")
            if missing_in_source:
                logger.warning(f"Columns missing in source: {missing_in_source}")
        
        # Check column order (case-insensitive)
        if source_columns_lower != target_columns_lower:
            results['column_order_match'] = False
            results['column_match'] = False
            
            # Find order mismatches
            for i, (src_col, tgt_col) in enumerate(zip(source_columns_lower, target_columns_lower)):
                if src_col != tgt_col:
                    results['details']['order_mismatches'].append({
                        'position': i + 1,
                        'source_column': source_columns[i] if i < len(source_columns) else None,
                        'target_column': target_columns[i] if i < len(target_columns) else None
                    })
            
            if results['details']['order_mismatches']:
                logger.warning(f"Column order mismatch at positions: {[m['position'] for m in results['details']['order_mismatches']]}")
        
        # Compare data types (when available)
        try:
            # Get data types
            source_dtypes = source_df.dtypes.to_dict()
            target_dtypes = target_df.dtypes.to_dict()
            
            # Determine if these are from database or file
            is_source_db = 'database' in str(source_info).lower() or 'db' in str(source_info).lower()
            is_target_db = 'database' in str(target_info).lower() or 'db' in str(target_info).lower()

            type_mismatches = []
            
            for col_lower in set(source_columns_lower).intersection(set(target_columns_lower)):
                src_col_name = source_col_map.get(col_lower)
                tgt_col_name = target_col_map.get(col_lower)
                
                if src_col_name in source_dtypes and tgt_col_name in target_dtypes:
                    src_type = str(source_dtypes[src_col_name])
                    tgt_type = str(target_dtypes[tgt_col_name])
                    
                    # Normalize type names for comparison WITH CONTEXT
                    src_type_norm = normalize_data_type(src_type, context='database' if is_source_db else 'pandas')
                    tgt_type_norm = normalize_data_type(tgt_type, context='database' if is_target_db else 'pandas')
                    
                    if src_type_norm != tgt_type_norm:
                        compatibility = check_data_type_compatibility(src_type_norm, tgt_type_norm)
                        
                        type_mismatches.append({
                            'column': src_col_name,
                            'source_type': src_type,
                            'target_type': tgt_type,
                            'normalized_source': src_type_norm,
                            'normalized_target': tgt_type_norm,
                            'compatible': compatibility['compatible'],
                            'compatibility_level': compatibility['level'],
                            'source_context': 'database' if is_source_db else 'file',
                            'target_context': 'database' if is_target_db else 'file'
                        })
            
            results['details']['type_mismatches'] = type_mismatches
            
            if type_mismatches:
                results['data_types_match'] = False
                results['column_match'] = False
                
                incompatible = [m for m in type_mismatches if not m['compatible']]
                if incompatible:
                    logger.warning(f"Found {len(incompatible)} incompatible data type mismatches")
                else:
                    logger.info(f"Found {len(type_mismatches)} data type mismatches (all compatible)")
        
        except Exception as e:
            logger.warning(f"Could not compare data types: {e}")
    
    else:
        # Case-sensitive comparison
        if source_columns != target_columns:
            results['column_names_match'] = False
            results['column_order_match'] = False
            results['column_match'] = False
            
            # Find exact mismatches
            results['details']['missing_in_target'] = [col for col in source_columns if col not in target_columns]
            results['details']['missing_in_source'] = [col for col in target_columns if col not in source_columns]
    
    logger.info(f"Column structure comparison complete. Match: {results['column_match']}")
    return results

def identify_key_fields(df, source_type="file", source_info=None):
    """
    Identify key fields - either REAL primary keys (database) or INFERRED keys (files)
    
    Args:
        df: DataFrame
        source_type: 'database' or 'file'
        source_info: For database, contains connection info
    
    Returns:
        List of identified key fields with metadata
    """
    if source_type == "database" and source_info:
        return get_database_primary_keys(df, source_info)
    else:
        return infer_key_fields_from_data(df)

def get_database_primary_keys(df, db_info):
    """
    Get REAL primary keys from database schema
    Returns empty list if not database or no PKs
    """
    # Default empty - actual implementation needs database connection
    # For now, we'll use inference for all sources
    # TODO: Implement actual database PK querying
    return []

def infer_key_fields_from_data(df):
    """
    Analyze DataFrame to suggest likely key fields
    Returns list of column names that could serve as identifiers
    """
    if df.empty or len(df.columns) == 0:
        return []
    
    key_candidates = []
    
    for column in df.columns:
        # Skip columns with too many nulls
        null_pct = df[column].isnull().sum() / len(df)
        if null_pct > 0.1:  # More than 10% nulls
            continue
        
        # Calculate uniqueness score
        unique_pct = df[column].nunique() / len(df)
        
        # Calculate score based on multiple factors
        score = 0
        
        # 1. Uniqueness (40 points max)
        if unique_pct == 1.0:  # 100% unique
            score += 40
        elif unique_pct >= 0.95:  # 95-99.9% unique
            score += 30
        elif unique_pct >= 0.9:   # 90-94.9% unique
            score += 20
        elif unique_pct >= 0.8:   # 80-89.9% unique
            score += 10
        
        # 2. No nulls (20 points)
        if null_pct == 0:
            score += 20
        
        # 3. Column name patterns (20 points)
        col_lower = str(column).lower()
        key_patterns = [
            'id', '_id', 'code', '_code', 'key', '_key', 
            'num', '_num', 'no', '_no', 'pk', 'sku', 'ref',
            'customer', 'user', 'employee', 'product', 'order'
        ]
        for pattern in key_patterns:
            if pattern in col_lower:
                score += 5
                break
        
        # 4. Data type suitability (20 points)
        dtype = str(df[column].dtype)
        if 'int' in dtype or 'float' in dtype:
            score += 15
        elif 'object' in dtype or 'str' in dtype:
            # Check if values look like identifiers
            sample = df[column].dropna().head(10)
            if len(sample) > 0:
                # Check if sample values look like codes/IDs
                id_like = all(str(val).replace(' ', '').isalnum() for val in sample)
                if id_like:
                    score += 10
        
        # Only consider if score meets threshold
        if score >= 60 and unique_pct == 1.0:
            confidence = 'high' if score >= 60 else 'medium' if score >= 45 else 'low'
            key_candidates.append({
                'column': column,
                'score': score,
                'unique_pct': round(unique_pct * 100, 1),
                'null_pct': round(null_pct * 100, 1),
                'confidence': confidence,
                'data_type': dtype
            })
    
    # Sort by score descending
    key_candidates.sort(key=lambda x: x['score'], reverse=True)
    
    # Return only column names (for backward compatibility)
    return [candidate['column'] for candidate in key_candidates]

def validate_primary_key_fields(source_df, target_df, key_fields=None, ui_data=None):
    """
    Validate primary key/unique fields
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame  
        key_fields: List of field names to check as keys (optional)
        ui_data: UI configuration data
    
    Returns:
        dict: Validation results with clear status
    """
    logger.info("Validating primary key/unique fields...")
    
    # Initialize results with clear defaults
    results = {
        'key_fields_found': [],
        'key_fields_missing': [],
        'unique_in_source': True,
        'unique_in_target': True,
        'validation_passed': None,  # Start as None (not applicable)
        'validation_status': 'NOT_APPLICABLE',  # New: Clear status
        'status_message': 'No key validation performed',
        'details': {
            'source_duplicates': [],
            'target_duplicates': [],
            'key_detection_method': 'unknown'
        }
    }
    
    # Determine source and target types from context
    source_type = 'file'  # Default
    target_type = 'file'  # Default
    
    # Try to get from UI data if available
    if ui_data:
        if 'source' in ui_data and 'source_type' in ui_data['source']:
            source_type = ui_data['source']['source_type']
        if 'target' in ui_data and 'source_type' in ui_data['target']:
            target_type = ui_data['target']['source_type']
    
    # Step 1: Get key fields (manual input OR auto-detect)
    if key_fields:
        # Use manually specified key fields
        if isinstance(key_fields, str):
            key_fields = [f.strip() for f in key_fields.split(',')]
        results['details']['key_detection_method'] = 'manual_input'
        logger.info(f"Using manually specified key fields: {key_fields}")
    elif ui_data and 'key_fields' in ui_data:
        # Get from UI data
        key_fields = ui_data['key_fields']
        if isinstance(key_fields, str):
            key_fields = [f.strip() for f in key_fields.split(',')]
        results['details']['key_detection_method'] = 'ui_input'
        logger.info(f"Using UI specified key fields: {key_fields}")
    else:
        # Auto-detect key fields from both datasets
        logger.info("Auto-detecting key fields from data...")
        
        # Identify key fields from source
        source_key_candidates = infer_key_fields_from_data(source_df)
        target_key_candidates = infer_key_fields_from_data(target_df)
        
        results['details']['source_key_candidates'] = source_key_candidates
        results['details']['target_key_candidates'] = target_key_candidates
        results['details']['key_detection_method'] = 'auto_detected'
        
        # Find common key fields (case-insensitive)
        source_cols_lower = [col.lower() for col in source_df.columns]
        target_cols_lower = [col.lower() for col in target_df.columns]
        
        common_keys = []
        for src_candidate in source_key_candidates:
            src_lower = src_candidate.lower()
            # Check if exists in target (case-insensitive)
            for tgt_col in target_df.columns:
                if tgt_col.lower() == src_lower:
                    common_keys.append({
                        'source_column': src_candidate,
                        'target_column': tgt_col,
                        'confidence': 'auto_detected'
                    })
                    break
        
        if common_keys:
            key_fields = [item['source_column'] for item in common_keys]
            results['details']['common_keys'] = common_keys
            logger.info(f"Auto-detected common key fields: {key_fields}")
        else:
            logger.info("No common key fields auto-detected")
            key_fields = []
    
    # Step 2: If no key fields found, return with appropriate status
    if not key_fields:
        results['validation_status'] = 'NO_KEYS_IDENTIFIED'
        results['status_message'] = 'No identifiable key fields found'
        results['validation_passed'] = None  # Not applicable
        logger.info("No key fields identified for validation")
        return results
    
    # Step 3: Check existence in both datasets
    for field in key_fields:
        exists_in_source = field in source_df.columns
        exists_in_target = False
        
        # Find matching column in target (case-insensitive)
        target_match = None
        for tgt_col in target_df.columns:
            if tgt_col.lower() == field.lower():
                exists_in_target = True
                target_match = tgt_col
                break
        
        if exists_in_source and exists_in_target:
            results['key_fields_found'].append({
                'source_column': field,
                'target_column': target_match
            })
        else:
            results['key_fields_missing'].append({
                'field': field,
                'exists_in_source': exists_in_source,
                'exists_in_target': exists_in_target
            })
    
    # Step 4: If fields missing, validation fails
    if results['key_fields_missing']:
        results['validation_status'] = 'KEYS_MISSING'
        results['validation_passed'] = False
        missing_list = [item['field'] for item in results['key_fields_missing']]
        results['status_message'] = f'Key fields missing in one dataset: {missing_list}'
        logger.warning(f"Key fields missing: {missing_list}")
        return results
    
    # Step 5: Check uniqueness for each key field
    all_unique = True
    validation_details = []
    
    for key_info in results['key_fields_found']:
        src_field = key_info['source_column']
        tgt_field = key_info['target_column']
        
        # Check uniqueness in source
        source_duplicates = source_df[source_df.duplicated(subset=[src_field], keep=False)]
        source_unique = source_duplicates.empty
        
        # Check uniqueness in target  
        target_duplicates = target_df[target_df.duplicated(subset=[tgt_field], keep=False)]
        target_unique = target_duplicates.empty
        
        if not source_unique or not target_unique:
            all_unique = False
            
            if not source_unique:
                dup_count = len(source_duplicates)
                sample_values = source_duplicates[src_field].unique().tolist()[:3]
                results['details']['source_duplicates'].append({
                    'field': src_field,
                    'duplicate_count': dup_count,
                    'sample_values': sample_values
                })
            
            if not target_unique:
                dup_count = len(target_duplicates)
                sample_values = target_duplicates[tgt_field].unique().tolist()[:3]
                results['details']['target_duplicates'].append({
                    'field': tgt_field,
                    'duplicate_count': dup_count,
                    'sample_values': sample_values
                })
        
        validation_details.append({
            'field': src_field,
            'target_field': tgt_field,
            'unique_in_source': source_unique,
            'unique_in_target': target_unique,
            'source_duplicate_count': len(source_duplicates) if not source_unique else 0,
            'target_duplicate_count': len(target_duplicates) if not target_unique else 0
        })
    
    # Step 6: Set final validation status
    if all_unique:
        results['validation_status'] = 'VALIDATION_PASSED'
        results['validation_passed'] = True
        results['status_message'] = f'All key fields unique: {[k["source_column"] for k in results["key_fields_found"]]}'
        logger.info(f"Primary key validation passed for fields: {[k['source_column'] for k in results['key_fields_found']]}")
    else:
        results['validation_status'] = 'DUPLICATE_VALUES'
        results['validation_passed'] = False
        dup_fields = []
        for detail in validation_details:
            if not detail['unique_in_source'] or not detail['unique_in_target']:
                dup_fields.append(detail['field'])
        results['status_message'] = f'Duplicate values found in key fields: {dup_fields}'
        logger.warning(f"Primary key validation failed - duplicates in: {dup_fields}")
    
    results['details']['validation_details'] = validation_details
    
    logger.info(f"Primary key validation complete. Status: {results['validation_status']}")
    return results

# def validate_primary_key_fields(source_df, target_df, key_fields=None, ui_data=None):
#     """
#     Validate primary key/unique fields existence and uniqueness
    
#     Args:
#         source_df: Source DataFrame
#         target_df: Target DataFrame  
#         key_fields: List of field names to check as keys
#         ui_data: UI configuration data
    
#     Returns:
#         dict: {
#             'key_fields_found': list,
#             'key_fields_missing': list,
#             'unique_in_source': bool,
#             'unique_in_target': bool,
#             'validation_passed': bool,
#             'details': {
#                 'source_duplicates': list of duplicate rows,
#                 'target_duplicates': list of duplicate rows
#             }
#         }
#     """
#     logger.info("Validating primary key/unique fields...")
    
#     results = {
#         'key_fields_found': [],
#         'key_fields_missing': [],
#         'unique_in_source': True,
#         'unique_in_target': True,
#         'validation_passed': True,
#         'details': {
#             'source_duplicates': [],
#             'target_duplicates': []
#         }
#     }
    
#     # Get key fields from UI data or use default detection
#     if ui_data and 'key_fields' in ui_data:
#         key_fields = ui_data['key_fields']
#         if isinstance(key_fields, str):
#             key_fields = [f.strip() for f in key_fields.split(',')]
#     elif not key_fields:
#         # Auto-detect common key field names
#         common_key_names = ['id', 'customerid', 'customer_id', 'userid', 'user_id', 
#                            'employeeid', 'employee_id', 'orderid', 'order_id',
#                            'productid', 'product_id', 'code', 'sku']
        
#         # Check which of these exist in both datasets
#         source_cols = [col.lower() for col in source_df.columns]
#         target_cols = [col.lower() for col in target_df.columns]
        
#         key_fields = []
#         for key_name in common_key_names:
#             # Check if exists in both (case-insensitive)
#             source_match = any(key_name in col for col in source_cols)
#             target_match = any(key_name in col for col in target_cols)
#             if source_match and target_match:
#                 # Find actual column names
#                 for src_col in source_df.columns:
#                     if key_name in src_col.lower():
#                         key_fields.append(src_col)
#                         break
        
#         if not key_fields:
#             logger.info("No common key fields auto-detected. Skipping PK validation.")
#             return results
    
#     logger.info(f"Checking key fields: {key_fields}")
    
#     # Check existence in source
#     for field in key_fields:
#         if field in source_df.columns and field in target_df.columns:
#             results['key_fields_found'].append(field)
#         else:
#             results['key_fields_missing'].append(field)
#             results['validation_passed'] = False
    
#     if not results['key_fields_found']:
#         logger.warning("No key fields found in both datasets")
#         results['validation_passed'] = False
#         return results
    
#     # Check uniqueness in source
#     for field in results['key_fields_found']:
#         try:
#             # Check for duplicates
#             source_duplicates = source_df[source_df.duplicated(subset=[field], keep=False)]
#             target_duplicates = target_df[target_df.duplicated(subset=[field], keep=False)]
            
#             if not source_duplicates.empty:
#                 results['unique_in_source'] = False
#                 results['validation_passed'] = False
#                 duplicate_indices = source_duplicates.index.tolist()[:10]  # First 10
#                 duplicate_values = source_duplicates[field].unique().tolist()[:5]
#                 results['details']['source_duplicates'].append({
#                     'field': field,
#                     'duplicate_count': len(source_duplicates),
#                     'sample_indices': duplicate_indices,
#                     'sample_values': duplicate_values
#                 })
#                 logger.warning(f"Field '{field}' has duplicates in source: {len(source_duplicates)} rows")
            
#             if not target_duplicates.empty:
#                 results['unique_in_target'] = False
#                 results['validation_passed'] = False
#                 duplicate_indices = target_duplicates.index.tolist()[:10]
#                 duplicate_values = target_duplicates[field].unique().tolist()[:5]
#                 results['details']['target_duplicates'].append({
#                     'field': field,
#                     'duplicate_count': len(target_duplicates),
#                     'sample_indices': duplicate_indices,
#                     'sample_values': duplicate_values
#                 })
#                 logger.warning(f"Field '{field}' has duplicates in target: {len(target_duplicates)} rows")
                
#         except Exception as e:
#             logger.warning(f"Error checking uniqueness for field '{field}': {e}")
#             continue
    
#     logger.info(f"Primary key validation complete. Passed: {results['validation_passed']}")
#     return results

def compare_table_metadata(source_df, target_df, source_info, target_info, ui_data=None):
    """
    Compare table-level metadata and statistics
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        source_info: Source description
        target_info: Target description
        ui_data: UI configuration data
    
    Returns:
        dict: Table metadata comparison results
    """
    logger.info("Comparing table-level metadata...")
    
    results = {
        'match': True,
        'source_metadata': {},
        'target_metadata': {},
        'differences': [],
        'summary': {
            'all_metadata_match': True,
            'statistics_match': True,
            'schema_match': True
        }
    }
    
    # Collect basic metadata
    results['source_metadata'] = {
        'row_count': len(source_df),
        'column_count': len(source_df.columns),
        'total_cells': len(source_df) * len(source_df.columns),
        'memory_usage_mb': round(source_df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        'data_types': {},
        'null_counts': {},
        'unique_counts': {}
    }
    
    results['target_metadata'] = {
        'row_count': len(target_df),
        'column_count': len(target_df.columns),
        'total_cells': len(target_df) * len(target_df.columns),
        'memory_usage_mb': round(target_df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        'data_types': {},
        'null_counts': {},
        'unique_counts': {}
    }
    
    # Collect column-level metadata for common columns
    source_cols_lower = [col.lower() for col in source_df.columns]
    target_cols_lower = [col.lower() for col in target_df.columns]
    
    # Create mapping
    source_col_map = {col.lower(): col for col in source_df.columns}
    target_col_map = {col.lower(): col for col in target_df.columns}
    
    common_cols_lower = set(source_cols_lower).intersection(set(target_cols_lower))
    
    for col_lower in common_cols_lower:
        src_col = source_col_map[col_lower]
        tgt_col = target_col_map[col_lower]
        
        # Data types
        src_dtype = str(source_df[src_col].dtype)
        tgt_dtype = str(target_df[tgt_col].dtype)
        results['source_metadata']['data_types'][src_col] = src_dtype
        results['target_metadata']['data_types'][tgt_col] = tgt_dtype
        
        # Null counts
        src_nulls = source_df[src_col].isnull().sum()
        tgt_nulls = target_df[tgt_col].isnull().sum()
        results['source_metadata']['null_counts'][src_col] = int(src_nulls)
        results['target_metadata']['null_counts'][tgt_col] = int(tgt_nulls)
        
        # Unique values (for categorical/text columns)
        if source_df[src_col].dtype in ['object', 'category']:
            src_unique = source_df[src_col].nunique()
            tgt_unique = target_df[tgt_col].nunique()
            results['source_metadata']['unique_counts'][src_col] = int(src_unique)
            results['target_metadata']['unique_counts'][tgt_col] = int(tgt_unique)
    
    # Compare statistics
    stats_comparison = compare_statistics(source_df, target_df, common_cols_lower, 
                                          source_col_map, target_col_map, ui_data)
    results.update(stats_comparison)
    
    # Check for differences
    differences = []
    
    # 1. Row count difference
    row_diff = abs(results['source_metadata']['row_count'] - results['target_metadata']['row_count'])
    if row_diff > 0:
        differences.append({
            'type': 'row_count',
            'source_value': results['source_metadata']['row_count'],
            'target_value': results['target_metadata']['row_count'],
            'difference': row_diff,
            'severity': 'high' if row_diff > 100 else 'medium'
        })
        results['match'] = False
        results['summary']['statistics_match'] = False
    
    # 2. Column count difference
    col_diff = abs(results['source_metadata']['column_count'] - results['target_metadata']['column_count'])
    if col_diff > 0:
        differences.append({
            'type': 'column_count',
            'source_value': results['source_metadata']['column_count'],
            'target_value': results['target_metadata']['column_count'],
            'difference': col_diff,
            'severity': 'high'
        })
        results['match'] = False
        results['summary']['schema_match'] = False
    
    # 3. Memory usage difference (>20% difference)
    src_mem = results['source_metadata']['memory_usage_mb']
    tgt_mem = results['target_metadata']['memory_usage_mb']
    if src_mem > 0 and tgt_mem > 0:
        mem_diff_pct = abs(src_mem - tgt_mem) / max(src_mem, tgt_mem) * 100
        if mem_diff_pct > 20:  # More than 20% difference
            differences.append({
                'type': 'memory_usage',
                'source_value': f"{src_mem} MB",
                'target_value': f"{tgt_mem} MB",
                'difference': f"{mem_diff_pct:.1f}%",
                'severity': 'low'
            })
            results['summary']['statistics_match'] = False
    
    results['differences'] = differences
    
    # Update overall match
    if not results['summary']['all_metadata_match'] or differences:
        results['match'] = False
    
    logger.info(f"Table metadata comparison complete. Match: {results['match']}")
    return results


def compare_statistics(source_df, target_df, common_cols_lower, source_col_map, target_col_map, ui_data):
    """
    Compare statistical properties of columns
    """
    results = {
        'statistics': {
            'numeric_comparison': {},
            'categorical_comparison': {},
            'date_comparison': {}
        },
        'summary': {
            'all_metadata_match': True,
            'statistics_match': True
        }
    }
    
    numeric_stats = {}
    categorical_stats = {}
    date_stats = {}
    
    for col_lower in common_cols_lower:
        src_col = source_col_map[col_lower]
        tgt_col = target_col_map[col_lower]
        
        src_series = source_df[src_col]
        tgt_series = target_df[tgt_col]
        
        src_dtype = str(src_series.dtype)
        
        # Numeric columns
        if 'int' in src_dtype or 'float' in src_dtype:
            try:
                src_stats = {
                    'min': float(src_series.min()) if not pd.isna(src_series.min()) else None,
                    'max': float(src_series.max()) if not pd.isna(src_series.max()) else None,
                    'mean': float(src_series.mean()) if not pd.isna(src_series.mean()) else None,
                    'std': float(src_series.std()) if not pd.isna(src_series.std()) else None,
                    'null_count': int(src_series.isnull().sum())
                }
                
                tgt_stats = {
                    'min': float(tgt_series.min()) if not pd.isna(tgt_series.min()) else None,
                    'max': float(tgt_series.max()) if not pd.isna(tgt_series.max()) else None,
                    'mean': float(tgt_series.mean()) if not pd.isna(tgt_series.mean()) else None,
                    'std': float(tgt_series.std()) if not pd.isna(tgt_series.std()) else None,
                    'null_count': int(tgt_series.isnull().sum())
                }
                
                # Check for significant differences (>10%)
                differences = []
                for stat in ['min', 'max', 'mean']:
                    if src_stats[stat] is not None and tgt_stats[stat] is not None:
                        if src_stats[stat] != 0:
                            diff_pct = abs(src_stats[stat] - tgt_stats[stat]) / abs(src_stats[stat]) * 100
                            if diff_pct > 10:
                                differences.append({
                                    'statistic': stat,
                                    'difference_percent': diff_pct,
                                    'source_value': src_stats[stat],
                                    'target_value': tgt_stats[stat]
                                })
                
                numeric_stats[src_col] = {
                    'source': src_stats,
                    'target': tgt_stats,
                    'differences': differences,
                    'match': len(differences) == 0
                }
                
                if differences:
                    results['summary']['statistics_match'] = False
                    results['summary']['all_metadata_match'] = False
                    
            except Exception as e:
                logger.debug(f"Could not compute stats for numeric column {src_col}: {e}")
        
        # Categorical columns
        elif src_dtype in ['object', 'category']:
            try:
                src_unique = src_series.nunique()
                tgt_unique = tgt_series.nunique()
                
                src_top_values = src_series.value_counts().head(5).to_dict()
                tgt_top_values = tgt_series.value_counts().head(5).to_dict()
                
                categorical_stats[src_col] = {
                    'source_unique': int(src_unique),
                    'target_unique': int(tgt_unique),
                    'source_top_values': src_top_values,
                    'target_top_values': tgt_top_values,
                    'unique_match': src_unique == tgt_unique
                }
                
                if src_unique != tgt_unique:
                    results['summary']['statistics_match'] = False
                    results['summary']['all_metadata_match'] = False
                    
            except Exception as e:
                logger.debug(f"Could not compute stats for categorical column {src_col}: {e}")
    
    results['statistics']['numeric_comparison'] = numeric_stats
    results['statistics']['categorical_comparison'] = categorical_stats
    results['statistics']['date_comparison'] = date_stats
    
    return results

def normalize_data_type(dtype_str, context='pandas'):
    """Normalize data type string for comparison"""
    if not dtype_str:
        return 'unknown'
    
    if context == 'database':
        return map_database_data_type(dtype_str)
    
    dtype_lower = dtype_str.lower()
    
    # Pandas data types
    if 'int' in dtype_lower:
        return 'integer'
    elif 'float' in dtype_lower:
        return 'float'
    elif 'object' in dtype_lower or 'str' in dtype_lower:
        return 'string'
    elif 'datetime' in dtype_lower or 'timestamp' in dtype_lower:
        return 'datetime'
    elif 'bool' in dtype_lower:
        return 'boolean'
    elif 'category' in dtype_lower:
        return 'categorical'
    else:
        return dtype_lower


def check_data_type_compatibility(source_type, target_type):
    """
    Check if two data types are compatible
    
    Returns:
        dict: {'compatible': bool, 'level': 'exact'|'compatible'|'incompatible'}
    """
    compatibility_matrix = {
        'integer': ['integer', 'float', 'string'],  # int can go to float or string
        'float': ['float', 'string'],  # float can go to string
        'string': ['string'],  # string can only stay as string
        'datetime': ['datetime', 'string'],
        'boolean': ['boolean', 'integer', 'string'],
        'categorical': ['categorical', 'string']
    }
    
    if source_type == target_type:
        return {'compatible': True, 'level': 'exact'}
    
    compatible_targets = compatibility_matrix.get(source_type, [])
    if target_type in compatible_targets:
        return {'compatible': True, 'level': 'compatible'}
    else:
        return {'compatible': False, 'level': 'incompatible'}

def map_database_data_type(db_type_str, db_engine='generic'):
    """
    Map database-specific data types to normalized categories
    
    Args:
        db_type_str: Database type string (e.g., 'VARCHAR(255)', 'INT UNSIGNED')
        db_engine: 'mysql', 'postgresql', 'sqlserver', 'oracle', 'sqlite'
    
    Returns:
        Normalized type category
    """
    db_type_lower = db_type_str.lower().strip()
    
    # Common patterns
    if any(x in db_type_lower for x in ['varchar', 'char', 'text', 'string', 'clob', 'nvarchar']):
        return 'string'
    
    elif any(x in db_type_lower for x in ['int', 'integer', 'bigint', 'smallint', 'tinyint']):
        return 'integer'
    
    elif any(x in db_type_lower for x in ['decimal', 'numeric', 'float', 'double', 'real', 'number']):
        # Check if it's actually integer (e.g., NUMBER(10) in Oracle)
        if 'number' in db_type_lower and '(' in db_type_lower:
            # Extract precision
            try:
                precision = int(db_type_lower.split('(')[1].split(')')[0])
                if precision <= 10:  # Often used for integers
                    return 'integer'
            except:
                pass
        return 'float'
    
    elif any(x in db_type_lower for x in ['date', 'datetime', 'timestamp', 'time']):
        return 'datetime'
    
    elif any(x in db_type_lower for x in ['bool', 'boolean', 'bit']):
        return 'boolean'
    
    elif 'binary' in db_type_lower or 'blob' in db_type_lower:
        return 'binary'
    
    else:
        return 'unknown'

def compare_rows_detailed(source_row, target_row, source_columns, target_columns, common_cols,
                         normalized_source=None, normalized_target=None, row_idx=None):
    """
    Compare two rows column by column and return detailed differences
    WITH SIGNIFICANCE FILTERING
    """
    differences = []
    source_data = {}
    target_data = {}
    
    # Compare each column
    for i, src_col in enumerate(source_columns):
        # Get corresponding target column
        tgt_col = target_columns[i] if i < len(target_columns) else src_col
        
        # Get values
        src_val = source_row[src_col] if src_col in source_row else None
        tgt_val = target_row[tgt_col] if tgt_col in target_row else None
        
        # Store original values for display
        source_data[src_col] = src_val if not pd.isna(src_val) else None
        target_data[tgt_col] = tgt_val if not pd.isna(tgt_val) else None
        
        if normalized_source is not None and row_idx is not None and src_col in normalized_source:
            # Get pre-normalized value (FAST!)
            src_norm = normalized_source[src_col].iloc[row_idx]
        else:
            # Normalize on the fly (fallback)
            src_norm = smart_normalize_value(src_val, src_col)
        
        if normalized_target is not None and row_idx is not None and tgt_col in normalized_target:
            # Get pre-normalized value (FAST!)
            tgt_norm = normalized_target[tgt_col].iloc[row_idx]
        else:
            # Normalize on the fly (fallback)
            tgt_norm = smart_normalize_value(tgt_val, tgt_col)
        
        # Check if values differ
        if src_norm != tgt_norm:
            # Calculate significance
            significance = calculate_mismatch_significance({
                'source': src_val,
                'target': tgt_val,
                'source_normalized': src_norm,
                'target_normalized': tgt_norm
            })
            
            # Only include significant differences (threshold = 3)
            if significance >= 3:
                # Format values for display
                src_display = "NULL" if src_val is None or pd.isna(src_val) else str(src_val)
                tgt_display = "NULL" if tgt_val is None or pd.isna(tgt_val) else str(tgt_val)
                
                differences.append({
                    'column': src_col,
                    'source': src_display,
                    'target': tgt_display,
                    'source_normalized': src_norm,
                    'target_normalized': tgt_norm,
                    'significance': significance
                })
    
    # Create mismatch summary string - only significant differences
    mismatch_summary_parts = []
    for diff in differences:
        mismatch_summary_parts.append(f"{diff['column']}: '{diff['source']}' ≠ '{diff['target']}'")
    
    mismatch_summary = "; ".join(mismatch_summary_parts) if mismatch_summary_parts else "No significant differences found"
    
    return {
        'differences': differences,
        'differences_count': len(differences),
        'source_data': source_data,
        'target_data': target_data,
        'mismatch_summary': mismatch_summary
    }


def format_row_data_for_display(row, columns, max_chars=1000):
    """
    Format row data for display in logs/database (like your screenshot format)
    
    Args:
        row: DataFrame row or pandas Series
        columns: List of column names to include
        max_chars: Maximum characters to return
    
    Returns:
        str: Formatted row data with column: value pairs
    """
    if row is None or (hasattr(row, 'empty') and row.empty) or len(columns) == 0:
        return "NO DATA"
    
    parts = []
    for col in columns:
        if col in row:
            value = row[col]
            if pd.isna(value) or value is None:
                value_str = "NULL"
            else:
                value_str = str(value)
                # Truncate very long values but keep them readable
                if len(value_str) > 100:
                    value_str = value_str[:97] + "..."
            parts.append(f"{col}: {value_str}")
        else:
            parts.append(f"{col}: NOT FOUND")
    
    result = "; ".join(parts)
    if len(result) > max_chars:
        # Try to keep important information
        if len(result) > max_chars:
            # Count how many columns we can show fully
            visible_cols = 0
            total_length = 0
            for part in parts:
                if total_length + len(part) + 2 <= max_chars - 20:  # 2 for "; ", 20 for "... (+X more)"
                    total_length += len(part) + 2
                    visible_cols += 1
                else:
                    break
            
            if visible_cols > 0:
                result = "; ".join(parts[:visible_cols])
                remaining = len(parts) - visible_cols
                if remaining > 0:
                    result += f"; ... (+{remaining} more columns)"
            else:
                # Show first column only with truncation
                result = parts[0][:max_chars-20] + f"... (+{len(parts)-1} more columns)"
    
    return result

def display_mismatch_table_cli(mismatch_data, source_label, target_label):
    """
    Display mismatch data in CLI as a table
    """
    if not mismatch_data:
        return "No mismatches to display"
    
    output_lines = []
    
    # Header
    output_lines.append("\n" + "="*120)
    output_lines.append(f"COMPARISON: {source_label} vs {target_label}")
    output_lines.append("="*120)
    output_lines.append(f"{'Row':<5} | {'Diff Count':<10} | {'Source (Sample)':<40} | {'Target (Sample)':<40}")
    output_lines.append("-"*120)
    
    for i, mismatch in enumerate(mismatch_data[:10], 1):  # Show first 10
        row_num = mismatch.get('excel_row', i + 1)
        diff_count = mismatch.get('differences_count', 0)
        
        # Get first column from source and target as sample
        source_sample = "NULL"
        target_sample = "NULL"
        
        source_data = mismatch.get('source_data', {})
        if source_data:
            first_key = list(source_data.keys())[0]
            first_val = source_data[first_key]
            if first_val is not None and not pd.isna(first_val):
                source_sample = f"{first_key}: {str(first_val)[:30]}"
            else:
                source_sample = f"{first_key}: NULL"
        
        target_data = mismatch.get('target_data', {})
        if target_data:
            first_key = list(target_data.keys())[0]
            first_val = target_data[first_key]
            if first_val is not None and not pd.isna(first_val):
                target_sample = f"{first_key}: {str(first_val)[:30]}"
            else:
                target_sample = f"{first_key}: NULL"
        
        output_lines.append(f"{row_num:<5} | {diff_count:<10} | {source_sample:<40} | {target_sample:<40}")
    
    # Footer with pagination info
    output_lines.append("-"*120)
    if len(mismatch_data) > 10:
        output_lines.append(f"Showing 1-10 of {len(mismatch_data)} mismatched rows. Use API for complete data.")
    else:
        output_lines.append(f"Showing all {len(mismatch_data)} mismatched rows.")
    output_lines.append("="*120)
    
    return "\n".join(output_lines)


def format_mismatch_for_cli_detail(mismatch, source_label, target_label):
    """
    Format a single mismatch with full details for CLI
    """
    output = []
    
    row_num = mismatch.get('excel_row', '?')
    diff_count = mismatch.get('differences_count', 0)
    
    output.append(f"\n{'='*100}")
    output.append(f"ROW {row_num} - {diff_count} DIFFERENCES")
    output.append(f"{'-'*100}")
    
    # Source data section
    source_data = mismatch.get('source_data', {})
    output.append(f"{source_label.upper()} DATA:")
    for col, val in source_data.items():
        if val is None or pd.isna(val):
            val_str = "NULL"
        else:
            val_str = str(val)
            if len(val_str) > 50:
                val_str = val_str[:47] + "..."
        output.append(f"  {col}: {val_str}")
    
    output.append(f"{'-'*100}")
    
    # Target data section
    target_data = mismatch.get('target_data', {})
    output.append(f"{target_label.upper()} DATA:")
    for col, val in target_data.items():
        if val is None or pd.isna(val):
            val_str = "NULL"
        else:
            val_str = str(val)
            if len(val_str) > 50:
                val_str = val_str[:47] + "..."
        output.append(f"  {col}: {val_str}")
    
    # Specific differences
    differences = mismatch.get('differences', [])
    if differences:
        output.append(f"{'-'*100}")
        output.append("COLUMN DIFFERENCES:")
        for diff in differences:
            output.append(f"  • {diff['column']}: '{diff['source']}' ≠ '{diff['target']}'")
    
    output.append(f"{'='*100}")
    
    return "\n".join(output)

def advanced_data_comparison(source_df, target_df, selected_columns, common_cols, error_logger, session_id, source_info, target_info, source_file, target_file):
    """SECOND CHECK: Hash-based comparison with PERFECT NORMALIZATION - FIXED FOR DUPLICATES"""
    logger.info("SECOND CHECK: HASH-BASED COMPARISON WITH PERFECT NORMALIZATION")
    
    logged_rows = set()

    import traceback
    current_stack = traceback.format_stack()
    logger.debug(f"Current call stack: {''.join(current_stack[-3:])}")

    source_label, target_label = get_dynamic_labels(source_info, target_info)
    
    total_rows = len(source_df) + len(target_df)
    processed_rows = 0
    last_progress_log = 0
    
    def log_progress(processed, total, operation="Processing"):
        """Log progress every 10% or 50,000 rows"""
        nonlocal last_progress_log
        progress_pct = (processed / total) * 100 if total > 0 else 0
        
        # Log every 10% progress or every 50,000 rows
        if progress_pct - last_progress_log >= 10 or (processed % 50000 == 0 and processed > 0):
            logger.info(f"   {operation}: {progress_pct:.1f}% complete ({processed:,}/{total:,} rows)")
            last_progress_log = progress_pct
        
        return last_progress_log
    
    logger.info(f"Starting comparison of {total_rows:,} total rows...")
    
    # TEMPORARY DEBUG: Log data types
    logger.info("=== DEBUG: DATA TYPE CHECK ===")
    for col in selected_columns[:3]:  # Check first 3 columns
        if col in source_df.columns:
            source_sample = source_df[col].iloc[0] if not source_df.empty else None
            target_sample = target_df[get_target_column_name(col, common_cols)].iloc[0] if not target_df.empty else None
            logger.info(f"Column: {col}")
            logger.info(f"  Source type: {type(source_sample)}, value: {source_sample}")
            logger.info(f"  Target type: {type(target_sample)}, value: {target_sample}")
            
            # Test normalization
            norm_source = smart_normalize_value(source_sample, col)
            norm_target = smart_normalize_value(target_sample, get_target_column_name(col, common_cols))
            logger.info(f"  Normalized: '{norm_source}' vs '{norm_target}'")
            logger.info(f"  Match: {norm_source == norm_target}")

    # DEBUG: Show column info
    logger.info(f"Source DF columns: {list(source_df.columns)}")
    logger.info(f"Target DF columns: {list(target_df.columns)}")
    logger.info(f"Selected columns for comparison: {selected_columns}")
    
    if not selected_columns:
        logger.warning("No columns selected for hash-based comparison")
        return 0, 0, 0, []
    
    # Get target column names
    target_columns = [get_target_column_name(col, common_cols) for col in selected_columns]
    
    # Only use columns that exist in BOTH datasets
    common_source_columns = [col for col in selected_columns if col in source_df.columns]
    common_target_columns = [get_target_column_name(col, common_cols) for col in common_source_columns 
                           if get_target_column_name(col, common_cols) in target_df.columns]
    
    if not common_source_columns or not common_target_columns:
        logger.warning("No common columns available for hash-based comparison")
        return 0, 0, 0, []
    
    logger.info(f"Using {len(common_source_columns)} common columns for hash comparison")
    logger.info(f"Source columns: {common_source_columns}")
    logger.info(f"Target columns: {common_target_columns}")
    
    def create_row_hash(row, columns, dataset_type="source"):
        """Create hash from PERFECTLY normalized row values - ADD MORE DEBUG"""
        
        normalized_values = []
        
        # DEBUG: Track what we're hashing
        debug_info = []
        
        for i, col in enumerate(columns):
            try:
                value = row[col]
                target_col_name = common_target_columns[i] if dataset_type == "target" else col
                
                norm_val = smart_normalize_value(value, target_col_name)
                normalized_values.append(str(norm_val))
                
                # DEBUG: Log first row details
                if dataset_type == "source" and i < 5:  # First 5 columns of first row
                    debug_info.append(f"{col}: '{value}' -> '{norm_val}'")
                    
            except Exception as e:
                logger.warning(f"Error normalizing value for column {col}: {e}")
                normalized_values.append("NULL")

        row_string = '|'.join(normalized_values)
        hash_result = hashlib.md5(row_string.encode('utf-8')).hexdigest()
        
        
        return hash_result
    
    # Generate hashes for source - TRACK ALL ROWS, NOT JUST UNIQUE
    source_hashes = {}
    source_row_hashes = {}
    logger.info(f"Generating hashes for source data ({len(source_df)} rows)...")
    
    for idx, row in source_df.iterrows():
        try:
            processed_rows += 1
            log_progress(processed_rows, total_rows, "Hashing source data")
            row_hash = create_row_hash(row, common_source_columns, "source")
            source_row_hashes[idx] = row_hash
            
            # Track count of each hash
            if row_hash in source_hashes:
                source_hashes[row_hash]['count'] += 1
                source_hashes[row_hash]['rows'].append(idx)
            else:
                source_hashes[row_hash] = {
                    'count': 1,
                    'rows': [idx],
                    'sample_row': idx
                }
                
        except Exception as e:
            logger.warning(f"Error hashing source row {idx}: {e}")
    
    # DEBUG: Show sample hashes
    logger.info("=== DEBUG: SAMPLE HASHES ===")
    hash_samples = list(source_hashes.keys())[:3]  # First 3 unique hashes
    for i, hash_val in enumerate(hash_samples):
        sample_idx = source_hashes[hash_val]['sample_row']
        logger.info(f"Hash {i+1}: {hash_val[:16]}... (sample row {sample_idx})")
    
    # Generate hashes for target - TRACK ALL ROWS
    target_hashes = {}
    target_row_hashes = {}
    logger.info(f"Generating hashes for target data ({len(target_df)} rows)...")
    
    for idx, row in target_df.iterrows():
        try:
            processed_rows += 1
            log_progress(processed_rows, total_rows, "Hashing target data")
            row_hash = create_row_hash(row, common_target_columns, "target")
            target_row_hashes[idx] = row_hash
            
            # Track count of each hash
            if row_hash in target_hashes:
                target_hashes[row_hash]['count'] += 1
                target_hashes[row_hash]['rows'].append(idx)
            else:
                target_hashes[row_hash] = {
                    'count': 1,
                    'rows': [idx],
                    'sample_row': idx
                }
                
        except Exception as e:
            logger.warning(f"Error hashing target row {idx}: {e}")
    
    # ===== FIX 3: ENHANCED DEBUG - COMPARE SAMPLE ROWS =====
    logger.info("\n" + "="*70)
    logger.info("🔍 DEBUG: ANALYZING WHY HASHES DON'T MATCH")
    logger.info("="*70)

    # Check first 3 rows
    sample_rows = min(3, len(source_df), len(target_df))
    if sample_rows > 0:
        for row_idx in range(sample_rows):
            logger.info(f"\n--- Analyzing Row {row_idx} ---")
            
            # Get actual data
            source_row = source_df.iloc[row_idx]
            target_row = target_df.iloc[row_idx]
            
            # Compare each column being used for hashing
            for i in range(min(5, len(common_source_columns))):  # First 5 columns max
                src_col = common_source_columns[i]
                tgt_col = common_target_columns[i]
                
                # Get values
                src_val = source_row[src_col] if src_col in source_row else None
                tgt_val = target_row[tgt_col] if tgt_col in target_row else None
                
                # Normalize
                src_norm = smart_normalize_value(src_val, src_col)
                tgt_norm = smart_normalize_value(tgt_val, tgt_col)
                
                # Check match
                matches = src_norm == tgt_norm
                
                # Detailed logging
                logger.info(f"Column '{src_col}' → '{tgt_col}':")
                logger.info(f"  Source: '{src_val}' (type: {type(src_val).__name__}) → '{src_norm}'")
                logger.info(f"  Target: '{tgt_val}' (type: {type(tgt_val).__name__}) → '{tgt_norm}'")
                logger.info(f"  Match after normalization: {'✅ YES' if matches else '❌ NO'}")
                
                if not matches:
                    # Special handling for date columns
                    if 'date' in src_col.lower():
                        logger.info(f"  ⚠️ DATE MISMATCH DETECTED!")
                        logger.info(f"    Source appears to be: {src_val}")
                        logger.info(f"    Target appears to be (timestamp): {tgt_val}")
                        
                        # Try to convert target timestamp to date
                        try:
                            # Check if it's nanoseconds (common in databases)
                            if isinstance(tgt_val, (int, np.integer)) and tgt_val > 1000000000000000000:
                                # Likely nanoseconds since epoch
                                seconds = tgt_val / 1_000_000_000
                                from datetime import datetime
                                dt = datetime.fromtimestamp(seconds)
                                logger.info(f"    Target as date (converted): {dt.strftime('%Y-%m-%d')}")
                        except Exception as e:
                            logger.info(f"    Could not convert timestamp: {e}")
            
            # Show what would be hashed for this row
            logger.info(f"\nHash components for Row {row_idx}:")
            hash_parts_source = []
            hash_parts_target = []
            
            for i in range(min(3, len(common_source_columns))):
                src_col = common_source_columns[i]
                tgt_col = common_target_columns[i]
                
                src_val = source_row[src_col] if src_col in source_row else None
                tgt_val = target_row[tgt_col] if tgt_col in target_row else None
                
                src_norm = smart_normalize_value(src_val, src_col)
                tgt_norm = smart_normalize_value(tgt_val, tgt_col)
                
                hash_parts_source.append(str(src_norm))
                hash_parts_target.append(str(tgt_norm))
            
            logger.info(f"Source hash string start: {'|'.join(hash_parts_source)}...")
            logger.info(f"Target hash string start: {'|'.join(hash_parts_target)}...")
            logger.info(f"Same? {'✅ YES' if '|'.join(hash_parts_source) == '|'.join(hash_parts_target) else '❌ NO'}")

    # Also check data types of all columns
    logger.info("\n--- Column Data Types Analysis ---")
    for i, (src_col, tgt_col) in enumerate(zip(common_source_columns[:5], common_target_columns[:5])):
        src_dtype = source_df[src_col].dtype if src_col in source_df.columns else 'N/A'
        tgt_dtype = target_df[tgt_col].dtype if tgt_col in target_df.columns else 'N/A'
        
        logger.info(f"Column {i+1}: '{src_col}' ({src_dtype}) ↔ '{tgt_col}' ({tgt_dtype})")
        
        # Check sample values
        if len(source_df) > 0 and len(target_df) > 0:
            src_sample = source_df[src_col].iloc[0] if src_col in source_df.columns else 'N/A'
            tgt_sample = target_df[tgt_col].iloc[0] if tgt_col in target_df.columns else 'N/A'
            logger.info(f"  Sample: Source='{src_sample}', Target='{tgt_sample}'")
            
        # ===== NEW: ENHANCED ROW COMPARISON WITH PAGINATION SUPPORT =====
    logger.info("PERFORMING DETAILED ROW-LEVEL COMPARISON WITH PAGINATION...")

    start_time = time.time()
    processed_count = 0

    # Count how many mismatches we have
    total_mismatches = 0
    for hash_val, info in source_hashes.items():
        if hash_val not in target_hashes:
            total_mismatches += len(info['rows'])
    for hash_val, info in target_hashes.items():
        if hash_val not in source_hashes:
            total_mismatches += len(info['rows'])

    logger.info(f"Found {total_mismatches:,} total mismatches")

    # Set a reasonable limit (adjust as needed)
    MAX_PROCESS = min(total_mismatches, 5000)  # Process max 5000 rows
    logger.info(f"Will process detailed comparisons for first {MAX_PROCESS:,} rows")

    # Helper functions
    def get_row_summary(df, row_idx, columns, dataset_label, max_cols=3):
        """Get formatted row data summary"""
        try:
            if row_idx >= len(df):
                return f"{dataset_label}: No data at row {row_idx + 2}"
            
            row_data = df.iloc[row_idx]
            summary_parts = []
            
            for i, col in enumerate(columns[:max_cols]):
                if col in df.columns:
                    value = row_data[col]
                    # Truncate long values
                    str_value = str(value)
                    if len(str_value) > 30:
                        str_value = str_value[:27] + "..."
                    summary_parts.append(f"{col}: {str_value}")
            
            summary = f"{dataset_label} Row {row_idx + 2}: " + ", ".join(summary_parts)
            if len(columns) > max_cols:
                summary += f" ... (+{len(columns) - max_cols} more columns)"
            
            return summary
        except Exception as e:
            logger.debug(f"Error getting row summary for {dataset_label} row {row_idx}: {e}")
            return f"{dataset_label} Row {row_idx + 2}: [Data extraction error]"
    
    def get_row_details(df, row_idx, columns):
        """Get detailed row data for JSON response"""
        try:
            if row_idx >= len(df):
                return None
            
            row_data = df.iloc[row_idx]
            details = {
                'excel_row': row_idx + 2,
                'dataframe_index': row_idx,
                'values': {}
            }
            
            for col in columns:
                if col in df.columns:
                    details['values'][col] = str(row_data[col])
            
            return details
        except Exception as e:
            logger.debug(f"Error getting row details for index {row_idx}: {e}")
            return None
    
    def compare_row_values(source_idx, target_idx):
        """Compare values at same row position and return difference summary"""
        try:
            if source_idx >= len(source_df) or target_idx >= len(target_df):
                return "Position mismatch"
            
            differences = []
            for col in selected_columns[:5]:  # Check first 5 columns
                target_col = get_target_column_name(col, common_cols)
                if col in source_df.columns and target_col in target_df.columns:
                    source_val = source_df[col].iloc[source_idx]
                    target_val = target_df[target_col].iloc[target_idx]
                    
                    # Normalize for comparison
                    norm_source = smart_normalize_value(source_val, col)
                    norm_target = smart_normalize_value(target_val, target_col)
                    
                    if norm_source != norm_target:
                        # Shorten values for display
                        src_str = str(source_val)[:20] + "..." if len(str(source_val)) > 20 else str(source_val)
                        tgt_str = str(target_val)[:20] + "..." if len(str(target_val)) > 20 else str(target_val)
                        differences.append(f"{col}: '{src_str}' ≠ '{tgt_str}'")
            
            if differences:
                return "; ".join(differences[:3])  # Max 3 differences
            return "Values are identical"
        except Exception as e:
            logger.debug(f"Error comparing row values: {e}")
            return "Comparison error"
    
    # Get target column names
    target_columns = [get_target_column_name(col, common_cols) for col in selected_columns]
        
    # Find matches considering duplicates
    source_hash_set = set(source_hashes.keys())
    target_hash_set = set(target_hashes.keys())
    common_hashes = source_hash_set.intersection(target_hash_set)
    
    # Calculate actual matches considering duplicates
    common_rows = 0
    for hash_val in common_hashes:
        source_count = source_hashes[hash_val]['count']
        target_count = target_hashes[hash_val]['count']
        common_rows += min(source_count, target_count)  # Only count matching pairs
    
        # ===== NEW: PROCESS MISMATCHES WITH DETAILED COMPARISON =====
    all_detailed_mismatches = []  # Store for UI response
    mismatch_counter = 0
    cli_display_limit = 10  # Show first 10 in CLI

    # Process source-only rows (hash in source but not in target)
    for hash_val, source_hash_info in source_hashes.items():
        if hash_val not in target_hashes:
            # ALL rows with this hash are mismatched
            for row_idx in source_hash_info['rows']:
                if processed_count >= MAX_PROCESS:
                    break
                processed_count += 1
                if processed_count % 500 == 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Progress: {processed_count:,}/{MAX_PROCESS:,} rows ({elapsed:.1f}s)")
                if row_idx in logged_rows:
                    logger.debug(f"Row {row_idx} already logged, skipping duplicate")
                    continue
                try:
                    mismatch_counter += 1
                    
                    # Get row data
                    source_row = source_df.iloc[row_idx]
                    
                    # Try to find corresponding target row (same position)
                    target_row = None
                    if row_idx < len(target_df):
                        target_row = target_df.iloc[row_idx]
                    
                    # Perform detailed column comparison
                    comparison_result = compare_rows_detailed(
                        source_row, 
                        target_row if target_row is not None else pd.Series(),
                        common_source_columns,  # Use common columns only
                        common_target_columns,
                        common_cols
                    )
                    
                    # Add metadata to comparison result
                    comparison_result['row_index'] = row_idx
                    comparison_result['excel_row'] = row_idx + 2
                    comparison_result['row_type'] = 'source_only'
                    comparison_result['hash_value'] = hash_val[:8]  # First 8 chars for reference
                    
                    # Log to database IMMEDIATELY
                    error_logger.log_comparison_mismatch_immediate(
                        session_id=session_id,
                        mismatch_data=comparison_result,
                        source_name=source_info,
                        target_name=target_info
                    )
                    
                    # Store for UI response (limit to reasonable amount)
                    if len(all_detailed_mismatches) < 5000:  # Keep first 1000 for immediate UI response
                        all_detailed_mismatches.append(comparison_result)
                    
                    logged_rows.add(row_idx)
                        
                    # # Format source data for display
                    # source_display = format_row_data_for_display(source_row, common_source_columns)
                    # logger.info(f"SOURCE DATA: {source_display}")
                    
                    # # Format target data for display
                    # if target_row is not None:
                    #     target_display = format_row_data_for_display(target_row, common_target_columns)
                    #     logger.info(f"TARGET DATA: {target_display}")
                    # else:
                    #     logger.info(f"TARGET DATA: ROW NOT FOUND (position {row_idx + 2})")
                    
                    # # Show specific differences
                    # if comparison_result['differences']:
                    #     logger.info(f"SPECIFIC DIFFERENCES:")
                    #     for diff in comparison_result['differences'][:5]:  # First 5 differences
                    #         logger.info(f"  • {diff['column']}: '{diff['source']}' ≠ '{diff['target']}'")
                    #     if len(comparison_result['differences']) > 5:
                    #         logger.info(f"  • ... and {len(comparison_result['differences']) - 5} more differences")
                
                except Exception as e:
                    logger.warning(f"Error processing mismatch row {row_idx}: {e}")

    # Process target-only rows (hash in target but not in source)
    for hash_val, target_hash_info in target_hashes.items():
        if hash_val not in source_hashes:
            for row_idx in target_hash_info['rows']:
                if processed_count >= MAX_PROCESS:
                    break
                processed_count += 1
                if processed_count % 500 == 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Progress: {processed_count:,}/{MAX_PROCESS:,} rows ({elapsed:.1f}s)")
                if row_idx in logged_rows:
                    logger.debug(f"Row {row_idx} already logged, skipping duplicate")
                    continue
                try:
                    mismatch_counter += 1
                    
                    # Get row data
                    target_row = target_df.iloc[row_idx]
                    
                    # Try to find corresponding source row
                    source_row = None
                    if row_idx < len(source_df):
                        source_row = source_df.iloc[row_idx]
                    
                    # Perform detailed comparison
                    comparison_result = compare_rows_detailed(
                        source_row if source_row is not None else pd.Series(),
                        target_row,
                        common_source_columns,
                        common_target_columns,
                        common_cols
                    )
                    
                    # Add metadata
                    comparison_result['row_index'] = row_idx
                    comparison_result['excel_row'] = row_idx + 2
                    comparison_result['row_type'] = 'target_only'
                    comparison_result['hash_value'] = hash_val[:8]
                    
                    # # Log to database
                    error_logger.log_comparison_mismatch_immediate(
                        session_id=session_id,
                        mismatch_data=comparison_result,
                        source_name=source_info,
                        target_name=target_info
                    )
                    
                    # Store for UI
                    if len(all_detailed_mismatches) < 5000:
                        all_detailed_mismatches.append(comparison_result)
                    
                    logged_rows.add(row_idx)
                        
                    # if source_row is not None:
                    #     source_display = format_row_data_for_display(source_row, common_source_columns)
                    #     logger.info(f"SOURCE DATA: {source_display}")
                    # else:
                    #     logger.info(f"SOURCE DATA: ROW NOT FOUND (position {row_idx + 2})")
                    
                    # target_display = format_row_data_for_display(target_row, common_target_columns)
                    # logger.info(f"TARGET DATA: {target_display}")
                        
                except Exception as e:
                    logger.warning(f"Error processing target-only row {row_idx}: {e}")

    # Summary
    if mismatch_counter > 0:
        logger.info(f"\n{'='*80}")
        logger.info(f"DETAILED MISMATCH SUMMARY:")
        logger.info(f"  Total mismatches found: {mismatch_counter}")
        logger.info(f"  Detailed comparisons logged: {len(all_detailed_mismatches)}")
        logger.info(f"  First {cli_display_limit} rows displayed above")
        logger.info(f"  All mismatches stored in database with complete data")
        if mismatch_counter > cli_display_limit:
            logger.info(f"  Use API to retrieve all {mismatch_counter} mismatches")
        logger.info(f"{'='*80}")

    # Calculate unique rows (considering duplicates)
    unique_to_source_count = 0
    for hash_val, info in source_hashes.items():
        if hash_val not in target_hashes:
            unique_to_source_count += info['count']
        else:
            # Some rows of this hash might not have matches
            source_count = info['count']
            target_count = target_hashes[hash_val]['count']
            if source_count > target_count:
                unique_to_source_count += (source_count - target_count)
    
    unique_to_target_count = 0
    for hash_val, info in target_hashes.items():
        if hash_val not in source_hashes:
            unique_to_target_count += info['count']
        else:
            # Some rows of this hash might not have matches
            source_count = source_hashes[hash_val]['count']
            target_count = info['count']
            if target_count > source_count:
                unique_to_target_count += (target_count - source_count)
    
    logger.info(f"Hash-based Comparison Results (with duplicate handling):")
    logger.info(f"  • Unique source hashes: {len(source_hash_set)}")
    logger.info(f"  • Unique target hashes: {len(target_hash_set)}")
    logger.info(f"  • Common hashes: {len(common_hashes)}")
    logger.info(f"  • Common rows (counting duplicates): {common_rows:,}")
    logger.info(f"  • Rows only in source (counting duplicates): {unique_to_source_count:,}")
    logger.info(f"  • Rows only in target (counting duplicates): {unique_to_target_count:,}")
    
    # Store ALL missing rows
    all_missing_rows = []
    for hash_val, source_hash_info in source_hashes.items():
        if hash_val not in target_hashes:
            # Add ALL rows with this hash
            all_missing_rows.extend(source_hash_info['rows'])
        else:
            # Some rows might be missing if counts don't match
            source_count = source_hash_info['count']
            target_count = target_hashes[hash_val]['count']
            if source_count > target_count:
                # Some source rows don't have matches
                missing_count = source_count - target_count
                # Take the first N rows as missing
                for i in range(missing_count):
                    if i < len(source_hash_info['rows']):
                        all_missing_rows.append(source_hash_info['rows'][i])
    
    # Store ALL extra rows  
    all_extra_rows = []
    for hash_val, target_hash_info in target_hashes.items():
        if hash_val not in source_hashes:
            # Add ALL rows with this hash
            all_extra_rows.extend(target_hash_info['rows'])
        else:
            # Some rows might be extra if counts don't match
            source_count = source_hashes[hash_val]['count']
            target_count = target_hash_info['count']
            if target_count > source_count:
                # Some target rows don't have matches
                extra_count = target_count - source_count
                # Take the first N rows as extra
                for i in range(extra_count):
                    if i < len(target_hash_info['rows']):
                        all_extra_rows.append(target_hash_info['rows'][i])
    
    # DEBUG: Check what we're sending
    print("DEBUG: About to log error. Checking error_data...")
    for key, value in locals().items():
        if key in ['source_info', 'target_info', 'session_id', 'source_file', 'target_file']:
            print(f"  {key}: {value} (type: {type(value)})")

    # Check if any of these are slice objects
    for key in ['source_info', 'target_info']:
        if key in locals() and isinstance(locals()[key], slice):
            print(f"ERROR: {key} is a slice object: {locals()[key]}")
            print(f"Stack trace: {''.join(traceback.format_stack()[-5:-1])}")
            # Convert slice to string
            locals()[key] = str(locals()[key])

    # HASH-ONLY COMPARISON - No column details for performance
    logger.info("HASH-ONLY COMPARISON COMPLETED")

    # Estimate column mismatches from hash results
    if unique_to_source_count > 0 or unique_to_target_count > 0:
        total_value_mismatches = (unique_to_source_count + unique_to_target_count) * len(selected_columns)
    else:
        total_value_mismatches = 0
    
    # Prepare mismatch details with hash results
    mismatch_details = []
    
    # Just return the counts - scoring will be calculated in generate_comparison_report
    logger.info(f"Returning counts for simple scoring:")
    logger.info(f"  Common rows: {common_rows:,}")
    logger.info(f"  Unique to source: {unique_to_source_count:,}")
    logger.info(f"  Unique to target: {unique_to_target_count:,}")
    logger.info(f"  Value mismatches: {total_value_mismatches}")
    logger.info(f"  Missing rows: {len(all_missing_rows):,}")
    logger.info(f"  Extra rows: {len(all_extra_rows):,}")
    
        # ===== PREPARE PAGINATION DATA =====
    # Store row indices for pagination
    source_only_indices = sorted(list(set(all_missing_rows)))
    target_only_indices = sorted(list(set(all_extra_rows)))
    
    # Prepare pagination metadata for mismatch_details
    mismatch_details.append({
        'type': 'hash_summary',
        'common_rows': common_rows,
        'unique_to_source': unique_to_source_count,
        'unique_to_target': unique_to_target_count,
        # 'source_only_indices': source_only_indices[:100],  # First 100
        # 'target_only_indices': target_only_indices[:100],  # First 100
        'total_source_only': len(source_only_indices),
        'total_target_only': len(target_only_indices),
        # 'pagination_info': {
        #     'source_only_api': f"/api/compare/{session_id}/rows/source_only",
        #     'target_only_api': f"/api/compare/{session_id}/rows/target_only",
        #     'default_page_size': 50
        # }
    })

    if all_detailed_mismatches:
        table_output = display_mismatch_table_cli(all_detailed_mismatches, source_label, target_label)
        logger.info(table_output)

    # ===== FIX: Return the enhanced structure correctly =====
# Create the enhanced return structure with proper dictionary
    enhanced_mismatch_details = {
        'summary': {
            'common_rows': common_rows,
            'unique_to_source': unique_to_source_count,
            'unique_to_target': unique_to_target_count,
            'total_mismatches': len(all_detailed_mismatches)
        },
        'detailed_mismatches': all_detailed_mismatches[:100],  # First 100 for immediate display
        'total_detailed_mismatches': len(all_detailed_mismatches)
    }

    # Add pagination info if available
    if mismatch_details and isinstance(mismatch_details, list) and len(mismatch_details) > 0:
        # Extract pagination from old structure
        for item in mismatch_details:
            if isinstance(item, dict) and 'type' in item and item['type'] == 'hash_summary':
                enhanced_mismatch_details['pagination'] = item
                break

    logger.info(f"Returning enhanced mismatch structure with {len(all_detailed_mismatches)} detailed mismatches")

    return common_rows, unique_to_source_count, unique_to_target_count, enhanced_mismatch_details


def analyze_data_quality(source_df, target_df, selected_columns, common_cols, error_logger, session_id, source_info, target_info):
    """Analyze data quality metrics with IMPROVED DUPLICATE COUNTING"""
    logger.info("Analyzing data quality metrics...")
    
    source_label, target_label = get_dynamic_labels(source_info, target_info)

    # Get target column names
    target_columns = [get_target_column_name(col, common_cols) for col in selected_columns]
    
    # Create normalized versions for duplicate checking
    def normalize_for_duplicates(df, columns):
        """Create normalized DataFrame for duplicate checking"""
        normalized_df = pd.DataFrame()
        
        for col in columns:
            if col in df.columns:
                # Apply normalization
                normalized_series = df[col].apply(lambda x: smart_normalize_value(x, col))
                normalized_df[col] = normalized_series
        return normalized_df
    
    # Create normalized DataFrames
    source_norm_df = normalize_for_duplicates(source_df, selected_columns)
    target_norm_df = normalize_for_duplicates(target_df, target_columns)
    
    # Null values comparison
    source_nulls = source_df[selected_columns].isnull().sum().sum()
    target_nulls = target_df[target_columns].isnull().sum().sum()
    
    total_cells_source = len(source_df) * len(selected_columns)
    total_cells_target = len(target_df) * len(selected_columns)
    
    source_null_percentage = (source_nulls / total_cells_source) * 100 if total_cells_source > 0 else 0
    target_null_percentage = (target_nulls / total_cells_target) * 100 if total_cells_target > 0 else 0
    
    logger.info(f"Null values - Source: {source_nulls} ({source_null_percentage:.2f}%), Target: {target_nulls} ({target_null_percentage:.2f}%)")
    
    null_match = source_nulls == target_nulls
    
    # Duplicate comparison with NORMALIZED data
    source_dups = source_norm_df.duplicated().sum()
    target_dups = target_norm_df.duplicated().sum()
    
    logger.info(f"Duplicate rows (normalized) - Source: {source_dups}, Target: {target_dups}")
    
    # Find duplicate samples
    if source_dups > 0:
        dup_rows = source_df[source_norm_df.duplicated(keep=False)].index.tolist()[:3]
        dup_samples = [f"Row {row + 2}" for row in dup_rows]
        logger.info(f"Source duplicate samples: {dup_samples}")
    
    if target_dups > 0:
        dup_rows = target_df[target_norm_df.duplicated(keep=False)].index.tolist()[:3]
        dup_samples = [f"Row {row + 2}" for row in dup_rows]
        logger.info(f"Target duplicate samples: {dup_samples}")
    
    # Log duplicate differences
    if source_dups != target_dups:
        error_logger.log_error({
            'session_id': session_id,
            'check_type': 'comparison',
            'source_name': source_info,
            'target_name': target_info,
            'actual_value': f"Source: {source_dups} duplicate rows (normalized)",
            'expected_value': f"Target: {target_dups} duplicate rows (normalized)",
            'error_type': 'duplicate_count_mismatch',
            'error_description': f"Duplicate count mismatch (normalized). Source={source_dups}, Target={target_dups}",
            'severity': 'low'
        })
    
    dup_match = source_dups == target_dups
    
    return null_match and dup_match

def generate_comparison_report(source_df, target_df, source_info, target_info, selected_columns, common_cols, ui_data=None):
    """Generate comprehensive comparison report with SIMPLE PERCENTAGE SCORING - DUAL MODE"""
    logger.info("GENERATING COMPREHENSIVE COMPARISON REPORT")
    
    source_label, target_label = get_dynamic_labels(source_info, target_info)
    
    logger.info(f"Report details - Source: {source_info}, Target: {target_info}, Selected columns: {len(selected_columns)}")
    
    # Create error logger
    logger.info("Initializing Error Logger...")
    error_logger = ErrorLogger()
    
    # Test database connection
    if not error_logger.test_connection():
        logger.error("Cannot connect to database. Errors will not be logged!")
    
    session_id = f"CMP_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"Session ID: {session_id}")
    
    # DEBUG: Log session ID and audit settings
    logger.debug(f"Session ID for audit: {session_id}")
    logger.debug(f"APP_SETTINGS = {APP_SETTINGS}")
    logger.debug(f"audit_enabled = {APP_SETTINGS.get('audit_enabled', False)}")
    
    # Initialize variables
    row_count_match = False
    missing_in_target = 0
    extra_in_target = 0
    data_quality_match = False
    value_mismatch_count = 0
    common_rows = 0
    unique_to_source = 0
    unique_to_target = 0
    column_structure_match = False
    column_structure_details = {}
    data_type_compatibility = True
    pk_validation_results = {}  
    metadata_results = {}       
    table_metadata_match = True 
    
    # Determine mode
    mode = 'cli'
    if ui_data:
        mode = 'ui'
        # Initialize input handler for UI mode
        if HAS_INPUT_HANDLER:
            init_input_handler(mode='ui', data=ui_data)
    
    if mode == 'cli':
        logger.info("\n" + "="*70)
        logger.info("🔄 RUNNING SOURCE-TARGET COMPARISON WITH PERFECT NORMALIZATION")
        logger.info("="*70)
        logger.info(f"Session ID: {session_id}")
        logger.info(f"Source: {source_info}")
        logger.info(f"Target: {target_info}")
        logger.info(f"Selected columns: {len(selected_columns)}")
        logger.info("="*70)
    
    # 1. FIRST: Row count check
    if mode == 'cli':
        logger.info("\n1️⃣ FIRST: ROW COUNT CHECK")
    
    source_count = len(source_df)
    target_count = len(target_df)
    
    if source_count == target_count:
        row_count_match = True
        logger.info(f"✅ Row counts match: {source_count:,} rows")
    else:
        row_count_match = False
        diff = abs(source_count - target_count)
        logger.warning(f"❌ Row count mismatch: Source={source_count:,}, Target={target_count:,}, Difference={diff:,}")
        
        if source_count > target_count:
            missing_in_target = diff
            extra_in_target = 0
            logger.warning(f"   Missing in target: {diff:,} rows")
        else:
            missing_in_target = 0
            extra_in_target = diff
            logger.warning(f"   Extra in target: {diff:,} rows")

    # ===== ADD THIS NEW SECTION =====
    # 1A. Column Structure Comparison
    if mode == 'cli':
        logger.info("\n2️⃣ COLUMN STRUCTURE COMPARISON")
    
    column_structure_results = compare_column_structures(source_df, target_df, source_info, target_info, ui_data)
    column_structure_match = column_structure_results['column_match']
    column_structure_details = column_structure_results
    
    # Log column structure errors
    if not column_structure_match:
        details = column_structure_results['details']
    
    # Get key fields from UI or auto-detect
    key_fields_from_ui = None
    if ui_data and 'key_fields' in ui_data:
        key_fields_from_ui = ui_data['key_fields']
        if isinstance(key_fields_from_ui, str):
            key_fields_from_ui = [f.strip() for f in key_fields_from_ui.split(',')]

    # ===== PRIMARY KEY VALIDATION =====
    if mode == 'cli':
        logger.info("\n3️⃣ PRIMARY KEY VALIDATION")

    # Get source and target types for better key detection
    source_type = 'database' if 'database' in str(source_info).lower() else 'file'
    target_type = 'database' if 'database' in str(target_info).lower() else 'file'

    # Add type info to UI data if not present
    validation_ui_data = ui_data.copy() if ui_data else {}
    if 'source' not in validation_ui_data:
        validation_ui_data['source'] = {}
    if 'target' not in validation_ui_data:
        validation_ui_data['target'] = {}
    validation_ui_data['source']['source_type'] = source_type
    validation_ui_data['target']['source_type'] = target_type

    # Run validation
    pk_validation_results = validate_primary_key_fields(
        source_df, target_df, key_fields_from_ui, validation_ui_data
    )

    # ===== ADD ERROR LOGGING HERE =====
    # Log PK validation errors
    status = pk_validation_results.get('validation_status')
    if status in ['KEYS_MISSING', 'DUPLICATE_VALUES']:
        # Log missing key fields
        for field in pk_validation_results['key_fields_missing']:
            error_logger.log_error({
                'session_id': session_id,
                'check_type': 'comparison',
                'source_name': source_info,
                'target_name': target_info,
                'column_name': field,
                'actual_value': f"Key field '{field}'",
                'expected_value': f"Should exist in both source and target",
                'error_type': 'key_field_missing',
                'error_description': f"Primary key field '{field}' missing in one or both datasets",
                'severity': 'high'
            })
        
        # Log duplicate key values
        for dup_info in pk_validation_results['details']['source_duplicates']:
            error_logger.log_error({
                'session_id': session_id,
                'check_type': 'comparison',
                'source_name': source_info,
                'target_name': target_info,
                'column_name': dup_info['field'],
                'actual_value': f"{dup_info['duplicate_count']} duplicate values",
                'expected_value': f"Should be unique",
                'error_type': 'duplicate_primary_key',
                'error_description': f"Primary key field '{dup_info['field']}' has {dup_info['duplicate_count']} duplicates in source",
                'severity': 'high'
            })
        
        for dup_info in pk_validation_results['details']['target_duplicates']:
            error_logger.log_error({
                'session_id': session_id,
                'check_type': 'comparison',
                'source_name': source_info,
                'target_name': target_info,
                'column_name': dup_info['field'],
                'actual_value': f"{dup_info['duplicate_count']} duplicate values",
                'expected_value': f"Should be unique",
                'error_type': 'duplicate_primary_key',
                'error_description': f"Primary key field '{dup_info['field']}' has {dup_info['duplicate_count']} duplicates in target",
                'severity': 'high'
            })
    # ===== END OF ERROR LOGGING =====

    # ===== PRIMARY KEY VALIDATION DISPLAY =====
    if mode == 'cli':
        logger.info("\n3️⃣ PRIMARY KEY / UNIQUE FIELD VALIDATION")
        logger.info("-"*50)
        
        status = pk_validation_results.get('validation_status', 'UNKNOWN')
        status_msg = pk_validation_results.get('status_message', 'No status message')
        
        # Get key fields for display
        key_fields_display = []
        for key_info in pk_validation_results.get('key_fields_found', []):
            if isinstance(key_info, dict):
                key_fields_display.append(f"{key_info.get('source_column')} ↔ {key_info.get('target_column')}")
            else:
                key_fields_display.append(str(key_info))
        
        # Display based on status
        if status == 'NO_KEYS_IDENTIFIED':
            logger.info(f"   🔍 Status: Not Applicable (no identifiable key fields)")
            logger.info(f"   📝 Message: {status_msg}")
            logger.info(f"   ⚠️  Note: Data comparison will continue without key-based validation")
            
            # Show what we tried to detect
            details = pk_validation_results.get('details', {})
            if 'source_key_candidates' in details and details['source_key_candidates']:
                logger.info(f"   🔎 Source potential keys: {details['source_key_candidates'][:3]}...")
            if 'target_key_candidates' in details and details['target_key_candidates']:
                logger.info(f"   🔎 Target potential keys: {details['target_key_candidates'][:3]}...")
            
        elif status == 'KEYS_MISSING':
            logger.info(f"   ❌ Status: Failed (key fields missing)")
            logger.info(f"   📝 Message: {status_msg}")
            
            # Show missing fields details
            for missing in pk_validation_results.get('key_fields_missing', []):
                if isinstance(missing, dict):
                    logger.info(f"   📍 Field '{missing.get('field')}':")
                    logger.info(f"        Source: {'✅ Present' if missing.get('exists_in_source') else '❌ Missing'}")
                    logger.info(f"        Target: {'✅ Present' if missing.get('exists_in_target') else '❌ Missing'}")
            
        elif status == 'DUPLICATE_VALUES':
            logger.info(f"   ❌ Status: Failed (duplicate values found)")
            logger.info(f"   📝 Message: {status_msg}")
            
            if key_fields_display:
                logger.info(f"   🔑 Validated fields: {', '.join(key_fields_display)}")
            
            # Show duplicate details
            details = pk_validation_results.get('details', {})
            
            # Source duplicates
            source_dups = details.get('source_duplicates', [])
            if source_dups:
                logger.info(f"   📊 Source duplicates found:")
                for dup in source_dups:
                    dup_field = dup.get('field', 'unknown')
                    dup_count = dup.get('duplicate_count', 0)
                    sample_vals = dup.get('sample_values', [])[:2]
                    logger.info(f"        • {dup_field}: {dup_count} duplicate rows")
                    if sample_vals:
                        logger.info(f"          Sample values: {sample_vals}")
            
            # Target duplicates
            target_dups = details.get('target_duplicates', [])
            if target_dups:
                logger.info(f"   📊 Target duplicates found:")
                for dup in target_dups:
                    dup_field = dup.get('field', 'unknown')
                    dup_count = dup.get('duplicate_count', 0)
                    sample_vals = dup.get('sample_values', [])[:2]
                    logger.info(f"        • {dup_field}: {dup_count} duplicate rows")
                    if sample_vals:
                        logger.info(f"          Sample values: {sample_vals}")
            
        elif status == 'VALIDATION_PASSED':
            logger.info(f"   ✅ Status: Passed")
            logger.info(f"   📝 Message: {status_msg}")
            
            if key_fields_display:
                logger.info(f"   🔑 Validated fields: {', '.join(key_fields_display)}")
            
            # Show validation details
            details = pk_validation_results.get('details', {})
            validation_details = details.get('validation_details', [])
            if validation_details:
                logger.info(f"   📊 Validation details:")
                for val_detail in validation_details:
                    field = val_detail.get('field', 'unknown')
                    logger.info(f"        • {field}:")
                    logger.info(f"          Source uniqueness: {'✅' if val_detail.get('unique_in_source') else '❌'}")
                    logger.info(f"          Target uniqueness: {'✅' if val_detail.get('unique_in_target') else '❌'}")
        
        else:
            # Fallback for unknown status
            logger.info(f"   ⚠️  Status: {status}")
            logger.info(f"   📝 Message: {status_msg}")
            if 'validation_passed' in pk_validation_results:
                passed = pk_validation_results['validation_passed']
                if passed is None:
                    logger.info(f"   📊 Validation: Not Applicable")
                else:
                    logger.info(f"   📊 Validation: {'✅ Passed' if passed else '❌ Failed'}")
        
        # Show detection method
        details = pk_validation_results.get('details', {})
        detection_method = details.get('key_detection_method', 'unknown')
        method_display = {
            'manual_input': 'Manually specified',
            'ui_input': 'UI specified', 
            'auto_detected': 'Auto-detected',
            'unknown': 'Unknown'
        }
        logger.info(f"   🔍 Detection method: {method_display.get(detection_method, detection_method)}")
        logger.info("-"*50)
    # ===== END OF SECTION =====

    # Display results in CLI mode
    if mode == 'cli':
        logger.info(f"   Column names match: {'✅' if column_structure_results['column_names_match'] else '❌'}")
        logger.info(f"   Column order match: {'✅' if column_structure_results['column_order_match'] else '❌'}")
        logger.info(f"   Data types match: {'✅' if column_structure_results['data_types_match'] else '❌'}")
        
        if not column_structure_match:
            details = column_structure_results['details']
            
            if details['missing_in_target']:
                logger.warning(f"   Missing in target: {details['missing_in_target']}")
            if details['missing_in_source']:
                logger.warning(f"   Missing in source: {details['missing_in_source']}")
            
            incompatible_types = [m for m in details['type_mismatches'] if not m['compatible']]
            if incompatible_types:
                logger.warning(f"   Incompatible data types: {len(incompatible_types)}")
                for mismatch in incompatible_types[:3]:  # Show first 3
                    logger.warning(f"     - {mismatch['column']}: {mismatch['source_type']} → {mismatch['target_type']}")
    # ===== END OF NEW SECTION =====
    
    # ===== ADD THIS NEW SECTION: Table Metadata Comparison =====
    # 1C. Table Metadata Comparison
    if mode == 'cli':
        logger.info("\n4️⃣ TABLE METADATA COMPARISON")

    metadata_results = compare_table_metadata(
        source_df, target_df, source_info, target_info, ui_data
    )
    table_metadata_match = metadata_results['match']

    # Log metadata differences
    # if not metadata_results['match']:
    #     for diff in metadata_results['differences']:
    #         error_logger.log_error({
    #             'session_id': session_id,
    #             'check_type': 'comparison',
    #             'source_name': source_info,
    #             'target_name': target_info,
    #             'actual_value': f"Source: {diff['source_value']}",
    #             'expected_value': f"Target: {diff['target_value']}",
    #             'error_type': f"metadata_{diff['type']}",
    #             'error_description': f"Metadata mismatch: {diff['type'].replace('_', ' ').title()}",
    #             'severity': diff.get('severity', 'low')
    #         })

    # Display results in CLI mode
    if mode == 'cli':
        logger.info(f"   Table metadata match: {'✅' if metadata_results['match'] else '❌'}")
        logger.info(f"   Source: {metadata_results['source_metadata']['row_count']:,} rows, "
                   f"{metadata_results['source_metadata']['column_count']} cols, "
                   f"{metadata_results['source_metadata']['memory_usage_mb']} MB")
        logger.info(f"   Target: {metadata_results['target_metadata']['row_count']:,} rows, "
                   f"{metadata_results['target_metadata']['column_count']} cols, "
                   f"{metadata_results['target_metadata']['memory_usage_mb']} MB")
        
        if metadata_results['differences']:
            logger.warning(f"   Found {len(metadata_results['differences'])} metadata differences:")
            for diff in metadata_results['differences'][:3]:  # Show first 3
                logger.warning(f"     • {diff['type'].replace('_', ' ').title()}: "
                             f"Source={diff['source_value']}, Target={diff['target_value']}")
    # ===== END OF SECTION =====

    # 2. SECOND: Hash-based comparison
    if mode == 'cli':
        logger.info("\n5️⃣ HASH-BASED COMPARISON")
        logger.info("ℹ️  Note: All NULL values normalized to 'NULL'")
        logger.info("ℹ️  Note: Dates normalized to YYYY-MM-DD")
        logger.info("ℹ️  Note: Strings trimmed and case-normalized")
        logger.info("ℹ️  Note: Phone numbers stripped to digits only, leading zeros removed")
    
    common_rows, unique_to_source, unique_to_target, comparison_results  = advanced_data_comparison(
    source_df, target_df, selected_columns, common_cols,
    error_logger, session_id, str(source_info), str(target_info), str(source_info), str(target_info)
    )

    # ===== FIX: Handle both old and new mismatch_details structure =====
    mismatch_details_list = []
    hash_summary = {}
    detailed_mismatches = []
    
    # FIXED: Use 'comparison_results' instead of 'mismatch_details'
    if isinstance(comparison_results, dict):
        # New structure (dictionary)
        hash_summary = comparison_results.get('pagination', {})
        detailed_mismatches = comparison_results.get('detailed_mismatches', [])
        total_detailed_mismatches = comparison_results.get('total_detailed_mismatches', 0)
        mismatch_details_list = detailed_mismatches[:20]  # For backward compatibility
        logger.info(f"New structure: {len(detailed_mismatches)} detailed mismatches")
    elif isinstance(comparison_results, list):
        # Old structure (list) - still named mismatch_details in return
        mismatch_details_list = comparison_results
        detailed_mismatches = mismatch_details_list  # Same data
        total_detailed_mismatches = len(detailed_mismatches)
        logger.info(f"Old structure: {len(mismatch_details_list)} mismatch details")
        # Try to extract hash_summary from old structure
        for item in comparison_results:
            if isinstance(item, dict) and 'type' in item and item.get('type') == 'hash_summary':
                hash_summary = item
                break
    else:
        logger.warning(f"Unknown comparison_results type: {type(comparison_results)}")
        mismatch_details_list = []
        detailed_mismatches = []
        total_detailed_mismatches = 0
    
    # 3. THIRD: Data quality analysis
    if mode == 'cli':
        logger.info("\n6️⃣ DATA QUALITY ANALYSIS")
    data_quality_match = analyze_data_quality(
        source_df, target_df, selected_columns, common_cols,
        error_logger, session_id, source_info, target_info
    )
    
    # Get errors from database
    try:
        errors = error_logger.get_error_logs_for_session(session_id, limit=100)
    except Exception as e:
        logger.warning(f"Could not get errors from database: {e}")
        errors = []
    
    if errors:
        # Count only column value mismatch errors
        mismatch_types = ['phone_mismatch', 'email_mismatch', 'numeric_mismatch', 'date_mismatch', 'value_mismatch']
        value_mismatch_count = sum(1 for error in errors if error['error_type'] in mismatch_types)
        
        # Collect mismatch details
        # mismatch_details_list = [error for error in errors if error['error_type'] in mismatch_types]
        
        # Collect missing/extra rows
        missing_rows_list = [error for error in errors if error['error_type'] in ['data_missing_in_target']]
        extra_rows_list = [error for error in errors if error['error_type'] in ['extra_data_in_target']]
    
    # ========== SIMPLE SCORING CALCULATION ==========
    # LIKE SCHOOL MARKS: (Common rows / Source rows) * 100
    
    logger.info(f"\n📊 SIMPLE SCORING CALCULATION:")
    logger.info(f"  Source rows: {source_count}")
    logger.info(f"  Common rows (matched): {common_rows}")
    
    if source_count > 0:
        # SIMPLE FORMULA: (Common rows / Source rows) * 100
        match_percentage = (common_rows / source_count) * 100
        
        # ROUND to 2 decimal places: 95.67 not 95.666666
        match_percentage = round(match_percentage, 2)
        match_rate = match_percentage  # Same value for consistency
        
        logger.info(f"  Calculation: ({common_rows} / {source_count}) * 100 = {match_percentage:.2f}%")
        logger.info(f"  Explanation: {common_rows} matched rows out of {source_count} total source rows")
    else:
        match_percentage = 0.0
        match_rate = 0.0
        logger.info("  No source rows, score is 0%")
    
    # NO CATEGORIES - JUST PERCENTAGE (Manager's instruction)
    assessment_category = ""  # Empty string
    
    source_label, target_label = get_dynamic_labels(source_info, target_info)

    # Build results dictionary with COMPLETE information
    results = {
        'session_id': session_id,
        'check_type': 'comparison',
        'row_count_match': row_count_match,
        'column_structure_match': column_structure_match, 
        'data_type_compatibility': data_type_compatibility,
        'primary_key_validation': pk_validation_results, 
        'table_metadata_comparison': metadata_results, 
        'table_metadata_match': table_metadata_match, 
        'missing_in_target': missing_in_target,
        'extra_in_target': extra_in_target,
        'common_rows': common_rows,
        'unique_to_source': unique_to_source,
        'unique_to_target': unique_to_target,
        'value_mismatch_count': value_mismatch_count,
        'data_quality_match': data_quality_match,
        'match_rate': match_rate,  
        'overall_score': match_percentage,  
        'assessment_category': assessment_category,  
        'source_info': source_info,
        'target_info': target_info,
        'selected_columns': selected_columns,
        'column_structure_details': column_structure_details,  
        'source_stats': {
            'rows': len(source_df),
            'columns': len(source_df.columns),
            'columns_list': list(source_df.columns)
        },
        'target_stats': {
            'rows': len(target_df),
            'columns': len(target_df.columns),
            'columns_list': list(target_df.columns)
        },
        'mismatches': detailed_mismatches[:20] if detailed_mismatches else [],  # Renamed from mismatch_details
        'total_mismatches': len(detailed_mismatches) if detailed_mismatches else 0,
        
        # Add hash summary if available
        'hash_summary': hash_summary if hash_summary else {},
        
        # Add pagination info
        'pagination': {
            'current_page': 1,
            'page_size': 20,
            'total_pages': max(1, (len(detailed_mismatches) + 19) // 20) if detailed_mismatches else 1,
            'total_items': len(detailed_mismatches) if detailed_mismatches else 0
        },
        # Add detailed mismatch information
        'hash_summary': hash_summary if hash_summary else {},
        
        'source_only_rows': [{'row': row_idx, 'description': f'{source_label} Row {row_idx + 2} - Not in {target_label}'} 
                        for row_idx in hash_summary.get('missing_rows', [])[:10]],
        'target_only_rows': [{'row': row_idx, 'description': f'{target_label} Row {row_idx + 2} - Not in {source_label}'} 
                    for row_idx in hash_summary.get('extra_rows', [])[:10]],
        # Add scoring explanation
        'scoring_explanation': f"Simple percentage: {common_rows} matched rows / {source_count} source rows = {match_percentage:.2f}%",
        
        # Add explanations with dynamic labels
        'explanations': {
            'identical_rows': f"Rows present in BOTH {source_label} and {target_label} with same data",
            'source_only_rows': f"Rows present ONLY in {source_label}, NOT in {target_label}",
            'target_only_rows': f"Rows present ONLY in {target_label}, NOT in {source_label}"
        }
    }

    
    # ============================================================================
    # AUDIT LOGGING
    # ============================================================================
    logger.info(f"\n=== AUDIT LOGGING SECTION ===")
    
    try:
        from dq_audit import DataQualityAudit
        
        if APP_SETTINGS.get('audit_enabled', False):
            logger.info(f"🔍 Audit logging is ENABLED in APP_SETTINGS")
            audit_instance = DataQualityAudit()
            
            # Determine source and target types
            source_type = 'csv' if 'CSV' in source_info.upper() else 'excel' if 'EXCEL' in source_info.upper() else 'database'
            target_type = 'csv' if 'CSV' in target_info.upper() else 'excel' if 'EXCEL' in target_info.upper() else 'database'
            
            # Prepare audit data
            audit_data = {
                'session_id': session_id,
                'check_type': 'comparison',
                'source_type': source_type,
                'source_name': source_info,
                'target_type': target_type, 
                'target_name': target_info,
                'source_row_count': len(source_df),
                'target_row_count': len(target_df),
                'common_row_count': common_rows,
                'match_rate': match_rate,
                'value_mismatch_count': value_mismatch_count,
                'overall_score': match_percentage,  # Simple percentage
                'assessment_category': assessment_category,  # Empty
                'check_timestamp': datetime.now(),
                'issues_summary': f"Hash matches: {common_rows}, Unique to source: {unique_to_source}, Unique to target: {unique_to_target}, Column mismatches: {value_mismatch_count}",
                # ✅ ADD THESE NEW FIELDS:
                'column_structure_match': column_structure_match,
                'primary_key_valid': pk_validation_results.get('validation_passed', False),
                'table_metadata_match': table_metadata_match,
                'total_checks': 9,  # Total number of checks performed
                'passed_checks': sum([
                                    int(row_count_match), 
                                    int(column_structure_match),
                                    1 if pk_validation_results.get('validation_passed') is True else 0,
                                    int(table_metadata_match), 
                                    int(data_quality_match)
                                ])
            }
            
            logger.info(f"Attempting to log audit record for session: {session_id}")
            
            # Log the audit record
            try:
                audit_id = audit_instance.log_audit_record(audit_data)
                if audit_id:
                    logger.info(f"✅ Audit record logged successfully (ID: {audit_id})")
                else:
                    logger.info(f"⚠️  Audit record logged but no ID returned")
                    
            except Exception as audit_error:
                logger.error(f"❌ Audit logging failed: {audit_error}")
                
                # Try fallback logging
                if APP_SETTINGS.get('fallback_logging', True):
                    logger.info("🔄 Attempting fallback logging...")
                    try:
                        # Check if fallback method exists
                        if hasattr(audit_instance, '_fallback_log_to_file'):
                            audit_instance._fallback_log_to_file(audit_data)
                            logger.info("✅ Fallback logging successful")
                        else:
                            logger.warning("⚠️  Fallback method not available")
                    except Exception as fallback_error:
                        logger.error(f"❌ Fallback logging also failed: {fallback_error}")
        else:
            logger.info("ℹ️  Audit logging is disabled in APP_SETTINGS")
            
    except ImportError as e:
        logger.warning(f"⚠️  Audit module not available: {e}")
    except Exception as e:
        logger.error(f"⚠️  Error in audit logging process: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    # ============================================================================
    # CLI MODE: Display to console
    # ============================================================================
    if mode == 'cli':
        logger.info("\n" + "="*70)
        logger.info("📊 COMPARISON RESULTS SUMMARY")
        logger.info("="*70)

        source_label, target_label = get_dynamic_labels(source_info, target_info)

        logger.info(f"Session ID: {session_id}")
        logger.info(f"Source: {source_info} ({len(source_df):,} rows)")
        logger.info(f"Target: {target_info} ({len(target_df):,} rows)")
        logger.info(f"Selected columns: {len(selected_columns)}")
        logger.info("-"*70)
        logger.info(f"1️⃣ Row count match: {'✅ Yes' if row_count_match else '❌ No'}")
        if not row_count_match:
            logger.info(f"   Missing in target: {missing_in_target:,} rows")
            logger.info(f"   Extra in target: {extra_in_target:,} rows")
        
        logger.info(f"2️⃣ Column structure match: {'✅ Yes' if column_structure_match else '❌ No'}")
        if not column_structure_match:
            details = column_structure_details['details']
            if details['missing_in_target']:
                logger.info(f"   Missing in target: {len(details['missing_in_target'])} columns")
                logger.info(f"     {details['missing_in_target'][:5]}")  # Show first 5
            if details['missing_in_source']:
                logger.info(f"   Missing in source: {len(details['missing_in_source'])} columns")
                logger.info(f"     {details['missing_in_source'][:5]}")
            if details['type_mismatches']:
                incompatible = [m for m in details['type_mismatches'] if not m['compatible']]
                logger.info(f"   Data type issues: {len(incompatible)} incompatible, {len(details['type_mismatches'])-len(incompatible)} compatible")
        # ============================

        # ===== ADD THIS RIGHT AFTER =====
        passed = pk_validation_results.get('validation_passed')
        if passed is None:
            logger.info(f"3️ Primary key validation: ⚠️ Not Applicable")
        elif passed:
            logger.info(f"3️ Primary key validation: ✅ Passed")
        else:
            logger.info(f"3️ Primary key validation: ❌ Failed")
        if pk_validation_results.get('key_fields_found'):
            logger.info(f"   Key fields: {pk_validation_results['key_fields_found']}")
        # ================================

        logger.info(f"4️ Table metadata: {'✅ Match' if table_metadata_match else '❌ Mismatch'}")
        if metadata_results.get('differences'):
            for diff in metadata_results['differences'][:2]:  # Show first 2
                logger.info(f"   • {diff['type'].replace('_', ' ').title()}: {diff['source_value']} vs {diff['target_value']}")

        logger.info(f"5️⃣ Common rows (both {source_label} and {target_label}): {common_rows:,} ({match_rate:.2f}% of {source_label})")
        logger.info(f"6️⃣ {source_label}-only rows: {unique_to_source:,} (present ONLY in {source_label}, NOT in {target_label})")
        logger.info(f"7️⃣ {target_label}-only rows: {unique_to_target:,} (present ONLY in {target_label}, NOT in {source_label})")
        logger.info(f"8️⃣ Column value mismatches: {value_mismatch_count}")
        logger.info(f"9️ Data quality match: {'✅ Yes' if data_quality_match else '❌ No'}")
        
        logger.info(f"📈 DQ Score: {match_percentage:.2f}%")  # JUST PERCENTAGE, NO LABEL
        logger.info(f"📊 Calculation: ({common_rows:,} / {source_count:,}) * 100 = {match_percentage:.2f}%")
        logger.info("="*70)
        
        # Show detailed missing row information FROM HASH RESULTS
        # Show detailed comparison results - CLEAR TERMINOLOGY
        if hash_summary:
            # Get dynamic labels
            source_label, target_label = get_dynamic_labels(source_info, target_info)
            
            # SOURCE-ONLY ROWS (in source but not in target)
            if 'missing_rows' in hash_summary and hash_summary['missing_rows']:
                source_only_count = len(hash_summary['missing_rows'])
                logger.info(f"\n🔍 SOURCE-ONLY ROWS ({source_only_count} rows):")
                logger.info(f"   Present in {source_label}, NOT in {target_label}")
                for i, row_idx in enumerate(hash_summary['missing_rows'][:5]):
                    logger.info(f"  {i+1}. {source_label} Row {row_idx + 2}")
            
            # TARGET-ONLY ROWS (in target but not in source)
            if 'extra_rows' in hash_summary and hash_summary['extra_rows']:
                target_only_count = len(hash_summary['extra_rows'])
                logger.info(f"\n🔍 TARGET-ONLY ROWS ({target_only_count} rows):")
                logger.info(f"   Present in {target_label}, NOT in {source_label}")
                for i, row_idx in enumerate(hash_summary['extra_rows'][:5]):
                    logger.info(f"  {i+1}. {target_label} Row {row_idx + 2}")
        
        # Show interpretation (simple only)
        if match_percentage == 100:
            logger.info("\n🎉 PERFECT MATCH! All data is identical after normalization.")
        elif match_percentage >= 95:
            logger.info("\n✅ HIGH MATCH! Minor differences only.")
        elif match_percentage >= 80:
            logger.info("\n⚠️  GOOD MATCH! Some differences found.")
        elif match_percentage >= 60:
            logger.info("\n⚠️  FAIR MATCH! Significant differences found.")
        else:
            logger.info("\n❌ LOW MATCH! Major differences found.")
        
        # Show normalization summary
        logger.info("\n🔍 NORMALIZATION APPLIED:")
        logger.info("   • NULL values → 'NULL' (consistent representation)")
        logger.info("   • Dates → YYYY-MM-DD format")
        logger.info("   • Strings → trimmed + lowercase")
        logger.info("   • Numeric values → standardized format")
        logger.info("   • Phone numbers → digits only, ALL leading zeros removed")
        logger.info("   • Extra whitespace removed")
        
        # Show detailed errors if any
        if unique_to_source > 0 or unique_to_target > 0 or value_mismatch_count > 0:
            logger.info(f"\n🔍 DETAILED ERROR ANALYSIS:")
            logger.info("-"*50)
            
            # Get dynamic labels
            source_label, target_label = get_dynamic_labels(source_info, target_info)
            
            if unique_to_source > 0:
                logger.info(f"📊 SOURCE-ONLY ROWS: {unique_to_source:,} rows")
                logger.info(f"   Present only in {source_label}, missing from {target_label}")
            
            if unique_to_target > 0:
                logger.info(f"📊 TARGET-ONLY ROWS: {unique_to_target:,} rows")
                logger.info(f"   Present only in {target_label}, extra (not in {source_label})")
            
            if value_mismatch_count > 0:
                logger.info(f"📊 COLUMN VALUE MISMATCHES: {value_mismatch_count}")
            
            # Get errors from database for more details
            errors = error_logger.get_errors_by_session(session_id, limit=10)
            
            if errors:
                logger.info(f"\n📋 SAMPLE ERRORS ({len(errors)} found):")
                for i, error in enumerate(errors[:3], 1):
                    error_type = error['error_type'].replace('_', ' ').title()
                    logger.info(f"\n{i}. Row {error['excel_row']} - {error_type}")
                    if error['column_name'] != 'ALL_COLUMNS':
                        logger.info(f"   Column: {error['column_name']}")
                    logger.info(f"   {error['error_description']}")
        
        # Provide SQL for detailed analysis
        logger.info(f"\n💡 For complete error details, run:")
        logger.info(f"   USE dq_checks;")
        logger.info(f"   SELECT excel_row, column_name, error_type, error_description,")
        logger.info(f"          actual_value, expected_value")
        logger.info(f"   FROM dq_error_logs")
        logger.info(f"   WHERE session_id = '{session_id}'")
        logger.info(f"   ORDER BY excel_row, column_name;")
        
        # Log overall session summary
        error_logger.log_error({
            'session_id': session_id,
            'check_type': 'comparison',
            'source_name': source_info,
            'target_name': target_info,
            'error_type': 'comparison_summary',
            'error_description': f"Comparison completed: DQ Score={match_percentage:.2f}%",
            'actual_value': f"Source: {len(source_df):,} rows, Target: {len(target_df):,} rows, Hash matches: {common_rows:,}, Column mismatches: {value_mismatch_count}",
            'expected_value': f"Perfect match: {len(source_df):,} rows",
            'severity': 'low'
        })
        
        logger.info(f"\n✅ Comparison completed! Session ID: {session_id}")
    
    # ============================================================================
    # ADD ERROR LOGS TO RESULTS FOR API MODE - ALWAYS DO THIS
    # ============================================================================
    results['error_logs'] = error_logger.get_error_logs_for_session(session_id, limit=100)
    results['error_summary'] = error_logger.get_error_summary_for_session(session_id)
    
    # ============================================================================
    # ADD AUDIT LOGS TO RESULTS
    # ============================================================================
    try:
        # Always create a new audit instance to ensure fresh connection
        from dq_audit import DataQualityAudit
        audit_logger = DataQualityAudit()
        
        # Fetch the logs
        audit_logs = audit_logger.get_audit_logs_for_session(session_id)
        
        # CRITICAL: Make sure we're adding to results
        results['audit_logs'] = audit_logs
            
    except Exception as e:
        logger.error(f"❌ Error getting audit logs: {e}")
        results['audit_logs'] = []  # Ensure it's always a list
    
    # ============================================================================
    # FINAL VERIFICATION
    # ============================================================================
    logger.info(f"\n🔍 Final results verification:")
    logger.info(f"   Session ID: {results.get('session_id')}")
    logger.info(f"   DQ Score (Simple %): {results.get('overall_score'):.2f}%")
    logger.info(f"   Source rows: {results.get('source_stats', {}).get('rows')}")
    logger.info(f"   Matched rows: {results.get('common_rows')}")
    logger.info(f"   Calculation: ({results.get('common_rows')} / {results.get('source_stats', {}).get('rows')}) * 100")
    
    # Add data for pagination endpoints - STORE IN RESULTS
    # results['_source_df_cache'] = source_df.to_dict('records') if not source_df.empty else []
    # results['_target_df_cache'] = target_df.to_dict('records') if not target_df.empty else []
    # results['_selected_columns_cache'] = selected_columns

    # # Store row indices for pagination
    # if 'hash_summary' in results:
    #     hash_summary = results['hash_summary']
    #     if 'source_only_indices' in hash_summary:
    #         results['hash_summary']['source_only_indices'] = hash_summary['source_only_indices'][:1000]  # Store first 1000
    #     if 'target_only_indices' in hash_summary:
    #         results['hash_summary']['target_only_indices'] = hash_summary['target_only_indices'][:1000]

    return results

def get_paginated_rows_data(source_df, target_df, selected_columns, common_cols, 
                           row_indices, row_type='source_only', page=1, page_size=50):
    """
    Get paginated row data with details
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        selected_columns: Selected columns for comparison
        common_cols: Common columns mapping
        row_indices: List of row indices
        row_type: 'source_only' or 'target_only'
        page: Page number (1-based)
        page_size: Number of rows per page
        
    Returns:
        dict with paginated results
    """
    if not row_indices:
        return {
            'rows': [],
            'page': page,
            'page_size': page_size,
            'total_pages': 0,
            'total_rows': 0,
            'start_index': 0,
            'end_index': 0
        }
    
    # Sort indices
    sorted_indices = sorted(row_indices)
    total_rows = len(sorted_indices)
    total_pages = (total_rows + page_size - 1) // page_size
    page = max(1, min(page, total_pages))
    
    start_index = (page - 1) * page_size
    end_index = min(start_index + page_size, total_rows)
    
    # Get paginated indices
    paginated_indices = sorted_indices[start_index:end_index]
    
    # Get detailed data for these indices
    rows_data = []
    
    for row_idx in paginated_indices:
        try:
            if row_type == 'source_only':
                # Get source row data
                source_details = get_row_details(source_df, row_idx, selected_columns)
                
                # Get corresponding target data (if exists)
                target_columns = [get_target_column_name(col, common_cols) for col in selected_columns]
                target_details = get_row_details(target_df, row_idx, target_columns)
                
                row_info = {
                    'excel_row': row_idx + 2,
                    'dataframe_index': row_idx,
                    'source_data': source_details['values'] if source_details else None,
                    'target_data': target_details['values'] if target_details else None,
                    'row_type': 'source_only',
                    'description': f"Present in source, not in target"
                }
                
            else:  # target_only
                # Get target row data
                target_columns = [get_target_column_name(col, common_cols) for col in selected_columns]
                target_details = get_row_details(target_df, row_idx, target_columns)
                
                # Get corresponding source data (if exists)
                source_details = get_row_details(source_df, row_idx, selected_columns)
                
                row_info = {
                    'excel_row': row_idx + 2,
                    'dataframe_index': row_idx,
                    'source_data': source_details['values'] if source_details else None,
                    'target_data': target_details['values'] if target_details else None,
                    'row_type': 'target_only',
                    'description': f"Present in target, not in source"
                }
            
            rows_data.append(row_info)
            
        except Exception as e:
            logger.warning(f"Error getting details for row {row_idx}: {e}")
            continue
    
    return {
        'rows': rows_data,
        'page': page,
        'page_size': page_size,
        'total_pages': total_pages,
        'total_rows': total_rows,
        'start_index': start_index,
        'end_index': end_index
    }


def get_row_details(df, row_idx, columns):
    """Get detailed row data"""
    try:
        if row_idx < 0 or row_idx >= len(df):
            return None
        
        row_data = df.iloc[row_idx]
        details = {
            'excel_row': row_idx + 2,
            'dataframe_index': row_idx,
            'values': {}
        }
        
        for col in columns:
            if col in df.columns:
                value = row_data[col]
                # Convert to string, handle NaN
                if pd.isna(value):
                    str_value = "NULL"
                else:
                    str_value = str(value)
                    # Truncate very long values
                    if len(str_value) > 100:
                        str_value = str_value[:97] + "..."
                details['values'][col] = str_value
        
        return details
    except Exception as e:
        logger.debug(f"Error getting row details for index {row_idx}: {e}")
        return None

# ============================================================================
# NEW FUNCTIONS FOR UI MODE SUPPORT
# ============================================================================

def run_comparison_analysis_ui(ui_data=None):
    """
    Run comparison analysis in UI mode
    
    Args:
        ui_data: Dictionary containing UI input data
        
    Returns:
        Dictionary with comparison results
    """
    try:
        # Initialize input handler for UI mode
        if ui_data and HAS_INPUT_HANDLER:
            init_input_handler(mode='ui', data=ui_data)
        
        logger.info("Starting comparison analysis in UI mode")
        
        # Load source and target data
        source_df, target_df, source_info, target_info, source_file, target_file = load_comparison_sources(ui_data)
        
        if source_df is None or (hasattr(source_df, 'empty') and source_df.empty) or target_df is None or (hasattr(target_df, 'empty') and target_df.empty):
            error_msg = "Failed to load source or target data"
            logger.error(error_msg)
            return {
                'error': error_msg,
                'session_id': f"CMP_ERROR_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'status': 'failed'
            }
        
        logger.info(f"Source data loaded: {len(source_df)} rows, {len(source_df.columns)} columns")
        logger.info(f"Target data loaded: {len(target_df)} rows, {len(target_df.columns)} columns")
        
        # Let user select which columns to compare
        columns_ui_data = None
        if ui_data and 'columns' in ui_data:
            columns_ui_data = ui_data['columns']
        elif ui_data and 'selected_columns' in ui_data:
            columns_ui_data = {'selected_columns': ui_data['selected_columns']}
        elif ui_data:
            # If no columns specified, use all common columns
            columns_ui_data = {'selected_columns': 'all'}
        
        selected_columns, common_cols = select_columns_for_comparison(source_df, target_df, columns_ui_data)
        
        if not selected_columns:
            error_msg = "No columns selected for comparison"
            logger.error(error_msg)
            return {
                'error': error_msg,
                'session_id': f"CMP_ERROR_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'status': 'failed'
            }
        
        logger.info(f"Selected {len(selected_columns)} columns for comparison: {selected_columns[:5]}...")
        
        # Run comparison
        results = generate_comparison_report(
            source_df, target_df, source_info, target_info, 
            selected_columns, common_cols, ui_data
        )
        
        logger.info("Comparison analysis completed successfully")
        
        # Ensure results has proper structure
        if 'error' not in results:
            results['status'] = 'completed'
            results['success'] = True
            results['comparison_score'] = results.get('overall_score', 0)
            results['match_percentage'] = results.get('overall_score', 0)
            
            # Add summary section with BOTH scores
            results['summary'] = {
                'overall_score': results.get('overall_score', 0),  # Weighted score
                'match_rate': results.get('match_rate', 0),        # Simple match rate
                'rows_compared': results.get('common_rows', 0),
                'source_rows': results.get('source_stats', {}).get('rows', 0),
                'target_rows': results.get('target_stats', {}).get('rows', 0),
                'rows_only_in_source': results.get('unique_to_source', 0),
                'rows_only_in_target': results.get('unique_to_target', 0),
                'column_mismatches': results.get('value_mismatch_count', 0),
                'assessment': results.get('assessment_category', 'UNKNOWN')
            }
        
        return results
        
    except Exception as e:
        error_msg = f"Error in comparison tool execution: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'error': error_msg,
            'session_id': f"CMP_ERROR_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'status': 'failed'
        }
    
def main(ui_data=None):
    """Main function for comparison tool - DUAL MODE"""
    logger.info("STARTING UNIVERSAL SOURCE-TARGET COMPARISON TOOL")
    
    try:
        # If UI data provided, run in UI mode
        if ui_data and HAS_INPUT_HANDLER:
            return run_comparison_analysis_ui(ui_data)
        
        # CLI Mode (original behavior)
        source_df, target_df, source_info, target_info, source_file, target_file = load_comparison_sources()
        
        if source_df is not None and target_df is not None:
            # Let user select which columns to compare
            selected_columns, common_cols = select_columns_for_comparison(source_df, target_df)
            
            if selected_columns:
                results = generate_comparison_report(source_df, target_df, source_info, target_info, selected_columns, common_cols)
                return results
        
        logger.info("Comparison analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Error in comparison tool main execution: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()