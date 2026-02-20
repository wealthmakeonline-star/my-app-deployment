# run_api.py - PROFESSIONAL VERSION WITH REQUEST LOGGING
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
import pandas as pd
import numpy as np
import os
from datetime import datetime
import logging
import uuid
import sys
import time
from logging.handlers import RotatingFileHandler
from database_navigator import DatabaseNavigator, navigate_database, get_database_hierarchy
import json

# Add parent directory to path to import your modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# SAFE BOOLEAN HANDLING FOR NUMPY
import numpy as np
import builtins

_original_bool = builtins.bool

# ==================== CUSTOM REQUEST LOGGER ====================
class RequestFormatter(logging.Formatter):
    """Custom formatter to show HTTP request details"""
    def format(self, record):
        if hasattr(record, 'method') and hasattr(record, 'path'):
            record.msg = f'{record.method} {record.path} - {record.msg}'
        return super().format(record)

# ==================== SETUP LOGGING ====================
def setup_api_logging():
    """Setup comprehensive logging for the API server"""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = f"{log_dir}/dq_api_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    if root_logger.handlers:
        root_logger.handlers.clear()
    
    # File handler for ALL logs (with rotation)
    file_handler = RotatingFileHandler(
        log_filename, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    # Console handler - Show important info and requests
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Show INFO and above
    console_format = RequestFormatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # Add handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Suppress werkzeug's default handler (we'll use our own)
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.WARNING)  # Only warnings from werkzeug
    werkzeug_logger.addHandler(file_handler)  # Log to file
    
    # Remove werkzeug's default stream handler if it exists
    for handler in werkzeug_logger.handlers[:]:
        if isinstance(handler, logging.StreamHandler) and handler is not console_handler:
            werkzeug_logger.removeHandler(handler)
    
    # Configure your DQ modules to log to file, minimal to console
    dq_module_names = [
        'dq_unified', 'dq_error_log', 'dq_audit', 
        'dq_comparison', 'dq_advanced', 'dq_rules'
    ]
    
    for module_name in dq_module_names:
        module_logger = logging.getLogger(module_name)
        module_logger.setLevel(logging.INFO)
        module_logger.addHandler(file_handler)
        # Keep only ERROR level for console from DQ modules
        module_logger.propagate = False  # Don't propagate to root
    
    # Create logger for this module
    api_logger = logging.getLogger(__name__)
    api_logger.info("🌐 API Server Logging Initialized")
    api_logger.info(f"📝 Logs saved to: {log_filename}")
    
    return api_logger

# Initialize logging
logger = setup_api_logging()

# ==================== MODULE IMPORTS ====================
try:
    # Import DQ modules
    from dq_unified import run_single_source_analysis_ui, get_dynamic_database_config, test_database_connection, get_config_file_database_config
    
    # Import other modules with fallbacks
    try:
        from dq_comparison import run_comparison_analysis_ui
    except ImportError:
        run_comparison_analysis_ui = None
        logger.warning("Comparison module not available")
    
    try:
        from dq_advanced import run_advanced_checks_ui as run_advanced_analysis_ui
    except ImportError:
        run_advanced_analysis_ui = None
        logger.warning("Advanced checks module not available")
    
    try:
        from dq_rules import run_rules_analysis_ui
    except ImportError:
        run_rules_analysis_ui = None
        logger.warning("Business rules module not available")
       
    from dq_error_log import ErrorLogger
    from dq_audit import DataQualityAudit
    from db_config import MYSQL_CONFIG

    # Import app_config safely
    try:
        from app_config import APP_SETTINGS
        logger.info(f"APP_SETTINGS loaded. Audit enabled: {APP_SETTINGS.get('audit_enabled', True)}")
    except ImportError:
        APP_SETTINGS = {'use_config_file': False, 'audit_enabled': True}
        logger.warning("APP_SETTINGS not found, using defaults")
    
    HAS_DQ_MODULES = True
    logger.info("✅ All DQ modules loaded successfully")
except ImportError as e:
    logger.critical(f"Failed to import DQ modules: {e}")
    HAS_DQ_MODULES = False
    APP_SETTINGS = {'use_config_file': False, 'audit_enabled': True}

# ==================== FLASK APP SETUP ====================
app = Flask(__name__)
CORS(app)

# Initialize components
if HAS_DQ_MODULES:
    try:
        error_logger = ErrorLogger()
        audit_logger = DataQualityAudit()
        logger.info("✅ ErrorLogger and DataQualityAudit initialized")
    except Exception as e:
        logger.error(f"Failed to initialize logging components: {e}")
        error_logger = None
        audit_logger = None
else:
    error_logger = None
    audit_logger = None

def extract_nested_data(data, prefix='', is_target=False):
    """
    Extract data from either flat or nested structure.
    
    Examples:
    Flat: {'source_type': 'csv', 'file_path': '...'}
    Nested: {'source': {'source_type': 'csv', 'file_path': '...'}}
    """
    result = {}
    
    # Determine if we're looking for source or target data
    key_prefix = 'target_' if is_target else ''
    
    # Try nested structure first
    if not is_target and 'source' in data and isinstance(data['source'], dict):
        # Source is nested: {'source': {...}}
        result = data['source'].copy()
    elif is_target and 'target' in data and isinstance(data['target'], dict):
        # Target is nested: {'target': {...}}
        result = data['target'].copy()
    else:
        # Flat structure: extract with prefix
        for key, value in data.items():
            if key.startswith(key_prefix):
                # Remove prefix for nested format
                clean_key = key[len(key_prefix):] if key_prefix else key
                result[clean_key] = value
            elif not key_prefix and not key.startswith('target_'):
                # Source fields without prefix
                result[key] = value
    
    return result

# Store API sessions in memory
sessions = {}

# ==================== REQUEST LOGGING MIDDLEWARE ====================
@app.before_request
def log_request_info():
    """Log incoming requests"""
    request.start_time = time.time()
    logger.info(f"{request.method} {request.path} - Request started")

@app.after_request
def log_response_info(response):
    """Log response details"""
    # Calculate request duration
    duration = time.time() - request.start_time
    
    # Create a custom log record with request info
    log_record = logging.LogRecord(
        name=__name__,
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg=f"Completed with status {response.status_code} in {duration:.3f}s",
        args=(),
        exc_info=None
    )
    
    # Add request info to the log record
    log_record.method = request.method
    log_record.path = request.path
    
    # Log the request completion
    logger.handle(log_record)
    
    return response

# ==================== HELPER FUNCTIONS ====================
def check_dq_modules():
    """Check if DQ modules are available"""
    if not HAS_DQ_MODULES:
        logger.error("DQ modules not available")
        return False
    return True

def convert_numpy_types(obj):
    """Convert numpy types to Python native types for JSON serialization"""
    if obj is None:
        return None
    
    # Handle numpy arrays FIRST (most important!)
    if isinstance(obj, np.ndarray):
        # Convert numpy array to list
        return obj.tolist()
    
    # Handle pandas Timestamp BEFORE checking if it's a dict
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8, np.uint64, np.uint32, np.uint16, np.uint8)): 
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32, np.float16)): 
        return float(obj)
    if isinstance(obj, np.bool_): 
        return bool(obj)
    if isinstance(obj, np.str_):
        return str(obj)
    
    if isinstance(obj, pd.Series): 
        return obj.tolist()
    if isinstance(obj, pd.DataFrame): 
        return obj.to_dict("records")
    
    try:
        if pd.isna(obj):
            return None
    except ValueError:
        # If pd.isna fails with array boolean error, it's not a single NaN value
        pass
    except Exception:
        # Any other error
        pass
    
    # Handle slices (common numpy/pandas object)
    if isinstance(obj, slice):
        return str(obj)
    
    # ✅ CRITICAL FIX: Handle dictionary BEFORE checking for numpy array attributes
    # This ensures Timestamp keys are converted
    if isinstance(obj, dict): 
        # Convert all keys to strings (especially important for Timestamp keys)
        converted_dict = {}
        for k, v in obj.items():
            # Convert key to string if it's a Timestamp
            if isinstance(k, (pd.Timestamp, datetime)):
                converted_key = k.isoformat()
            elif isinstance(k, np.generic):
                converted_key = convert_numpy_types(k)
            else:
                converted_key = k
            
            # Make sure key is string for JSON
            if not isinstance(converted_key, str):
                converted_key = str(converted_key)
            
            converted_dict[converted_key] = convert_numpy_types(v)
        return converted_dict
    
    if isinstance(obj, list): 
        return [convert_numpy_types(x) for x in obj]
    if isinstance(obj, tuple):
        return tuple(convert_numpy_types(x) for x in obj)
    
    if isinstance(obj, set):
        return [convert_numpy_types(x) for x in obj]
    
    # Handle other numpy objects that might have boolean evaluation issues
    try:
        # Check if it has numpy array attributes
        if hasattr(obj, 'dtype') and hasattr(obj, 'shape'):
            # This looks like a numpy array-like object
            return obj.tolist()
    except:
        pass
    return obj

def prepare_ui_data_for_analysis(request_data, analysis_type):
    """Prepare UI data for analysis modules"""
    ui_data = {'analysis_type': analysis_type}
    

    # ADD THIS: Get mandatory fields from request data
    mandatory_fields = request_data.get('mandatory_fields', '')
    if mandatory_fields:
        ui_data['mandatory_fields'] = mandatory_fields
    
    if analysis_type == 'single':
        ui_data['source_type'] = request_data.get('source_type', '').lower()
        # Single Source Analysis
        if ui_data['source_type'] == 'database':
            ui_data['db_config'] = {
                'type': request_data.get('db_type', 'mysql').lower(),   
                'host': request_data.get('host', 'localhost'),           
                'port': int(request_data.get('port', 3306)),             
                'database': request_data.get('database'),                
                'schema': request_data.get('schema', ''),                
                'table': request_data.get('table'),                      
                'user': request_data.get('user'),                        
                'password': request_data.get('password')
            }
            # NORMALIZE THE CONFIG HERE (Add this!)
            ui_data['db_config'] = normalize_database_config(ui_data['db_config'])

            if ui_data['db_config']['type'] == 'sqlite':
                ui_data['db_config']['file_path'] = request_data.get('file_path')
                # Remove unnecessary fields for SQLite
                for field in ['host', 'port', 'user', 'password', 'database']:
                    if field in ui_data['db_config']:  # ✅ FIX: Use ui_data['db_config']
                        del ui_data['db_config'][field]
            elif ui_data['db_config']['type'] == 'oracle':
                ui_data['db_config']['service_name'] = request_data.get('service_name', 'XE')
                ui_data['db_config']['encoding'] = 'UTF-8'
        
        elif ui_data['source_type'] == 'csv':
            file_path = request_data.get('file_path') or request_data.get('value')
            if isinstance(file_path, dict):
                file_path = file_path.get('value') or file_path.get('filepath')
            ui_data['file_path'] = file_path
            ui_data['mandatory_fields'] = request_data.get('mandatory_fields', '')
        
        elif ui_data['source_type'] == 'excel':
            # Excel file handling
            file_path = request_data.get('file_path') or request_data.get('value')
            if isinstance(file_path, dict):
                file_path = file_path.get('value') or file_path.get('filepath') or file_path.get('full_path')
            
            ui_data['file_path'] = file_path
            ui_data['sheet_name'] = request_data.get('sheet_name', '')
            ui_data['mandatory_fields'] = request_data.get('mandatory_fields', '')
    
    elif analysis_type == 'comparison':
    
        # If data has 'source' key, it's nested structure
        has_nested_source = 'source' in request_data and isinstance(request_data['source'], dict)
        has_nested_target = 'target' in request_data and isinstance(request_data['target'], dict)
    
        # ===== STEP 2: EXTRACT SOURCE DATA =====
        if has_nested_source:
            # Nested structure: {'source': {...}}
            source_data = request_data['source']
            logger.info(f"DEBUG source_data from nested: {source_data}")
            source_type = source_data.get('source_type', '').lower()
            
            # FIX: Keep ALL source data
            ui_data['source'] = source_data.copy()
            
        else:
            # Flat structure: {'source_type': 'csv', ...}
            source_data = request_data
            logger.info(f"DEBUG source_data from flat: {source_data}")
            source_type = request_data.get('source_type', '').lower()
            
            # FIX: Create source dict with all fields
            ui_data['source'] = {'source_type': source_type}
            # Copy other source fields
            for key in ['db_type', 'host', 'port', 'database', 'table', 'user', 'password', 'file_path', 'source_file_path']:
                if key in request_data:
                    ui_data['source'][key] = request_data[key]

        # ===== STEP 3: EXTRACT TARGET DATA =====
        if has_nested_target:
            # Nested structure: {'target': {...}}
            target_data = request_data['target']
            target_source_type = target_data.get('source_type', '').lower()
            
            # FIX: Keep ALL target data
            ui_data['target'] = target_data.copy()
            
        else:
            # Flat structure
            target_data = request_data
            #target_source_type = request_data.get('target_source_type', '').lower()
            
            target_source_type = request_data.get('target_source_type', '') or request_data.get('target', {}).get('source_type', '').lower()

            # FIX: Create target dict with target_ prefixed fields
            ui_data['target'] = {'source_type': target_source_type}
            # Copy target_ prefixed fields
            for key, value in request_data.items():
                if key.startswith('target_'):
                    ui_data['target'][key[7:]] = value  # Remove 'target_' prefix
        
        if ui_data['source']['source_type'] == 'database':
            ui_data['source']['db_config'] = {
                'type': source_data.get('db_type', 'mysql').lower(),
                'host': source_data.get('host', 'localhost'),
                'port': int(source_data.get('port', 3306)),
                'database': source_data.get('database'),
                'table': source_data.get('table'),
                'user': source_data.get('user'),
                'password': source_data.get('password')
            }
            
            if ui_data['source']['db_config']['type'] == 'sqlite':
                ui_data['source']['db_config']['file_path'] = source_data.get('file_path')
            elif ui_data['source']['db_config']['type'] == 'oracle':
                ui_data['source']['db_config']['service_name'] = source_data.get('service_name', 'XE')
                ui_data['source']['db_config']['encoding'] = 'UTF-8'
        
        elif ui_data['source']['source_type'] == 'csv':
            file_path = source_data.get('source_file_path') or source_data.get('file_path')

            # Add this: Also check root level for flat structure
            if not file_path and 'source_file_path' in request_data:
                file_path = request_data['source_file_path']

            if isinstance(file_path, dict):
                file_path = file_path.get('value') or file_path.get('filepath')
            
            if file_path:
                ui_data['source']['file_path'] = file_path
                ui_data['source']['source_file_path'] = file_path  # Add this too!
            else:
                logger.warning(f"No file path found for source CSV")
        
        elif ui_data['source']['source_type'] == 'excel':
            file_path = source_data.get('source_file_path') or source_data.get('file_path')

            # Add this: Also check root level for flat structure
            if not file_path and 'source_file_path' in request_data:
                file_path = request_data['source_file_path']
            
            if isinstance(file_path, dict):
                file_path = file_path.get('value') or file_path.get('filepath')
            
            if file_path:
                ui_data['source']['file_path'] = file_path
                ui_data['source']['source_file_path'] = file_path  # Add this too!
                ui_data['source']['sheet_name'] = source_data.get('source_sheet_name')
            else:
                logger.warning(f"No file path found for source Excel")
        
        # Target configuration
        target_source_type = request_data.get('target_source_type', '').lower()
        
        if target_source_type == 'database':
            ui_data['target']['db_config'] = {
                    'type': target_data.get('target_db_type', target_data.get('db_type', 'mysql')).lower(),    
                    'host': target_data.get('target_host', target_data.get('host', 'localhost')),              
                    'port': int(target_data.get('target_port', target_data.get('port', 3306))),                
                    'database': target_data.get('target_database', target_data.get('database')),               
                    'user': target_data.get('target_user', target_data.get('user')),                           
                    'password': target_data.get('target_password', target_data.get('password')),               
                    'table': target_data.get('target_table', target_data.get('table'))                         
                }
            
            if ui_data['target']['db_config']['type'] == 'sqlite':
                ui_data['target']['db_config']['file_path'] = target_data.get('target_file_path', target_data.get('file_path'))
            elif ui_data['target']['db_config']['type'] == 'oracle':
                ui_data['target']['db_config']['service_name'] = target_data.get('target_service_name', target_data.get('service_name', 'XE'))
                ui_data['target']['db_config']['encoding'] = 'UTF-8'
            
        elif target_source_type == 'csv':
            file_path = target_data.get('target_file_path', target_data.get('file_path'))
            
            # Add this: Also check root level for flat structure
            if not file_path and 'target_file_path' in request_data:
                file_path = request_data['target_file_path']
            
            if isinstance(file_path, dict):
                file_path = file_path.get('value') or file_path.get('filepath')
            if file_path:
                ui_data['target']['file_path'] = file_path
                ui_data['target']['target_file_path'] = file_path
            else:
                logger.warning(f"No file path found for target CSV")
        
        elif target_source_type == 'excel':
            file_path = target_data.get('target_file_path', target_data.get('file_path'))

            # Add this: Also check root level for flat structure  
            if not file_path and 'target_file_path' in request_data:
                file_path = request_data['target_file_path']

            if isinstance(file_path, dict):
                file_path = file_path.get('value') or file_path.get('filepath')

            if file_path:  # Only set if we got a valid file path
                ui_data['target']['file_path'] = file_path
                ui_data['target']['target_file_path'] = file_path
                ui_data['target']['sheet_name'] = target_data.get('target_sheet_name', target_data.get('sheet_name'))
            else:
                logger.warning(f"No file path found for target Excel")
        
        # Columns selection
        ui_data['columns'] = {}
        selected_cols = request_data.get('selected_columns', [])
        if isinstance(selected_cols, str):
            if selected_cols.lower() == 'all':
                ui_data['columns']['selected_columns'] = 'all'
            else:
                ui_data['columns']['selected_columns'] = [col.strip() for col in selected_cols.split(',')]
        else:
            ui_data['columns']['selected_columns'] = selected_cols
    
    elif analysis_type == 'advanced':
            # Extract source_type from different possible locations
        if 'source_type' in request_data:
            ui_data['source_type'] = request_data['source_type']
        elif 'source' in request_data and isinstance(request_data['source'], dict) and 'source_type' in request_data['source']:
            ui_data['source_type'] = request_data['source']['source_type']
        else:
            # Default or error
            ui_data['source_type'] = 'unknown'
        # Advanced Checks
        if ui_data['source_type'] == 'database':
            ui_data['db_config'] = {
                'type': request_data.get('db_type', 'mysql').lower(),
                'host': request_data.get('host', 'localhost'),
                'port': int(request_data.get('port', 3306)),
                'database': request_data.get('database'),
                'user': request_data.get('user'),
                'password': request_data.get('password'),
                'table': request_data.get('table')
            }
        
        elif ui_data['source_type'] == 'csv':
            file_path = request_data.get('file_path') or request_data.get('value')
            if isinstance(file_path, dict):
                file_path = file_path.get('value') or file_path.get('filepath')
            ui_data['file_path'] = file_path
        
        elif ui_data['source_type'] == 'excel':
            file_path = request_data.get('file_path') or request_data.get('value')
            if isinstance(file_path, dict):
                file_path = file_path.get('value') or file_path.get('filepath')
            ui_data['file_path'] = file_path
            ui_data['sheet_name'] = request_data.get('sheet_name')
        
        # FIXED: Better columns parsing
        columns_input = request_data.get('columns_to_check', [])
        logger.info(f"DEBUG: Received columns_input: {repr(columns_input)} (type: {type(columns_input)})")
        
        if isinstance(columns_input, str):
            # Clean the string
            columns_input = columns_input.strip()
            
            # Check if it's a Python list string
            if columns_input.startswith('[') and columns_input.endswith(']'):
                # Remove brackets and parse
                try:
                    # Try to parse as JSON first
                    import json
                    ui_data['columns_to_check'] = json.loads(columns_input)
                    logger.info(f"DEBUG: Parsed as JSON list: {ui_data['columns_to_check']}")
                except json.JSONDecodeError:
                    # Try ast.literal_eval
                    try:
                        import ast
                        ui_data['columns_to_check'] = ast.literal_eval(columns_input)
                        logger.info(f"DEBUG: Parsed with ast.literal_eval: {ui_data['columns_to_check']}")
                    except:
                        # Manual parsing
                        clean_str = columns_input[1:-1]  # Remove brackets
                        ui_data['columns_to_check'] = [
                            col.strip().strip("'\"") 
                            for col in clean_str.split(',') 
                            if col.strip()
                        ]
                        logger.info(f"DEBUG: Manually parsed: {ui_data['columns_to_check']}")
            elif columns_input.lower() == 'all':
                ui_data['columns_to_check'] = 'all'
                logger.info(f"DEBUG: Set to 'all'")
            else:
                # Simple comma-separated
                ui_data['columns_to_check'] = [
                    col.strip() 
                    for col in columns_input.split(',') 
                    if col.strip()
                ]
                logger.info(f"DEBUG: Parsed as comma-separated: {ui_data['columns_to_check']}")
        else:
            # Already a list or other iterable
            ui_data['columns_to_check'] = columns_input
            logger.info(f"DEBUG: Using as-is: {ui_data['columns_to_check']}")
        
        ui_data['check_whitespace'] = request_data.get('check_whitespace', True)
        ui_data['check_zero_padding'] = request_data.get('check_zero_padding', True)
        ui_data['check_numeric'] = request_data.get('check_numeric', True)
        
    elif analysis_type == 'rules':
        # Option 1: From nested structure
        if 'source' in request_data and isinstance(request_data['source'], dict):
            source_data = request_data['source']
            ui_data['source_type'] = source_data.get('source_type', '').lower()
        # Option 2: From flat structure
        elif 'source_type' in request_data:
            ui_data['source_type'] = request_data['source_type'].lower()
        else:
            logger.error("ERROR: source_type not found in request_data for rules analysis")
            ui_data['source_type'] = 'unknown'
        
        logger.info(f"DEBUG Rules: source_type={ui_data['source_type']}")

        if ui_data['source_type'] == 'database':
            # Get db_config from nested or flat structure
            db_config_data = request_data.get('db_config', {})
            if not db_config_data and 'source' in request_data:
                db_config_data = request_data['source'].get('db_config', {})
            
            ui_data['db_config'] = {
                'type': db_config_data.get('db_type', db_config_data.get('type', 'mysql')).lower(),
                'host': db_config_data.get('host', 'localhost'),
                'port': int(db_config_data.get('port', 3306)),
                'database': db_config_data.get('database'),
                'table': db_config_data.get('table'),
                'user': db_config_data.get('user'),
                'password': db_config_data.get('password'),
                'schema': db_config_data.get('schema', '')
            }
            
            if ui_data['db_config']['type'] == 'sqlite':
                ui_data['db_config']['file_path'] = db_config_data.get('file_path')
                # Remove unnecessary fields for SQLite
                for field in ['host', 'port', 'user', 'password', 'database']:
                    if field in ui_data['db_config']:
                        del ui_data['db_config'][field]
            elif ui_data['db_config']['type'] == 'oracle':
                ui_data['db_config']['service_name'] = db_config_data.get('service_name', 'XE')
                ui_data['db_config']['encoding'] = 'UTF-8'
        
        # File-based sources
        elif ui_data['source_type'] in ['csv', 'excel']:
            # Get file path from different locations
            file_path = None
            
            # Check nested structure first
            if 'source' in request_data and isinstance(request_data['source'], dict):
                file_path = request_data['source'].get('file_path') or request_data['source'].get('value')
            # Check flat structure
            else:
                file_path = request_data.get('file_path') or request_data.get('value')
            
            # Handle dictionary file paths
            if isinstance(file_path, dict):
                file_path = file_path.get('value') or file_path.get('filepath') or file_path.get('full_path')
            
            if file_path:
                ui_data['file_path'] = file_path
                logger.info(f"DEBUG Rules: File path set to {file_path}")
            else:
                logger.error("ERROR: No file path found for rules analysis")
            
            if ui_data['source_type'] == 'excel':
                ui_data['sheet_name'] = request_data.get('sheet_name', '')
        
        # Rule type selection
        ui_data['rule_type'] = request_data.get('rule_type', '1')
        
        # Individual rule settings (if provided)
        if 'threshold_rules' in request_data:
            ui_data['threshold_rules'] = request_data['threshold_rules']
        if 'pattern_rules' in request_data:
            ui_data['pattern_rules'] = request_data['pattern_rules']
        if 'range_rules' in request_data:
            ui_data['range_rules'] = request_data['range_rules']
        if 'kpi_rules' in request_data:
            ui_data['kpi_rules'] = request_data['kpi_rules']
        
        # Text comparison settings
        ui_data['run_text_comparison'] = request_data.get('run_text_comparison', False)
        if ui_data['run_text_comparison']:
            ui_data['actual_column'] = request_data.get('actual_column')
            ui_data['expected_column'] = request_data.get('expected_column')
        
        # Metadata rules flag
        ui_data['use_metadata_rules'] = request_data.get('use_metadata_rules', False)
        
        # Column selections for specific rules
        if 'threshold_column' in request_data:
            ui_data['threshold_column'] = request_data['threshold_column']
        if 'pattern_column' in request_data:
            ui_data['pattern_column'] = request_data['pattern_column']
        if 'range_column' in request_data:
            ui_data['range_column'] = request_data['range_column']
        
        # KPI rule settings
        if 'kpi_type' in request_data:
            ui_data['kpi_type'] = request_data['kpi_type']
        if 'kpi_column' in request_data:
            ui_data['kpi_column'] = request_data['kpi_column']
        if 'kpi_threshold_value' in request_data:
            ui_data['kpi_threshold_value'] = request_data['kpi_threshold_value']
        
        logger.info(f"DEBUG Rules: Prepared UI data with {len(ui_data)} fields")
    
    logger.debug(f"Prepared UI data for {analysis_type} analysis")
    return ui_data

def validate_rules_request(request_data):
    """Validate business rules request parameters"""
    errors = []
    
    # Check source_type
    source_type = None
    if 'source' in request_data and isinstance(request_data['source'], dict):
        source_type = request_data['source'].get('source_type', '').lower()
    elif 'source_type' in request_data:
        source_type = request_data['source_type'].lower()
    
    if not source_type or source_type not in ['csv', 'excel', 'database']:
        errors.append("Invalid or missing source_type. Must be 'csv', 'excel', or 'database'")
    
    # Validate source-specific fields
    if source_type == 'database':
        # Check for db_config
        db_config = request_data.get('db_config', {})
        if not db_config and 'source' in request_data:
            db_config = request_data['source'].get('db_config', {})
        
        if not db_config:
            errors.append("Database configuration (db_config) is required for database source")
        else:
            # Check required fields
            if 'table' not in db_config and 'table' not in request_data:
                errors.append("Table name is required for database source")
    
    elif source_type in ['csv', 'excel']:
        # Check for file_path
        file_path = None
        if 'source' in request_data and isinstance(request_data['source'], dict):
            file_path = request_data['source'].get('file_path') or request_data['source'].get('value')
        else:
            file_path = request_data.get('file_path') or request_data.get('value')
        
        if not file_path:
            errors.append(f"File path is required for {source_type} source")
    
    return errors

def normalize_database_config(db_config):
    """
    Ensure all database configs have consistent schema field
    This is CRITICAL for API mode!
    """
    if not db_config or 'type' not in db_config:
        return db_config
    
    db_type = db_config['type'].lower()
    
    # Ensure schema field exists and is appropriate for each database type
    if 'schema' not in db_config or not db_config['schema']:
        if db_type == 'postgresql':
            db_config['schema'] = 'public'
        elif db_type == 'mysql':
            db_config['schema'] = db_config.get('database', '')  # MySQL uses database as schema
        elif db_type == 'oracle':
            db_config['schema'] = db_config.get('user', db_config.get('database', ''))  # Oracle uses username as schema
        elif db_type == 'sqlserver':
            db_config['schema'] = 'dbo'
        elif db_type == 'sqlite':
            db_config['schema'] = 'main'
    
    # ✅ CORRECTED: Different validation for different database types
    if db_type == 'sqlite':
        # SQLite only needs file_path and table
        required_fields = ['type', 'file_path', 'table']
        for field in required_fields:
            if field not in db_config:
                logger.warning(f"SQLite missing field '{field}' in database config.")
                if field == 'file_path':
                    # Can't set default for file_path - it's required
                    continue
    else:
        # For all other databases (MySQL, PostgreSQL, Oracle, SQL Server)
        required_fields = ['type', 'host', 'port', 'database', 'table', 'user', 'password']
        
        # Set sensible defaults for missing fields
        for field in required_fields:
            if field not in db_config:
                logger.warning(f"Missing field '{field}' in database config. Setting default.")
                if field == 'port':
                    # Default ports by database type
                    if db_type == 'mysql':
                        db_config[field] = 3306
                    elif db_type == 'postgresql':
                        db_config[field] = 5432
                    elif db_type == 'oracle':
                        db_config[field] = 1521
                    elif db_type == 'sqlserver':
                        db_config[field] = 1433
                elif field == 'host':
                    db_config[field] = 'localhost'
                elif field == 'user':
                    db_config[field] = 'root' if db_type == 'mysql' else 'postgres' if db_type == 'postgresql' else 'system'
                elif field == 'password':
                    db_config[field] = ''  # Empty password as default
                elif field == 'database':
                    # Can't set default for database - it's required
                    continue
    
    # ✅ ADD THIS: Ensure table field is always present (most critical!)
    if 'table' not in db_config:
        logger.error("Table field is missing in database config!")
        # Don't set default for table - it MUST be provided by user
    
    # ✅ ADD THIS: Special handling for Oracle
    if db_type == 'oracle' and 'service_name' not in db_config:
        db_config['service_name'] = 'XE'
        db_config['encoding'] = 'UTF-8'
    
    return db_config

# ==================== FLASK ERROR HANDLERS ====================
@app.errorhandler(400)
def bad_request(error):
    """Handle 400 Bad Request errors"""
    logger.warning(f"Bad Request: {error}")
    return jsonify({
        'status': 400,
        'message': 'Bad Request',
        'error': str(error) if str(error) else 'Invalid request parameters'
    }), 400

@app.errorhandler(404)
def not_found(error):
    """Handle 404 Not Found errors"""
    logger.warning(f"Not Found: {error}")
    return jsonify({
        'status': 404,
        'message': 'Not Found',
        'error': 'The requested resource was not found'
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    """Handle 405 Method Not Allowed errors"""
    logger.warning(f"Method Not Allowed: {error}")
    return jsonify({
        'status': 405,
        'message': 'Method Not Allowed',
        'error': 'This method is not allowed for the requested URL'
    }), 405

@app.errorhandler(500)
def internal_server_error(error):
    """Handle 500 Internal Server Error"""
    logger.error(f"Internal Server Error: {error}")
    return jsonify({
        'status': 500,
        'message': 'Internal Server Error',
        'error': 'An unexpected error occurred on the server'
    }), 500

@app.errorhandler(503)
def service_unavailable(error):
    """Handle 503 Service Unavailable"""
    logger.error(f"Service Unavailable: {error}")
    return jsonify({
        'status': 503,
        'message': 'Service Unavailable',
        'error': 'The service is temporarily unavailable'
    }), 503

# ==================== API ENDPOINTS ====================
@app.route('/')
def home():
    """Home endpoint - API status"""
    return jsonify({
        'status': 200,
        'message': 'Data Quality Framework API is running',
        'version': '1.0.0',
        'modules_available': HAS_DQ_MODULES,
        'timestamp': datetime.now().isoformat(),
        'comparison_endpoints': {
            'run_comparison': 'POST /api/compare',
            'get_results': 'GET /api/compare/{session_id}',
            'mismatches': 'GET /api/compare/{session_id}/mismatches?page=1&page_size=20',
            'mismatch_summary': 'GET /api/compare/{session_id}/mismatches/summary',
            # 'source_only_rows': 'GET /api/compare/{session_id}/rows/source_only?page=1&page_size=20',
            # 'target_only_rows': 'GET /api/compare/{session_id}/rows/target_only?page=1&page_size=20',
            'row_counts': 'GET /api/compare/{session_id}/rows/count'
        }
    }), 200

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 200,
        'message': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'modules_available': HAS_DQ_MODULES,
        'sessions_active': len(sessions)
    }), 200

@app.route('/api/test-db-connection', methods=['POST'])
def test_db_connection():
    """Test database connection"""
    if not check_dq_modules():
        return jsonify({
            'status': 503,
            'message': 'Service Unavailable',
            'error': 'DQ modules not available'
        }), 503
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Request body is required'
            }), 400
        
        use_config_file = data.get('use_config_file', False)
        db_type = data.get('db_type')
        
        if not db_type:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Missing db_type parameter'
            }), 400
        
        if use_config_file:
            db_type_map = {
                'mysql': '2',
                'postgresql': '1',
                'oracle': '3',
                'sqlserver': '4',
                'sqlite': '5'
            }
            
            db_type_choice = db_type_map.get(db_type.lower())
            if not db_type_choice:
                return jsonify({
                    'status': 400,
                    'message': 'Bad Request',
                    'error': 'Invalid database type'
                }), 400
            
            db_config = get_config_file_database_config(db_type_choice)
            if not db_config:
                return jsonify({
                    'status': 400,
                    'message': 'Bad Request',
                    'error': 'Failed to get config from file'
                }), 400
            
            success = test_database_connection(db_config)
            message = f'{db_type.upper()} connection from config file {"successful" if success else "failed"}'
        else:
            db_config = {'type': db_type}
            
            if db_type == 'sqlite':
                db_config['file_path'] = data.get('file_path')
                if not db_config['file_path']:
                    return jsonify({
                        'status': 400,
                        'message': 'Bad Request',
                        'error': 'Missing file_path for SQLite'
                    }), 400
            else:
                required_fields = ['database', 'user', 'password']
                missing_fields = [field for field in required_fields if not data.get(field)]
                if missing_fields:
                    return jsonify({
                        'status': 400,
                        'message': 'Bad Request',
                        'error': f'Missing required fields: {", ".join(missing_fields)}'
                    }), 400
                
                db_config['host'] = data.get('host', 'localhost')
                db_config['port'] = int(data.get('port', 3306))
                db_config['database'] = data.get('database')
                db_config['user'] = data.get('user')
                db_config['password'] = data.get('password')
                
                if db_type == 'oracle':
                    db_config['service_name'] = data.get('service_name', 'XE')
                    db_config['encoding'] = 'UTF-8'
            
            success = test_database_connection(db_config)
            message = f'{db_type.upper()} connection {"successful" if success else "failed"}'
        
        logger.info(f"Database connection test: {message}")
        return jsonify({
            'status': 200,
            'success': success,
            'message': message,
            'mode': 'config_file' if use_config_file else 'dynamic'
        }), 200
        
    except Exception as e:
        logger.error(f"Error in test-db-connection: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

@app.route('/api/database-hierarchy', methods=['POST'])
def get_database_hierarchy_api():
    """
    Get database hierarchy STEP-BY-STEP for UI dropdowns
    
    Expected JSON:
    {
        "db_type": "mysql",
        "db_config": {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "root"
        },
        "database": "optional_selected_database",  # If provided, get schemas
        "schema": "optional_selected_schema"       # If provided, get tables
    }
    
    Returns:
    Case 1: No database selected → {"databases": ["db1", "db2"]}
    Case 2: Database selected, no schema → {"schemas": ["schema1", "schema2"]}
    Case 3: Database + schema selected → {"tables": ["table1", "table2"]}
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Request body is required'
            }), 400
        
        db_type = data.get('db_type')
        db_config = data.get('db_config')
        
        if not db_type or not db_config:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Missing db_type or db_config'
            }), 400
        
        logger.info(f"Getting database hierarchy for {db_type}")
        logger.info(f"Database: {data.get('database', 'Not provided')}")
        logger.info(f"Schema: {data.get('schema', 'Not provided')}")
        
        # Get optional parameters
        database = data.get('database')
        schema = data.get('schema')
        
        # Step 1: Normalize and connect
        db_config_normalized = normalize_database_config(db_config.copy())

        # Step 2: Create DatabaseNavigator
        navigator = DatabaseNavigator(mode='ui', ui_data=db_config_normalized)
        navigator.db_type = db_type
        
        # For MySQL, we need to connect without specific database first
        if db_type == 'mysql' and not database:
            # MySQL needs to connect without database first to get database list
            db_config_for_connection = db_config_normalized.copy()
            if 'database' in db_config_for_connection:
                del db_config_for_connection['database']
        else:
            db_config_for_connection = db_config_normalized
        
        logger.info(f"Connecting with config: {db_config_for_connection}")
        
        if not navigator.connect_to_database(db_type, db_config_for_connection):
            return jsonify({
                'status': 500,
                'message': 'Database Error',
                'error': 'Connection failed'
            }), 500
        
        # Step 3: Determine what to return
        if not database:
            # Get databases list
            logger.info("Getting databases list...")
            databases = navigator.get_databases()
            logger.info(f"Found {len(databases)} databases")
            
            return jsonify({
                'status': 200,
                'message': 'Databases retrieved',
                'data': {'databases': databases}
            }), 200
            
        elif database and not schema:
            # Get schemas for selected database
            logger.info(f"Getting schemas for database: {database}")
            
            # For PostgreSQL/Redshift, reconnect to selected database
            if db_type in ['postgresql', 'redshift']:
                if not navigator.reconnect_to_database(database):
                    return jsonify({
                        'status': 500,
                        'message': 'Database Error',
                        'error': f'Failed to connect to database: {database}'
                    }), 500
            else:
                # For other databases, just select it
                navigator.selected_database = database
            
            schemas = navigator.get_schemas()
            logger.info(f"Found {len(schemas)} schemas")
            
            return jsonify({
                'status': 200,
                'message': 'Schemas retrieved',
                'data': {'schemas': schemas}
            }), 200
            
        elif database and schema:
            # Get tables for selected schema
            logger.info(f"Getting tables for database: {database}, schema: {schema}")
            
            # Set selections
            navigator.selected_database = database
            navigator.selected_schema = schema
            
            tables = navigator.get_tables()
            logger.info(f"Found {len(tables)} tables")
            
            return jsonify({
                'status': 200,
                'message': 'Tables retrieved',
                'data': {'tables': tables}
            }), 200
            
        else:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Invalid parameter combination'
            }), 400
        
    except Exception as e:
        logger.error(f"Error getting database hierarchy: {str(e)}", exc_info=True)
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500
    
@app.route('/api/check-single', methods=['POST'])
def check_single_source():
    """Single source analysis"""
    if not check_dq_modules():
        return jsonify({
            'status': 503,
            'message': 'Service Unavailable',
            'error': 'DQ modules not available'
        }), 503
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Request body is required'
            }), 400
        
        logger.info("Single source analysis request received")
        
        # Prepare UI data
        ui_data = prepare_ui_data_for_analysis(data, 'single')
        
        # Generate session ID
        session_id = f"SINGLE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        logger.info(f"Starting analysis for session {session_id}")
        
        # DEBUG: Check what we're passing to dq_unified
        logger.info(f"UI Data being passed to dq_unified:")
        logger.info(f"  source_type: {ui_data.get('source_type')}")
        
        # Different logging based on source type
        source_type = ui_data.get('source_type')
        if source_type in ['csv', 'excel']:
            logger.info(f"  file_path: {ui_data.get('file_path')}")
            logger.info(f"  sheet_name: {ui_data.get('sheet_name')}")
            logger.info(f"  mandatory_fields: {ui_data.get('mandatory_fields')}")
            
            # Check if file_path is missing for CSV/Excel
            if not ui_data.get('file_path'):
                logger.error(f"ERROR: file_path is missing for {source_type}!")
                return jsonify({
                    'status': 400,
                    'message': 'Bad Request',
                    'error': f'File path is required for {source_type.upper()} analysis'
                }), 400
                
        elif source_type == 'database':
            logger.info(f"  db_config: {ui_data.get('db_config')}")
            logger.info(f"  mandatory_fields: {ui_data.get('mandatory_fields')}")
            
            # Check if db_config is missing for database
            if not ui_data.get('db_config'):
                logger.error(f"ERROR: db_config is missing for database analysis!")
                return jsonify({
                    'status': 400,
                    'message': 'Bad Request',
                    'error': 'Database configuration is required for database analysis'
                }), 400
            
            # ✅ ENHANCED VALIDATION: Check required fields
            db_config = ui_data['db_config']
            db_type = db_config.get('type', '').lower()
            
            # Different databases need different fields
            if db_type == 'sqlite':
                required_fields = ['file_path', 'table']
                missing_fields = [f for f in required_fields if not db_config.get(f)]
                if missing_fields:
                    logger.error(f"ERROR: SQLite missing fields: {missing_fields}")
                    return jsonify({
                        'status': 400,
                        'message': 'Bad Request',
                        'error': f'SQLite requires: {", ".join(missing_fields)}'
                    }), 400
            else:
                # For MySQL, PostgreSQL, Oracle, SQL Server
                required_fields = ['host', 'port', 'database', 'table', 'user', 'password']
                missing_fields = [f for f in required_fields if not db_config.get(f)]
                if missing_fields:
                    logger.error(f"ERROR: Database missing fields: {missing_fields}")
                    return jsonify({
                        'status': 400,
                        'message': 'Bad Request',
                        'error': f'Missing required database fields: {", ".join(missing_fields)}'
                    }), 400
                
                # Special check for Oracle
                if db_type == 'oracle' and 'service_name' not in db_config:
                    logger.warning("Oracle service_name not provided, using default 'XE'")
                    db_config['service_name'] = 'XE'
                
        else:
            logger.error(f"ERROR: Unknown source type: {source_type}")
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': f'Unknown source type: {source_type}'
            }), 400

        # Run analysis
        results = run_single_source_analysis_ui(ui_data)
        
        # Handle analysis errors (still return 200 with error in data)
        if 'error' in results:
            logger.warning(f"Analysis completed with errors: {results['error']}")
            return jsonify({
                'status': 200,
                'message': 'Analysis completed with errors',
                'data': results
            }), 200
        
        # Ensure session_id in results
        if 'session_id' not in results:
            results['session_id'] = session_id
        
        # Get error logs
        try:
            results['error_logs'] = error_logger.get_error_logs_for_session(results['session_id'], limit=200) if error_logger else []
            results['error_summary'] = error_logger.get_error_summary_for_session(results['session_id']) if error_logger else {}
        except Exception as e:
            logger.warning(f"Could not get error logs: {e}")
            results['error_logs'] = []
            results['error_summary'] = {'total_errors': 0, 'errors_by_type': [], 'errors_by_column': []}
        
        # Get audit logs
        try:
            results['audit_logs'] = audit_logger.get_audit_logs_for_session(results['session_id']) if audit_logger else []
        except Exception as e:
            logger.warning(f"Could not get audit logs: {e}")
            results['audit_logs'] = []
        
        # Store session
        sessions[results['session_id']] = {
            'type': 'single',
            'timestamp': datetime.now().isoformat(),
            'results': results
        }
        
        logger.info(f"Analysis completed for session {results['session_id']}")
        
        return jsonify({
            'status': 200,
            'message': 'Analysis completed successfully',
            'data': convert_numpy_types(results),
            'pagination_endpoints': {
                'data_rows': f'/api/single/{session_id}/data',
                'error_details': f'/api/single/{session_id}/errors',
                'session_summary': f'/api/single/{session_id}'
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error in check-single: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

def clean_comparison_results(results, aggressive_cleanup=False):
    """
    Configurable cleaning - you choose how much to clean
    
    Parameters:
    - results: The comparison results
    - aggressive_cleanup: If True, removes more data (for very large responses)
                         If False (default), keeps most data, removes only true duplicates
    """
    if not isinstance(results, dict):
        return results
    
    cleaned = results.copy()
    
    # ALWAYS remove these (they're always duplicates)
    if 'data' in cleaned and isinstance(cleaned['data'], dict):
        if 'results' in cleaned['data'] and 'data' in cleaned['data']:
            del cleaned['data']['data']
    
    # ONLY if aggressive cleanup is requested
    if aggressive_cleanup:
        # Remove large data structures
        large_fields = ['_source_df_cache', '_target_df_cache', 
                       '_selected_columns_cache', 'data_preview']
        for field in large_fields:
            if field in cleaned:
                del cleaned[field]
        
        # Limit mismatch details
        if 'detailed_mismatches' in cleaned and isinstance(cleaned['detailed_mismatches'], list):
            if len(cleaned['detailed_mismatches']) > 100:
                cleaned['detailed_mismatches'] = cleaned['detailed_mismatches'][:100]
                cleaned['optimization_note'] = f"Showing first 100 of {len(cleaned['detailed_mismatches'])} mismatches"
    
    # ALWAYS add API endpoints (helpful for frontend)
    if 'session_id' in cleaned:
        session_id = cleaned['session_id']
        cleaned['api_endpoints'] = {
            'mismatches': f'/api/compare/{session_id}/mismatches?page=1&page_size=50',
            'mismatches_summary': f'/api/compare/{session_id}/mismatches/summary',
            # 'source_only': f'/api/compare/{session_id}/rows/source_only?page=1&page_size=50',
            # 'target_only': f'/api/compare/{session_id}/rows/target_only?page=1&page_size=50'
        }
    
    return cleaned

def optimize_large_response(response_data, max_size_mb=5):
    """
    Optimize large JSON responses by trimming unnecessary data
    """
    import json
    
    # Estimate size
    json_str = json.dumps(response_data)
    size_mb = len(json_str) / (1024 * 1024)
    
    if size_mb > max_size_mb:
        logger.warning(f"Response too large: {size_mb:.2f}MB, optimizing...")
        
        # Trim large arrays
        if 'detailed_mismatches' in response_data and isinstance(response_data['detailed_mismatches'], list):
            if len(response_data['detailed_mismatches']) > 100:
                original_count = len(response_data['detailed_mismatches'])
                response_data['detailed_mismatches'] = response_data['detailed_mismatches'][:100]
                response_data['optimization_note'] = f"Showing first 100 of {original_count} mismatches. Use pagination for full data."
        
        # Remove verbose debug data
        for key in ['_source_df_cache', '_target_df_cache', '_selected_columns_cache', 'data_preview']:
            if key in response_data:
                del response_data[key]
    
    return response_data

@app.route('/api/compare', methods=['POST'])
def source_target_comparison():
    """Source-target comparison"""
    if not check_dq_modules():
        return jsonify({
            'status': 503,
            'message': 'Service Unavailable',
            'error': 'DQ modules not available'
        }), 503
    
    try:
        data = request.get_json()
        # Add this instead (sanitized logging):
        if data:
            source_type = data.get('source', {}).get('source_type') if isinstance(data.get('source'), dict) else data.get('source_type')
            target_type = data.get('target', {}).get('source_type') if isinstance(data.get('target'), dict) else data.get('target_source_type')
            logger.info(f"Comparison request - Source: {source_type}, Target: {target_type}")

        if not data:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Request body is required'
            }), 400
        
        if run_comparison_analysis_ui is None:
            return jsonify({
                'status': 501,
                'message': 'Not Implemented',
                'error': 'Comparison module not available'
            }), 501
        
        logger.info("Comparison analysis request received")
        
        # Prepare UI data
        ui_data = prepare_ui_data_for_analysis(data, 'comparison')
        
        # Generate session ID
        session_id = f"CMP_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting comparison for session {session_id}")
        
        # Run comparison
        results = run_comparison_analysis_ui(ui_data)
        
        # Handle errors
        if 'error' in results:
            logger.warning(f"Comparison completed with errors: {results['error']}")
            return jsonify({
                'status': 200,
                'message': 'Comparison completed with errors',
                'data': results
            }), 200
        
        # Ensure session_id
        if 'session_id' not in results:
            results['session_id'] = session_id
        
        # ===== NEW: Add mismatch API endpoints =====
        results['api_endpoints'] = {
            'mismatches': f'/api/compare/{results["session_id"]}/mismatches',
            'mismatches_summary': f'/api/compare/{results["session_id"]}/mismatches/summary',
            'session_data': f'/api/compare/{results["session_id"]}',
            'error_logs': f'/api/compare/{results["session_id"]}?include_errors=true'
        }

        # ===== NEW: Add mismatch display guidance =====
        try:
            # Check if results has the new structure
            if isinstance(results, dict):
                if 'detailed_mismatches_sample' in results:
                    results['mismatch_display_info'] = {
                        'total_mismatches': results.get('total_detailed_mismatches', 0),
                        'sample_count': len(results.get('detailed_mismatches_sample', [])),
                        'display_format': 'table',
                        'columns': ['Row', 'Mismatch Summary', 'Source Data', 'Target Data'],
                        'pagination_size': 50
                    }
                # If old structure, try to extract from mismatch_details
                elif 'mismatch_details' in results and isinstance(results['mismatch_details'], list):
                    # Old structure - extract what we can
                    results['mismatch_display_info'] = {
                        'total_mismatches': len(results['mismatch_details']),
                        'sample_count': min(20, len(results['mismatch_details'])),
                        'display_format': 'table',
                        'columns': ['Row', 'Mismatch Summary', 'Source Data', 'Target Data'],
                        'pagination_size': 50
                    }
        except Exception as e:
            logger.warning(f"Could not add mismatch display info: {e}")

        # Get audit logs
        try:
            results['audit_logs'] = audit_logger.get_audit_logs_for_session(results['session_id']) if audit_logger else []
        except Exception as e:
            logger.warning(f"Could not get audit logs: {e}")
            results['audit_logs'] = []
        
        # Get error logs
        if error_logger:
            results['error_logs'] = error_logger.get_error_logs_for_session(results['session_id'], limit=200)
            results['error_summary'] = error_logger.get_error_summary_for_session(results['session_id'])
        
                # ===== ENHANCED: CLEAN AND OPTIMIZE RESPONSE =====
        aggressive = request.args.get('aggressive_clean', 'false').lower() == 'true'

        # 1. Clean duplicate nested structures using the helper function
        if isinstance(results, dict):
            cleaned_results = clean_comparison_results(results, aggressive_cleanup=aggressive)
        else:
            cleaned_results = results
        
        # 2. Convert numpy types (dates, arrays, etc.) to JSON-safe types
        response_data = convert_numpy_types(cleaned_results)
        
        # 3. Optimize for large responses (trim if too big)
        # if isinstance(response_data, dict):
        #     response_data = optimize_large_response(response_data, max_size_mb=5)
        
        # # 4. Ensure clean structure for frontend
        # final_response = {
        #     'status': 200,
        #     'message': 'Comparison completed successfully',
        #     'session_id': session_id,
        #     'data': response_data,
        # }
        
        # # Store session with CLEANED results
        # sessions[session_id] = {
        #     'type': 'comparison',
        #     'timestamp': datetime.now().isoformat(),
        #     'results': response_data  # Store cleaned version
        # }
        
        # logger.info(f"Comparison completed for session {session_id}")
        # logger.info(f"Response size: {len(str(final_response)):,} bytes")
        
        return jsonify({
            'status': 200,
            'message': 'Comparison completed successfully',
            'session_id': session_id,
            'data': response_data,
            'cleaning_applied': 'aggressive' if aggressive else 'minimal'
        }), 200
        
    except Exception as e:
        logger.error(f"Error in comparison: {str(e)}")
        # ADD THESE LINES FOR DEBUGGING:
        import traceback
        logger.error(f"Error in comparison endpoint: {str(e)}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")

        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500


@app.route('/api/advanced', methods=['POST'])
def advanced_checks():
    """Advanced data quality checks"""
    if not check_dq_modules():
        return jsonify({
            'status': 503,
            'message': 'Service Unavailable',
            'error': 'DQ modules not available'
        }), 503
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Request body is required'
            }), 400
        
        if run_advanced_analysis_ui is None:
            return jsonify({
                'status': 501,
                'message': 'Not Implemented',
                'error': 'Advanced checks module not available'
            }), 501
        
        logger.info("Advanced checks request received")
        
        # Prepare UI data
        ui_data = prepare_ui_data_for_analysis(data, 'advanced')
        
        # Generate session ID
        session_id = f"ADV_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        logger.info(f"Starting advanced checks for session {session_id}")
        
        # Add session_id to ui_data so it can be used by the module
        ui_data['session_id'] = session_id
        
        # Run advanced checks
        results = run_advanced_analysis_ui(ui_data)
        
        # Handle errors
        if 'error' in results:
            logger.warning(f"Advanced checks completed with errors: {results['error']}")
            return jsonify({
                'status': 200,
                'message': 'Advanced checks completed with errors',
                'session_id': session_id,
                'data': results
            }), 200
        
        # Ensure session_id is in results
        if 'session_id' not in results:
            results['session_id'] = session_id
            logger.info(f"Added session_id to results: {session_id}")
        
        # Get error logs
        try:
            error_logs = error_logger.get_error_logs_for_session(results['session_id'], limit=100) if error_logger else []
            error_summary = error_logger.get_error_summary_for_session(results['session_id']) if error_logger else {}
        except Exception as e:
            logger.warning(f"Could not get error logs: {e}")
            error_logs = []
            error_summary = {'total_errors': 0, 'errors_by_type': [], 'errors_by_column': []}
        
        # Get audit logs
        try:
            audit_logs = audit_logger.get_audit_logs_for_session(results['session_id']) if audit_logger else []
        except Exception as e:
            logger.warning(f"Could not get audit logs: {e}")
            audit_logs = []
        
        # ===== FIX: Create CLEAN response structure =====
        # Remove nested duplicates and organize data logically
        clean_response = {
            'session_id': results.get('session_id', session_id),
            'validation_summary': {
                'quality_score': results.get('quality_metrics', {}).get('quality_score', 0),
                'total_records': results.get('quality_metrics', {}).get('total_records', 0),
                'good_records': results.get('quality_metrics', {}).get('good_records', 0),
                'bad_records': results.get('quality_metrics', {}).get('bad_records', 0),
                'assessment_category': results.get('quality_metrics', {}).get('assessment_category', 'UNKNOWN')
            },
            'column_summary': {
                'total_columns': results.get('data_stats', {}).get('total_columns', 0),
                'columns_checked': results.get('data_stats', {}).get('columns_checked', 0),
                'columns_with_errors': results.get('summary', {}).get('columns_with_errors', 0)
            },
            'column_data_types': results.get('column_data_types', {}),
            'error_summary': error_summary,
            'check_breakdown': results.get('summary', {}).get('check_breakdown', {}),
            'recommendations': results.get('recommendations', []),
            'numeric_issues': results.get('numeric_tab_issues', {}),
            'logs': {
                'error_logs': error_logs[:50],  # Limit to 50 for initial response
                'audit_logs': audit_logs[:10]   # Limit to 10 for initial response
            },
            'pagination_info': {
                'error_logs_total': len(error_logs),
                'error_logs_per_page': 50,
                'error_logs_pages': max(1, (len(error_logs) + 49) // 50),
                'detailed_data_available': f'/api/advanced/{session_id}'
            }
        }
        
        # Store session with full results for detailed queries
        sessions[session_id] = {
            'type': 'advanced',
            'timestamp': datetime.now().isoformat(),
            'full_results': results,  # Store full results for detailed queries
            'clean_response': clean_response  # Store clean response for quick access
        }
        
        logger.info(f"Advanced checks completed for session {results['session_id']}")
        
        # Convert numpy types in clean response
        response_data = convert_numpy_types(clean_response)
        
        return jsonify({
            'status': 200,
            'message': 'Advanced checks completed successfully',
            'session_id': session_id,
            'data': response_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error in advanced checks: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

@app.route('/api/advanced/<session_id>', methods=['GET'])
def get_advanced_session_details(session_id):
    """Get detailed advanced checks session data with pagination"""
    try:
        # Check if session exists
        session_info = sessions.get(session_id)
        if not session_info or session_info.get('type') != 'advanced':
            return jsonify({
                'status': 404,
                'message': 'Session not found',
                'error': f'Advanced checks session {session_id} not found'
            }), 404
        
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))
        data_type = request.args.get('data_type', 'errors')
        
        # Validate parameters
        if page < 1:
            page = 1
        if page_size > 100:
            page_size = 50
        
        offset = (page - 1) * page_size
        
        # Get full results from session
        full_results = session_info.get('full_results', {})
        
        # Prepare response based on requested data type
        response_data = {
            'session_id': session_id,
            'data_type': data_type,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total_items': 0,
                'total_pages': 1
            },
            'data': []
        }
        
        # Handle different data types
        if data_type == 'errors':
            if error_logger:
                # Get ALL errors from DB
                all_errors = error_logger.get_error_logs_for_session(session_id, limit=100000)
            else:
                all_errors = []
        
            total_items = len(all_errors)
            total_pages = max(1, (total_items + page_size - 1) // page_size)

            # ===== FIX: Add pagination links for errors =====
            base_url = f"/api/advanced/{session_id}"
            links = {
                "first": f"{base_url}?data_type=errors&page=1&page_size={page_size}",
                "last": f"{base_url}?data_type=errors&page={total_pages}&page_size={page_size}",
                "prev": f"{base_url}?data_type=errors&page={page-1}&page_size={page_size}" if page > 1 else None,
                "next": f"{base_url}?data_type=errors&page={page+1}&page_size={page_size}" if page < total_pages else None
            }
            # ================================================

            paginated_data = all_errors[offset:offset + page_size]
        
            response_data.update({
                'data': paginated_data,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_items': total_items,
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_previous': page > 1,
                    'links': links  # ADD THIS LINE
                }
            })
            
        elif data_type == 'numeric_issues':
            # Get numeric issues with pagination
            numeric_issues = full_results.get('numeric_tab_issues', {}).get('non_numeric_values', [])
            total_items = len(numeric_issues)
            total_pages = max(1, (total_items + page_size - 1) // page_size)
            
            # ===== FIX: Add pagination links for numeric_issues =====
            base_url = f"/api/advanced/{session_id}"
            links = {
                "first": f"{base_url}?data_type=numeric_issues&page=1&page_size={page_size}",
                "last": f"{base_url}?data_type=numeric_issues&page={total_pages}&page_size={page_size}",
                "prev": f"{base_url}?data_type=numeric_issues&page={page-1}&page_size={page_size}" if page > 1 else None,
                "next": f"{base_url}?data_type=numeric_issues&page={page+1}&page_size={page_size}" if page < total_pages else None
            }
            # ======================================================
 
            paginated_data = numeric_issues[offset:offset + page_size]
            
            response_data.update({
                'data': paginated_data,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_items': total_items,
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_previous': page > 1,
                    'links': links  # ADD THIS LINE
                }
            })
            
        elif data_type == 'column_details':
            # Get column details
            column_results = full_results.get('column_results', {})
            columns_list = []
            
            for col_name, col_data in column_results.items():
                columns_list.append({
                    'column_name': col_name,
                    'inferred_type': col_data.get('inferred_type'),
                    'total_errors': col_data.get('total_errors', 0),
                    'checks_run': col_data.get('checks_run', []),
                    'check_results': col_data.get('check_results', {})
                })
            
            total_items = len(columns_list)
            total_pages = max(1, (total_items + page_size - 1) // page_size)
            paginated_columns = columns_list[offset:offset + page_size]
            
            # ===== Column details already has links, but make it consistent =====
            base_url = f"/api/advanced/{session_id}"
            links = {
                "first": f"{base_url}?data_type=column_details&page=1&page_size={page_size}",
                "last": f"{base_url}?data_type=column_details&page={total_pages}&page_size={page_size}",
                "prev": f"{base_url}?data_type=column_details&page={page-1}&page_size={page_size}" if page > 1 else None,
                "next": f"{base_url}?data_type=column_details&page={page+1}&page_size={page_size}" if page < total_pages else None
            }
            # ===================================================================
            
            response_data.update({
                'data': paginated_columns,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_items': total_items,
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_previous': page > 1,
                    'links': links  # UPDATE THIS LINE
                }
            })
            
        elif data_type == 'full_summary':
            # Return full summary (limited size)
            clean_response = session_info.get('clean_response', {})
            response_data['data'] = clean_response
            
        else:
            return jsonify({
                'status': 400,
                'message': 'Invalid data_type',
                'error': f"data_type must be one of: errors, numeric_issues, column_details, full_summary"
            }), 400
        
        return jsonify({
            'status': 200,
            'message': 'Advanced session details retrieved',
            'session_id': session_id,
            'data': convert_numpy_types(response_data)
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting advanced session details: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# ==================== BUSINESS RULES ENDPOINTS ====================

# 1. GET /api/rules - List all rules
@app.route('/api/rules', methods=['GET'])
def get_all_business_rules():
    """Get all business rules"""
    try:
        from dq_rules import BusinessRuleEngine
        engine = BusinessRuleEngine()
        
        active_only = request.args.get('active_only', 'true').lower() == 'true'
        rules = engine.get_all_rules(active_only=active_only)
        
        return jsonify({
            'status': 200,
            'message': 'Business rules retrieved',
            'count': len(rules),
            'rules': rules
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting business rules: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# 2. GET /api/rules/<rule_id> - Get specific rule
@app.route('/api/rules/<int:rule_id>', methods=['GET'])
def get_business_rule(rule_id):
    """Get specific business rule"""
    try:
        from dq_rules import BusinessRuleEngine
        engine = BusinessRuleEngine()
        
        rule = engine.get_rule_by_id(rule_id)
        
        if not rule:
            return jsonify({
                'status': 404,
                'message': 'Rule not found',
                'error': f'Rule ID {rule_id} not found'
            }), 404
        
        return jsonify({
            'status': 200,
            'message': 'Rule retrieved',
            'rule': rule
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting rule {rule_id}: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# 3. POST /api/rules - Create new rule
@app.route('/api/rules', methods=['POST'])
def create_business_rule():
    """Create new business rule"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Request body is required'
            }), 400
        
        from dq_rules import BusinessRuleEngine
        engine = BusinessRuleEngine()
        
        rule_id = engine.create_rule(data)
        
        if not rule_id:
            return jsonify({
                'status': 500,
                'message': 'Failed to create rule',
                'error': 'Database error'
            }), 500
        
        return jsonify({
            'status': 201,
            'message': 'Rule created successfully',
            'rule_id': rule_id,
            'rule_url': f'/api/rules/{rule_id}'
        }), 201
        
    except Exception as e:
        logger.error(f"Error creating rule: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# 4. PUT /api/rules/<rule_id> - Update rule
@app.route('/api/rules/<int:rule_id>', methods=['PUT'])
def update_business_rule(rule_id):
    """Update existing business rule"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Request body is required'
            }), 400
        
        from dq_rules import BusinessRuleEngine
        engine = BusinessRuleEngine()
        
        success = engine.update_rule(rule_id, data)
        
        if not success:
            return jsonify({
                'status': 404,
                'message': 'Rule not found or update failed',
                'error': f'Rule ID {rule_id} not found'
            }), 404
        
        return jsonify({
            'status': 200,
            'message': 'Rule updated successfully',
            'rule_id': rule_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error updating rule {rule_id}: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# 5. DELETE /api/rules/<rule_id> - Delete (deactivate) rule
@app.route('/api/rules/<int:rule_id>', methods=['DELETE'])
def delete_business_rule(rule_id):
    """Delete (deactivate) business rule"""
    try:
        from dq_rules import BusinessRuleEngine
        engine = BusinessRuleEngine()
        
        success = engine.delete_rule(rule_id)
        
        if not success:
            return jsonify({
                'status': 404,
                'message': 'Rule not found',
                'error': f'Rule ID {rule_id} not found'
            }), 404
        
        return jsonify({
            'status': 200,
            'message': 'Rule deactivated successfully',
            'rule_id': rule_id
        }), 200
        
    except Exception as e:
        logger.error(f"Error deleting rule {rule_id}: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# 6. POST /api/rules/execute - Execute rules on data
@app.route('/api/rules/execute', methods=['POST'])
def execute_business_rules():
    """Execute business rules on data"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Request body is required'
            }), 400
        
        from dq_rules import run_rules_analysis_ui
        results = run_rules_analysis_ui(data)
        
        # Store session
        session_id = results.get('session_id', f"RULES_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        sessions[session_id] = {
            'type': 'rules',
            'timestamp': datetime.now().isoformat(),
            'results': results
        }
        
        return jsonify({
            'status': 200,
            'message': 'Business rules executed successfully',
            'session_id': session_id,
            'data': convert_numpy_types(results)
        }), 200
        
    except Exception as e:
        logger.error(f"Error executing business rules: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# 7. GET /api/rules/history - View execution history
@app.route('/api/rules/history', methods=['GET'])
def get_rules_execution_history():
    """Get rule execution history"""
    try:
        from dq_rules import BusinessRuleEngine
        engine = BusinessRuleEngine()
        
        conn = engine.connect_to_db()
        if not conn:
            return jsonify({
                'status': 500,
                'message': 'Database connection failed',
                'error': 'Cannot connect to database'
            }), 500
        
        cursor = conn.cursor(dictionary=True)
        
        # Get pagination parameters
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        
        query = """
        SELECT * FROM dq_rule_execution_history 
        ORDER BY execution_timestamp DESC 
        LIMIT %s OFFSET %s
        """
        
        cursor.execute(query, (limit, offset))
        history = cursor.fetchall()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) as total FROM dq_rule_execution_history")
        total = cursor.fetchone()['total']
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 200,
            'message': 'Execution history retrieved',
            'history': history,
            'pagination': {
                'limit': limit,
                'offset': offset,
                'total': total,
                'has_more': (offset + limit) < total
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting execution history: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# 8. GET /api/rules/sessions/<session_id> - Get session results
@app.route('/api/rules/sessions/<session_id>', methods=['GET'])
def get_rules_session(session_id):
    """Get business rules session results"""
    try:
        # Check in-memory sessions first
        session_data = sessions.get(session_id, {}).get('results')
        
        if not session_data:
            # Try to reconstruct from database
            from dq_rules import BusinessRuleEngine
            engine = BusinessRuleEngine()
            
            conn = engine.connect_to_db()
            if conn:
                cursor = conn.cursor(dictionary=True)
                
                # Get execution history for this session
                cursor.execute("""
                    SELECT * FROM dq_rule_execution_history 
                    WHERE session_id = %s
                """, (session_id,))
                history = cursor.fetchall()
                
                # Get violations for this session
                cursor.execute("""
                    SELECT * FROM dq_rule_violations 
                    WHERE session_id = %s
                    ORDER BY excel_row
                    LIMIT 100
                """, (session_id,))
                violations = cursor.fetchall()
                
                cursor.close()
                conn.close()
                
                if history:
                    session_data = {
                        'session_id': session_id,
                        'execution_history': history,
                        'violations': violations,
                        'from_database': True
                    }
        
        if not session_data:
            return jsonify({
                'status': 404,
                'message': 'Session not found',
                'error': f'Session {session_id} not found'
            }), 404
        
        return jsonify({
            'status': 200,
            'message': 'Session data retrieved',
            'session_id': session_id,
            'data': convert_numpy_types(session_data)
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting session {session_id}: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# 9. GET /api/rules/violations/<session_id> - Get violations with pagination
@app.route('/api/rules/violations/<session_id>', methods=['GET'])
def get_rule_violations(session_id):
    """Get rule violations with pagination"""
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))
        
        if page < 1:
            page = 1
        if page_size > 100:
            page_size = 50
        
        from dq_rules import BusinessRuleEngine
        engine = BusinessRuleEngine()
        
        conn = engine.connect_to_db()
        if not conn:
            return jsonify({
                'status': 500,
                'message': 'Database connection failed'
            }), 500
        
        cursor = conn.cursor(dictionary=True)
        
        # Get total count
        cursor.execute("""
            SELECT COUNT(*) as total 
            FROM dq_rule_violations 
            WHERE session_id = %s
        """, (session_id,))
        total_result = cursor.fetchone()
        total = total_result['total'] if total_result else 0
        
        # Get paginated data
        offset = (page - 1) * page_size
        query = """
        SELECT * FROM dq_rule_violations 
        WHERE session_id = %s 
        ORDER BY excel_row
        LIMIT %s OFFSET %s
        """
        
        cursor.execute(query, (session_id, page_size, offset))
        violations = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        total_pages = max(1, (total + page_size - 1) // page_size)
        
        return jsonify({
            'status': 200,
            'message': 'Rule violations retrieved',
            'session_id': session_id,
            'data': violations,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total': total,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_previous': page > 1
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting rule violations: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Upload file endpoint"""
    try:
        if 'file' not in request.files:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'No file part'
            }), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'No selected file'
            }), 400
        
        upload_dir = 'uploads'
        os.makedirs(upload_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{timestamp}_{secure_filename(file.filename)}"
        filepath = os.path.join(upload_dir, filename)
        file.save(filepath)
        
        if filename.lower().endswith('.csv'):
            file_type = 'csv'
        elif filename.lower().endswith(('.xlsx', '.xls')):
            file_type = 'excel'
        else:
            return jsonify({
                'status': 400,
                'message': 'Bad Request',
                'error': 'Unsupported file format. Only CSV and Excel files are supported.'
            }), 400
        
        logger.info(f"File uploaded: {filename} ({file_type})")
        
        return jsonify({
            'status': 200,
            'message': 'File uploaded successfully',
            'filename': filename,
            'filepath': filepath,
            'file_type': file_type,
            'full_path': os.path.abspath(filepath)
        }), 200
        
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# @app.route('/api/advanced/<session_id>', methods=['GET'])
# def get_advanced_session(session_id):
#     """Get advanced checks session data"""
#     try:
#         # Check if session exists in memory
#         session_data = sessions.get(session_id, {}).get('results')
        
#         if not session_data:
#             # Try to get from database
#             try:
#                 if audit_logger:
#                     audit_logs = audit_logger.get_audit_logs_for_session(session_id)
#                     if audit_logs:
#                         # Reconstruct session data from database
#                         session_data = {
#                             'session_id': session_id,
#                             'audit_logs': audit_logs,
#                             'error_logs': error_logger.get_error_logs_for_session(session_id, limit=200) if error_logger else [],
#                             'error_summary': error_logger.get_error_summary_for_session(session_id) if error_logger else {}
#                         }
#             except Exception as e:
#                 logger.warning(f"Could not retrieve session {session_id} from database: {e}")
        
#         if not session_data:
#             return jsonify({
#                 'status': 404,
#                 'message': 'Session not found',
#                 'error': f'Session {session_id} not found in memory or database'
#             }), 404
        
#         # Check if it's an advanced session
#         if session_id.startswith('ADV_') or session_data.get('check_type') == 'advanced':
#             response_data = {
#                 'status': 200,
#                 'message': 'Advanced session data retrieved',
#                 'session_id': session_id,
#                 'data': convert_numpy_types(session_data)
#             }
            
#             # Get additional logs if not already in session_data
#             if 'audit_logs' not in session_data and audit_logger:
#                 response_data['data']['audit_logs'] = audit_logger.get_audit_logs_for_session(session_id)
            
#             if 'error_logs' not in session_data and error_logger:
#                 response_data['data']['error_logs'] = error_logger.get_error_logs_for_session(session_id, limit=200)
#                 response_data['data']['error_summary'] = error_logger.get_error_summary_for_session(session_id)
            
#             logger.info(f"Retrieved advanced session data for {session_id}")
#             return jsonify(response_data), 200
#         else:
#             return jsonify({
#                 'status': 400,
#                 'message': 'Invalid session type',
#                 'error': f'Session {session_id} is not an advanced checks session'
#             }), 400
        
#     except Exception as e:
#         logger.error(f"Error getting advanced session data: {str(e)}")
#         return jsonify({
#             'status': 500,
#             'message': 'Internal Server Error',
#             'error': str(e)
#         }), 500

# ==================== SINGLE SOURCE ENDPOINTS ====================
@app.route('/api/single/<session_id>', methods=['GET'])
def get_single_session(session_id):
    """Get single source analysis session data"""
    try:
        # Check if session exists in memory
        session_data = sessions.get(session_id, {}).get('results')
        
        if not session_data:
            # Try to get from database
            try:
                if audit_logger:
                    audit_logs = audit_logger.get_audit_logs_for_session(session_id)
                    if audit_logs:
                        # Reconstruct session data from database
                        session_data = {
                            'session_id': session_id,
                            'audit_logs': audit_logs,
                            'error_logs': error_logger.get_error_logs_for_session(session_id, limit=200) if error_logger else [],
                            'error_summary': error_logger.get_error_summary_for_session(session_id) if error_logger else {}
                        }
            except Exception as e:
                logger.warning(f"Could not retrieve session {session_id} from database: {e}")
        
        if not session_data:
            return jsonify({
                'status': 404,
                'message': 'Session not found',
                'error': f'Session {session_id} not found in memory or database'
            }), 404
        
        # Check if it's a single source session
        if session_id.startswith('SINGLE_') or session_data.get('check_type') in ['single', 'basic', 'unified']:
            response_data = {
                'status': 200,
                'message': 'Single source session data retrieved',
                'session_id': session_id,
                'data': convert_numpy_types(session_data)
            }
            
            # Get additional logs if not already in session_data
            if 'audit_logs' not in session_data and audit_logger:
                response_data['data']['audit_logs'] = audit_logger.get_audit_logs_for_session(session_id)
            
            # ADD ERROR LOGS FETCHING HERE:
            if 'error_logs' not in session_data and error_logger:
                response_data['data']['error_logs'] = error_logger.get_error_logs_for_session(session_id, limit=200)
                response_data['data']['error_summary'] = error_logger.get_error_summary_for_session(session_id)
            
            logger.info(f"Retrieved single source session data for {session_id}")
            return jsonify(response_data), 200
        else:
            return jsonify({
                'status': 400,
                'message': 'Invalid session type',
                'error': f'Session {session_id} is not a single source analysis session'
            }), 400
        
    except Exception as e:
        logger.error(f"Error getting single source session data: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# ========== ADD THESE TWO NEW ENDPOINTS RIGHT HERE ==========

@app.route('/api/single/<session_id>/data', methods=['GET'])
def get_single_source_data_paginated(session_id):
    """Get paginated data rows from single source analysis"""
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))
        
        # Validate parameters
        if page < 1:
            page = 1
        if page_size > 200:  # Limit for performance
            page_size = 100
        if page_size < 10:
            page_size = 20
        
        offset = (page - 1) * page_size
        
        # Check if session exists in memory storage
        try:
            from dq_unified import single_sessions
        except ImportError:
            return jsonify({
                'status': 500,
                'message': 'Internal Server Error',
                'error': 'Cannot access session storage'
            }), 500
        
        if session_id not in single_sessions:
            return jsonify({
                'status': 404,
                'message': 'Session not found',
                'error': f'Session {session_id} not found in memory storage'
            }), 404
        
        session_info = single_sessions[session_id]
        df = session_info.get('df')
        
        if df is None:
            return jsonify({
                'status': 404,
                'message': 'Data not available',
                'error': 'DataFrame not found in session storage'
            }), 404
        
        total_rows = len(df)
        
        # Calculate pagination
        total_pages = max(1, (total_rows + page_size - 1) // page_size)
        page = max(1, min(page, total_pages))
        
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_rows)
        
        # Get paginated data
        page_data = df.iloc[start_idx:end_idx]
        
        # Convert to records for JSON response
        data_records = page_data.to_dict('records')
        
        # Get column data types from session
        column_data_types = session_info.get('column_data_types', {})
        
        response_data = {
            'session_id': session_id,
            'data': data_records,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total_pages': total_pages,
                'total_rows': total_rows,
                'start_index': start_idx,
                'end_index': end_idx,
                'has_next': page < total_pages,
                'has_previous': page > 1
            },
            'column_data_types': column_data_types,
            'summary': {
                'current_page': f'Rows {start_idx + 1}-{end_idx} of {total_rows}',
                'source_info': session_info.get('source_info', ''),
                'columns': list(df.columns) if df is not None else []
            }
        }
        
        # Convert numpy types for JSON serialization
        response_data = convert_numpy_types(response_data)
        
        return jsonify({
            'status': 200,
            'message': 'Data retrieved successfully',
            'data': response_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting paginated data: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

@app.route('/api/single/<session_id>/errors', methods=['GET'])
def get_single_source_errors_paginated(session_id):
    """Get paginated error details from single source analysis"""
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))
        error_type = request.args.get('error_type', 'all')  # all, nulls, duplicates, formats, mandatory
        
        # Validate parameters
        if page < 1:
            page = 1
        if page_size > 200:
            page_size = 100
        
        # Check if session exists in global sessions
        session_data = sessions.get(session_id, {}).get('results')
        
        if not session_data:
            return jsonify({
                'status': 404,
                'message': 'Session not found',
                'error': f'Session {session_id} not found in memory'
            }), 404
        
        # Collect all errors from results
        all_errors = []
        
        # Collect null errors
        if 'results' in session_data and 'nulls' in session_data['results']:
            null_results = session_data['results']['nulls']
            if 'issue_rows' in null_results:
                for column, rows in null_results['issue_rows'].items():
                    for row in rows:
                        all_errors.append({
                            'error_type': 'null',
                            'column': column,
                            'row_index': row.get('row_index'),
                            'excel_row': row.get('excel_row'),
                            'db_row': row.get('db_row'),
                            'identifier': row.get('identifier', ''),
                            'description': f'Null/empty value in column {column}',
                            'severity': 'medium'
                        })
        
        # Collect duplicate errors
        if 'results' in session_data and 'duplicates' in session_data['results']:
            duplicate_results = session_data['results']['duplicates']
            if 'duplicate_details' in duplicate_results:
                for row in duplicate_results['duplicate_details']:
                    all_errors.append({
                        'error_type': 'duplicate',
                        'column': 'multiple',
                        'row_index': row.get('row_index'),
                        'excel_row': row.get('excel_row'),
                        'db_row': row.get('db_row'),
                        'identifier': '',
                        'description': 'Duplicate row found',
                        'severity': 'low'
                    })
        
        # Collect format errors
        if 'results' in session_data and 'formats' in session_data['results']:
            format_results = session_data['results']['formats']
            if 'format_issue_details' in format_results:
                for issue in format_results['format_issue_details']:
                    all_errors.append({
                        'error_type': 'format',
                        'column': issue.get('column'),
                        'row_index': issue.get('row_index'),
                        'excel_row': issue.get('excel_row'),
                        'db_row': issue.get('db_row'),
                        'identifier': issue.get('identifier', ''),
                        'description': f'Format issue in {issue.get("column")}: {issue.get("actual_value", "")[:50]}',
                        'severity': 'medium'
                    })
        
        # Collect mandatory field errors
        if 'results' in session_data and 'mandatory_fields' in session_data['results']:
            mandatory_results = session_data['results']['mandatory_fields']
            if 'issue_details' in mandatory_results:
                for column, rows in mandatory_results['issue_details'].items():
                    for row in rows:
                        all_errors.append({
                            'error_type': 'mandatory',
                            'column': column,
                            'row_index': row.get('row_index'),
                            'excel_row': row.get('excel_row'),
                            'db_row': row.get('db_row'),
                            'identifier': row.get('identifier', ''),
                            'description': f'Missing mandatory field: {column}',
                            'severity': 'high'
                        })
        
        # Filter by error type if specified
        if error_type != 'all':
            all_errors = [err for err in all_errors if err['error_type'] == error_type]
        
        total_errors = len(all_errors)
        
        # Calculate pagination
        total_pages = max(1, (total_errors + page_size - 1) // page_size)
        page = max(1, min(page, total_pages))
        
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_errors)
        
        # Get paginated errors
        page_errors = all_errors[start_idx:end_idx]
        
        # Count by error type for summary
        error_counts = {}
        for err in all_errors:
            error_type_name = err['error_type']
            error_counts[error_type_name] = error_counts.get(error_type_name, 0) + 1
        
        response_data = {
            'session_id': session_id,
            'errors': page_errors,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total_pages': total_pages,
                'total_errors': total_errors,
                'start_index': start_idx,
                'end_index': end_idx,
                'has_next': page < total_pages,
                'has_previous': page > 1
            },
            'summary': {
                'total_errors': total_errors,
                'error_counts': error_counts,
                'error_types_available': ['null', 'duplicate', 'format', 'mandatory']
            }
        }
        
        return jsonify({
            'status': 200,
            'message': 'Errors retrieved successfully',
            'data': response_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting paginated errors: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# ========== END NEW ENDPOINTS ==========
@app.route('/api/compare/<session_id>/mismatches/summary', methods=['GET'])
def get_mismatches_summary(session_id):
    """Get quick summary of mismatches"""
    try:
        if not error_logger:
            return jsonify({
                'status': 500,
                'message': 'Error logger not available'
            }), 500
        
        conn = error_logger._get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get counts
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                MIN(excel_row) as first_mismatch,
                MAX(excel_row) as last_mismatch,
                AVG(LENGTH(difference_summary)) as avg_differences_length
            FROM dq_error_logs 
            WHERE session_id = %s AND error_type = 'row_data_mismatch'
        """, (session_id,))
        
        summary = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 200,
            'message': 'Mismatch summary retrieved',
            'data': {
                'session_id': session_id,
                'summary': summary,
                'endpoint': f'/api/compare/{session_id}/mismatches'
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting mismatch summary: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500
    
# ==================== COMPARISON ENDPOINTS ====================
@app.route('/api/compare/<session_id>', methods=['GET'])
def get_comparison_session(session_id):
    """Get comparison analysis session data"""
    try:
        # Check if session exists in memory
        session_data = sessions.get(session_id, {}).get('results')
        
        if not session_data:
            # Try to get from database
            try:
                if audit_logger:
                    audit_logs = audit_logger.get_audit_logs_for_session(session_id)
                    if audit_logs:
                        # Reconstruct session data from database
                        session_data = {
                            'session_id': session_id,
                            'audit_logs': audit_logs,
                            'error_logs': error_logger.get_error_logs_for_session(session_id, limit=200) if error_logger else [],
                            'error_summary': error_logger.get_error_summary_for_session(session_id) if error_logger else {},
                            # Add mismatch API endpoints
                            'api_endpoints': {
                                'mismatches': f'/api/compare/{session_id}/mismatches',
                                'mismatches_summary': f'/api/compare/{session_id}/mismatches/summary'
                            }
                        }
            except Exception as e:
                logger.warning(f"Could not retrieve session {session_id} from database: {e}")
        
        if not session_data:
            return jsonify({
                'status': 404,
                'message': 'Session not found',
                'error': f'Session {session_id} not found in memory or database'
            }), 404
        
        # Check if it's a comparison session
        if session_id.startswith('CMP_') or session_data.get('check_type') in ['comparison', 'compare']:
            response_data = {
                'status': 200,
                'message': 'Comparison session data retrieved',
                'session_id': session_id,
                'data': convert_numpy_types(session_data)
            }
            
            # Get additional logs if not already in session_data
            if 'audit_logs' not in session_data and audit_logger:
                response_data['data']['audit_logs'] = audit_logger.get_audit_logs_for_session(session_id)
            
            if 'error_logs' not in session_data and error_logger:
                response_data['data']['error_logs'] = error_logger.get_error_logs_for_session(session_id, limit=200)
                response_data['data']['error_summary'] = error_logger.get_error_summary_for_session(session_id)
            
            if 'api_endpoints' not in response_data['data']:
                response_data['data']['api_endpoints'] = {
                    'mismatches': f'/api/compare/{session_id}/mismatches',
                    'mismatches_summary': f'/api/compare/{session_id}/mismatches/summary'
                }

            logger.info(f"Retrieved comparison session data for {session_id}")
            return jsonify(response_data), 200
        else:
            return jsonify({
                'status': 400,
                'message': 'Invalid session type',
                'error': f'Session {session_id} is not a comparison analysis session'
            }), 400
        
    except Exception as e:
        logger.error(f"Error getting comparison session data: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

@app.route('/api/compare/<session_id>/mismatches', methods=['GET'])
def get_comparison_mismatches(session_id):
    """Get paginated mismatch data - FIXED VERSION"""
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))
        
        # Validate
        if page < 1:
            page = 1
        if page_size > 200:  # Limit for performance
            page_size = 200
        if page_size < 10:
            page_size = 20
        
        offset = (page - 1) * page_size
        
        if error_logger:
            conn = error_logger._get_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Get total count efficiently
            cursor.execute("""
                SELECT COUNT(*) as total 
                FROM dq_error_logs 
                WHERE session_id = %s 
                AND (error_type LIKE '%mismatch%' OR error_type LIKE '%missing%' OR error_type LIKE '%extra%')
            """, (session_id,))
            total_count = cursor.fetchone()['total']
            
            # Use database pagination for > 1000 records (better performance)
            if total_count > 1000:
                logger.info(f"Using database pagination for {total_count:,} mismatches")
                
                # Get paginated data directly from database
                query = """
                SELECT 
                    excel_row,
                    column_name,
                    error_type,
                    error_description,
                    actual_value,
                    expected_value,
                    source_actual_value,
                    target_actual_value,
                    difference_summary,
                    severity
                FROM dq_error_logs 
                WHERE session_id = %s 
                AND (error_type LIKE '%mismatch%' OR error_type LIKE '%missing%' OR error_type LIKE '%extra%')
                ORDER BY excel_row ASC
                LIMIT %s OFFSET %s
                """
                
                cursor.execute(query, (session_id, page_size, offset))
                db_mismatches = cursor.fetchall()
                cursor.close()
                conn.close()
                
                # Process database results
                processed_mismatches = []
                for mismatch in db_mismatches:
                    # Clean data for frontend
                    processed_mismatches.append({
                        'row': mismatch['excel_row'],
                        'column': mismatch.get('column_name', ''),
                        'error_type': mismatch['error_type'],
                        'summary': mismatch.get('error_description', ''),
                        'severity': mismatch.get('severity', 'medium'),
                        'difference': mismatch.get('difference_summary', ''),
                        'source_actual_value': mismatch.get('source_actual_value', ''),
                        'target_actual_value': mismatch.get('target_actual_value', '')
                    })
                
                total_pages = max(1, (total_count + page_size - 1) // page_size)
                
                return jsonify({
                    'status': 200,
                    'message': 'Mismatches retrieved (database pagination)',
                    'data': {
                        'session_id': session_id,
                        'mismatches': processed_mismatches,
                        'pagination': {
                            'page': page,
                            'page_size': page_size,
                            'total_pages': total_pages,
                            'total_rows': total_count,
                            'has_next': page < total_pages,
                            'has_previous': page > 1
                        }
                    }
                }), 200
            
        if not error_logger:
            return jsonify({
                'status': 500,
                'message': 'Error logger not available'
            }), 500
        
        # Check if session exists in memory first
        session_data = sessions.get(session_id, {}).get('results')
        
        if session_data and 'detailed_mismatches' in session_data:
            # Get from memory (recent session)
            all_mismatches = session_data.get('detailed_mismatches', [])
            total_count = len(all_mismatches)
            
            # Paginate in memory
            start_idx = offset
            end_idx = min(offset + page_size, total_count)
            page_mismatches = all_mismatches[start_idx:end_idx]
            
            processed_mismatches = []
            for mismatch in page_mismatches:
                processed_mismatches.append({
                    'row': mismatch.get('excel_row', mismatch.get('row_index', 0) + 2),
                    'row_index': mismatch.get('row_index'),
                    'differences_count': mismatch.get('differences_count', 0),
                    'mismatch_summary': mismatch.get('mismatch_summary', ''),
                    'source_data': mismatch.get('source_data', {}),
                    'target_data': mismatch.get('target_data', {}),
                    'differences': mismatch.get('differences', []),
                    'row_type': mismatch.get('row_type', 'unknown')
                })
            
        else:
            # Get from database (fallback)
            conn = error_logger._get_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Get total count
            count_query = """
            SELECT COUNT(*) as total 
            FROM dq_error_logs 
            WHERE session_id = %s 
            AND (error_type LIKE '%mismatch%' OR error_type LIKE '%missing%' OR error_type LIKE '%extra%')
            """
            cursor.execute(count_query, (session_id,))
            total_count = cursor.fetchone()['total']
            
            if total_count > 0:
                # Get paginated data
                query = """
                SELECT 
                    excel_row,
                    column_name,
                    error_type,
                    error_description,
                    actual_value,
                    expected_value,
                    source_actual_value,
                    target_actual_value,
                    difference_summary,
                    severity
                FROM dq_error_logs 
                WHERE session_id = %s 
                AND (error_type LIKE '%mismatch%' OR error_type LIKE '%missing%' OR error_type LIKE '%extra%')
                ORDER BY excel_row ASC
                LIMIT %s OFFSET %s
                """
                
                cursor.execute(query, (session_id, page_size, offset))
                db_mismatches = cursor.fetchall()
                
                processed_mismatches = []
                for mismatch in db_mismatches:
                    # Parse mismatch data
                    source_data = {}
                    target_data = {}
                    
                    try:
                        if mismatch.get('source_actual_value'):
                            source_data = json.loads(mismatch['source_actual_value'])
                    except:
                        pass
                    
                    try:
                        if mismatch.get('target_actual_value'):
                            target_data = json.loads(mismatch['target_actual_value'])
                    except:
                        pass
                    
                    processed_mismatches.append({
                        'row': mismatch['excel_row'],
                        'column': mismatch.get('column_name', ''),
                        'error_type': mismatch['error_type'],
                        'summary': mismatch.get('error_description', ''),
                        'source_data': source_data,
                        'target_data': target_data,
                        'severity': mismatch.get('severity', 'medium')
                    })
                
                cursor.close()
                conn.close()
            else:
                processed_mismatches = []
                total_count = 0
        
        # Calculate pagination
        total_pages = max(1, (total_count + page_size - 1) // page_size)
        
        return jsonify({
            'status': 200,
            'message': 'Mismatches retrieved',
            'data': {
                'session_id': session_id,
                'mismatches': processed_mismatches,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_pages': total_pages,
                    'total_rows': total_count,
                    'has_next': page < total_pages,
                    'has_previous': page > 1
                }
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting mismatches: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500
    
# @app.route('/api/compare/<session_id>/rows/<row_type>', methods=['GET'])
# def get_comparison_rows_paginated(session_id, row_type):
#     """Get paginated row details for comparison - FIXED"""
#     try:
#         # Validate row_type
#         if row_type not in ['source_only', 'target_only']:
#             return jsonify({
#                 'status': 400,
#                 'message': 'Bad Request',
#                 'error': f"Invalid row_type: {row_type}"
#             }), 400
        
#         # Get pagination parameters
#         page = int(request.args.get('page', 1))
#         page_size = int(request.args.get('page_size', 50))
        
#         if page < 1:
#             page = 1
#         if page_size > 100:
#             page_size = 50
        
#         # Check session in memory
#         session_data = sessions.get(session_id, {}).get('results')
        
#         if not session_data:
#             return jsonify({
#                 'status': 404,
#                 'message': 'Session not found',
#                 'error': f'Session {session_id} not found in memory'
#             }), 404
        
#         # Get row indices from session data
#         if row_type == 'source_only':
#             row_indices = session_data.get('hash_summary', {}).get('source_only_indices', [])
#             total_label = f"Rows only in source: {len(row_indices)}"
#         else:
#             row_indices = session_data.get('hash_summary', {}).get('target_only_indices', [])
#             total_label = f"Rows only in target: {len(row_indices)}"
        
#         total_rows = len(row_indices)
        
#         if total_rows == 0:
#             return jsonify({
#                 'status': 200,
#                 'message': f'No {row_type.replace("_", " ")} rows found',
#                 'data': {
#                     'session_id': session_id,
#                     'row_type': row_type,
#                     'rows': [],
#                     'pagination': {
#                         'page': 1,
#                         'page_size': page_size,
#                         'total_pages': 1,
#                         'total_rows': 0
#                     }
#                 }
#             }), 200
        
#         # Calculate pagination
#         total_pages = max(1, (total_rows + page_size - 1) // page_size)
#         page = max(1, min(page, total_pages))
        
#         start_idx = (page - 1) * page_size
#         end_idx = min(start_idx + page_size, total_rows)
        
#         # Get paginated indices
#         page_indices = row_indices[start_idx:end_idx]
        
#         # Get source/target DataFrames from session if available
#         source_df = session_data.get('_source_df_cache')
#         target_df = session_data.get('_target_df_cache')
#         selected_columns = session_data.get('selected_columns', [])
        
#         rows_data = []
        
#         if source_df is not None and target_df is not None and selected_columns:
#             # Get actual row data
#             for row_idx in page_indices:
#                 try:
#                     if row_type == 'source_only':
#                         # Source-only row
#                         if row_idx < len(source_df):
#                             row_data = source_df.iloc[row_idx]
#                             values = {}
#                             for col in selected_columns[:5]:  # First 5 columns
#                                 if col in source_df.columns:
#                                     val = row_data[col]
#                                     values[col] = str(val) if not pd.isna(val) else "NULL"
                            
#                             rows_data.append({
#                                 'excel_row': row_idx + 2,
#                                 'dataframe_index': row_idx,
#                                 'values': values,
#                                 'description': f'Present only in source (row {row_idx + 2})'
#                             })
#                     else:
#                         # Target-only row  
#                         if row_idx < len(target_df):
#                             row_data = target_df.iloc[row_idx]
#                             values = {}
#                             for col in selected_columns[:5]:
#                                 # Find target column name
#                                 target_col = col  # Simplified - should use common_cols mapping
#                                 if target_col in target_df.columns:
#                                     val = row_data[target_col]
#                                     values[col] = str(val) if not pd.isna(val) else "NULL"
                            
#                             rows_data.append({
#                                 'excel_row': row_idx + 2,
#                                 'dataframe_index': row_idx,
#                                 'values': values,
#                                 'description': f'Present only in target (row {row_idx + 2})'
#                             })
#                 except Exception as e:
#                     logger.debug(f"Error getting row {row_idx}: {e}")
#                     continue
#         else:
#             # Just return indices if dataframes not available
#             for row_idx in page_indices:
#                 rows_data.append({
#                     'excel_row': row_idx + 2,
#                     'dataframe_index': row_idx,
#                     'description': f'{row_type.replace("_", " ").title()} row {row_idx + 2}'
#                 })
        
#         response = {
#             'status': 200,
#             'message': f'{row_type.replace("_", " ").title()} rows retrieved',
#             'data': {
#                 'session_id': session_id,
#                 'row_type': row_type,
#                 'row_type_description': {
#                     'source_only': 'Rows present ONLY in source (NOT in target)',
#                     'target_only': 'Rows present ONLY in target (NOT in source)'
#                 }.get(row_type, ''),
#                 'rows': rows_data,
#                 'pagination': {
#                     'page': page,
#                     'page_size': page_size,
#                     'total_pages': total_pages,
#                     'total_rows': total_rows,
#                     'start_index': start_idx,
#                     'end_index': end_idx
#                 },
#                 'summary': {
#                     'current_page': f'Rows {start_idx + 1}-{end_idx} of {total_rows}',
#                     'total_rows': total_rows,
#                     'api_endpoint': f'/api/compare/{session_id}/rows/{row_type}'
#                 }
#             }
#         }
        
#         return jsonify(response), 200
        
#     except ValueError as e:
#         logger.error(f"Invalid parameter: {str(e)}")
#         return jsonify({
#             'status': 400,
#             'message': 'Bad Request',
#             'error': f'Invalid parameter: {str(e)}'
#         }), 400
#     except Exception as e:
#         logger.error(f"Error getting paginated rows: {str(e)}")
#         return jsonify({
#             'status': 500,
#             'message': 'Internal Server Error',
#             'error': str(e)
#         }), 500


@app.route('/api/compare/<session_id>/rows/count', methods=['GET'])
def get_comparison_row_counts(session_id):
    """Get row counts for comparison - SIMPLE VERSION"""
    try:
        # Check if session exists
        session_data = sessions.get(session_id, {}).get('results')
        
        if not session_data:
            return jsonify({
                'status': 404,
                'message': 'Session not found',
                'error': f'Session {session_id} not found'
            }), 404
        
        # Extract row counts from session data
        row_counts = {
            'identical_rows': session_data.get('common_rows', 0),
            'source_only_rows': session_data.get('unique_to_source', 0),
            'target_only_rows': session_data.get('unique_to_target', 0),
            'source_total': session_data.get('source_stats', {}).get('rows', 0),
            'target_total': session_data.get('target_stats', {}).get('rows', 0),
            'match_percentage': session_data.get('overall_score', 0)
        }
        
        # Also get from hash_summary if available
        if 'hash_summary' in session_data:
            hash_summary = session_data['hash_summary']
            row_counts['source_only_indices_count'] = len(hash_summary.get('source_only_indices', []))
            row_counts['target_only_indices_count'] = len(hash_summary.get('target_only_indices', []))
            row_counts['identical_indices_count'] = len(hash_summary.get('identical_indices', []))
        
        return jsonify({
            'status': 200,
            'message': 'Row counts retrieved',
            'data': {
                'session_id': session_id,
                'row_counts': row_counts,
                'api_endpoints': {
                    'source_only': f'/api/compare/{session_id}/rows/source_only',
                    'target_only': f'/api/compare/{session_id}/rows/target_only',
                    'mismatches': f'/api/compare/{session_id}/mismatches'
                }
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting row counts: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# ==================== DELETE ENDPOINTS ====================
@app.route('/api/sessions/<session_id>', methods=['DELETE'])
def delete_session(session_id):
    """Delete a session from memory (does not delete from database)"""
    try:
        if session_id in sessions:
            deleted_session = sessions.pop(session_id)
            logger.info(f"Deleted session {session_id} from memory")
            
            return jsonify({
                'status': 200,
                'message': 'Session deleted from memory',
                'session_id': session_id,
                'session_type': deleted_session.get('type'),
                'timestamp': deleted_session.get('timestamp')
            }), 200
        else:
            return jsonify({
                'status': 404,
                'message': 'Session not found in memory',
                'error': f'Session {session_id} not found in memory (may still exist in database)'
            }), 404
        
    except Exception as e:
        logger.error(f"Error deleting session: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# ==================== CLEANUP ENDPOINTS ====================
@app.route('/api/cleanup', methods=['POST'])
def cleanup_sessions():
    """Clean up old sessions from memory"""
    try:
        data = request.get_json() or {}
        max_age_hours = data.get('max_age_hours', 24)  # Default: 24 hours
        max_sessions = data.get('max_sessions', 100)   # Default: keep 100 sessions
        
        current_time = datetime.now()
        sessions_to_delete = []
        
        # Clean by age
        for session_id, session_info in list(sessions.items()):
            try:
                session_time = datetime.fromisoformat(session_info['timestamp'])
                age_hours = (current_time - session_time).total_seconds() / 3600
                
                if age_hours > max_age_hours:
                    sessions_to_delete.append(session_id)
            except:
                pass
        
        # Clean by count if still too many
        if len(sessions) > max_sessions:
            # Sort sessions by timestamp (oldest first)
            sorted_sessions = sorted(
                sessions.items(),
                key=lambda x: x[1]['timestamp']
            )
            
            # Add oldest sessions to delete list
            for session_id, _ in sorted_sessions[:len(sessions) - max_sessions]:
                if session_id not in sessions_to_delete:
                    sessions_to_delete.append(session_id)
        
        # Delete sessions
        deleted_count = 0
        for session_id in sessions_to_delete:
            if session_id in sessions:
                sessions.pop(session_id)
                deleted_count += 1
        
        logger.info(f"Cleanup: Deleted {deleted_count} sessions from memory")
        
        return jsonify({
            'status': 200,
            'message': f'Cleanup completed: Deleted {deleted_count} sessions',
            'deleted_count': deleted_count,
            'remaining_sessions': len(sessions),
            'max_age_hours': max_age_hours,
            'max_sessions': max_sessions
        }), 200
        
    except Exception as e:
        logger.error(f"Error in cleanup: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

@app.route('/api/sessions/<session_id>', methods=['GET'])
def get_session_data(session_id):
    """Get session data"""
    try:
        session_data = sessions.get(session_id, {}).get('results')
        
        response = {
            'status': 200,
            'message': 'Session data retrieved',
            'data': {
                'session_id': session_id,
                'session_in_memory': session_id in sessions,
                'session_data': session_data
            }
        }
        
        # Get logs from database
        try:
            if error_logger:
                response['data']['error_logs'] = error_logger.get_error_logs_for_session(session_id, limit=100)
                response['data']['error_summary'] = error_logger.get_error_summary_for_session(session_id)
            
            if audit_logger:
                response['data']['audit_logs'] = audit_logger.get_audit_logs_for_session(session_id)
        except Exception as e:
            logger.warning(f"Could not get logs for session {session_id}: {e}")
        
        logger.info(f"Retrieved data for session {session_id}")
        return jsonify(convert_numpy_types(response)), 200
        
    except Exception as e:
        logger.error(f"Error getting session data: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

@app.route('/api/sessions', methods=['GET'])
def get_all_sessions():
    """Get all sessions"""
    try:
        sessions_list = []
        for session_id, info in sessions.items():
            sessions_list.append({
                'session_id': session_id,
                'type': info['type'],
                'timestamp': info['timestamp']
            })
        
        logger.info(f"Retrieved {len(sessions_list)} sessions")
        return jsonify({
            'status': 200,
            'message': 'Sessions retrieved',
            'data': {
                'total_sessions': len(sessions_list),
                'sessions': sessions_list
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting sessions: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

@app.route('/api/debug-db', methods=['GET'])
def debug_database():
    """Debug database endpoint"""
    try:
        if not check_dq_modules():
            return jsonify({
                'status': 503,
                'message': 'Service Unavailable',
                'error': 'DQ modules not available'
            }), 503
        
        # Test connection
        db_connected = error_logger.test_connection() if error_logger else False
        
        # Get counts
        import mysql.connector
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT COUNT(*) as total FROM dq_error_logs")
        error_count = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM dq_audit_logs")
        audit_count = cursor.fetchone()['total']
        
        cursor.execute("""
            SELECT session_id, check_type, overall_score, check_timestamp
            FROM dq_audit_logs 
            ORDER BY check_timestamp DESC 
            LIMIT 5
        """)
        recent_sessions = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Database debug: connected={db_connected}, errors={error_count}, audits={audit_count}")
        
        return jsonify({
            'status': 200,
            'message': 'Database debug info',
            'data': {
                'database_connected': db_connected,
                'error_logs_count': error_count,
                'audit_logs_count': audit_count,
                'recent_sessions': recent_sessions
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error in debug-db: {str(e)}")
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error',
            'error': str(e)
        }), 500

# ==================== COMPARISON PAGINATION ENDPOINTS ====================

# @app.route('/api/compare/<session_id>/rows/source_only', methods=['GET'])
# def get_source_only_rows(session_id):
#     """Get source-only rows (rows present only in source, not in target)"""
#     return get_comparison_rows_paginated(session_id, 'source_only')

# @app.route('/api/compare/<session_id>/rows/target_only', methods=['GET'])
# def get_target_only_rows(session_id):
#     """Get target-only rows (rows present only in target, not in source)"""
#     return get_comparison_rows_paginated(session_id, 'target_only')


    
# ==================== MAIN EXECUTION ====================
if __name__ == '__main__':
    logger.info("🚀 Starting Data Quality Framework API")
    logger.info(f"📊 Modules available: {HAS_DQ_MODULES}")
    logger.info(f"🔧 Config file mode: {APP_SETTINGS.get('use_config_file', False)}")
    logger.info("📋 Available endpoints:")
    logger.info("  GET  /              - API status")
    logger.info("  GET  /api/health    - Health check")
    logger.info("  POST /api/check-single - Single source analysis")
    logger.info("  POST /api/compare   - Source-target comparison")
    logger.info("  GET  /api/compare/{id} - Get comparison results")
    logger.info("  GET  /api/compare/{id}/mismatches - Paginated mismatches")
    # logger.info("  GET  /api/compare/{id}/rows/source_only - Source-only rows")
    # logger.info("  GET  /api/compare/{id}/rows/target_only - Target-only rows")
    logger.info("  GET  /api/compare/{id}/rows/count - Row counts")
    logger.info("  POST /api/upload    - File upload")
    logger.info("  GET  /api/sessions  - List sessions")
    logger.info("  GET  /api/sessions/<id> - Get session data")
    logger.info("  GET  /api/debug-db  - Database debug")
    
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    debug = os.environ.get('DEBUG', 'false').lower() == 'true'
    
    logger.info(f"🌍 Server starting on {host}:{port} (debug={debug})")
    
    app.run(host=host, port=port, debug=debug)