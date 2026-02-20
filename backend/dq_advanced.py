# dq_advanced.py - ENHANCED COMPREHENSIVE GENERIC DATA QUALITY VALIDATION SYSTEM
import pandas as pd
import numpy as np
import os
import re
import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict

from dq_unified import select_data_source, load_data_from_source
from app_config import APP_SETTINGS, QUALITY_THRESHOLDS, DATA_PATTERNS
from dq_error_log import ErrorLogger

# Import the dual-mode input handler
try:
    from input_handler import init_input_handler, get_input, get_choice, get_multiple_choice, get_input_handler
    HAS_INPUT_HANDLER = True
except ImportError:
    HAS_INPUT_HANDLER = False
    print("⚠️  Input handler not found. Running in CLI-only mode.")

# Import database navigator
try:
    from database_navigator import navigate_database
    HAS_DATABASE_NAVIGATOR = True
except ImportError:
    HAS_DATABASE_NAVIGATOR = False
    print("⚠️  Database Navigator not found. Hierarchical selection unavailable.")

# Setup logger
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

# Validation types with descriptions
VALIDATION_TYPES = {
    'data_type': 'Data Type Validation - Ensures values match column type',
    'whitespace': 'Whitespace Validation - Checks for leading/trailing spaces',
    'zero_padding': 'Zero Padding Validation - Detects unnecessary leading zeros',
    'length': 'Length Consistency - Validates trimmed string lengths',
    'format': 'Data Format Validation - Validates email, phone, date formats',
    'currency': 'Currency Handling - Removes currency symbols for numeric checks',
    'numeric': 'Numeric Consistency - Ensures numeric columns contain valid numbers'
}

# Default validation thresholds
VALIDATION_THRESHOLDS = {
    'min_length_threshold': 1,
    'max_length_threshold': 1000,
    'max_decimal_places': 4,
    'email_domain_check': True,
    'phone_international': False,
    'date_strict_validation': False
}

# Common patterns for format detection
PATTERN_DETECTORS = {
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'phone_us': r'^\+?1?\d{10}$',
    'phone_international': r'^\+?[1-9]\d{1,14}$',
    'date_iso': r'^\d{4}-\d{2}-\d{2}$',
    'date_us': r'^\d{2}/\d{2}/\d{4}$',
    'date_eu': r'^\d{2}\.\d{2}\.\d{4}$',
    'ssn': r'^\d{3}-\d{2}-\d{4}$',
    'zip_code': r'^\d{5}(-\d{4})?$',
    'url': r'^https?://[^\s/$.?#].[^\s]*$',
    'ip_address': r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
}

# Currency symbols to strip
CURRENCY_SYMBOLS = ['$', '€', '£', '¥', '₹', '₽', '₩', '฿', '₴', '₪', '₫']

# Add at the top of dq_advanced.py
import psutil  # For memory monitoring
import math

# Add this function
def get_optimal_batch_size(df: pd.DataFrame, available_memory_mb: int = None) -> int:
    """
    Calculate optimal batch size based on available memory and dataset size.
    
    Args:
        df: DataFrame to process
        available_memory_mb: Available memory in MB (auto-detected if None)
    
    Returns:
        Optimal batch size
    """
    if available_memory_mb is None:
        # Auto-detect available memory
        available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
    
    # Estimate memory per row (rough calculation)
    sample_rows = min(1000, len(df))
    if sample_rows > 0:
        sample = df.iloc[:sample_rows]
        sample_memory = sample.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
        memory_per_row = sample_memory / sample_rows
    else:
        memory_per_row = 0.001  # Default 1KB per row
    
    # Calculate safe batch size (use 30% of available memory)
    safe_memory_mb = available_memory_mb * 0.3
    calculated_batch = int(safe_memory_mb / memory_per_row) if memory_per_row > 0 else 10000
    
    # Apply limits
    min_batch = 1000
    max_batch = 50000  # Maximum batch size
    
    optimal_batch = max(min_batch, min(calculated_batch, max_batch))
    
    # For very large datasets, use smaller batches
    if len(df) > 1000000:  # Over 1 million rows
        optimal_batch = min(optimal_batch, 10000)
    elif len(df) > 500000:  # Over 500k rows
        optimal_batch = min(optimal_batch, 20000)
    elif len(df) > 100000:  # Over 100k rows
        optimal_batch = min(optimal_batch, 30000)
    
    logger.info(f"Memory: {available_memory_mb:.0f}MB available, {memory_per_row:.4f}MB/row, "
                f"Optimal batch: {optimal_batch} rows")
    
    return optimal_batch

# ============================================================================
# CORE COLUMN ANALYSIS ENGINE
# ============================================================================

def analyze_column(df: pd.DataFrame, column_name: str) -> Dict[str, Any]:
    """
    Dynamically analyze a column to infer characteristics and suggest validations.
    
    Args:
        df: DataFrame containing the data
        column_name: Name of column to analyze
        
    Returns:
        Dictionary with column analysis including:
        - data_type: Inferred data type
        - suggested_checks: List of appropriate validations
        - patterns: Detected data patterns
        - statistics: Column statistics
        - metadata: Additional column information
    """
    if column_name not in df.columns:
        logger.warning(f"Column '{column_name}' not found in DataFrame")
        return {}
    
    column_data = df[column_name]
    non_null_count = column_data.notna().sum()
    total_count = len(column_data)
    
    # Initialize analysis result
    analysis = {
        'column_name': column_name,
        'total_values': total_count,
        'non_null_count': non_null_count,
        'null_percentage': (total_count - non_null_count) / total_count * 100 if total_count > 0 else 0,
        'unique_values': column_data.nunique(),
        'data_type': str(column_data.dtype),
        'inferred_type': None,
        'suggested_checks': [],
        'patterns': [],
        'statistics': {},
        'metadata': {}
    }
    
    # Skip analysis if no data
    if non_null_count == 0:
        analysis['inferred_type'] = 'unknown'
        analysis['suggested_checks'] = ['data_type']  # Only check for empty column
        return analysis
    
    # Sample data for pattern detection
    sample_values = column_data.dropna().head(100).tolist()
    
    # STEP 1: Infer data type based on values and dtype
    inferred_type = infer_data_type(column_data, sample_values)
    analysis['inferred_type'] = inferred_type
    
    # STEP 2: Detect patterns in the data
    detected_patterns = detect_patterns(sample_values)
    analysis['patterns'] = detected_patterns
    
    # STEP 3: Collect statistics based on inferred type
    if inferred_type == 'numeric':
        try:
            numeric_series = pd.to_numeric(column_data, errors='coerce')
            valid_numeric = numeric_series.dropna()
            if len(valid_numeric) > 0:
                analysis['statistics'] = {
                    'min': float(valid_numeric.min()),
                    'max': float(valid_numeric.max()),
                    'mean': float(valid_numeric.mean()),
                    'median': float(valid_numeric.median()),
                    'std': float(valid_numeric.std()),
                    'has_negative': (valid_numeric < 0).any(),
                    'has_zero': (valid_numeric == 0).any(),
                    'has_decimal': (valid_numeric % 1 != 0).any()
                }
        except:
            pass
    
    elif inferred_type == 'text':
        # Analyze text column
        text_lengths = column_data.dropna().astype(str).str.len()
        if len(text_lengths) > 0:
            analysis['statistics'] = {
                'min_length': int(text_lengths.min()),
                'max_length': int(text_lengths.max()),
                'avg_length': float(text_lengths.mean()),
                'empty_strings': (column_data == '').sum()
            }
    
    elif inferred_type == 'date':
        # Date statistics
        try:
            date_series = pd.to_datetime(column_data, errors='coerce')
            valid_dates = date_series.dropna()
            if len(valid_dates) > 0:
                analysis['statistics'] = {
                    'min_date': valid_dates.min().isoformat(),
                    'max_date': valid_dates.max().isoformat(),
                    'date_range_days': (valid_dates.max() - valid_dates.min()).days
                }
        except:
            pass
    
    elif inferred_type == 'boolean':
        # Boolean statistics
        bool_series = column_data.dropna()
        if len(bool_series) > 0:
            true_count = bool_series.astype(str).str.lower().isin(['true', 't', 'yes', 'y', '1']).sum()
            false_count = len(bool_series) - true_count
            analysis['statistics'] = {
                'true_count': int(true_count),
                'false_count': int(false_count),
                'true_percentage': true_count / len(bool_series) * 100
            }
    
    # STEP 4: Suggest appropriate validations based on inferred type and patterns
    analysis['suggested_checks'] = suggest_validations(inferred_type, detected_patterns)
    
    logger.info(f"Analyzed column '{column_name}': {inferred_type} with {len(detected_patterns)} patterns")
    return analysis

def infer_data_type(column_data: pd.Series, sample_values: List) -> str:
    """Infer the most likely data type for a column."""
    dtype_str = str(column_data.dtype)
    
    # Check for datetime dtype
    if 'datetime' in dtype_str or 'date' in dtype_str or 'time' in dtype_str:
        return 'date'
    
    # Check for boolean dtype
    if 'bool' in dtype_str:
        return 'boolean'
    
    # For object/string dtype, analyze sample values
    if len(sample_values) == 0:
        return 'text'  # Default to text for empty columns
    
    # Test for date values
    date_count = 0
    date_patterns = [PATTERN_DETECTORS['date_iso'], PATTERN_DETECTORS['date_us'], PATTERN_DETECTORS['date_eu']]
    for val in sample_values:
        str_val = str(val)
        for pattern in date_patterns:
            if re.match(pattern, str_val):
                date_count += 1
                break
    
    if date_count / len(sample_values) > 0.7:  # 70% of samples match date patterns
        return 'date'
    
    # Test for boolean values
    bool_count = 0
    bool_patterns = ['true', 'false', 'yes', 'no', 't', 'f', 'y', 'n', '1', '0']
    for val in sample_values:
        str_val = str(val).lower().strip()
        if str_val in bool_patterns:
            bool_count += 1
    
    if bool_count / len(sample_values) > 0.8:  # 80% of samples are boolean
        return 'boolean'
    
    # ===== CRITICAL FIX: Better numeric detection =====
    all_numeric = True
    total_numeric_like = 0  # Count values that look numeric
    
    for val in sample_values:
        if isinstance(val, (int, float, np.integer, np.floating)):
            total_numeric_like += 1
            continue  # Already numeric
            
        elif isinstance(val, str):
            str_val = str(val).strip()
            
            # Skip empty strings
            if not str_val:
                continue
            
            # Check if string contains letters (like "ABC123")
            if any(c.isalpha() for c in str_val) and not any(c.isdigit() for c in str_val):
                # Pure alphabetic string
                all_numeric = False
                continue
            elif any(c.isalpha() for c in str_val):
                # Mixed alphanumeric like "ABC123" - NOT numeric!
                all_numeric = False
                continue
                
            # Try to convert string to number (handles currency symbols, commas)
            try:
                # Remove currency symbols and commas
                cleaned = str_val
                for symbol in CURRENCY_SYMBOLS:
                    cleaned = cleaned.replace(symbol, '')
                cleaned = cleaned.replace(',', '')
                
                # Check if it's a number
                float(cleaned)
                total_numeric_like += 1
            except:
                # If ANY value fails conversion, column is NOT purely numeric
                all_numeric = False
                continue
        else:
            # Non-string, non-numeric type
            all_numeric = False
            continue
    
    # Determine if column should be treated as numeric
    # Criteria: All values must be numeric OR >80% of samples are numeric
    numeric_percentage = (total_numeric_like / len(sample_values)) * 100 if sample_values else 0
    
    if all_numeric or numeric_percentage > 80:
        return 'numeric'
    
    # Default to text
    return 'text'

def detect_patterns(sample_values: List) -> List[Dict[str, Any]]:
    """Detect data patterns in sample values."""
    if not sample_values:
        return []
    
    patterns = []
    pattern_counts = defaultdict(int)
    
    for val in sample_values:
        str_val = str(val).strip()
        
        # Skip empty strings
        if not str_val:
            continue
        
        # Check for each pattern type
        for pattern_name, pattern_regex in PATTERN_DETECTORS.items():
            if re.match(pattern_regex, str_val):
                pattern_counts[pattern_name] += 1
    
    # Calculate pattern percentages
    total_non_empty = sum(1 for v in sample_values if str(v).strip())
    
    for pattern_name, count in pattern_counts.items():
        percentage = (count / total_non_empty * 100) if total_non_empty > 0 else 0
        if percentage > 30:  # At least 30% match this pattern
            patterns.append({
                'pattern': pattern_name,
                'percentage': percentage,
                'regex': PATTERN_DETECTORS[pattern_name],
                'description': get_pattern_description(pattern_name)
            })
    
    # Check for numeric patterns (already handled by infer_data_type)
    
    # Check for consistent length
    if len(sample_values) >= 5:
        lengths = [len(str(v)) for v in sample_values if str(v).strip()]
        if lengths and max(lengths) == min(lengths):
            patterns.append({
                'pattern': 'fixed_length',
                'percentage': 100,
                'length': lengths[0],
                'description': f'All values have fixed length of {lengths[0]} characters'
            })
    
    return patterns

def get_pattern_description(pattern_name: str) -> str:
    """Get human-readable description for a pattern."""
    descriptions = {
        'email': 'Email address format',
        'phone_us': 'US phone number format',
        'phone_international': 'International phone number format',
        'date_iso': 'ISO date format (YYYY-MM-DD)',
        'date_us': 'US date format (MM/DD/YYYY)',
        'date_eu': 'European date format (DD.MM.YYYY)',
        'ssn': 'Social Security Number format',
        'zip_code': 'US ZIP code format',
        'url': 'URL format',
        'ip_address': 'IP address format',
        'fixed_length': 'Fixed length strings'
    }
    return descriptions.get(pattern_name, pattern_name)

def suggest_validations(inferred_type: str, patterns: List[Dict]) -> List[str]:
    """Suggest appropriate validations based on column type and patterns."""
    base_validations = []
    
    # Base validations by type
    if inferred_type == 'numeric':
        base_validations = ['data_type', 'numeric', 'currency', 'zero_padding']
    elif inferred_type == 'text':
        base_validations = ['data_type', 'whitespace', 'length']
    elif inferred_type == 'date':
        base_validations = ['data_type', 'format']
    elif inferred_type == 'boolean':
        base_validations = ['data_type']
    else:
        base_validations = ['data_type']
    
    # Add format validations based on detected patterns
    for pattern in patterns:
        pattern_name = pattern['pattern']
        if pattern_name in ['email', 'phone_us', 'phone_international', 'date_iso', 'date_us', 'date_eu', 'ssn', 'zip_code', 'url', 'ip_address']:
            if 'format' not in base_validations:
                base_validations.append('format')
    
    # Remove duplicates
    return list(set(base_validations))

# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_data_type(df: pd.DataFrame, column_name: str, column_analysis: Dict) -> Tuple[int, List[Dict]]:
    """Validate that values match the inferred data type."""
    errors = []
    column_data = df[column_name]
    inferred_type = column_analysis['inferred_type']
    
    logger.debug(f"  Data Type Check: Column '{column_name}' inferred as {inferred_type}")

    for idx, value in column_data.items():
        if pd.isna(value):
            continue
        
        is_valid = False
        
        if inferred_type == 'numeric':
            try:
                # Remove currency symbols and commas
                cleaned = str(value).strip()
                for symbol in CURRENCY_SYMBOLS:
                    cleaned = cleaned.replace(symbol, '')
                cleaned = cleaned.replace(',', '')
                
                # Try to convert to float
                float(cleaned)
                is_valid = True
            except:
                is_valid = False
        
        elif inferred_type == 'date':
            try:
                # Try to parse as date
                pd.to_datetime(str(value), errors='raise')
                is_valid = True
            except:
                is_valid = False
        
        elif inferred_type == 'boolean':
            str_val = str(value).lower().strip()
            is_valid = str_val in ['true', 'false', 'yes', 'no', 't', 'f', 'y', 'n', '1', '0', '']
        
        elif inferred_type == 'text':
            # Text can be anything
            is_valid = True
        
        if not is_valid:
            errors.append({
                'column_name': column_name,
                'row_index': idx,
                'excel_row': idx + 2,
                'actual_value': str(value),
                'expected_value': f'{inferred_type} value',
                'error_type': 'data_type_mismatch',
                'error_description': f'Value does not match inferred type: {inferred_type}',
                'severity': 'medium'
            })
    if errors:
        logger.debug(f"  Found {len(errors)} data type mismatches in '{column_name}'")

    return len(errors), errors

def validate_whitespace(df: pd.DataFrame, column_name: str, column_analysis: Dict) -> Tuple[int, List[Dict]]:
    """Validate no leading/trailing whitespace in text columns."""
    errors = []
    column_data = df[column_name]
    
    for idx, value in column_data.items():
        if pd.isna(value) or value == '':
            continue
        
        str_val = str(value)
        if str_val != str_val.strip():
            errors.append({
                'column_name': column_name,
                'row_index': idx,
                'excel_row': idx + 2,
                'actual_value': f"'{str_val}'",
                'expected_value': f"'{str_val.strip()}'",
                'error_type': 'whitespace_issue',
                'error_description': f'Leading/trailing whitespace found',
                'severity': 'low'
            })
    
    return len(errors), errors

def validate_zero_padding(df: pd.DataFrame, column_name: str, column_analysis: Dict) -> Tuple[int, List[Dict]]:
    """Validate no unnecessary zero padding in numeric columns."""
    errors = []
    column_data = df[column_name]
    
    logger.debug(f"  Zero Padding Check: Scanning '{column_name}' for unnecessary zeros")

    for idx, value in column_data.items():
        if pd.isna(value) or value == '':
            continue
        
        str_val = str(value).strip()
        
        # Check if it's a numeric string
        if str_val.replace('-', '').replace('+', '').replace('.', '').isdigit():
            unsigned_val = str_val.lstrip('+-')
            
            # MODIFIED: Also check if ALL digits are zeros after stripping!
            if (len(unsigned_val) > 1 and 
                unsigned_val.startswith('0') and 
                '.' not in unsigned_val):
                
                # Check if all digits are zeros (like "000")
                if all(d == '0' for d in unsigned_val):
                    # All zeros - valid (like "000", "00")
                    continue
                
                # Check if meaningful part is valid
                meaningful_digits = unsigned_val.lstrip('0')
                if meaningful_digits:  # Has unnecessary zeros
                    # SPECIAL CHECK: If meaningful part starts with 0 but has other digits
                    # like "000111222" → "111222" (valid)
                    # But we still want to flag the zero-padding
                    errors.append({
                        'column_name': column_name,
                        'row_index': idx,
                        'excel_row': idx + 2,
                        'actual_value': f"'{str_val}'",
                        'expected_value': f"'{meaningful_digits}'",
                        'error_type': 'zero_padding_issue',
                        'error_description': f'Unnecessary zero-padding found',
                        'severity': 'low'
                    })
    
    if errors:
        logger.debug(f"  Found {len(errors)} zero-padding issues in '{column_name}'")
        # Show first example
        if errors:
            err = errors[0]
            logger.debug(f"    Example: '{err.get('original_value')}' → '{err.get('cleaned_value')}'")

    return len(errors), errors

def validate_length(df: pd.DataFrame, column_name: str, column_analysis: Dict, 
                    min_length: int = None, max_length: int = None) -> Tuple[int, List[Dict]]:
    """Validate string length consistency."""
    errors = []
    column_data = df[column_name]
    
    # Use analysis statistics if available
    if 'statistics' in column_analysis:
        stats = column_analysis['statistics']
        if min_length is None and 'min_length' in stats:
            min_length = stats['min_length']
        if max_length is None and 'max_length' in stats:
            max_length = stats['max_length']
    
    # Default thresholds
    if min_length is None:
        min_length = VALIDATION_THRESHOLDS['min_length_threshold']
    if max_length is None:
        max_length = VALIDATION_THRESHOLDS['max_length_threshold']
    
    for idx, value in column_data.items():
        if pd.isna(value):
            continue
        
        str_val = str(value)
        length = len(str_val.strip())
        
        if length < min_length or length > max_length:
            errors.append({
                'column_name': column_name,
                'row_index': idx,
                'excel_row': idx + 2,
                'actual_value': f"'{str_val}' (length: {length})",
                'expected_value': f'Length between {min_length} and {max_length}',
                'error_type': 'length_violation',
                'error_description': f'String length {length} outside allowed range [{min_length}, {max_length}]',
                'severity': 'medium'
            })
    
    return len(errors), errors

def validate_format(df: pd.DataFrame, column_name: str, column_analysis: Dict) -> Tuple[int, List[Dict]]:
    """Validate data format based on detected patterns."""
    errors = []
    column_data = df[column_name]
    
    # Get relevant patterns for this column
    patterns = column_analysis.get('patterns', [])
    pattern_regexes = {}
    
    for pattern_info in patterns:
        pattern_name = pattern_info['pattern']
        if pattern_name in PATTERN_DETECTORS:
            pattern_regexes[pattern_name] = PATTERN_DETECTORS[pattern_name]
    
    if not pattern_regexes:
        logger.debug(f"  No patterns to validate for '{column_name}'")
        return 0, []  # No patterns to validate against
    
    logger.debug(f"  Format Check: Validating {len(pattern_regexes)} patterns in '{column_name}'")
    logger.debug(f"  Patterns: {', '.join(pattern_regexes.keys())}")

    for idx, value in column_data.items():
        if pd.isna(value) or value == '':
            continue
        
        str_val = str(value).strip()
        matches_any = False
        
        for pattern_name, regex in pattern_regexes.items():
            if re.match(regex, str_val):
                matches_any = True
                break
        
        if not matches_any and pattern_regexes:
            expected_formats = ', '.join(pattern_regexes.keys())
            errors.append({
                'column_name': column_name,
                'row_index': idx,
                'excel_row': idx + 2,
                'actual_value': f"'{str_val}'",
                'expected_value': f'Match one of: {expected_formats}',
                'error_type': 'format_violation',
                'error_description': f'Value does not match expected format patterns',
                'severity': 'medium'
            })
    
    if errors:
        logger.debug(f"  Found {len(errors)} format violations in '{column_name}'")

    return len(errors), errors

def validate_currency(df: pd.DataFrame, column_name: str, column_analysis: Dict) -> Tuple[int, List[Dict]]:
    """Validate currency fields can be converted to numeric."""
    errors = []
    column_data = df[column_name]
    
    for idx, value in column_data.items():
        if pd.isna(value) or value == '':
            continue
        
        str_val = str(value).strip()
        
        # Remove currency symbols
        cleaned = str_val
        for symbol in CURRENCY_SYMBOLS:
            cleaned = cleaned.replace(symbol, '')
        
        # Remove commas (thousands separators)
        cleaned = cleaned.replace(',', '')
        
        # Try to convert to float
        try:
            float(cleaned)
        except ValueError:
            errors.append({
                'column_name': column_name,
                'row_index': idx,
                'excel_row': idx + 2,
                'actual_value': f"'{str_val}'",
                'expected_value': 'Valid numeric value (currency symbols allowed)',
                'error_type': 'currency_parsing_error',
                'error_description': f'Cannot parse as numeric after removing currency symbols',
                'severity': 'medium'
            })
    
    return len(errors), errors

def validate_numeric(df: pd.DataFrame, column_name: str, column_analysis: Dict, 
                    allow_negative: bool = True) -> Tuple[int, List[Dict]]:
    """Validate numeric consistency with optional negative value check."""
    errors = []
    column_data = df[column_name]
    
    for idx, value in column_data.items():
        if pd.isna(value) or value == '':
            continue
        
        str_val = str(value).strip()
        
        # Remove currency symbols and commas
        cleaned = str_val
        for symbol in CURRENCY_SYMBOLS:
            cleaned = cleaned.replace(symbol, '')
        cleaned = cleaned.replace(',', '')
        
        # Try to convert to numeric
        try:
            num_val = float(cleaned)
            
            # Check for negative values if not allowed
            if not allow_negative and num_val < 0:
                errors.append({
                    'column_name': column_name,
                    'row_index': idx,
                    'excel_row': idx + 2,
                    'actual_value': f"'{str_val}'",
                    'expected_value': 'Non-negative numeric value',
                    'error_type': 'negative_value',
                    'error_description': f'Negative value found in non-negative column',
                    'severity': 'medium'
                })
        except ValueError:
            errors.append({
                'column_name': column_name,
                'row_index': idx,
                'excel_row': idx + 2,
                'actual_value': f"'{str_val}'",
                'expected_value': 'Valid numeric value',
                'error_type': 'non_numeric_value',
                'error_description': f'Non-numeric value found in numeric column',
                'severity': 'medium'
            })
    
    return len(errors), errors

# Validation function mapping
VALIDATION_FUNCTIONS = {
    'data_type': validate_data_type,
    'whitespace': validate_whitespace,
    'zero_padding': validate_zero_padding,
    'length': validate_length,
    'format': validate_format,
    'currency': validate_currency,
    'numeric': validate_numeric
}

# ============================================================================
# BATCH PROCESSING ENGINE
# ============================================================================

def process_data_in_batches(df: pd.DataFrame, batch_size: int = 10000):
    """Generator to process DataFrame in batches."""
    total_rows = len(df)
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch = df.iloc[start_idx:end_idx]
        yield batch, start_idx, end_idx, total_rows

# ============================================================================
# BATCH PROCESSING - QUIETER VERSION
# ============================================================================

def run_validations_on_column(df: pd.DataFrame, column_name: str, column_analysis: Dict, 
                             checks_to_run: List[str], ui_data: Dict = None,
                             verbose: bool = False) -> Dict[str, Any]:
    """Run specified validations on a single column with optional verbosity."""
    results = {
        'column_name': column_name,
        'inferred_type': column_analysis['inferred_type'],
        'checks_run': [],
        'total_errors': 0,
        'error_details': [],
        'check_results': {},
        'numeric_issues': [],
        'zero_padding_issues': []
    }
    
    # Determine which checks to run
    available_checks = column_analysis.get('suggested_checks', [])
    
    # If 'all' specified, use suggested checks
    if 'all' in checks_to_run:
        checks_to_run = available_checks
    
    # Filter to only run available checks
    checks_to_run = [check for check in checks_to_run if check in VALIDATION_FUNCTIONS]
    
    if verbose:
        logger.info(f"\n📊 VALIDATING COLUMN: '{column_name}' ({column_analysis['inferred_type']})")
        logger.info(f"   Total rows: {len(df):,}, Nulls: {column_analysis['null_percentage']:.1f}%")
        logger.info(f"   Checks to run: {', '.join(checks_to_run)}")

    # Run each validation
    for check_name in checks_to_run:
        if check_name not in VALIDATION_FUNCTIONS:
            continue
        
        validation_func = VALIDATION_FUNCTIONS[check_name]
        
        # Prepare arguments based on check type
        kwargs = {}
        if check_name == 'length' and ui_data:
            col_key = f'{column_name}_length'
            if col_key in ui_data:
                kwargs.update(ui_data[col_key])
        
        if check_name == 'numeric' and ui_data:
            allow_negative_key = f'allow_negative_{column_name}'
            kwargs['allow_negative'] = ui_data.get(allow_negative_key, True)
        
        # Run validation
        start_time = time.time()
        error_count, errors = validation_func(df, column_name, column_analysis, **kwargs)
        duration = time.time() - start_time
        
        # Store results
        results['checks_run'].append(check_name)
        results['total_errors'] += error_count
        results['error_details'].extend(errors)
        results['check_results'][check_name] = {
            'passed': error_count == 0,
            'error_count': error_count,
            'duration_seconds': duration
        }
        
        # Only log if verbose mode is enabled
        if verbose:
            check_desc = VALIDATION_TYPES.get(check_name, check_name)
            status = "✅ PASS" if error_count == 0 else f"❌ FAIL ({error_count} errors)"
            
            logger.info(f"   {status} {check_desc}")
            
            # Show sample errors if any
            if error_count > 0 and errors:
                # Get first few error examples
                sample_errors = errors[:3]  # Show first 3 errors
                for err in sample_errors:
                    row_num = err.get('excel_row', 'N/A')
                    actual_val = err.get('actual_value', 'N/A')[:50]  # Limit length
                    expected_val = err.get('expected_value', 'N/A')[:50]
                    
                    # Show context-specific message
                    if check_name == 'whitespace':
                        logger.info(f"     Row {row_num}: '{actual_val}' → '{expected_val}'")
                    elif check_name == 'data_type':
                        logger.info(f"     Row {row_num}: '{actual_val}' is not {expected_val}")
                    elif check_name == 'format':
                        logger.info(f"     Row {row_num}: '{actual_val}' doesn't match expected format")
                    elif check_name == 'zero_padding':
                        logger.info(f"     Row {row_num}: '{actual_val}' has unnecessary zeros")
                    elif check_name == 'numeric':
                        logger.info(f"     Row {row_num}: '{actual_val}' is not valid numeric")

    for error in results['error_details']:
        error_type = error.get('error_type', '')
        if error_type in ['non_numeric_value', 'data_type_mismatch']:
            results['numeric_issues'].append(error)
        elif error_type == 'zero_padding_issue':
            results['zero_padding_issues'].append(error)
    
    if verbose:
        if results['total_errors'] == 0:
            logger.info(f"   🎉 Column '{column_name}' passed all validations!")
        else:
            logger.info(f"   ⚠️  Column '{column_name}' has {results['total_errors']} issues")
            logger.info(f"   🔍 Errors by type: {', '.join(results['checks_run'])}")

    return results

def analyze_all_columns(df: pd.DataFrame, ui_data: Dict = None) -> Dict[str, Dict]:
    """Analyze all columns in the DataFrame with progress indication."""
    logger.info(f"Analyzing {len(df.columns)} columns...")
    
    # Check if verbose mode is enabled
    verbose = False
    if ui_data:
        verbose = ui_data.get('verbose', False)
    
    # CLI mode with progress indication
    if not ui_data or (HAS_INPUT_HANDLER and get_input_handler().mode == 'cli'):
        if verbose:
            print(f"\n🔍 Analyzing {len(df.columns)} columns...")
        else:
            print(f"\n🔍 Analyzing columns...", end='', flush=True)
    
    column_analyses = {}
    total_columns = len(df.columns)
    
    for idx, col in enumerate(df.columns, 1):
        # Show progress in CLI mode
        if not ui_data or (HAS_INPUT_HANDLER and get_input_handler().mode == 'cli'):
            if verbose:
                print(f"  [{idx}/{total_columns}] {col}")
            elif idx % 10 == 0 or idx == total_columns:
                print(f".", end='', flush=True)
        
        # Analyze column
        analysis = analyze_column(df, col)
        column_analyses[col] = analysis
        if verbose:
            inferred_type = analysis.get('inferred_type', 'unknown')
            patterns = analysis.get('patterns', [])
            null_percentage = analysis.get('null_percentage', 0)
            
            print(f"   Type: {inferred_type.upper()}")
            print(f"   Nulls: {null_percentage:.1f}%")
            print(f"   Unique: {analysis.get('unique_values', 0)} values")
            
            if patterns:
                pattern_names = [p['pattern'] for p in patterns]
                print(f"   Patterns: {', '.join(pattern_names)}")
            
            # Show sample values
            sample_count = min(3, analysis.get('unique_values', 0))
            if sample_count > 0:
                print(f"   Sample: ", end='')
                # Get sample values (you might need to adjust this)
                try:
                    sample_values = df[col].dropna().unique()[:sample_count]
                    for val in sample_values:
                        print(f"'{str(val)[:20]}' ", end='')
                    print()
                except:
                    pass

    # Complete progress line
    if not ui_data or (HAS_INPUT_HANDLER and get_input_handler().mode == 'cli'):
        if not verbose:
            print(" ✓")
        print(f"✅ Column analysis complete: {len(column_analyses)} columns analyzed")

        if verbose:
            print("\n📊 COLUMN ANALYSIS SUMMARY")
            print("="*60)
            
            type_counts = {}
            for col, analysis in column_analyses.items():
                col_type = analysis.get('inferred_type', 'unknown')
                type_counts[col_type] = type_counts.get(col_type, 0) + 1
            
            for col_type, count in type_counts.items():
                print(f"{col_type.upper()}: {count} columns")
            
            # Show columns with patterns detected
            pattern_columns = []
            for col, analysis in column_analyses.items():
                if analysis.get('patterns'):
                    pattern_columns.append(col)
            
            if pattern_columns:
                print(f"\n📋 Columns with detected patterns:")
                for col in pattern_columns:
                    patterns = analysis['patterns']
                    pattern_names = [p['pattern'] for p in patterns]
                    print(f"  {col}: {', '.join(pattern_names)}")

    return column_analyses

# ============================================================================
# SCORING SYSTEM
# ============================================================================

# ============================================================================
# SCORING SYSTEM - MODIFIED FOR RECORDS-BASED SCORING
# ============================================================================

def calculate_quality_score(df: pd.DataFrame, validation_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate quality score using the formula:
    Quality Score = (Good Records / Total Records) × 100
    
    Where:
    - Total Records = Number of rows in dataset
    - Good Records = Records without ANY validation errors
    - Bad Records = Records with at least one validation error
    """
    total_rows = len(df)
    
    if total_rows == 0:
        return {
            'quality_score': 0,
            'total_records': 0,
            'good_records': 0,
            'bad_records': 0,
            'assessment_category': 'UNKNOWN'
        }
    
    # Collect all rows with errors
    error_rows = set()
    total_errors = 0
    
    for column_name, column_result in validation_results['column_results'].items():
        for error in column_result['error_details']:
            error_rows.add(error['row_index'])
        total_errors += column_result['total_errors']
    
    # Calculate records-based metrics
    bad_records = len(error_rows)
    good_records = total_rows - bad_records
    
    # Calculate quality score based on records
    quality_score = (good_records / total_rows) * 100 if total_rows > 0 else 0
    
    # Determine assessment category
    if quality_score >= 95:
        assessment_category = "EXCELLENT"
    elif quality_score >= 80:
        assessment_category = "GOOD"
    elif quality_score >= 60:
        assessment_category = "FAIR"
    else:
        assessment_category = "POOR"
    
    return {
        'quality_score': round(quality_score, 2),
        'total_records': total_rows,
        'good_records': good_records,
        'bad_records': bad_records,
        'total_errors': total_errors,  # Keep for reference
        'errors_per_bad_record': total_errors / bad_records if bad_records > 0 else 0,
        'assessment_category': assessment_category
    }

# ============================================================================
# MAIN VALIDATION ENGINE
# ============================================================================

def run_comprehensive_validations(df: pd.DataFrame, source_info: str, 
                                error_logger: ErrorLogger, session_id: str,
                                ui_data: Dict = None, verbose: bool = False) -> Dict[str, Any]:
    """Run comprehensive validations on the DataFrame with optional verbosity."""
    logger.info(f"Starting comprehensive validations on {len(df):,} rows, {len(df.columns)} columns")
    
    start_time = time.time() 

    batch_size = ui_data.get('batch_size', None) if ui_data else None
    
    if batch_size is None:
        # Auto-calculate optimal batch size
        try:
            batch_size = get_optimal_batch_size(df)
            logger.info(f"Auto-calculated batch size: {batch_size} rows")
        except:
            batch_size = 10000  # Fallback
            logger.info(f"Using default batch size: {batch_size} rows")
    
    # Add memory monitoring during processing
    start_memory = psutil.Process().memory_info().rss / (1024 * 1024)  # MB

    all_numeric_issues = []
    all_zero_padding_issues = []
    mode = 'cli'
    if ui_data or (HAS_INPUT_HANDLER and get_input_handler().mode == 'ui'):
        mode = 'ui'
    
    if mode == 'cli' and verbose:
        print(f"\n🔍 Starting comprehensive validations")
        print(f"   Rows: {len(df):,}, Columns: {len(df.columns)}")
        print("="*50)
    
    # Get configuration from UI data or use defaults
    if ui_data:
        checks_to_run = ui_data.get('checks_to_run', ['all'])
        column_overrides = ui_data.get('column_overrides', {})
        batch_size = ui_data.get('batch_size', 10000)
        verbose = ui_data.get('verbose', False)
    else:
        checks_to_run = ['all']
        column_overrides = {}
        batch_size = 10000
    
    # Analyze all columns
    column_analyses = analyze_all_columns(df, ui_data)
    
    # Prepare results structure
    validation_results = {
        'session_id': session_id,
        'source_info': source_info,
        'data_stats': {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns_checked': 0,
            'column_analyses': column_analyses
        },
        'column_data_types': {col: analysis.get('data_type', 'unknown') for col, analysis in column_analyses.items()},
        'validation_config': {
            'checks_to_run': checks_to_run,
            'column_overrides': column_overrides,
            'batch_size': batch_size,
            'verbose': verbose
        },
        'column_results': {},
        'summary': {
            'total_checks': 0,
            'total_errors': 0,
            'columns_with_errors': 0,
            'check_breakdown': {}
        },
        'quality_metrics': {},
        'errors': [],
        'recommendations': []
    }
    
    # Track progress
    total_columns = len(df.columns)
    processed_columns = 0
    
    # Process each column
    all_errors = []
    
    for col_idx, column_name in enumerate(df.columns, 1):
        if mode == 'cli' and verbose:
            print(f"  [{col_idx}/{total_columns}] Validating: {column_name}")
        
        column_analysis = column_analyses[column_name]
        
        # Apply column overrides if any
        column_checks = checks_to_run.copy()
        if column_name in column_overrides:
            override = column_overrides[column_name]
            if 'checks' in override:
                column_checks = override['checks']
        
        # Run validations on this column
        column_result = run_validations_on_column(df, column_name, column_analysis, 
                                                 column_checks, ui_data, verbose)
        
        # Store results
        validation_results['column_results'][column_name] = column_result
        all_errors.extend(column_result['error_details'])
        
        # Update summary
        validation_results['summary']['total_checks'] += len(column_result['checks_run'])
        validation_results['summary']['total_errors'] += column_result['total_errors']
        
        if column_result['total_errors'] > 0:
            validation_results['summary']['columns_with_errors'] += 1
        
        # Update check breakdown
        for check_name, check_result in column_result['check_results'].items():
            if check_name not in validation_results['summary']['check_breakdown']:
                validation_results['summary']['check_breakdown'][check_name] = {
                    'total_runs': 0,
                    'total_errors': 0,
                    'passed_runs': 0,
                    'failed_runs': 0
                }
            
            breakdown = validation_results['summary']['check_breakdown'][check_name]
            breakdown['total_runs'] += 1
            breakdown['total_errors'] += check_result['error_count']
            
            if check_result['passed']:
                breakdown['passed_runs'] += 1
            else:
                breakdown['failed_runs'] += 1
        
        processed_columns += 1
    
    validation_results['data_stats']['columns_checked'] = processed_columns
    
    # Calculate quality score (NOW RECORDS-BASED)
    validation_results['quality_metrics'] = calculate_quality_score(df, validation_results)
    
    # Generate recommendations
    validation_results['recommendations'] = generate_recommendations(validation_results)
    
    # Log errors to database in batches for performance (WITH QUIETER LOGGING)
    if all_errors and error_logger:
        if mode == 'cli' and verbose:
            print(f"\n📋 Logging {len(all_errors)} validation errors to database...")
        
        # Batch log errors with quieter logging
        logged_count = error_logger.log_batch_errors(session_id, 'advanced', source_info, all_errors)
        
        if mode == 'cli' and verbose:
            print(f"✅ Logged {logged_count} errors to database")
    
    # Store errors in results (limited for UI display)
    validation_results['errors'] = all_errors[:100]  # Limit to 100 for UI
    
    for col_name, col_result in validation_results['column_results'].items():
        all_numeric_issues.extend(col_result.get('numeric_issues', []))
        all_zero_padding_issues.extend(col_result.get('zero_padding_issues', []))

    validation_results['numeric_tab_issues'] = {
        'non_numeric_values': all_numeric_issues,
        'zero_padding_issues': all_zero_padding_issues,
        'total_count': len(all_numeric_issues) + len(all_zero_padding_issues)
    }
    if mode == 'cli' and verbose:
        print(f"\n✅ Validations completed:")
        print(f"   • Total checks: {validation_results['summary']['total_checks']}")
        print(f"   • Total errors: {validation_results['summary']['total_errors']}")
        print(f"   • Quality score: {validation_results['quality_metrics']['quality_score']:.1f}%")
    
    # At the end, where performance metrics are added:
    end_time = time.time()
    processing_time = end_time - start_time

    end_memory = psutil.Process().memory_info().rss / (1024 * 1024)  # MB
    memory_used = end_memory - start_memory
    
    # Add memory usage to results
    validation_results['performance_metrics'] = {
        'memory_used_mb': round(memory_used, 2),
        'batch_size_used': batch_size,
        'processing_time_seconds': round(processing_time, 2),  # Use processing_time instead
        'rows_per_second': round(len(df) / processing_time, 2) if processing_time > 0 else 0
    }

    if mode == 'cli':
        log_validation_summary(validation_results, mode)
    return validation_results

def generate_recommendations(validation_results: Dict[str, Any]) -> List[str]:
    """Generate actionable recommendations based on validation results with records-based insights."""
    recommendations = []
    summary = validation_results['summary']
    column_results = validation_results['column_results']
    metrics = validation_results['quality_metrics']
    
    # Get quality score for context
    quality_score = metrics['quality_score']
    
    # 1. General recommendations based on quality score
    if quality_score >= 95:
        recommendations.append("Data quality is excellent. Maintain current validation standards.")
    elif quality_score >= 80:
        recommendations.append("Data quality is good. Minor improvements can be made.")
    elif quality_score >= 60:
        recommendations.append("Data quality is fair. Significant improvements needed.")
    else:
        recommendations.append("Data quality is poor. Major data cleansing required.")
    
    # 2. Records-based recommendations
    if metrics['bad_records'] > 0:
        bad_record_percentage = (metrics['bad_records'] / metrics['total_records']) * 100
        recommendations.append(f"Focus on cleaning {metrics['bad_records']:,} bad records ({bad_record_percentage:.1f}% of total).")
        
        if metrics['errors_per_bad_record'] > 5:
            recommendations.append(f"Bad records have multiple issues (avg {metrics['errors_per_bad_record']:.1f} errors each). Clean them comprehensively.")
    
    # 3. Check-specific recommendations
    for check_name, breakdown in summary['check_breakdown'].items():
        if breakdown['failed_runs'] > 0:
            if check_name == 'whitespace':
                recommendations.append("Trim leading/trailing whitespace from text fields using .strip() in Python or TRIM() in SQL")
            elif check_name == 'zero_padding':
                recommendations.append("Remove unnecessary zero-padding from numeric fields using CAST() in SQL or int()/float() in Python")
            elif check_name == 'data_type':
                recommendations.append("Review and correct data type mismatches in columns with validation errors")
            elif check_name == 'format':
                recommendations.append("Ensure data follows expected format patterns (email, phone, date, etc.)")
            elif check_name == 'numeric':
                recommendations.append("Validate numeric fields contain only valid numbers and appropriate ranges")
            elif check_name == 'currency':
                recommendations.append("Standardize currency formatting or ensure proper numeric conversion")
            elif check_name == 'length':
                recommendations.append("Validate string lengths are within expected ranges")
    
    # 4. Column-specific recommendations
    problem_columns = []
    for col_name, col_result in column_results.items():
        if col_result['total_errors'] > 0:
            problem_columns.append((col_name, col_result['total_errors']))
    
    if problem_columns:
        problem_columns.sort(key=lambda x: x[1], reverse=True)
        top_problems = problem_columns[:3]
        
        # Group by error count
        high_error_cols = [col for col, count in top_problems if count > 100]
        if high_error_cols:
            recommendations.append(f"Prioritize fixing columns with high error counts: {', '.join(high_error_cols)}")
        
        # Specific column advice
        for col, count in top_problems:
            col_type = column_results[col]['inferred_type']
            error_rate = (count / metrics['total_records']) * 100
            
            if error_rate > 50:
                recommendations.append(f"Column '{col}' ({col_type}) has errors in {error_rate:.1f}% of records - needs major review")
            elif error_rate > 20:
                recommendations.append(f"Column '{col}' ({col_type}) has errors in {error_rate:.1f}% of records - needs attention")
            elif error_rate > 5:
                recommendations.append(f"Column '{col}' ({col_type}) has errors in {error_rate:.1f}% of records - minor cleanup needed")
    
    # 5. Performance recommendations for large datasets
    if metrics['total_records'] > 100000:
        recommendations.append("For large datasets, consider implementing incremental validation to improve performance.")
    
    # Remove duplicates while preserving order
    seen = set()
    unique_recommendations = []
    for rec in recommendations:
        if rec not in seen:
            seen.add(rec)
            unique_recommendations.append(rec)
    
    # Limit to 10 most important recommendations
    return unique_recommendations[:10]

def print_validation_summary(results: Dict[str, Any], mode: str = 'cli'):
    """Print formatted validation summary for CLI."""
    if mode != 'cli':
        return
    
    quality_metrics = results.get('quality_metrics', {})
    data_stats = results.get('data_stats', {})
    summary = results.get('summary', {})
    
    print("\n" + "="*70)
    print("📊 ADVANCED DATA QUALITY VALIDATION SUMMARY")
    print("="*70)
    
    # Quality Score with emoji
    score = quality_metrics.get('quality_score', 0)
    if score >= 95:
        score_icon = "🏆"
        score_color = "\033[92m"  # Green
    elif score >= 80:
        score_icon = "✅"
        score_color = "\033[93m"  # Yellow
    elif score >= 60:
        score_icon = "⚠️"
        score_color = "\033[93m"  # Yellow
    else:
        score_icon = "❌"
        score_color = "\033[91m"  # Red
    
    print(f"{score_icon} Quality Score: {score_color}{score:.1f}%\033[0m")
    print(f"📈 Assessment: {quality_metrics.get('assessment_category', 'UNKNOWN')}")
    print("-"*70)
    
    # Records Summary
    print("📋 RECORDS SUMMARY:")
    print(f"   • Total Records: {quality_metrics.get('total_records', 0):,}")
    print(f"   • Good Records: {quality_metrics.get('good_records', 0):,} "
          f"({quality_metrics.get('good_records', 0)/max(quality_metrics.get('total_records', 1), 1)*100:.1f}%)")
    print(f"   • Bad Records: {quality_metrics.get('bad_records', 0):,} "
          f"({quality_metrics.get('bad_records', 0)/max(quality_metrics.get('total_records', 1), 1)*100:.1f}%)")
    
    # Column Summary
    print("\n📊 COLUMN ANALYSIS:")
    print(f"   • Total Columns: {data_stats.get('total_columns', 0)}")
    print(f"   • Columns Checked: {data_stats.get('columns_checked', 0)}")
    print(f"   • Columns with Errors: {summary.get('columns_with_errors', 0)}")
    
    # Check Breakdown
    if summary.get('check_breakdown'):
        print("\n🔍 VALIDATION CHECK BREAKDOWN:")
        for check_name, breakdown in summary['check_breakdown'].items():
            check_desc = VALIDATION_TYPES.get(check_name, check_name.replace('_', ' ').title())
            success_rate = (breakdown['passed_runs'] / breakdown['total_runs'] * 100) if breakdown['total_runs'] > 0 else 0
            
            if success_rate == 100:
                status_icon = "✅"
            elif success_rate >= 80:
                status_icon = "⚠️"
            else:
                status_icon = "❌"
            
            print(f"   {status_icon} {check_desc}:")
            print(f"     Runs: {breakdown['total_runs']}, "
                  f"Errors: {breakdown['total_errors']:,}, "
                  f"Success Rate: {success_rate:.0f}%")
    
    # Performance Metrics
    if results.get('performance_metrics'):
        perf = results['performance_metrics']
        print("\n⏱️  PERFORMANCE METRICS:")
        print(f"   • Processing Time: {perf.get('processing_time_seconds', 0):.1f}s")
        print(f"   • Memory Used: {perf.get('memory_used_mb', 0):.0f} MB")
        print(f"   • Throughput: {perf.get('rows_per_second', 0):.0f} rows/second")
        print(f"   • Batch Size: {perf.get('batch_size_used', 0):,} rows")
    
    # Top Problematic Columns
    column_results = results.get('column_results', {})
    problem_columns = [(col, res['total_errors']) 
                      for col, res in column_results.items() 
                      if res.get('total_errors', 0) > 0]
    
    if problem_columns:
        problem_columns.sort(key=lambda x: x[1], reverse=True)
        print("\n🚨 TOP 5 PROBLEMATIC COLUMNS:")
        for col, count in problem_columns[:5]:
            col_type = column_results[col].get('inferred_type', 'unknown')
            error_rate = (count / quality_metrics.get('total_records', 1)) * 100
            
            if error_rate > 50:
                severity = "🔴 CRITICAL"
            elif error_rate > 20:
                severity = "🟠 HIGH"
            elif error_rate > 5:
                severity = "🟡 MEDIUM"
            else:
                severity = "🟢 LOW"
            
            print(f"   {severity} {col} ({col_type}): {count:,} errors ({error_rate:.1f}% of records)")
    
    # Recommendations
    if results.get('recommendations'):
        print("\n💡 RECOMMENDATIONS:")
        for i, rec in enumerate(results['recommendations'][:5], 1):
            print(f"   {i}. {rec}")
        
        if len(results['recommendations']) > 5:
            print(f"   ... and {len(results['recommendations']) - 5} more")
    
    print("\n📋 Session ID:", results.get('session_id', 'N/A'))
    print("="*70)

# ============================================================================
# REPORT GENERATION - MODIFIED FOR RECORDS DISPLAY
# ============================================================================

def generate_comprehensive_report(validation_results: Dict[str, Any], mode: str = 'cli') -> Dict[str, Any]:
    """Generate comprehensive validation report with records-based metrics."""
    results = validation_results.copy()
    
    if mode == 'cli':
        print("\n" + "="*70)
        print("📊 COMPREHENSIVE DATA QUALITY VALIDATION REPORT")
        print("="*70)
        
        # Header
        print(f"Session ID: {results['session_id']}")
        print(f"Source: {results['source_info']}")
        print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-"*70)
        
        # Data Statistics
        stats = results['data_stats']
        print(f"\n📈 DATA STATISTICS:")
        print(f"   • Total rows: {stats['total_rows']:,}")
        print(f"   • Total columns: {stats['total_columns']}")
        print(f"   • Columns checked: {stats['columns_checked']}")
        print("-"*70)
        
        # Quality Metrics - NOW RECORDS-BASED
        metrics = results['quality_metrics']
        print(f"\n🎯 QUALITY METRICS:")
        print(f"   • Quality Score: {metrics['quality_score']:.1f}%")
        print(f"   • Assessment: {metrics['assessment_category']}")
        print(f"   • Total records: {metrics['total_records']:,}")
        print(f"   • Good records: {metrics['good_records']:,} ({metrics['good_records']/metrics['total_records']*100:.1f}%)")
        print(f"   • Bad records: {metrics['bad_records']:,} ({metrics['bad_records']/metrics['total_records']*100:.1f}%)")
        
        print("-"*70)
        
        # Validation Summary
        summary = results['summary']
        print(f"\n🔍 VALIDATION SUMMARY:")
        print(f"   • Total checks run: {summary['total_checks']}")
        print(f"   • Total errors found: {summary['total_errors']:,}")
        print(f"   • Columns with errors: {summary['columns_with_errors']}")

        if metrics['bad_records'] > 0:
            print(f"   • Avg errors per bad record: {metrics['errors_per_bad_record']:.1f}")
        
        # Check Breakdown
        if summary['check_breakdown']:
            print(f"\n📊 CHECK BREAKDOWN:")
            for check_name, breakdown in summary['check_breakdown'].items():
                check_desc = VALIDATION_TYPES.get(check_name, check_name)
                success_rate = (breakdown['passed_runs'] / breakdown['total_runs'] * 100) if breakdown['total_runs'] > 0 else 0
                print(f"   • {check_desc}:")
                print(f"     - Runs: {breakdown['total_runs']}, "
                      f"Passed: {breakdown['passed_runs']} ({success_rate:.0f}%), "
                      f"Failed: {breakdown['failed_runs']}")
                print(f"     - Errors: {breakdown['total_errors']:,}")
        
        # Top Problematic Columns
        column_results = results['column_results']
        problem_columns = [(col, res['total_errors']) 
                          for col, res in column_results.items() 
                          if res['total_errors'] > 0]
        
        if problem_columns:
            problem_columns.sort(key=lambda x: x[1], reverse=True)
            print(f"\n🚨 TOP PROBLEMATIC COLUMNS:")
            for col, count in problem_columns[:5]:
                col_type = column_results[col]['inferred_type']
                error_percentage = (count / stats['total_rows']) * 100
                print(f"   • {col} ({col_type}): {count:,} errors ({error_percentage:.1f}% of rows)")
        
        # Recommendations
        if results['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(results['recommendations'], 1):
                print(f"   {i}. {rec}")
        
        print("\n📋 ERROR DETAILS:")
        print("   Detailed errors have been logged to dq_error_logs table")
        print(f"   Session ID for query: {results['session_id']}")
        
        print("\n" + "="*70)
    
    # Always include these in results for UI/API mode
    results['report_generated'] = datetime.now().isoformat()
    results['validation_types'] = VALIDATION_TYPES
    
    return results

# ============================================================================
# AUDIT LOGGING
# ============================================================================

# ============================================================================
# AUDIT LOGGING - UPDATED FOR RECORDS-BASED METRICS
# ============================================================================

def log_audit_record(validation_results: Dict[str, Any], source_type: str, source_name: str):
    """Log validation results to audit database with records-based metrics."""
    try:
        from dq_audit import DataQualityAudit
        
        audit = DataQualityAudit()
        metrics = validation_results['quality_metrics']
        summary = validation_results['summary']
        
        # Prepare audit data with records-based metrics
        audit_data = {
            'session_id': validation_results['session_id'],
            'check_type': 'advanced',
            'source_type': source_type,
            'source_name': source_name,
            'source_row_count': validation_results['data_stats']['total_rows'],
            'overall_score': float(metrics['quality_score']),
            'assessment_category': metrics['assessment_category'],
            'total_rules_executed': summary['total_checks'],
            'rules_passed': summary['total_checks'] - len([v for v in summary['check_breakdown'].values() if v['failed_runs'] > 0]),
            'rules_failed': len([v for v in summary['check_breakdown'].values() if v['failed_runs'] > 0]),
            'total_null_count': 0,  # Keep for compatibility
            'duplicate_row_count': 0,  # Keep for compatibility
            # Add records-based metrics
            'source_row_count': metrics['total_records'],  # Total records
            'target_row_count': metrics['good_records'],   # Good records
            'common_row_count': metrics['bad_records'],    # Bad records (for comparison)
            'issues_summary': f"Total records: {metrics['total_records']:,}, "
                            f"Good: {metrics['good_records']:,} ({metrics['quality_score']:.1f}%), "
                            f"Bad: {metrics['bad_records']:,}, "
                            f"Errors: {metrics['total_errors']:,}"
        }
        
        # Add specific error counts if available
        for check_name in ['whitespace', 'formatting', 'numeric']:
            if check_name in summary['check_breakdown']:
                audit_data[f'{check_name}_issues'] = summary['check_breakdown'][check_name]['total_errors']
            else:
                # Set to 0 for compatibility
                audit_data[f'{check_name}_issues'] = 0
        
        if APP_SETTINGS['audit_enabled']:
            audit_id = audit.log_audit_record(audit_data)
            logger.info(f"Audit record logged with ID: {audit_id}")
            return audit_id
        
    except ImportError as e:
        logger.warning(f"Audit module not available: {e}")
    except Exception as e:
        logger.warning(f"Audit logging failed: {e}")
    
    return None

# ============================================================================
# MAIN ENTRY POINTS
# ============================================================================

# ============================================================================
# MAIN ENTRY POINTS WITH VERBOSITY CONTROL
# ============================================================================

def run_advanced_checks_ui(ui_data: Dict = None) -> Dict[str, Any]:
    """
    Run advanced data checks in UI/API mode with quieter logging.
    
    Args:
        ui_data: Dictionary containing configuration with optional 'verbose' flag
    """
    logger.info("Starting advanced checks in UI/API mode")
    
    try:
        # Initialize input handler if available
        if ui_data and HAS_INPUT_HANDLER:
            init_input_handler(mode='ui', data=ui_data)
        
        # Load data
        df, source_info, source_file = load_data_for_advanced_checks(ui_data)
        
        if df is None or df.empty:
            return {'error': 'No data loaded or dataset is empty'}
        
        # Initialize error logger
        error_logger = ErrorLogger()
        if not error_logger.test_connection():
            logger.error("Cannot connect to database. Errors will not be logged!")
            return {'error': 'Cannot connect to database'}
        
        # Generate or use existing session ID
        if ui_data and 'session_id' in ui_data:
            session_id = ui_data['session_id']
            logger.info(f"Using provided Session ID: {session_id}")
        else:
            session_id = f"ADV_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{np.random.randint(1000, 9999)}"
            logger.info(f"Generated new Session ID: {session_id}")
        
        # Check for verbose flag (default to False for UI/API)
        verbose = ui_data.get('verbose', False) if ui_data else False
        
        # Run comprehensive validations with verbosity control
        validation_results = run_comprehensive_validations(
            df, source_info, error_logger, session_id, ui_data, verbose
        )
        
        # Determine source type for audit logging
        if 'csv' in source_info.lower():
            source_type = 'csv'
        elif 'excel' in source_info.lower():
            source_type = 'excel'
        else:
            source_type = 'database'
        
        # Log audit record
        log_audit_record(validation_results, source_type, source_info)
        
        # Generate report
        final_results = generate_comprehensive_report(validation_results, mode='ui')
        
        # Add error logs for API response (limited)
        try:
            final_results['error_logs'] = error_logger.get_error_logs_for_session(session_id, limit=100)
            final_results['error_summary'] = error_logger.get_error_summary_for_session(session_id)
        except Exception as e:
            logger.warning(f"Could not get error logs: {e}")
            final_results['error_logs'] = []
            final_results['error_summary'] = {'total_errors': 0}
        
        # Add audit logs if available
        try:
            from dq_audit import DataQualityAudit
            audit_logger = DataQualityAudit()
            final_results['audit_logs'] = audit_logger.get_audit_logs_for_session(session_id)
        except:
            final_results['audit_logs'] = []
        
        logger.info(f"Advanced checks completed for session {session_id}")
        return final_results
        
    except Exception as e:
        logger.error(f"Error in advanced data quality analysis: {str(e)}", exc_info=True)
        return {'error': str(e)}

# ============================================================================
# ALIAS FUNCTIONS FOR COMPATIBILITY
# ============================================================================

def run_advanced_analysis_ui(ui_data: Dict = None) -> Dict[str, Any]:
    """
    Alias for run_advanced_checks_ui for API compatibility.
    This ensures backward compatibility with existing API calls.
    """
    return run_advanced_checks_ui(ui_data)

def log_validation_summary(validation_results: Dict[str, Any], mode: str = 'cli'):
    """Log a meaningful summary of validation results."""
    if mode != 'cli':
        return
    
    total_errors = validation_results['summary']['total_errors']
    columns_with_errors = validation_results['summary']['columns_with_errors']
    total_columns = validation_results['data_stats']['total_columns']
    
    print(f"\n📊 VALIDATION SUMMARY")
    print("="*60)
    print(f"Total Columns Checked: {total_columns}")
    print(f"Columns with Issues: {columns_with_errors}")
    print(f"Total Issues Found: {total_errors}")
    
    if total_errors > 0:
        print(f"\n🚨 ISSUES BY COLUMN:")
        print("-"*40)
        
        column_results = validation_results['column_results']
        for col_name, col_data in column_results.items():
            if col_data['total_errors'] > 0:
                errors_by_type = {}
                for error in col_data['error_details']:
                    error_type = error.get('error_type', 'unknown')
                    errors_by_type[error_type] = errors_by_type.get(error_type, 0) + 1
                
                error_types_str = ', '.join([f"{k}: {v}" for k, v in errors_by_type.items()])
                print(f"  {col_name}: {col_data['total_errors']} issues ({error_types_str})")
        
        print(f"\n💡 TOP RECOMMENDATIONS:")
        print("-"*40)
        recommendations = validation_results.get('recommendations', [])
        for i, rec in enumerate(recommendations[:3], 1):
            print(f"  {i}. {rec}")
    
    # Quality score
    quality_metrics = validation_results.get('quality_metrics', {})
    if quality_metrics:
        print(f"\n🎯 QUALITY SCORE: {quality_metrics.get('quality_score', 0):.1f}%")
        print(f"   Good Records: {quality_metrics.get('good_records', 0):,}")
        print(f"   Bad Records: {quality_metrics.get('bad_records', 0):,}")
    
    print("="*60)

def main(ui_data: Dict = None):
    """Main function for CLI mode with verbosity control."""
    logger.info("Starting advanced data quality analysis")
    
    try:
        # If UI data provided, run in UI mode
        if ui_data and HAS_INPUT_HANDLER:
            return run_advanced_checks_ui(ui_data)
        
        # CLI Mode with improved output
        print("\n" + "="*60)
        print("⚡ COMPREHENSIVE DATA QUALITY VALIDATION SYSTEM")
        print("="*60)
        print("ℹ️  This system automatically:")
        print("   • Analyzes column characteristics")
        print("   • Detects data patterns")
        print("   • Runs appropriate validations")
        print("   • Calculates quality score")
        print("   • Logs errors to database")
        print("="*60)
        
        # Load data
        df, source_info, source_file = load_data_for_advanced_checks()
        
        if df is None or df.empty:
            print("❌ No data loaded or dataset is empty")
            return
        
        # Show data statistics
        print(f"\n📥 DATA LOADED SUCCESSFULLY:")
        print(f"   • Source: {source_info}")
        print(f"   • Rows: {len(df):,}")
        print(f"   • Columns: {len(df.columns)}")
        print(f"   • Memory Usage: {df.memory_usage(deep=True).sum() / (1024*1024):.1f} MB")
        
        # Initialize error logger
        print("\n🔧 Initializing Error Logger...")
        error_logger = ErrorLogger()
        
        if not error_logger.test_connection():
            print("❌ ERROR: Cannot connect to database. Errors will not be logged!")
            proceed = input("Continue without database logging? (y/n): ").strip().lower()
            if proceed != 'y':
                return
        
        # Get configuration from user
        print("\n📋 VALIDATION CONFIGURATION")
        print("-"*40)
        
        checks_to_run = []
        print("\nSelect validations to run (comma-separated):")
        for i, (check_name, check_desc) in enumerate(VALIDATION_TYPES.items(), 1):
            print(f"  {i}. {check_name}: {check_desc}")
        print("  all. Run all suggested validations")
        
        choice = input("\nEnter choices: ").strip()
        
        if choice.lower() == 'all':
            checks_to_run = ['all']
        else:
            choices = [c.strip() for c in choice.split(',')]
            valid_checks = list(VALIDATION_TYPES.keys())
            checks_to_run = [valid_checks[int(c)-1] for c in choices if c.isdigit() and 1 <= int(c) <= len(valid_checks)]
        
        if not checks_to_run:
            print("⚠️  No valid checks selected. Using 'all'.")
            checks_to_run = ['all']
        
        # Ask about verbosity
        print("\n📢 VERBOSITY LEVEL:")
        print("  1. Quiet (show only final results)")
        print("  2. Detailed (show progress during validation)")
        
        verbose_choice = input("\nSelect verbosity level (1-2, default=1): ").strip()
        verbose = (verbose_choice == '2')
        
        # Create UI data structure
        ui_data = {
            'checks_to_run': checks_to_run,
            'batch_size': 10000,
            'verbose': verbose
        }
        
        # Generate session ID
        session_id = f"ADV_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"\n📋 Session ID: {session_id}")
        
        # Run validations
        validation_results = run_comprehensive_validations(
            df, source_info, error_logger, session_id, ui_data, verbose
        )
        
        # Generate report
        generate_comprehensive_report(validation_results, mode='cli')
        
        # Determine source type for audit logging
        if 'csv' in source_info.lower():
            source_type = 'csv'
        elif 'excel' in source_info.lower():
            source_type = 'excel'
        else:
            source_type = 'database'
        
        # Log audit record
        log_audit_record(validation_results, source_type, source_info)
        
        print(f"\n✅ Advanced data quality analysis completed!")
        print(f"📋 Session ID: {session_id}")
        print("="*60)
        
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        logger.info("Advanced checks cancelled by user")
    except Exception as e:
        logger.error(f"Error in advanced data quality analysis: {str(e)}", exc_info=True)
        print(f"\n❌ Error in advanced checks: {e}")
        print("Please check the log file for details.")

# ============================================================================
# HELPER FUNCTIONS (from original code - kept for compatibility)
# ============================================================================

def get_database_config(mode='cli', ui_data=None):
    """Get database configuration with hierarchical browsing option."""
    logger.info("GETTING DATABASE CONFIGURATION")
    
    # If UI data provided and contains db_config, use it directly
    if ui_data and 'db_config' in ui_data:
        db_config = ui_data['db_config']
        
        # Check if this came from hierarchical selection
        if 'selection_mode' in db_config and db_config['selection_mode'] == 'hierarchical':
            logger.info("Using hierarchical database selection from UI data")
            if mode == 'cli' and not ui_data:
                print(f"   • Using pre-selected database: {db_config.get('database', 'N/A')}.{db_config.get('table', 'N/A')}")
            return db_config
        
        # Normalize database config
        return normalize_database_config(db_config)
    
    # Check if config file mode is enabled
    use_config_file = APP_SETTINGS.get('use_config_file', False) or os.environ.get('DQ_USE_CONFIG_FILE', '').lower() == 'true'
    
    # CLI Mode - interactive selection
    if mode == 'cli' and not ui_data:
        print("\n📋 DATABASE CONNECTION METHOD:")
        
        # Build menu options
        options = []
        if use_config_file:
            options.append("Use config file")
        options.append("Manual entry")
        
        if HAS_DATABASE_NAVIGATOR:
            options.append("Browse and select database/schema/table")
        
        # Display menu
        for i, option in enumerate(options, 1):
            print(f"{i}. {option}")
        
        # Get user choice
        try:
            choice = int(input(f"\nEnter choice (1-{len(options)}): ").strip())
            if choice < 1 or choice > len(options):
                print("❌ Invalid choice. Using manual entry.")
                choice = 2 if use_config_file else 1
        except ValueError:
            print("❌ Invalid input. Using manual entry.")
            choice = 2 if use_config_file else 1
        
        # Handle choice
        selected_option = options[choice - 1]
        
        if selected_option == "Use config file" and use_config_file:
            # Option 1: Use config file
            try:
                from dq_unified import get_config_file_database_config
                print("\n📋 Select database type:")
                print("1. PostgreSQL")
                print("2. MySQL")
                print("3. Oracle")
                print("4. SQL Server")
                print("5. SQLite")
                
                db_choice = input("\nEnter choice (1-5): ").strip()
                
                if db_choice not in ['1', '2', '3', '4', '5']:
                    print("❌ Invalid choice. Using manual entry instead.")
                    return get_database_config_manual(mode)
                
                db_config = get_config_file_database_config(db_choice)
                
                if db_config:
                    logger.info(f"Database config loaded from file: {db_config.get('type')}")
                    
                    # Ask for table name
                    table_name = input("\nEnter table name: ").strip()
                    if table_name:
                        db_config['table'] = table_name
                        # Normalize the config
                        db_config = normalize_database_config(db_config)
                        return db_config
                    else:
                        print("❌ Table name is required")
                        return None
                else:
                    print("❌ Could not load database config from file")
                    return get_database_config_manual(mode)
                    
            except ImportError:
                print("⚠️  Config file functionality not available")
                return get_database_config_manual(mode)
        
        elif selected_option == "Manual entry":
            # Option 2: Manual entry
            return get_database_config_manual(mode)
        
        elif selected_option == "Browse and select database/schema/table" and HAS_DATABASE_NAVIGATOR:
            # Option 3: Hierarchical browsing
            print("\n🔍 LAUNCHING DATABASE BROWSER...")
            print("-" * 50)
            
            # Call the database navigator
            result = navigate_database(mode='cli')
            
            if result and 'db_config' in result:
                db_config = result['db_config']
                
                # Add selection mode flag
                db_config['selection_mode'] = 'hierarchical'
                
                # Ensure schema is normalized
                db_config = normalize_database_config(db_config)
                
                logger.info(f"Database selected via browser: {db_config.get('type')} - {db_config.get('database')}.{db_config.get('schema')}.{db_config.get('table')}")
                print(f"✅ Selected: {db_config.get('database')}.{db_config.get('schema')}.{db_config.get('table')}")
                
                return db_config
            else:
                print("❌ Database selection cancelled or failed")
                return None
    
    else:
        # UI/API Mode or fallback - use manual entry
        return get_database_config_manual(mode)

def get_database_config_manual(mode='cli'):
    """Manual database configuration entry with better user feedback."""
    logger.info("GETTING MANUAL DATABASE CONFIGURATION")
    
    if mode == 'cli':
        print("\n📝 MANUAL DATABASE CONFIGURATION")
        print("-" * 40)
    
    # Database type selection
    if mode == 'cli':
        print("\nSelect database type:")
        print("1. PostgreSQL")
        print("2. MySQL")
        print("3. Oracle") 
        print("4. SQL Server")
        print("5. SQLite")
        
        db_choice = input("\nEnter choice (1-5): ").strip()
    else:
        db_choice = "2"  # Default to MySQL for non-interactive
    
    db_type_map = {
        '1': 'postgresql',
        '2': 'mysql',
        '3': 'oracle',
        '4': 'sqlserver',
        '5': 'sqlite'
    }
    
    db_type = db_type_map.get(db_choice, 'mysql')
    
    # Initialize config
    db_config = {
        'type': db_type,
        'selection_mode': 'manual'
    }
    
    # Get connection details based on database type
    if db_type == 'sqlite':
        if mode == 'cli':
            print(f"\n🔧 Configuring {db_type.upper()} connection:")
            
            file_path = input("Enter SQLite file path: ").strip()
            if not file_path:
                print("❌ File path is required for SQLite")
                return None
            
            if not os.path.exists(file_path):
                print(f"⚠️  File not found: {file_path}")
                create_file = input("Create new SQLite database? (y/n): ").strip().lower()
                if create_file != 'y':
                    return None
            
            db_config['file_path'] = file_path
            
            table_name = input("Enter table name: ").strip()
            if not table_name:
                print("❌ Table name is required")
                return None
            db_config['table'] = table_name
            
        else:
            # For non-interactive mode, these should be in ui_data
            if mode == 'cli':
                print(f"   • Configuring SQLite database")
            return db_config
            
    else:
        if mode == 'cli':
            print(f"\n🔧 Configuring {db_type.upper()} connection:")
        
        # Get connection details
        if mode == 'cli':
            db_config['host'] = input(f"Enter host [localhost]: ").strip() or 'localhost'
            
            default_port = '5432' if db_type == 'postgresql' else '3306' if db_type == 'mysql' else '1521' if db_type == 'oracle' else '1433'
            port_input = input(f"Enter port [{default_port}]: ").strip()
            db_config['port'] = int(port_input) if port_input else int(default_port)
            
            db_config['database'] = input("Enter database name: ").strip()
            if not db_config['database']:
                print("❌ Database name is required")
                return None
            
            db_config['user'] = input("Enter username: ").strip()
            db_config['password'] = input("Enter password: ").strip()
            
            table_name = input("Enter table name: ").strip()
            if not table_name:
                print("❌ Table name is required")
                return None
            db_config['table'] = table_name
            
            # Oracle-specific settings
            if db_type == 'oracle':
                db_config['service_name'] = input("Enter service name [XE]: ").strip() or 'XE'
                db_config['encoding'] = 'UTF-8'
        else:
            # For non-interactive mode
            if mode == 'cli':
                print(f"   • Configuring {db_type.upper()} database")
            db_config.update({
                'host': 'localhost',
                'port': 3306 if db_type == 'mysql' else 5432 if db_type == 'postgresql' else 1521,
                'database': '',
                'user': '',
                'password': '',
                'table': ''
            })
    
    # Normalize the config (ensure schema field)
    db_config = normalize_database_config(db_config)
    
    if mode == 'cli':
        print(f"   ✓ Database configuration complete")
    
    logger.info(f"Manual database config created: {db_type}")
    return db_config

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

# ============================================================================
# DATA LOADING WITH PROGRESS INDICATION
# ============================================================================

def load_data_for_advanced_checks(ui_data=None):
    """Load data from any source for advanced checks with progress indication."""
    logger.info("LOADING DATA FOR ADVANCED CHECKS")
    
    # Determine mode
    is_ui_mode = False
    if ui_data and HAS_INPUT_HANDLER:
        init_input_handler(mode='ui', data=ui_data)
        is_ui_mode = True
    
    # Show loading message
    if not is_ui_mode and not ui_data:
        print("\n🔍 LOADING DATA...")
    
    # Load data using existing select_data_source
    if ui_data and HAS_INPUT_HANDLER:
        source_type, source_config = select_data_source(ui_data)
    else:
        source_type, source_config = select_data_source()
    
    if source_config is None:
        logger.error("No data source configuration available")
        return None, None, None
    
    # Show loading progress
    if not is_ui_mode and not ui_data:
        if source_type in ['csv', 'excel']:
            filename = os.path.basename(source_config) if isinstance(source_config, str) else 'file'
            print(f"   • Loading {source_type.upper()}: {filename}")
        else:
            print(f"   • Loading Database table")
    
    # Load data
    df = load_data_from_source(source_type, source_config)
    
    if df is None:
        logger.error("Failed to load data")
        return None, None, None
    
    # Create source info string
    if source_type in ['csv', 'excel']:
        source_info = f"{source_type.upper()}: {os.path.basename(source_config)}"
        source_file = os.path.basename(source_config)
    else:
        source_info = f"Database: {source_config['type']} - Table: {source_config['table']}"
        source_file = f"{source_config['type']}_{source_config['table']}"
    
    # Show loaded statistics
    if not is_ui_mode and not ui_data:
        print(f"   ✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
    
    logger.info(f"Data loaded: {len(df):,} rows, {len(df.columns)} columns from {source_info}")
    return df, source_info, source_file

if __name__ == "__main__":
    main()