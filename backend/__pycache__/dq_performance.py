# dq_comparison.py - UPDATED VERSION (WITHOUT LENGTH CHECK, WITH ERROR LOGGING)
import pandas as pd
import os
import logging
import re
import hashlib
from datetime import datetime
from dq_unified import select_data_source, load_data_from_source
from app_config import APP_SETTINGS, QUALITY_THRESHOLDS, DATA_PATTERNS
from dq_error_log import ErrorLogger  # NEW IMPORT

# Setup logger
logger = logging.getLogger(__name__)

def load_comparison_sources():
    """Load source and target from any combination"""
    logger.info("📂 LOADING SOURCE AND TARGET DATA")
    print("\n" + "="*50)
    print("📂 LOADING SOURCE AND TARGET DATA")
    print("="*50)
    
    print("\n📥 SOURCE DATA:")
    source_type, source_config = select_data_source()
    if source_config is None:
        return None, None, None, None
    
    print("\n📤 TARGET DATA:")
    target_type, target_config = select_data_source()
    if target_config is None:
        return None, None, None, None
    
    # Load both datasets
    source_df = load_data_from_source(source_type, source_config)
    target_df = load_data_from_source(target_type, target_config)
    
    if source_df is None or target_df is None:
        return None, None, None, None
    
    # Create source info strings
    if source_type in ['csv', 'excel']:
        source_info = f"{source_type.upper()}: {os.path.basename(source_config)}"
        source_file = os.path.basename(source_config)
    else:
        source_info = f"Database: {source_config['type']} - Table: {source_config['table']}"
        source_file = f"{source_config['type']}_{source_config['table']}"
    
    if target_type in ['csv', 'excel']:
        target_info = f"{target_type.upper()}: {os.path.basename(target_config)}"
        target_file = os.path.basename(target_config)
    else:
        target_info = f"Database: {target_config['type']} - Table: {target_config['table']}"
        target_file = f"{target_config['type']}_{target_config['table']}"
    
    return source_df, target_df, source_info, target_info, source_file, target_file

def select_columns_for_comparison(source_df, target_df):
    """Let user select specific columns to compare with case-insensitive matching"""
    logger.info("🎯 SELECTING COLUMNS FOR COMPARISON")
    print("\n" + "="*50)
    print("🎯 SELECT COLUMNS FOR COMPARISON")
    print("="*50)
    
    # Convert all column names to lowercase for case-insensitive comparison
    source_cols_lower = [col.lower() for col in source_df.columns]
    target_cols_lower = [col.lower() for col in target_df.columns]
    
    # Find common columns (case-insensitive)
    common_cols_lower = list(set(source_cols_lower).intersection(set(target_cols_lower)))
    
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
    
    logger.info(f"Found {len(common_cols)} common columns between source and target (case-insensitive)")
    
    if not common_cols:
        logger.warning("No common columns found between source and target datasets")
        print("❌ No common columns found between source and target!")
        
        # Show available columns for debugging
        print(f"\n🔍 DEBUG: Source columns: {list(source_df.columns)}")
        print(f"🔍 DEBUG: Target columns: {list(target_df.columns)}")
        
        return [], []
    
    print(f"\n📋 Available common columns ({len(common_cols)}):")
    for i, col_info in enumerate(common_cols, 1):
        source_col = col_info['source_original']
        target_col = col_info['target_original']
        
        # Show mapping if names are different
        if source_col != target_col:
            print(f"   {i}. {source_col} (Source) ↔ {target_col} (Target)")
        else:
            print(f"   {i}. {source_col}")
    
    print(f"\n💡 Select columns to compare (e.g.: 1,3,5 or 'all' for all columns)")
    selection = input("Enter column numbers: ").strip()
    logger.info(f"User column selection input: {selection}")
    
    if selection.lower() == 'all':
        logger.info("User selected ALL common columns for comparison")
        print("✅ Comparing ALL common columns")
        # Return both the selected columns and the mapping info
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
            
            if not selected_source_cols:
                logger.warning("No valid columns selected by user, using all common columns")
                print("❌ No valid columns selected. Using all common columns.")
                selected_source_cols = [col_info['source_original'] for col_info in common_cols]
                selected_common_cols = common_cols
            
            logger.info(f"User selected {len(selected_source_cols)} columns: {selected_source_cols}")
            print(f"✅ Selected {len(selected_source_cols)} columns: {selected_source_cols}")
            return selected_source_cols, selected_common_cols
            
        except Exception as e:
            logger.warning(f"Invalid column selection input, using all common columns. Error: {e}")
            print("❌ Invalid selection. Using all common columns.")
            selected_source_cols = [col_info['source_original'] for col_info in common_cols]
            return selected_source_cols, common_cols

def get_target_column_name(source_column, common_cols):
    """Get the corresponding target column name for a source column"""
    for col_info in common_cols:
        if col_info['source_original'] == source_column:
            return col_info['target_original']
    return source_column  # Fallback to same name

def smart_normalize_value(value, column_name=""):
    """
    SMART normalization that handles various data formats dynamically
    """
    if pd.isna(value) or value is None:
        return ""  # Standardize ALL null representations
    
    # Convert to string and clean
    str_value = str(value).strip()
    
    if not str_value or str_value.lower() in ['nan', 'none', 'null', 'nat', '']:
        return ""
    
    # Handle numeric values - remove formatting differences
    try:
        # Try to convert to float to handle numeric formatting
        float_val = float(str_value)
        if float_val.is_integer():
            return str(int(float_val))  # Return as integer string
        else:
            return f"{float_val:.2f}"  # Return as float with 2 decimal places
    except (ValueError, TypeError):
        # If not numeric, handle dates and other formats
        pass
    
    # IMPROVED: Handle date/datetime formats
    try:
        # Try to parse as datetime first
        if 'date' in column_name.lower() or 'time' in column_name.lower():
            # Handle various date formats
            date_formats = [
                '%Y-%m-%d %H:%M:%S',  # 2023-01-15 00:00:00
                '%Y-%m-%d',           # 2023-01-15
                '%d-%m-%Y',           # 15-01-2023
                '%m/%d/%Y',           # 01/15/2023
                '%d/%m/%Y',           # 15/01/2023
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(str_value, fmt)
                    return parsed_date.strftime('%Y-%m-%d')  # Standardize to YYYY-MM-DD
                except ValueError:
                    continue
    except:
        pass
    
    # Handle phone numbers - remove formatting and ensure consistent representation
    if 'phone' in column_name.lower() or 'mobile' in column_name.lower():
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', str_value)
        if digits_only:
            return digits_only
    
    # Handle postal codes - ensure consistent formatting
    if 'postal' in column_name.lower() or 'zip' in column_name.lower():
        return str_value.upper().strip()
    
    # For text values, return cleaned and case-normalized string
    return str_value.strip()

def compare_structures(source_df, target_df, selected_columns, common_cols):
    """Compare dataset structures for selected columns"""
    logger.info("🏗️  STARTING STRUCTURE COMPARISON")
    print("\n🏗️  STRUCTURE COMPARISON")
    print("-" * 30)
    
    source_cols = set(source_df.columns)
    target_cols = set(target_df.columns)
    
    logger.info(f"Source columns: {len(source_cols)}, Target columns: {len(target_cols)}, Selected: {len(selected_columns)}")
    print(f"📊 Column Summary:")
    print(f"   • Source columns: {len(source_cols)}")
    print(f"   • Target columns: {len(target_cols)}")
    print(f"   • Selected for comparison: {len(selected_columns)}")
    
    # Data type comparison for selected columns
    logger.info("🔧 Starting data type comparison for selected columns")
    print(f"\n🔧 Data Type Comparison (Selected Columns):")
    type_issues = 0
    
    for col in selected_columns:
        target_col = get_target_column_name(col, common_cols)
        if col in source_df.columns and target_col in target_df.columns:
            source_dtype = str(source_df[col].dtype)
            target_dtype = str(target_df[target_col].dtype)
            
            # Check if data types are compatible (not necessarily identical)
            is_compatible = check_data_type_compatibility(source_dtype, target_dtype, col)
            
            if not is_compatible:
                logger.warning(f"Data type mismatch for column '{col}': Source={source_dtype} vs Target={target_dtype}")
                print(f"   ❌ {col}: Source={source_dtype} vs Target={target_dtype}")
                type_issues += 1
            else:
                print(f"   ✅ {col}: Source={source_dtype}, Target={target_dtype} (compatible)")
        else:
            logger.warning(f"Column '{col}' or its target counterpart not found in datasets")
            print(f"   ⚠️  {col}: Column not found in both datasets")
    
    if type_issues == 0:
        logger.info("All selected columns have compatible data types")
        print("   ✅ All selected columns have compatible data types")
    else:
        logger.info(f"Found {type_issues} data type compatibility issues")
    
    return type_issues

def check_data_type_compatibility(source_dtype, target_dtype, column_name):
    """
    Check if data types are compatible for comparison
    """
    numeric_types = ['int64', 'float64', 'int32', 'float32', 'integer', 'numeric']
    string_types = ['object', 'string', 'str', 'varchar', 'text']
    date_types = ['datetime64[ns]', 'timestamp', 'date']
    
    source_dtype_lower = str(source_dtype).lower()
    target_dtype_lower = str(target_dtype).lower()
    
    # Same types are always compatible
    if source_dtype_lower == target_dtype_lower:
        return True
    
    # Numeric types are compatible with each other
    if source_dtype_lower in numeric_types and target_dtype_lower in numeric_types:
        return True
    
    # String types are compatible with each other
    if source_dtype_lower in string_types and target_dtype_lower in string_types:
        return True
    
    # Date types are compatible with each other
    if source_dtype_lower in date_types and target_dtype_lower in date_types:
        return True
    
    # Numeric can be compared with string for phone numbers, IDs, etc.
    if (source_dtype_lower in numeric_types and target_dtype_lower in string_types) or \
       (source_dtype_lower in string_types and target_dtype_lower in numeric_types):
        if 'phone' in column_name.lower() or 'id' in column_name.lower():
            return True
    
    return False

def compare_row_counts(source_df, target_df):
    """Compare row counts and identify gaps"""
    logger.info("🔢 STARTING ROW COUNT COMPARISON")
    print("\n🔢 ROW COUNT COMPARISON")
    print("-" * 30)
    
    source_count = len(source_df)
    target_count = len(target_df)
    
    logger.info(f"Source rows: {source_count:,}, Target rows: {target_count:,}")
    print(f"📊 Row Counts:")
    print(f"   • Source: {source_count:,} rows")
    print(f"   • Target: {target_count:,} rows")
    
    if source_count == target_count:
        logger.info("✅ Row counts match perfectly")
        print("   ✅ Row counts match perfectly!")
        return True, 0, 0
    else:
        diff = abs(source_count - target_count)
        percentage_diff = (diff / max(source_count, target_count)) * 100
        logger.warning(f"Row count difference: {diff:,} rows ({percentage_diff:.2f}%)")
        
        print(f"   ❌ Row count difference: {diff:,} rows ({percentage_diff:.2f}%)")
        
        if source_count > target_count:
            logger.warning(f"Source has {diff:,} more rows than target")
            print(f"   ⚠️  Source has {diff:,} more rows than target")
            missing_in_target = diff
            extra_in_target = 0
        else:
            logger.warning(f"Target has {diff:,} more rows than source")
            print(f"   ⚠️  Target has {diff:,} more rows than source")
            missing_in_target = 0
            extra_in_target = diff
        
        return False, missing_in_target, extra_in_target

def compare_data_quality(source_df, target_df, selected_columns, common_cols, error_logger=None, session_id=None, source_info="", target_info=""):
    """Compare data quality metrics for selected columns with error logging"""
    logger.info("🎯 STARTING DATA QUALITY COMPARISON")
    print("\n🎯 DATA QUALITY COMPARISON")
    print("-" * 30)
    
    # Get target column names for selected columns
    target_columns = [get_target_column_name(col, common_cols) for col in selected_columns]
    
    # Null values comparison for selected columns
    source_nulls = source_df[selected_columns].isnull().sum().sum()
    target_nulls = target_df[target_columns].isnull().sum().sum()
    
    total_cells_source = len(source_df) * len(selected_columns)
    total_cells_target = len(target_df) * len(selected_columns)
    
    source_null_percentage = (source_nulls / total_cells_source) * 100 if total_cells_source > 0 else 0
    target_null_percentage = (target_nulls / total_cells_target) * 100 if total_cells_target > 0 else 0
    
    logger.info(f"Null values - Source: {source_nulls} ({source_null_percentage:.2f}%), Target: {target_nulls} ({target_null_percentage:.2f}%)")
    print(f"📊 Empty Cells (Selected Columns):")
    print(f"   • Source: {source_nulls:,} ({source_null_percentage:.2f}%)")
    print(f"   • Target: {target_nulls:,} ({target_null_percentage:.2f}%)")
    
    # Log null differences if error logger provided
    if error_logger and session_id:
        if source_nulls != target_nulls:
            error_logger.log_error({
                'session_id': session_id,
                'check_type': 'comparison',
                'source_name': source_info,
                'target_name': target_info,
                'column_name': 'ALL',
                'error_type': 'null_count_mismatch',
                'error_description': f"Null count mismatch: Source={source_nulls}, Target={target_nulls}",
                'actual_value': f"Source nulls: {source_nulls}",
                'expected_value': f"Target nulls: {target_nulls}"
            })
    
    null_match = source_nulls == target_nulls
    if null_match:
        logger.info("✅ Null counts match between source and target")
        print("   ✅ Null counts match")
    elif target_nulls > source_nulls:
        logger.warning("❌ Target has MORE nulls than source")
        print("   ❌ Target has MORE nulls than source")
    else:
        logger.warning("⚠️  Target has FEWER nulls than source")
        print("   ⚠️  Target has FEWER nulls than source")
    
    # Duplicate comparison for selected columns
    source_dups = source_df[selected_columns].duplicated().sum()
    target_dups = target_df[target_columns].duplicated().sum()
    
    logger.info(f"Duplicate rows - Source: {source_dups}, Target: {target_dups}")
    print(f"\n📊 Duplicate Rows (Selected Columns):")
    print(f"   • Source: {source_dups:,} duplicates")
    print(f"   • Target: {target_dups:,} duplicates")
    
    # Log duplicate differences if error logger provided
    if error_logger and session_id:
        if source_dups != target_dups:
            error_logger.log_error({
                'session_id': session_id,
                'check_type': 'comparison',
                'source_name': source_info,
                'target_name': target_info,
                'column_name': 'ALL',
                'error_type': 'duplicate_count_mismatch',
                'error_description': f"Duplicate count mismatch: Source={source_dups}, Target={target_dups}",
                'actual_value': f"Source duplicates: {source_dups}",
                'expected_value': f"Target duplicates: {target_dups}"
            })
    
    dup_match = source_dups == target_dups
    if dup_match:
        logger.info("✅ Duplicate counts match between source and target")
        print("   ✅ Duplicate counts match")
    elif target_dups > source_dups:
        logger.warning("❌ Target has MORE duplicates than source")
        print("   ❌ Target has MORE duplicates than source")
    else:
        logger.warning("⚠️  Target has FEWER duplicates than source")
        print("   ⚠️  Target has FEWER duplicates than source")
    
    return null_match and dup_match

def compare_sample_data(source_df, target_df, selected_columns, common_cols, sample_size=None, error_logger=None, session_id=None, source_info="", target_info=""):
    """IMPROVED: Compare sample data with smart normalization and error logging"""
    if sample_size is None:
        sample_size = APP_SETTINGS['default_sample_size']
        
    logger.info(f"🔍 STARTING IMPROVED SAMPLE DATA COMPARISON (first {sample_size} rows)")
    print(f"\n🔍 IMPROVED SAMPLE DATA COMPARISON (first {sample_size} rows)")
    print("-" * 40)
    
    if not selected_columns:
        logger.warning("No columns selected for sample data comparison")
        print("   ⚠️  No columns selected for comparison")
        return 0, 0
    
    sample_col = selected_columns[0]  # Use first selected column
    target_sample_col = get_target_column_name(sample_col, common_cols)
    
    logger.info(f"Using column '{sample_col}' (source) ↔ '{target_sample_col}' (target) for improved sample comparison")
    
    print(f"   Checking column: '{sample_col}' (Source) ↔ '{target_sample_col}' (Target)")
    print(f"   {'Row':<4} {'Source Value':<25} {'Target Value':<25} {'Status':<12}")
    print(f"   {'-'*4:<4} {'-'*25:<25} {'-'*25:<25} {'-'*12:<12}")
    
    matches = 0
    differences = 0
    
    for i in range(min(sample_size, len(source_df), len(target_df))):
        if i < len(source_df) and i < len(target_df):
            source_val = str(source_df[sample_col].iloc[i])[:23]  # Truncate long values
            target_val = str(target_df[target_sample_col].iloc[i])[:23]
            
            # Use smart normalized values for comparison
            norm_source = smart_normalize_value(source_df[sample_col].iloc[i], sample_col)
            norm_target = smart_normalize_value(target_df[target_sample_col].iloc[i], target_sample_col)
            
            if norm_source == norm_target:
                status = "✅ MATCH"
                matches += 1
                logger.debug(f"Row {i}: MATCH - Source: '{source_val}', Target: '{target_val}'")
            else:
                status = "❌ DIFFERENT"
                differences += 1
                logger.warning(f"Row {i}: MISMATCH - Source: '{source_val}', Target: '{target_val}'")
                
                # Log error if error logger provided
                if error_logger and session_id:
                    error_logger.log_error({
                        'session_id': session_id,
                        'check_type': 'comparison',
                        'source_name': source_info,
                        'target_name': target_info,
                        'column_name': sample_col,
                        'row_index': i,
                        'excel_row': i + 2,
                        'actual_value': str(source_df[sample_col].iloc[i])[:200],
                        'expected_value': str(target_df[target_sample_col].iloc[i])[:200],
                        'error_type': 'sample_value_mismatch',
                        'error_description': f"Sample row {i+1} mismatch after normalization"
                    })
            
            print(f"   {i+1:<4} {source_val:<25} {target_val:<25} {status:<12}")
    
    logger.info(f"Improved sample comparison completed: {matches} matches, {differences} differences")
    print(f"\n   📈 IMPROVED Sample Summary: {matches} matches, {differences} differences")
    
    return matches, differences

def detect_value_mismatches(source_df, target_df, selected_columns, common_cols, error_logger=None, session_id=None, source_info="", target_info=""):
    """IMPROVED: Detect REAL value mismatches with smart normalization and error logging"""
    logger.info("🔬 STARTING IMPROVED VALUE MISMATCH DETECTION")
    print(f"\n🔬 IMPROVED VALUE MISMATCH DETECTION")
    print("-" * 30)
    
    mismatch_details = []
    real_mismatches = 0
    
    for col in selected_columns:
        target_col = get_target_column_name(col, common_cols)
        if col in source_df.columns and target_col in target_df.columns:
            # Compare each row for this column
            col_mismatches = 0
            for i in range(min(len(source_df), len(target_df))):
                source_val = source_df[col].iloc[i]
                target_val = target_df[target_col].iloc[i]
                
                # Use smart normalized values for comparison
                normalized_source = smart_normalize_value(source_val, col)
                normalized_target = smart_normalize_value(target_val, target_col)
                
                # Only report if there's a REAL difference after normalization
                if normalized_source != normalized_target:
                    mismatch_details.append({
                        'column': col,
                        'target_column': target_col,
                        'row': i,
                        'source_value': source_val,
                        'target_value': target_val,
                        'normalized_source': normalized_source,
                        'normalized_target': normalized_target
                    })
                    real_mismatches += 1
                    col_mismatches += 1
                    
                    # Log error if error logger provided
                    if error_logger and session_id:
                        error_logger.log_error({
                            'session_id': session_id,
                            'check_type': 'comparison',
                            'source_name': source_info,
                            'target_name': target_info,
                            'column_name': col,
                            'row_index': i,
                            'excel_row': i + 2,
                            'actual_value': str(source_val)[:200],
                            'expected_value': str(target_val)[:200],
                            'error_type': 'value_mismatch',
                            'error_description': f"Value mismatch in column '{col}', row {i+1}"
                        })
                    
                    # Show first 2 REAL mismatches per column
                    if col_mismatches <= 2:
                        logger.warning(f"Value mismatch - {col}[Row {i+1}]: '{source_val}' ≠ '{target_val}'")
                        print(f"   ❌ {col}[Row {i+1}]: '{source_val}' ≠ '{target_val}'")
                        # Show normalization details for debugging
                        if normalized_source != str(source_val) or normalized_target != str(target_val):
                            print(f"        Normalized: '{normalized_source}' ≠ '{normalized_target}'")
    
    if real_mismatches > 0:
        logger.warning(f"Found {real_mismatches} value mismatches across {len(selected_columns)} columns")
        print(f"\n   📊 Value mismatches: {real_mismatches} across {len(selected_columns)} columns")
        
        # Show summary of differences
        print(f"\n   🔍 DIFFERENCES SUMMARY:")
        mismatch_by_column = {}
        for mismatch in mismatch_details:
            col = mismatch['column']
            mismatch_by_column[col] = mismatch_by_column.get(col, 0) + 1
        
        for col, count in mismatch_by_column.items():
            target_col = get_target_column_name(col, common_cols)
            print(f"      • {col} (Source) ↔ {target_col} (Target): {count} differences")
    else:
        logger.info("✅ No value mismatches found in selected columns")
        print("   ✅ No value mismatches found in selected columns")
    
    return real_mismatches

def advanced_data_comparison(source_df, target_df, selected_columns, common_cols, source_info="", target_info="", source_file="", target_file=""):
    """IMPROVED: Advanced comparison using hash-based matching with smart normalization and ERROR LOGGING"""
    logger.info("🔬 STARTING IMPROVED HASH-BASED COMPARISON")
    print("\n🔬 IMPROVED HASH-BASED COMPARISON")
    print("-" * 30)
    
    if not selected_columns:
        logger.warning("No columns selected for hash-based comparison")
        print("   ⚠️  No columns selected for comparison")
        return 0, 0, 0, []
    
    # Get target column names for selected columns
    target_columns = [get_target_column_name(col, common_cols) for col in selected_columns]
    
    # Only use columns that exist in BOTH datasets
    common_source_columns = [col for col in selected_columns if col in source_df.columns]
    common_target_columns = [get_target_column_name(col, common_cols) for col in common_source_columns if get_target_column_name(col, common_cols) in target_df.columns]
    
    if not common_source_columns or not common_target_columns:
        logger.warning("No common columns available for hash-based comparison")
        print("   ⚠️  No common columns available for comparison")
        return 0, 0, 0, []
    
    logger.info(f"Using {len(common_source_columns)} common columns for hash comparison")
    print(f"🔑 Using common columns:")
    for i, (src_col, tgt_col) in enumerate(zip(common_source_columns, common_target_columns)):
        if src_col != tgt_col:
            print(f"   {i+1}. {src_col} (Source) ↔ {tgt_col} (Target)")
        else:
            print(f"   {i+1}. {src_col}")
    
    def create_row_hash(row, source_columns, target_columns, is_target=False):
        """Create hash from SMART normalized row values"""
        normalized_values = []
        columns_to_use = target_columns if is_target else source_columns
        
        for col in columns_to_use:
            try:
                normalized_value = smart_normalize_value(row[col], col)
                normalized_values.append(normalized_value)
            except Exception as e:
                # If any error, use original value as string
                logger.warning(f"Error normalizing {col}: {e}")
                normalized_values.append(str(row[col]))
        
        # Create consistent hash string
        row_string = '|'.join(normalized_values)
        return hashlib.md5(row_string.encode('utf-8')).hexdigest()
    
    logger.info(f"Generating SMART row hashes for {len(common_source_columns)} common columns")
    
    # Generate hashes for both datasets using SMART normalization
    source_hashes = {}
    target_hashes = {}
    
    for idx, row in source_df.iterrows():
        try:
            row_hash = create_row_hash(row, common_source_columns, common_target_columns, is_target=False)
            source_hashes[row_hash] = idx
        except Exception as e:
            logger.warning(f"Error hashing source row {idx}: {e}")
    
    for idx, row in target_df.iterrows():
        try:
            row_hash = create_row_hash(row, common_source_columns, common_target_columns, is_target=True)
            target_hashes[row_hash] = idx
        except Exception as e:
            logger.warning(f"Error hashing target row {idx}: {e}")
    
    # Find matches and differences
    common_hashes = set(source_hashes.keys()).intersection(set(target_hashes.keys()))
    unique_to_source = set(source_hashes.keys()) - set(target_hashes.keys())
    unique_to_target = set(target_hashes.keys()) - set(source_hashes.keys())
    
    logger.info(f"IMPROVED Hash comparison - Common: {len(common_hashes)}, Unique to source: {len(unique_to_source)}, Unique to target: {len(unique_to_target)}")
    print(f"📊 IMPROVED Hash-based Comparison:")
    print(f"   • Common rows: {len(common_hashes):,}")
    print(f"   • Rows only in source: {len(unique_to_source):,}")
    print(f"   • Rows only in target: {len(unique_to_target):,}")
    
    # Initialize error logger
    error_logger = ErrorLogger()
    session_id = f"COMP_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    error_records = []
    
    # Log rows missing in target
    for hash_val in unique_to_source:
        source_idx = source_hashes[hash_val]
        error_records.append({
            'session_id': session_id,
            'check_type': 'comparison',
            'source_name': source_info,
            'target_name': target_info,
            'column_name': 'ALL_COLUMNS',
            'row_index': source_idx,
            'excel_row': source_idx + 2,
            'actual_value': 'PRESENT_IN_SOURCE',
            'expected_value': 'MISSING_IN_TARGET',
            'error_type': 'row_missing_in_target',
            'error_description': f'Row present in source but missing in target (Hash: {hash_val[:10]}...)',
            'source_file': source_file,
            'target_file': target_file
        })
        print(f"   ❌ Row {source_idx + 2} missing in target")
    
    # Log rows extra in target
    for hash_val in unique_to_target:
        target_idx = target_hashes[hash_val]
        error_records.append({
            'session_id': session_id,
            'check_type': 'comparison',
            'source_name': source_info,
            'target_name': target_info,
            'column_name': 'ALL_COLUMNS',
            'row_index': target_idx,
            'excel_row': target_idx + 2,
            'actual_value': 'EXTRA_IN_TARGET',
            'expected_value': 'NOT_IN_SOURCE',
            'error_type': 'row_extra_in_target',
            'error_description': f'Row present in target but not in source (Hash: {hash_val[:10]}...)',
            'source_file': source_file,
            'target_file': target_file
        })
        print(f"   ❌ Row {target_idx + 2} extra in target (not in source)")
    
    # Now do detailed value comparison for common rows
    value_mismatches = 0
    if common_hashes:
        print(f"\n🔍 DETAILED VALUE COMPARISON FOR COMMON ROWS:")
        # Check first 100 common rows for value mismatches
        for hash_val in list(common_hashes)[:100]:
            source_idx = source_hashes[hash_val]
            target_idx = target_hashes[hash_val]
            
            for src_col, tgt_col in zip(common_source_columns, common_target_columns):
                source_val = source_df[src_col].iloc[source_idx]
                target_val = target_df[tgt_col].iloc[target_idx]
                
                norm_source = smart_normalize_value(source_val, src_col)
                norm_target = smart_normalize_value(target_val, tgt_col)
                
                if norm_source != norm_target:
                    value_mismatches += 1
                    error_records.append({
                        'session_id': session_id,
                        'check_type': 'comparison',
                        'source_name': source_info,
                        'target_name': target_info,
                        'column_name': src_col,
                        'row_index': source_idx,
                        'excel_row': source_idx + 2,
                        'actual_value': str(source_val)[:200],
                        'expected_value': str(target_val)[:200],
                        'error_type': 'value_mismatch_in_common_row',
                        'error_description': f'Value mismatch in common row (Hash match but values differ)',
                        'source_file': source_file,
                        'target_file': target_file
                    })
                    print(f"   ❌ Row {source_idx + 2}, Column '{src_col}': Value mismatch")
    
    # Store all errors in database
    for error in error_records:
        error_logger.log_error(error)
    
    # Calculate ACCURATE match rate
    total_source_rows = len(source_df)
    total_target_rows = len(target_df)
    
    if total_source_rows > 0 and total_target_rows > 0:
        match_rate = (len(common_hashes) / min(total_source_rows, total_target_rows)) * 100
    else:
        match_rate = 0
    
    print(f"\n📈 ACCURATE DATA QUALITY INSIGHTS:")
    print(f"   • Source rows analyzed: {total_source_rows:,}")
    print(f"   • Target rows analyzed: {total_target_rows:,}")
    print(f"   • Real match rate: {match_rate:.1f}%")
    print(f"   • Total errors logged: {len(error_records)}")
    
    if len(unique_to_source) == 0 and len(unique_to_target) == 0 and value_mismatches == 0:
        logger.info("✅ All data matches perfectly using IMPROVED hash comparison")
        print("   ✅ All data matches perfectly!")
    else:
        if len(unique_to_source) > 0:
            logger.warning(f"❌ {len(unique_to_source):,} rows missing in target")
            print(f"   ❌ {len(unique_to_source):,} rows missing in target")
        if len(unique_to_target) > 0:
            logger.warning(f"❌ {len(unique_to_target):,} rows extra in target")
            print(f"   ❌ {len(unique_to_target):,} rows extra in target")
        if value_mismatches > 0:
            logger.warning(f"❌ {value_mismatches} value mismatches in common rows")
            print(f"   ❌ {value_mismatches} value mismatches in common rows")
    
    return len(common_hashes), len(unique_to_source), len(unique_to_target), error_records

def generate_comparison_report(source_df, target_df, source_info, target_info, selected_columns, common_cols):
    """Generate comprehensive comparison report with ERROR LOGGING"""
    logger.info("📊 GENERATING COMPREHENSIVE COMPARISON REPORT")
    print("\n" + "="*60)
    print("📊 COMPREHENSIVE COMPARISON REPORT")
    print("="*60)
    
    print(f"📁 Source: {source_info}")
    print(f"📁 Target: {target_info}")
    print(f"📅 Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Selected Columns: {len(selected_columns)} columns")
    
    logger.info(f"Report details - Source: {source_info}, Target: {target_info}, Selected columns: {len(selected_columns)}")
    
    # Get file names for error logging
    source_file = source_info
    target_file = target_info
    
    # Create error logger
    error_logger = ErrorLogger()
    session_id = f"CMP_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Initialize variables to store results
    structure_issues = 0
    row_count_match = False
    missing_in_target = 0
    extra_in_target = 0
    data_quality_match = False
    sample_matches = 0
    sample_differences = 0
    real_mismatch_count = 0
    common_rows = 0
    match_rate = 0.0
    
    # Run all comparison modules with SELECTED COLUMNS
    structure_issues = compare_structures(source_df, target_df, selected_columns, common_cols)
    
    # REMOVED: length_issues = check_length_consistency(source_df, target_df, selected_columns, common_cols)
    
    row_count_match, missing_in_target, extra_in_target = compare_row_counts(source_df, target_df)
    
    data_quality_match = compare_data_quality(
        source_df, target_df, selected_columns, common_cols,
        error_logger, session_id, source_info, target_info
    )
    
    sample_matches, sample_differences = compare_sample_data(
        source_df, target_df, selected_columns, common_cols,
        error_logger=error_logger, session_id=session_id,
        source_info=source_info, target_info=target_info
    )
    
    real_mismatch_count = detect_value_mismatches(
        source_df, target_df, selected_columns, common_cols,
        error_logger=error_logger, session_id=session_id,
        source_info=source_info, target_info=target_info
    )
    
    # Get results from advanced comparison (with file info for error logging)
    advanced_results = advanced_data_comparison(
        source_df, target_df, selected_columns, common_cols,
        source_info, target_info, source_file, target_file
    )
    
    if advanced_results:
        common_rows, unique_to_source, unique_to_target, error_records = advanced_results
        # Calculate match rate
        total_source_rows = len(source_df)
        if total_source_rows > 0:
            match_rate = (common_rows / total_source_rows) * 100
        else:
            match_rate = 0.0
        
        # Log advanced comparison errors with our session ID
        for error in error_records:
            error['session_id'] = session_id
            error_logger.log_error(error)
    
    # Overall assessment
    logger.info("🏆 CALCULATING OVERALL ASSESSMENT SCORE")
    print("\n" + "="*40)
    print("🏆 OVERALL ASSESSMENT")
    print("="*40)
    
    # Calculate match score based on REAL differences only
    score = 0
    max_score = 6  # Reduced from 7 since we removed length check
    
    # Structure match (1 point)
    if structure_issues == 0:
        score += 1
        logger.info("✅ Structure: Perfect column match")
        print("✅ Structure: Perfect column match")
    else:
        logger.warning("⚠️  Structure: Column differences found")
        print("⚠️  Structure: Column differences found")
    
    # Row count match (1 point)
    if row_count_match:
        score += 1
        logger.info("✅ Row Count: Perfect match")
        print("✅ Row Count: Perfect match")
    else:
        logger.warning("⚠️  Row Count: Differences found")
        print("⚠️  Row Count: Differences found")
    
    # Data type match (1 point) - now checks compatibility, not exact match
    if structure_issues == 0:
        score += 1
        logger.info("✅ Data Types: All selected columns compatible")
        print("✅ Data Types: All selected columns compatible")
    else:
        logger.warning("⚠️  Data Types: Compatibility issues found")
        print("⚠️  Data Types: Compatibility issues found")
    
    # REMOVED: Length consistency check (1 point)
    
    # Data quality comparison (1 point)
    if data_quality_match:
        score += 1
        logger.info("✅ Data Quality: Null and duplicate counts match")
        print("✅ Data Quality: Null and duplicate counts match")
    else:
        logger.warning("⚠️  Data Quality: Data quality differences")
        print("⚠️  Data Quality: Data quality differences")
    
    # Sample data match (1 point) - based on differences
    if sample_differences == 0:
        score += 1
        logger.info("✅ Sample Data: No differences found")
        print("✅ Sample Data: No differences found")
    else:
        logger.warning("⚠️  Sample Data: Differences found")
        print("⚠️  Sample Data: Differences found")
    
    # Advanced comparison (1 point) - based on REAL matches
    if common_rows > 0:
        score += 1
        logger.info("✅ Advanced: Significant data matching found")
        print("✅ Advanced: Significant data matching found")
    else:
        logger.warning("⚠️  Advanced: Limited data matching")
        print("⚠️  Advanced: Limited data matching")
    
    match_percentage = (score / max_score) * 100
    logger.info(f"Overall match score: {score}/{max_score} ({match_percentage:.1f}%)")
    print(f"\n🎯 REAL MATCH SCORE: {score}/{max_score} ({match_percentage:.1f}%)")
    
    # Determine assessment category
    if match_percentage >= QUALITY_THRESHOLDS['excellent_score']:
        assessment_category = "EXCELLENT"
        logger.info("🎉 EXCELLENT - Data synchronization is near perfect!")
        print("   🎉 EXCELLENT - Data synchronization is near perfect!")
    elif match_percentage >= QUALITY_THRESHOLDS['good_score']:
        assessment_category = "GOOD"
        logger.info("⚠️  GOOD - Minor differences found")
        print("   ⚠️  GOOD - Minor differences found")
    elif match_percentage >= QUALITY_THRESHOLDS['fair_score']:
        assessment_category = "FAIR"
        logger.info("🔶 FAIR - Some differences need attention")
        print("   🔶 FAIR - Some differences need attention")
    else:
        assessment_category = "POOR"
        logger.warning("🚨 POOR - Major synchronization issues!")
        print("   🚨 POOR - Major synchronization issues!")
    
    # AUDIT LOGGING
    try:
        from dq_audit import DataQualityAudit
        audit = DataQualityAudit()
        
        # Extract source and target types from info
        source_type = 'csv' if 'CSV' in source_info.upper() else 'excel' if 'EXCEL' in source_info.upper() else 'database'
        target_type = 'csv' if 'CSV' in target_info.upper() else 'excel' if 'EXCEL' in target_info.upper() else 'database'
        
        audit_data = {
            'session_id': session_id,  # Use same session ID for correlation
            'check_type': 'comparison',
            'source_type': source_type,
            'source_name': source_info,
            'target_type': target_type, 
            'target_name': target_info,
            'source_row_count': len(source_df),
            'target_row_count': len(target_df),
            'common_row_count': common_rows,
            'match_rate': match_rate,
            'value_mismatch_count': real_mismatch_count,
            'structure_issues_count': structure_issues,
            'overall_score': match_percentage,
            'assessment_category': assessment_category,
            'issues_summary': f"Structure issues: {structure_issues}, Value mismatches: {real_mismatch_count}, Missing in target: {missing_in_target}, Extra in target: {extra_in_target}"
        }
        
        if APP_SETTINGS['audit_enabled']:
            audit.log_audit_record(audit_data)
        
        # Also log a summary error for the overall session
        if match_percentage < 100:
            error_logger.log_error({
                'session_id': session_id,
                'check_type': 'comparison',
                'source_name': source_info,
                'target_name': target_info,
                'error_type': 'overall_mismatch',
                'error_description': f"Overall match score: {match_percentage:.1f}% ({score}/{max_score})",
                'actual_value': f"Score: {match_percentage:.1f}%",
                'expected_value': "100%"
            })
        
    except ImportError as e:
        logger.warning(f"Audit module not available: {e}")
    except Exception as e:
        logger.warning(f"Audit logging failed: {e}")
    
    print("\n📋 ERROR SUMMARY:")
    print(f"   • Session ID: {session_id}")
    print(f"   • Total errors logged: Check dq_error_logs table")
    print(f"   • Use session ID '{session_id}' to view detailed errors")
    
    print("\n💡 NEXT STEPS:")
    print(f"   1. Check dq_error_logs table for error details")
    print(f"   2. Use session ID: {session_id} to filter errors")
    print(f"   3. Review actual vs expected values for mismatches")
    
    print("="*60)
    
    # Return all the results for potential further processing
    return {
        'session_id': session_id,
        'structure_issues': structure_issues,
        'row_count_match': row_count_match,
        'missing_in_target': missing_in_target,
        'extra_in_target': extra_in_target,
        'data_quality_match': data_quality_match,
        'real_mismatch_count': real_mismatch_count,
        'common_rows': common_rows,
        'match_rate': match_rate,
        'overall_score': match_percentage,
        'assessment_category': assessment_category
    }

def main():
    """Main function for comparison tool"""
    logger.info("🚀 STARTING UNIVERSAL SOURCE-TARGET COMPARISON TOOL")
    print("🚀 UNIVERSAL SOURCE-TARGET COMPARISON TOOL")
    print("⭐ Compare: CSV vs CSV, DB vs DB, CSV vs DB, Excel vs Anything")
    
    try:
        source_df, target_df, source_info, target_info, source_file, target_file = load_comparison_sources()
        
        if source_df is not None and target_df is not None:
            # Let user select which columns to compare
            selected_columns, common_cols = select_columns_for_comparison(source_df, target_df)
            
            if selected_columns:
                generate_comparison_report(source_df, target_df, source_info, target_info, selected_columns, common_cols)
        
        logger.info("✅ Comparison analysis completed successfully")
        print("\n✅ Comparison analysis completed!")
        print("📋 Errors logged in dq_error_logs table")
        print("📊 Overall scores logged in dq_audit_logs table")
        
    except Exception as e:
        logger.error(f"❌ Error in comparison tool main execution: {str(e)}", exc_info=True)
        print(f"\n❌ Error in comparison analysis: {e}")

if __name__ == "__main__":
    main()