# dq_rules.py - COMPREHENSIVE BUSINESS RULES ENGINE WITH FINANCE DATA SUPPORT
import pandas as pd
import numpy as np
import os
import re
import json
import math
from datetime import datetime, timedelta
import logging
import time
from typing import Dict, List, Tuple, Any, Optional, Union
from collections import defaultdict
import difflib

from dq_unified import select_data_source, load_data_from_source
from app_config import APP_SETTINGS
from dq_error_log import ErrorLogger
from dq_audit import DataQualityAudit

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
# DATABASE CONNECTION & SCHEMA SETUP
# ============================================================================

def setup_rule_tables():
    """Create tables for business rules if they don't exist"""
    try:
        from db_config import MYSQL_CONFIG
        import mysql.connector
        
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # Create business rules table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dq_business_rules (
            rule_id INT PRIMARY KEY AUTO_INCREMENT,
            rule_name VARCHAR(100) NOT NULL,
            rule_type ENUM('kpi', 'threshold', 'cross_column', 'mandatory', 'aggregate', 'custom', 'pattern', 'range', 'text_comparison') NOT NULL,
            rule_logic TEXT,
            description TEXT,
            source_column VARCHAR(100),
            target_column VARCHAR(100),
            threshold_value DECIMAL(15,2),
            operator VARCHAR(10),
            expected_value TEXT,
            pattern_type VARCHAR(50),
            min_value DECIMAL(15,2),
            max_value DECIMAL(15,2),
            severity ENUM('low', 'medium', 'high', 'critical') DEFAULT 'medium',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(100),
            updated_at TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_rule_type (rule_type),
            INDEX idx_is_active (is_active),
            UNIQUE KEY uniq_rule_name (rule_name)
        )
        """)
        
        # Create rule execution history table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dq_rule_execution_history (
            execution_id INT PRIMARY KEY AUTO_INCREMENT,
            session_id VARCHAR(50) NOT NULL,
            rule_id INT,
            rule_name VARCHAR(100),
            execution_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            total_records INT,
            passed_records INT,
            failed_records INT,
            success_rate DECIMAL(5,2),
            dq_score DECIMAL(5,2),  -- <-- ADD THIS COLUMN
            execution_time_seconds DECIMAL(10,2),
            error_count INT DEFAULT 0,
            status ENUM('success', 'partial', 'failed', 'error') DEFAULT 'success',
            details TEXT,
            FOREIGN KEY (rule_id) REFERENCES dq_business_rules(rule_id) ON DELETE SET NULL,
            INDEX idx_session_id (session_id),
            INDEX idx_execution_time (execution_timestamp)
        )
        """)
        
        # Create rule violation details table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dq_rule_violations (
            violation_id INT PRIMARY KEY AUTO_INCREMENT,
            session_id VARCHAR(50) NOT NULL,
            rule_id INT,
            rule_name VARCHAR(100),
            row_index INT,
            excel_row INT,
            column_name VARCHAR(100),
            actual_value TEXT,
            expected_value TEXT,
            variance DECIMAL(15,2),
            variance_percentage DECIMAL(10,2),
            violation_type VARCHAR(50),
            severity ENUM('low', 'medium', 'high', 'critical') DEFAULT 'medium',
            violation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            details TEXT,
            source_data JSON,
            comparison_context VARCHAR(200),
            FOREIGN KEY (rule_id) REFERENCES dq_business_rules(rule_id) ON DELETE SET NULL,
            INDEX idx_session_rule (session_id, rule_id),
            INDEX idx_row_index (session_id, row_index),
            INDEX idx_violation_type (violation_type)
        )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Business rules tables created/verified successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error setting up rule tables: {str(e)}")
        return False

# ============================================================================
# CORE RULE ENGINE CLASS
# ============================================================================

class BusinessRuleEngine:
    """Main engine for executing business rules with finance data support"""
    
    def __init__(self, db_config=None):
        self.db_config = db_config
        self.error_logger = ErrorLogger(db_config) if ErrorLogger else None
        self.audit_logger = DataQualityAudit(db_config) if DataQualityAudit else None
        self.session_id = None
        self.currency_symbols = ['$', '€', '£', '¥', '₹', '₽', '₩', '฿', '₴', '₪', '₫']
        self.date_formats = [
            '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d.%m.%Y',
            '%Y%m%d', '%d-%b-%Y', '%d-%B-%Y', '%b %d, %Y', '%B %d, %Y'
        ]
        
    def set_session_id(self, session_id):
        """Set session ID for tracking"""
        self.session_id = session_id
        logger.info(f"BusinessRuleEngine session set to: {session_id}")
    
    def connect_to_db(self):
        """Establish database connection"""
        try:
            from db_config import MYSQL_CONFIG
            import mysql.connector
            return mysql.connector.connect(**MYSQL_CONFIG)
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            return None
    
    # ============================================================================
    # RULE MANAGEMENT FUNCTIONS
    # ============================================================================
    
    def get_all_rules(self, active_only=True):
        """Retrieve all business rules from database"""
        try:
            conn = self.connect_to_db()
            if not conn:
                return []
            
            cursor = conn.cursor(dictionary=True)
            
            query = "SELECT * FROM dq_business_rules"
            if active_only:
                query += " WHERE is_active = TRUE"
            query += " ORDER BY rule_type, rule_name"
            
            cursor.execute(query)
            rules = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            logger.info(f"Retrieved {len(rules)} business rules")
            return rules
            
        except Exception as e:
            logger.error(f"Error retrieving rules: {str(e)}")
            return []
    
    def get_rule_by_id(self, rule_id):
        """Get specific rule by ID"""
        try:
            conn = self.connect_to_db()
            if not conn:
                return None
            
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM dq_business_rules WHERE rule_id = %s", (rule_id,))
            rule = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return rule
            
        except Exception as e:
            logger.error(f"Error getting rule {rule_id}: {str(e)}")
            return None
    
    def create_rule(self, rule_data):
        """Create a new business rule"""
        try:
            conn = self.connect_to_db()
            if not conn:
                return None
            
            cursor = conn.cursor()
            
            # Build dynamic INSERT query
            columns = []
            values = []
            placeholders = []
            
            # Map frontend fields to database columns
            field_map = {
                'rule_name': 'rule_name',
                'rule_type': 'rule_type',
                'description': 'description',
                'source_column': 'source_column',
                'target_column': 'target_column',
                'threshold_value': 'threshold_value',
                'operator': 'operator',
                'pattern_type': 'pattern_type',
                'operator_type': 'operator_type',
                'comparison_type': 'comparison_type',
                'case_sensitive': 'case_sensitive',
                'is_case_sensitive': 'is_case_sensitive',
                'custom_pattern': 'custom_pattern',
                'similarity_threshold': 'similarity_threshold',
                'min_value': 'min_value',
                'max_value': 'max_value',
                'severity': 'severity',
                'kpi_type': 'kpi_type',
                'aggregation': 'aggregation',
                'rule_logic': 'rule_logic',
                'is_active': 'is_active',
                'created_by': 'created_by'
            }
            
            for frontend_field, db_column in field_map.items():
                value = rule_data.get(frontend_field)
                if value is not None:
                    columns.append(db_column)
                    values.append(value)
                    placeholders.append('%s')
            
            if not columns:
                return None
            
            query = f"""
            INSERT INTO dq_business_rules ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            """
            
            print(f"📊 Executing query: {query}")  # Debug
            print(f"📊 With values: {values}")     # Debug
            
            cursor.execute(query, values)
            conn.commit()
            rule_id = cursor.lastrowid
            
            cursor.close()
            conn.close()
            
            logger.info(f"Created new rule: {rule_data.get('rule_name')} (ID: {rule_id})")
            return rule_id
            
        except Exception as e:
            logger.error(f"Error creating rule: {str(e)}")
            return None
    
    def update_rule(self, rule_id, rule_data):
        """Update an existing rule"""
        try:
            conn = self.connect_to_db()
            if not conn:
                return False
            
            cursor = conn.cursor()
            
            # Build dynamic update query
            fields = []
            values = []
            
            for key, value in rule_data.items():
                if key in ['rule_name', 'rule_type', 'rule_logic', 'description', 
                          'source_column', 'target_column', 'expected_value', 
                          'pattern_type', 'severity', 'operator']:  # <-- Added 'operator' here
                    fields.append(f"{key} = %s")
                    values.append(value)
                elif key in ['threshold_value', 'min_value', 'max_value']:
                    fields.append(f"{key} = %s")
                    values.append(float(value) if value is not None else None)
                elif key == 'is_active':
                    fields.append(f"{key} = %s")
                    values.append(bool(value))
            
            if not fields:
                return False
            
            values.append(rule_id)
            query = f"UPDATE dq_business_rules SET {', '.join(fields)} WHERE rule_id = %s"
            
            cursor.execute(query, values)
            conn.commit()
            affected = cursor.rowcount
            
            cursor.close()
            conn.close()
            
            logger.info(f"Updated rule {rule_id}: {affected} rows affected")
            return affected > 0
            
        except Exception as e:
            logger.error(f"Error updating rule {rule_id}: {str(e)}")
            return False
    
    def delete_rule(self, rule_id):
        """Soft delete a rule (deactivate)"""
        try:
            conn = self.connect_to_db()
            if not conn:
                return False
            
            cursor = conn.cursor()
            cursor.execute("UPDATE dq_business_rules SET is_active = FALSE WHERE rule_id = %s", (rule_id,))
            conn.commit()
            affected = cursor.rowcount
            
            cursor.close()
            conn.close()
            
            logger.info(f"Deactivated rule {rule_id}: {affected} rows affected")
            return affected > 0
            
        except Exception as e:
            logger.error(f"Error deleting rule {rule_id}: {str(e)}")
            return False
    
    # ============================================================================
    # RULE EXECUTION FUNCTIONS
    # ============================================================================
    
    def execute_rules_on_dataframe(self, df: pd.DataFrame, rules: List[Dict], 
                                  source_name: str = "unknown") -> Dict[str, Any]:
        """Execute multiple rules on a DataFrame"""
        if df is None or df.empty:
            return {'error': 'No data to validate', 'total_rules': 0}
        
        if not rules:
            return {'error': 'No rules to execute', 'total_rules': 0}
        
        if not self.session_id:
            self.session_id = f"RULES_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{np.random.randint(1000, 9999)}"
        
        start_time = time.time()
        results = {
            'session_id': self.session_id,
            'source_name': source_name,
            'data_stats': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'columns_checked': 0
            },
            'rule_results': {},
            'summary': {
                'total_rules_executed': 0,
                'rules_passed': 0,
                'rules_failed': 0,
                'total_violations': 0,
                'total_records_checked': len(df)
            },
            'violations': [],
            'dq_score': 0,
            'recommendations': []
        }
        
        # Track columns that were checked
        checked_columns = set()
        
        # Execute each rule
        for rule in rules:
            rule_name = rule.get('rule_name', f'Rule_{rule.get("rule_id", "unknown")}')
            rule_type = rule.get('rule_type', 'custom')
            
            logger.info(f"Executing rule: {rule_name} ({rule_type})")
            
            # Execute based on rule type
            rule_result = None
            if rule_type == 'mandatory':
                rule_result = self.execute_mandatory_field_rule(df, rule)
            elif rule_type == 'threshold':
                rule_result = self.execute_threshold_rule(df, rule)
            elif rule_type == 'cross_column':
                rule_result = self.execute_cross_column_rule(df, rule)
            elif rule_type == 'text_comparison':
                rule_result = self.execute_text_comparison_rule(df, rule)
            elif rule_type == 'pattern':
                rule_result = self.execute_pattern_rule(df, rule)
            elif rule_type == 'range':
                rule_result = self.execute_range_rule(df, rule)
            elif rule_type == 'kpi':
                rule_result = self.execute_kpi_rule(df, rule)
            elif rule_type == 'aggregate':
                rule_result = self.execute_aggregate_rule(df, rule)
            elif rule_type == 'custom':
                rule_result = self.execute_custom_rule(df, rule)
            else:
                logger.warning(f"Unknown rule type: {rule_type}")
                continue
            
            if rule_result:
                # Update checked columns
                source_col = rule.get('source_column')
                target_col = rule.get('target_column')
                if source_col:
                    checked_columns.add(source_col)
                if target_col:
                    checked_columns.add(target_col)
                
                # Store results
                results['rule_results'][rule_name] = rule_result
                results['summary']['total_rules_executed'] += 1
                
                if rule_result.get('passed', False):
                    results['summary']['rules_passed'] += 1
                else:
                    results['summary']['rules_failed'] += 1
                
                results['summary']['total_violations'] += len(rule_result.get('violations', []))
                results['violations'].extend(rule_result.get('violations', []))
        
        # Calculate DQ Score
        total_checks = results['summary']['total_rules_executed']
        if total_checks > 0:
            # Weighted score based on rule severity AND compliance rate
            total_weight = 0
            weighted_score = 0
    
            for rule_name, rule_result in results['rule_results'].items():
                # Find rule severity
                rule_severity = 'medium'
                for rule in rules:
                    if rule.get('rule_name') == rule_name:
                        rule_severity = rule.get('severity', 'medium')
                        break
        
                # Assign weights based on severity
                severity_weights = {
                    'critical': 4,
                    'high': 3,
                    'medium': 2,
                    'low': 1
                }
                weight = severity_weights.get(rule_severity, 2)
        
                # Calculate actual compliance rate for this rule
                violations_count = len(rule_result.get('violations', []))
                total_records = results['summary']['total_records_checked']
        
                if total_records > 0:
                    passed_records = total_records - violations_count
                    compliance_rate = (passed_records / total_records) * 100
                else:
                    compliance_rate = 100 if rule_result.get('passed', False) else 0
        
                # Add weighted score based on actual compliance rate
                weighted_score += (compliance_rate / 100) * weight
                total_weight += weight
    
            if total_weight > 0:
                results['dq_score'] = (weighted_score / total_weight) * 100
        
        # Update data stats
        results['data_stats']['columns_checked'] = len(checked_columns)
        
        # Generate recommendations
        results['recommendations'] = self.generate_recommendations(results)
        
        # Calculate execution time
        execution_time = time.time() - start_time
        results['execution_time_seconds'] = execution_time
        
        # Log execution to database
        self.log_rule_execution(results, rules)
        
        # Log violations to error table
        if results['violations'] and self.error_logger:
            self.log_rule_violations(results['violations'])
        
        logger.info(f"Rule execution completed: {results['summary']['total_rules_executed']} rules, "
                   f"{results['summary']['total_violations']} violations, "
                   f"DQ Score: {results['dq_score']:.1f}%")
        
        return results
    
    # ============================================================================
    # SPECIFIC RULE TYPE EXECUTORS
    # ============================================================================
    
    def execute_mandatory_field_rule(self, df: pd.DataFrame, rule: Dict) -> Dict:
        """Execute mandatory field validation rule"""
        column = rule.get('source_column')
        if not column or column not in df.columns:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'mandatory',
                'passed': False,
                'error': f"Column '{column}' not found in data",
                'violations': []
            }
        
        violations = []
        for idx, value in df[column].items():
            if pd.isna(value) or (isinstance(value, str) and value.strip() == ''):
                violations.append({
                    'row_index': idx,
                    'excel_row': idx + 2,
                    'column_name': column,
                    'actual_value': str(value),
                    'expected_value': 'Non-empty value',
                    'violation_type': 'mandatory_field_missing',
                    'severity': rule.get('severity', 'high'),
                    'details': f'Mandatory field {column} is empty or null',
                    # FIX: Add rule metadata
                    'rule_name': rule.get('rule_name'),
                    'rule_id': rule.get('rule_id')
                })
        
        return {
            'rule_name': rule.get('rule_name'),
            'rule_type': 'mandatory',
            'column': column,
            'passed': len(violations) == 0,
            'violations': violations,
            'stats': {
                'total_records': len(df),
                'missing_count': len(violations),
                'completeness_rate': ((len(df) - len(violations)) / len(df)) * 100 if len(df) > 0 else 0
            }
        }
    def insert_into_rule_violations(self, violations):
        """Insert violations into dq_rule_violations table"""
        try:
            conn = self.connect_to_db()
            cursor = conn.cursor()
        
            for v in violations:
                cursor.execute("""
                    INSERT INTO dq_rule_violations 
                    (session_id, rule_id, rule_name, row_index, excel_row, column_name, 
                    actual_value, expected_value, variance, variance_percentage, 
                    violation_type, severity, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    self.session_id,
                    v.get('rule_id'),
                    v.get('rule_name', 'unknown'),
                    v.get('row_index'),
                    v.get('excel_row'),
                    v.get('column_name', ''),
                    str(v.get('actual_value', '')),
                    str(v.get('expected_value', '')),
                    None,  # variance
                    None,  # variance_percentage
                    v.get('violation_type', 'rule_violation'),
                    v.get('severity', 'medium'),
                    v.get('details', '')
                ))
        
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Inserted {len(violations)} into dq_rule_violations")

        except Exception as e:
            logger.error(f"Error inserting into rule_violations: {e}")

    def execute_threshold_rule(self, df: pd.DataFrame, rule: Dict) -> Dict:
        """Execute threshold validation rule (e.g., value ≤ 100)"""
        column = rule.get('source_column')
        threshold = rule.get('threshold_value')
        # operator = rule.get('operator', '≤')  # ≤, ≥, <, >, =, ≠

        operator = rule.get('operator')  # ≤, ≥, <, >, =, ≠, <=, >=, !=, ==

        # Normalize operator format
        if operator:
            operator = str(operator).strip()
            # Convert text operators to standard symbols
            if operator == '<=':
                operator = '≤'
            elif operator == '>=':
                operator = '≥'
            elif operator == '!=':
                operator = '≠'
            elif operator == '==':
                operator = '='
    
            # ADD DEBUG LINE TO SEE WHAT'S HAPPENING
            print(f"DEBUG: Rule '{rule.get('rule_name')}' has operator '{rule.get('operator')}' -> normalized to '{operator}'")
        
        # If operator not specified, try to infer from rule name
        if not operator:
            rule_name = rule.get('rule_name', '').lower()
            if any(word in rule_name for word in ['more', 'greater', 'above', 'over']):
                operator = '≥'  # Greater than or equal
            elif any(word in rule_name for word in ['less', 'below', 'under']):
                operator = '≤'  # Less than or equal
            else:
                operator = '≤'  # Default
                
        if not column or column not in df.columns:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'threshold',
                'passed': False,
                'error': f"Column '{column}' not found in data",
                'violations': []
            }
        
        if threshold is None:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'threshold',
                'passed': False,
                'error': 'No threshold value specified',
                'violations': []
            }
        
        # Convert column to numeric, handling currency symbols
        numeric_series = self.convert_to_numeric(df[column])
        
        violations = []
        for idx, num_val in enumerate(numeric_series):
            if pd.isna(num_val):
                continue
            
            violates = False
            if operator in ['≤', '<='] and num_val > threshold:
                violates = True
            elif operator in ['≥', '>='] and num_val < threshold:
                violates = True
            elif operator == '<' and num_val >= threshold:
                violates = True
            elif operator == '>' and num_val <= threshold:
                violates = True
            elif operator in ['=', '=='] and num_val != threshold:
                violates = True
            elif operator in ['≠', '!=', '<>'] and num_val == threshold:
                violates = True
            
            if violates:
                original_value = df[column].iloc[idx] if idx < len(df) else None
                violations.append({
                    'row_index': idx,
                    'excel_row': idx + 2,
                    'column_name': column,
                    'actual_value': str(original_value),
                    'expected_value': f'{operator} {threshold}',
                    'violation_type': 'threshold_violation',
                    'severity': rule.get('severity', 'medium'),
                    'details': f'Value {num_val} violates threshold {operator} {threshold}',
                    'numeric_actual': float(num_val) if not pd.isna(num_val) else None,
                    'threshold': float(threshold),
                    # FIX: Add rule metadata
                    'rule_name': rule.get('rule_name'),
                    'rule_id': rule.get('rule_id'),
                    'operator': operator,
                    # ADD THIS LINE for comparison type
                    'comparison_type': operator
                })
        
        return {
            'rule_name': rule.get('rule_name'),
            'rule_type': 'threshold',
            'column': column,
            'passed': len(violations) == 0,
            'violations': violations,
            'stats': {
                'total_records': len(numeric_series.dropna()),
                'violations_count': len(violations),
                'compliance_rate': ((len(numeric_series.dropna()) - len(violations)) / len(numeric_series.dropna())) * 100 if len(numeric_series.dropna()) > 0 else 0,
                'threshold': threshold,
                'operator': operator
            }
        }
    
    def execute_cross_column_rule(self, df: pd.DataFrame, rule: Dict) -> Dict:
        """Execute cross-column validation (e.g., end_date > start_date)"""
        col1 = rule.get('source_column')
        col2 = rule.get('target_column')
        operator = rule.get('operator', '>')  # >, <, =, ≥, ≤
        
        if not col1 or col1 not in df.columns:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'cross_column',
                'passed': False,
                'error': f"Source column '{col1}' not found in data",
                'violations': []
            }
        
        if not col2 or col2 not in df.columns:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'cross_column',
                'passed': False,
                'error': f"Target column '{col2}' not found in data",
                'violations': []
            }
        
        violations = []
        
        for idx, row in df.iterrows():
            val1 = row[col1]
            val2 = row[col2]
            
            if pd.isna(val1) or pd.isna(val2):
                continue
            
            # Try to parse as dates first
            date1 = self.parse_date(val1)
            date2 = self.parse_date(val2)
            
            if date1 and date2:
                # Date comparison
                violates = False
                if operator in ['>', 'gt'] and not (date1 > date2):
                    violates = True
                elif operator in ['<', 'lt'] and not (date1 < date2):
                    violates = True
                elif operator in ['=', '==', 'eq'] and not (date1 == date2):
                    violates = True
                elif operator in ['≥', '>=', 'gte'] and not (date1 >= date2):
                    violates = True
                elif operator in ['≤', '<=', 'lte'] and not (date1 <= date2):
                    violates = True
                
                if violates:
                    violations.append({
                        'row_index': idx,
                        'excel_row': idx + 2,
                        'column_name': f'{col1} vs {col2}',
                        'actual_value': f'{col1}: {val1}, {col2}: {val2}',
                        'expected_value': f'{col1} {operator} {col2}',
                        'violation_type': 'cross_column_date_violation',
                        'severity': rule.get('severity', 'medium'),
                        'details': f'Date comparison failed: {date1} is NOT {operator} {date2}',
                        'source_value': str(val1),
                        'target_value': str(val2),
                        # FIX: Add rule metadata
                        'rule_name': rule.get('rule_name'),
                        'rule_id': rule.get('rule_id'),
                        'operator': operator
                    })
            
            else:
                # Try numeric comparison
                num1 = self.convert_to_numeric(pd.Series([val1])).iloc[0]
                num2 = self.convert_to_numeric(pd.Series([val2])).iloc[0]
                
                if not pd.isna(num1) and not pd.isna(num2):
                    violates = False
                    if operator == '>' and not (num1 > num2):
                        violates = True
                    elif operator == '<' and not (num1 < num2):
                        violates = True
                    elif operator == '=' and not (abs(num1 - num2) < 0.0001):  # Float tolerance
                        violates = True
                    elif operator == '≥' and not (num1 >= num2):
                        violates = True
                    elif operator == '≤' and not (num1 <= num2):
                        violates = True
                    
                    if violates:
                        violations.append({
                            'row_index': idx,
                            'excel_row': idx + 2,
                            'column_name': f'{col1} vs {col2}',
                            'actual_value': f'{col1}: {val1}, {col2}: {val2}',
                            'expected_value': f'{col1} {operator} {col2}',
                            'violation_type': 'cross_column_numeric_violation',
                            'severity': rule.get('severity', 'medium'),
                            'details': f'Numeric comparison failed: {num1} is NOT {operator} {num2}',
                            'source_value': str(val1),
                            'target_value': str(val2),
                            'numeric_source': float(num1),
                            'numeric_target': float(num2)
                        })
        
        return {
            'rule_name': rule.get('rule_name'),
            'rule_type': 'cross_column',
            'columns': [col1, col2],
            'passed': len(violations) == 0,
            'violations': violations,
            'stats': {
                'total_comparisons': len(df),
                'violations_count': len(violations),
                'compliance_rate': ((len(df) - len(violations)) / len(df)) * 100 if len(df) > 0 else 0,
                'operator': operator
            }
        }
    
    def execute_text_comparison_rule(self, df: pd.DataFrame, rule: Dict) -> Dict:
        """Execute text comparison rule with fuzzy matching support"""
        col1 = rule.get('source_column')
        col2 = rule.get('target_column')
        comparison_type = rule.get('comparison_type', 'exact')  # exact, substring, fuzzy
        case_sensitive = rule.get('case_sensitive', False)
        
        if not col1 or col1 not in df.columns:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'text_comparison',
                'passed': False,
                'error': f"Source column '{col1}' not found in data",
                'violations': []
            }
        
        if not col2 or col2 not in df.columns:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'text_comparison',
                'passed': False,
                'error': f"Target column '{col2}' not found in data",
                'violations': []
            }
        
        violations = []
        
        for idx, row in df.iterrows():
            val1 = str(row[col1]) if not pd.isna(row[col1]) else ""
            val2 = str(row[col2]) if not pd.isna(row[col2]) else ""
            
            if not case_sensitive:
                val1 = val1.lower()
                val2 = val2.lower()
            
            matches = False
            
            if comparison_type == 'exact':
                matches = (val1 == val2)
            
            elif comparison_type == 'substring':
                # Check if val2 is a substring of val1 OR val1 is substring of val2
                matches = (val2 in val1) or (val1 in val2)
                
                # Also check with spaces removed (e.g., "pavankumar bandaru" vs "bandaru")
                val1_no_spaces = val1.replace(' ', '')
                val2_no_spaces = val2.replace(' ', '')
                if not matches:
                    matches = (val2_no_spaces in val1_no_spaces) or (val1_no_spaces in val2_no_spaces)
            
            elif comparison_type == 'fuzzy':
                # Use difflib for fuzzy matching
                similarity = difflib.SequenceMatcher(None, val1, val2).ratio()
                threshold = rule.get('similarity_threshold', 0.8)
                matches = (similarity >= threshold)
            
            if not matches:
                violation_type = 'text_mismatch'
                details = ''
                
                if comparison_type == 'exact':
                    details = f'Exact match failed: "{val1}" != "{val2}"'
                elif comparison_type == 'substring':
                    details = f'Substring match failed: "{val2}" not found in "{val1}" and vice versa'
                elif comparison_type == 'fuzzy':
                    similarity = difflib.SequenceMatcher(None, val1, val2).ratio()
                    details = f'Fuzzy match failed: Similarity {similarity:.2f} < threshold {threshold}'
                
                violations.append({
                    'row_index': idx,
                    'excel_row': idx + 2,
                    'column_name': f'{col1} vs {col2}',
                    'actual_value': f'{col1}: {row[col1]}, {col2}: {row[col2]}',
                    'expected_value': f'Text match ({comparison_type})',
                    'violation_type': violation_type,
                    'severity': rule.get('severity', 'medium'),
                    'details': details,
                    'source_text': str(row[col1]) if not pd.isna(row[col1]) else "",
                    'target_text': str(row[col2]) if not pd.isna(row[col2]) else "",
                    'comparison_type': comparison_type,
                    # FIX: Add rule metadata
                    'rule_name': rule.get('rule_name'),
                    'rule_id': rule.get('rule_id')
                })
        
        return {
            'rule_name': rule.get('rule_name'),
            'rule_type': 'text_comparison',
            'columns': [col1, col2],
            'passed': len(violations) == 0,
            'violations': violations,
            'stats': {
                'total_comparisons': len(df),
                'violations_count': len(violations),
                'compliance_rate': ((len(df) - len(violations)) / len(df)) * 100 if len(df) > 0 else 0,
                'comparison_type': comparison_type,
                'case_sensitive': case_sensitive
            }
        }
    
    def execute_pattern_rule(self, df: pd.DataFrame, rule: Dict) -> Dict:
        """Execute pattern matching rule (regex)"""
        column = rule.get('source_column')
        pattern = rule.get('pattern_type')
        
        if not column or column not in df.columns:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'pattern',
                'passed': False,
                'error': f"Column '{column}' not found in data",
                'violations': []
            }
        
        # Define common patterns
        patterns = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'phone_us': r'^\+?1?\d{10}$',
            'phone_international': r'^\+?[1-9]\d{1,14}$',
            'date_iso': r'^\d{4}-\d{2}-\d{2}$',
            'ssn': r'^\d{3}-\d{2}-\d{4}$',
            'zip_code': r'^\d{5}(-\d{4})?$',
            'url': r'^https?://[^\s/$.?#].[^\s]*$',
            'ip_address': r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$',
            'currency': r'^[$€£¥₹]\s?\d+(,\d{3})*(\.\d{2})?$',
            'percentage': r'^\d+(\.\d+)?%$'
        }
        
        regex_pattern = patterns.get(pattern, pattern)
        
        if not regex_pattern:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'pattern',
                'passed': False,
                'error': 'No pattern specified',
                'violations': []
            }
        
        violations = []
        for idx, value in df[column].items():
            if pd.isna(value):
                continue
            
            str_val = str(value)
            if not re.match(regex_pattern, str_val):
                violations.append({
                    'row_index': idx,
                    'excel_row': idx + 2,
                    'column_name': column,
                    'actual_value': str_val,
                    'expected_value': f'Match pattern: {pattern}',
                    'violation_type': 'pattern_violation',
                    'severity': rule.get('severity', 'medium'),
                    'details': f'Value does not match {pattern} pattern',
                    'pattern': pattern,
                    'regex': regex_pattern,
                    # FIX: Add rule metadata
                    'rule_name': rule.get('rule_name'),
                    'rule_id': rule.get('rule_id')
                })
        
        return {
            'rule_name': rule.get('rule_name'),
            'rule_type': 'pattern',
            'column': column,
            'passed': len(violations) == 0,
            'violations': violations,
            'stats': {
                'total_records': len(df),
                'violations_count': len(violations),
                'compliance_rate': ((len(df) - len(violations)) / len(df)) * 100 if len(df) > 0 else 0,
                'pattern_type': pattern
            }
        }
    
    def execute_range_rule(self, df: pd.DataFrame, rule: Dict) -> Dict:
        """Execute range validation rule (min ≤ value ≤ max)"""
        column = rule.get('source_column')
        min_val = rule.get('min_value')
        max_val = rule.get('max_value')
        
        if not column or column not in df.columns:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'range',
                'passed': False,
                'error': f"Column '{column}' not found in data",
                'violations': []
            }
        
        if min_val is None and max_val is None:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'range',
                'passed': False,
                'error': 'No range specified (min and/or max required)',
                'violations': []
            }
        
        # Convert column to numeric
        numeric_series = self.convert_to_numeric(df[column])
        
        violations = []
        for idx, num_val in enumerate(numeric_series):
            if pd.isna(num_val):
                continue
            
            violates = False
            reason = ""
            
            if min_val is not None and max_val is not None:
                if num_val < min_val or num_val > max_val:
                    violates = True
                    reason = f'Value {num_val} outside range [{min_val}, {max_val}]'
            elif min_val is not None and num_val < min_val:
                violates = True
                reason = f'Value {num_val} below minimum {min_val}'
            elif max_val is not None and num_val > max_val:
                violates = True
                reason = f'Value {num_val} above maximum {max_val}'
            
            if violates:
                original_value = df[column].iloc[idx] if idx < len(df) else None
                violations.append({
                    'row_index': idx,
                    'excel_row': idx + 2,
                    'column_name': column,
                    'actual_value': str(original_value),
                    'expected_value': f'Between {min_val if min_val is not None else "-∞"} and {max_val if max_val is not None else "∞"}',
                    'violation_type': 'range_violation',
                    'severity': rule.get('severity', 'medium'),
                    'details': reason,
                    'numeric_actual': float(num_val) if not pd.isna(num_val) else None,
                    'min_value': float(min_val) if min_val is not None else None,
                    'max_value': float(max_val) if max_val is not None else None,
                    # FIX: Add rule metadata
                    'rule_name': rule.get('rule_name'),
                    'rule_id': rule.get('rule_id')
            })
        
        return {
            'rule_name': rule.get('rule_name'),
            'rule_type': 'range',
            'column': column,
            'passed': len(violations) == 0,
            'violations': violations,
            'stats': {
                'total_records': len(numeric_series.dropna()),
                'violations_count': len(violations),
                'compliance_rate': ((len(numeric_series.dropna()) - len(violations)) / len(numeric_series.dropna())) * 100 if len(numeric_series.dropna()) > 0 else 0,
                'min_value': min_val,
                'max_value': max_val
            }
        }
    
    # def store_kpi_to_history(self, session_id, rule_id, rule_name, kpi_type, 
    #                      kpi_value, threshold, passed, source_info):
    #     """Store KPI results in kpi_history table"""
    #     try:
    #         conn = self.connect_to_db()
    #         if not conn:
    #             return
            
    #         cursor = conn.cursor()
            
    #         query = """
    #         INSERT INTO kpi_history 
    #         (session_id, rule_id, rule_name, kpi_type, kpi_value, 
    #         threshold_value, expected_value, actual_value, passed,
    #         variance, variance_percentage, data_source, created_at)
    #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #         """
            
    #         # Calculate variance
    #         variance = None
    #         variance_percentage = None
            
    #         if threshold is not None:
    #             if isinstance(kpi_value, (int, float)) and isinstance(threshold, (int, float)):
    #                 variance = kpi_value - threshold
    #                 if threshold != 0:
    #                     variance_percentage = (variance / threshold) * 100
            
    #         values = (
    #             session_id,
    #             rule_id,
    #             rule_name,
    #             kpi_type,
    #             float(kpi_value) if isinstance(kpi_value, (int, float)) else None,
    #             float(threshold) if threshold is not None else None,
    #             str(threshold) if threshold is not None else "N/A",
    #             str(kpi_value),
    #             passed,
    #             float(variance) if variance is not None else None,
    #             float(variance_percentage) if variance_percentage is not None else None,
    #             source_info,
    #             datetime.now()
    #         )
            
    #         cursor.execute(query, values)
    #         conn.commit()
            
    #         kpi_id = cursor.lastrowid
    #         cursor.close()
    #         conn.close()
            
    #         logger.info(f"Stored KPI to history: {rule_name} = {kpi_value} (ID: {kpi_id})")
    #         return kpi_id
            
    #     except Exception as e:
    #         logger.error(f"Error storing KPI to history: {str(e)}")
    #         return None

    def store_kpi_to_history(self, session_id, rule_id, rule_name, kpi_type, 
                         kpi_value, threshold, passed):
        """Store KPI results in kpi_history table"""
        try:
            conn = self.connect_to_db()
            if not conn:
                return
            
            cursor = conn.cursor()
            
            # Simplified query matching your table structure
            query = """
            INSERT INTO kpi_history 
            (session_id, kpi_name, kpi_value, threshold_value, check_timestamp)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            # Create a descriptive KPI name
            kpi_name = f"{rule_name} ({kpi_type})"
            
            values = (
                session_id,
                kpi_name,
                float(kpi_value) if isinstance(kpi_value, (int, float)) else 0.0,
                float(threshold) if threshold is not None else None,
                datetime.now()
            )
            
            cursor.execute(query, values)
            conn.commit()
            
            kpi_id = cursor.lastrowid
            cursor.close()
            conn.close()
            
            logger.info(f"Stored KPI to history: {kpi_name} = {kpi_value} (ID: {kpi_id})")
            return kpi_id
            
        except Exception as e:
            logger.error(f"Error storing KPI to history: {str(e)}")
            return None
        
    def execute_kpi_rule(self, df: pd.DataFrame, rule: Dict) -> Dict:
        """Execute KPI validation rule"""
        kpi_type = rule.get('kpi_type', 'count')
        column = rule.get('source_column')
        threshold = rule.get('threshold_value')
        operator = rule.get('operator', '≤')
        
        # Calculate KPI
        kpi_value = None
        details = ""
        
        if kpi_type == 'count':
            kpi_value = len(df)
            details = f'Total row count: {kpi_value}'
        
        elif kpi_type == 'sum' and column:
            numeric_series = self.convert_to_numeric(df[column])
            kpi_value = numeric_series.sum()
            details = f'Sum of {column}: {kpi_value}'
        
        elif kpi_type == 'average' and column:
            numeric_series = self.convert_to_numeric(df[column])
            kpi_value = numeric_series.mean()
            details = f'Average of {column}: {kpi_value:.2f}'
        
        elif kpi_type == 'min' and column:
            numeric_series = self.convert_to_numeric(df[column])
            kpi_value = numeric_series.min()
            details = f'Minimum of {column}: {kpi_value}'
        
        elif kpi_type == 'max' and column:
            numeric_series = self.convert_to_numeric(df[column])
            kpi_value = numeric_series.max()
            details = f'Maximum of {column}: {kpi_value}'
        
        elif kpi_type == 'null_count' and column:
            kpi_value = df[column].isna().sum()
            details = f'Null count in {column}: {kpi_value}'
        
        elif kpi_type == 'distinct_count' and column:
            kpi_value = df[column].nunique()
            details = f'Distinct values in {column}: {kpi_value}'
        
        if kpi_value is None:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'kpi',
                'passed': False,
                'error': f'Invalid KPI type or column: {kpi_type}, {column}',
                'violations': []
            }
        
        # Check against threshold if provided
        passed = True
        violations = []
        
        if threshold is not None:
            passed = False
            if operator == '≤' and kpi_value <= threshold:
                passed = True
            elif operator == '≥' and kpi_value >= threshold:
                passed = True
            elif operator == '<' and kpi_value < threshold:
                passed = True
            elif operator == '>' and kpi_value > threshold:
                passed = True
            elif operator == '=' and abs(kpi_value - threshold) < 0.0001:
                passed = True
            elif operator == '≠' and abs(kpi_value - threshold) >= 0.0001:
                    passed = True
            
            if not passed:
                violations.append({
                    'row_index': -1,  # KPI applies to entire dataset
                    'excel_row': -1,
                    'column_name': column or 'dataset',
                    'actual_value': f'{kpi_value}',
                    'expected_value': f'{operator} {threshold}',
                    'violation_type': 'kpi_violation',
                    'severity': rule.get('severity', 'high'),
                    'details': f'KPI violation: {details} - Expected: {operator} {threshold}',
                    'kpi_type': kpi_type,
                    'kpi_value': float(kpi_value) if isinstance(kpi_value, (int, float, np.number)) else kpi_value,
                    'threshold': float(threshold),
                    # FIX: Add rule metadata
                    'rule_name': rule.get('rule_name'),
                    'rule_id': rule.get('rule_id'),
                    'operator': operator
                })
        
        self.store_kpi_to_history(
            session_id=self.session_id,
            rule_id=rule.get('rule_id'),
            rule_name=rule.get('rule_name'),
            kpi_type=kpi_type,
            kpi_value=kpi_value,
            threshold=threshold,
            passed=passed
        )
        return {
            'rule_name': rule.get('rule_name'),
            'rule_type': 'kpi',
            'passed': passed,
            'violations': violations,
            'kpi_value': kpi_value,
            'kpi_type': kpi_type,
            'details': details,
            'threshold': threshold,
            'operator': operator
        }
    
    def execute_aggregate_rule(self, df: pd.DataFrame, rule: Dict) -> Dict:
        """Execute aggregate rule (e.g., sum ≤ 100,000)"""
        # Similar to KPI but with custom aggregation logic
        column = rule.get('source_column')
        aggregation = rule.get('aggregation', 'sum')
        threshold = rule.get('threshold_value')
        operator = rule.get('operator', '≤')
        
        if not column or column not in df.columns:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'aggregate',
                'passed': False,
                'error': f"Column '{column}' not found in data",
                'violations': []
            }
        
        numeric_series = self.convert_to_numeric(df[column])
        
        # Calculate aggregation
        agg_value = None
        if aggregation == 'sum':
            agg_value = numeric_series.sum()
        elif aggregation == 'average':
            agg_value = numeric_series.mean()
        elif aggregation == 'count':
            agg_value = len(numeric_series.dropna())
        elif aggregation == 'min':
            agg_value = numeric_series.min()
        elif aggregation == 'max':
            agg_value = numeric_series.max()
        elif aggregation == 'median':
            agg_value = numeric_series.median()
        elif aggregation == 'std':
            agg_value = numeric_series.std()
        
        if agg_value is None:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'aggregate',
                'passed': False,
                'error': f'Invalid aggregation: {aggregation}',
                'violations': []
            }
        
        # Check against threshold
        passed = True
        violations = []
        
        if threshold is not None:
            passed = False
            if operator in ['≤', '<='] and agg_value <= threshold:
                passed = True
            elif operator in ['≥', '>='] and agg_value >= threshold:
                passed = True
            elif operator == '<' and agg_value < threshold:
                passed = True
            elif operator == '>' and agg_value > threshold:
                passed = True
            elif operator in ['=', '=='] and abs(agg_value - threshold) < 0.0001:
                passed = True
            
            if not passed:
                violations.append({
                    'row_index': -1,
                    'excel_row': -1,
                    'column_name': column,
                    'actual_value': f'{agg_value}',
                    'expected_value': f'{aggregation} {operator} {threshold}',
                    'violation_type': 'aggregate_violation',
                    'severity': rule.get('severity', 'high'),
                    'details': f'Aggregate violation: {aggregation}({column}) = {agg_value} - Expected: {operator} {threshold}',
                    'aggregation': aggregation,
                    'aggregate_value': float(agg_value) if isinstance(agg_value, (int, float, np.number)) else agg_value,
                    'threshold': float(threshold),
                    'operator': operator,
                    # FIX: Add rule metadata
                    'rule_name': rule.get('rule_name'),
                    'rule_id': rule.get('rule_id')
                })
        
        return {
            'rule_name': rule.get('rule_name'),
            'rule_type': 'aggregate',
            'passed': passed,
            'violations': violations,
            'aggregation': aggregation,
            'aggregate_value': agg_value,
            'column': column,
            'threshold': threshold,
            'operator': operator
        }
    
    def execute_custom_rule(self, df: pd.DataFrame, rule: Dict) -> Dict:
        """Execute custom SQL/Python rule"""
        rule_logic = rule.get('rule_logic')
        if not rule_logic:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'custom',
                'passed': False,
                'error': 'No rule logic provided',
                'violations': []
            }
        
        # Try to execute as Python expression
        violations = []
        
        try:
            # Create a safe environment for evaluation
            safe_globals = {
                'df': df,
                'pd': pd,
                'np': np,
                'len': len,
                'sum': sum,
                'min': min,
                'max': max,
                'mean': np.mean,
                'std': np.std,
                'count': lambda x: len(x) if hasattr(x, '__len__') else None,
                'null_count': lambda x: x.isna().sum() if hasattr(x, 'isna') else None,
                'distinct_count': lambda x: x.nunique() if hasattr(x, 'nunique') else None
            }
            
            # Evaluate the rule logic
            result = eval(rule_logic, {"__builtins__": {}}, safe_globals)
            
            if isinstance(result, bool):
                passed = result
                if not passed:
                    violations.append({
                        'row_index': -1,
                        'excel_row': -1,
                        'column_name': 'dataset',
                        'actual_value': 'Custom rule evaluated to False',
                        'expected_value': 'Custom rule should evaluate to True',
                        'violation_type': 'custom_rule_violation',
                        'severity': rule.get('severity', 'medium'),
                        'details': f'Custom rule failed: {rule_logic}'
                    })
            elif isinstance(result, pd.Series):
                # Boolean series indicating violations
                violation_indices = result[result == False].index.tolist()
                for idx in violation_indices:
                    violations.append({
                        'row_index': idx,
                        'excel_row': idx + 2,
                        'column_name': 'custom_rule',
                        'actual_value': 'Row failed custom rule',
                        'expected_value': 'Row should pass custom rule',
                        'violation_type': 'custom_rule_violation',
                        'severity': rule.get('severity', 'medium'),
                        'details': f'Custom rule failed at row {idx + 2}: {rule_logic}'
                    })
                passed = len(violation_indices) == 0
            else:
                passed = bool(result)
                if not passed:
                    violations.append({
                        'row_index': -1,
                        'excel_row': -1,
                        'column_name': 'dataset',
                        'actual_value': f'Rule result: {result}',
                        'expected_value': 'Truthy value',
                        'violation_type': 'custom_rule_violation',
                        'severity': rule.get('severity', 'medium'),
                        'details': f'Custom rule returned non-truthy value: {result}'
                    })
            
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'custom',
                'passed': passed,
                'violations': violations,
                'rule_logic': rule_logic
            }
            
        except Exception as e:
            return {
                'rule_name': rule.get('rule_name'),
                'rule_type': 'custom',
                'passed': False,
                'error': f'Error executing custom rule: {str(e)}',
                'violations': []
            }
    
    # ============================================================================
    # HELPER FUNCTIONS
    # ============================================================================
    
    def convert_to_numeric(self, series: pd.Series) -> pd.Series:
        """Convert series to numeric, handling currency symbols and commas"""
        def clean_value(x):
            if pd.isna(x):
                return np.nan
            try:
                str_val = str(x).strip()
                # Remove currency symbols
                for symbol in self.currency_symbols:
                    str_val = str_val.replace(symbol, '')
                # Remove commas (thousands separators)
                str_val = str_val.replace(',', '')
                # Remove parentheses for negative numbers (accounting format)
                if str_val.startswith('(') and str_val.endswith(')'):
                    str_val = '-' + str_val[1:-1]
                # Remove percentage signs and convert
                if '%' in str_val:
                    str_val = str_val.replace('%', '')
                    val = float(str_val) / 100
                else:
                    val = float(str_val)
                return val
            except:
                return np.nan
        
        return series.apply(clean_value)
    
    def parse_date(self, value) -> Optional[datetime]:
        """Try to parse a value as date using multiple formats"""
        if pd.isna(value):
            return None
        
        str_val = str(value).strip()
        
        for fmt in self.date_formats:
            try:
                return datetime.strptime(str_val, fmt)
            except:
                continue
        
        # Try pandas parsing as fallback
        try:
            return pd.to_datetime(str_val)
        except:
            return None
    
    def generate_recommendations(self, results: Dict) -> List[str]:
        """Generate actionable recommendations based on rule violations"""
        recommendations = []
        summary = results.get('summary', {})
        rule_results = results.get('rule_results', {})
        
        # 1. Overall DQ score recommendation
        dq_score = results.get('dq_score', 0)
        if dq_score >= 95:
            recommendations.append("Data quality is excellent. Maintain current rule standards.")
        elif dq_score >= 80:
            recommendations.append("Data quality is good. Minor rule adjustments may improve score.")
        elif dq_score >= 60:
            recommendations.append("Data quality is fair. Review and fix major rule violations.")
        else:
            recommendations.append("Data quality is poor. Immediate attention needed for rule violations.")
        
        # 2. Rule-specific recommendations
        for rule_name, rule_result in rule_results.items():
            violations = rule_result.get('violations', [])
            if violations:
                rule_type = rule_result.get('rule_type', 'unknown')
                violation_count = len(violations)
                
                if rule_type == 'mandatory':
                    col = rule_result.get('column', 'unknown')
                    recommendations.append(f"Fix {violation_count} missing values in mandatory column '{col}'")
                
                elif rule_type == 'threshold':
                    col = rule_result.get('column', 'unknown')
                    threshold = rule_result.get('stats', {}).get('threshold')
                    operator = rule_result.get('stats', {}).get('operator', '≤')
                    recommendations.append(f"Review {violation_count} values in '{col}' violating {operator} {threshold}")
                
                elif rule_type in ['text_comparison', 'cross_column']:
                    cols = rule_result.get('columns', ['unknown'])
                    recommendations.append(f"Check {violation_count} mismatches between columns: {', '.join(cols)}")
                
                elif rule_type == 'pattern':
                    col = rule_result.get('column', 'unknown')
                    pattern = rule_result.get('stats', {}).get('pattern_type', 'unknown')
                    recommendations.append(f"Fix {violation_count} values in '{col}' not matching {pattern} pattern")
        
        # 3. High severity violations
        high_severity_count = sum(1 for violation in results.get('violations', []) 
                                 if violation.get('severity') in ['high', 'critical'])
        if high_severity_count > 0:
            recommendations.append(f"Prioritize fixing {high_severity_count} high/critical severity violations")
        
        # 4. Performance recommendations for large datasets
        total_rows = results.get('data_stats', {}).get('total_rows', 0)
        if total_rows > 100000:
            recommendations.append("For large datasets, consider batch processing or sampling for rule validation")
        
        # Remove duplicates
        seen = set()
        unique_recs = []
        for rec in recommendations:
            if rec not in seen:
                seen.add(rec)
                unique_recs.append(rec)
        
        return unique_recs[:10]  # Limit to 10 recommendations
    
    def log_rule_execution(self, results: Dict, rules: List[Dict]):
        """Log rule execution to database"""
        try:
            conn = self.connect_to_db()
            if not conn:
                return
            
            cursor = conn.cursor()
            
            for rule in rules:
                rule_name = rule.get('rule_name')
                rule_id = rule.get('rule_id')
                rule_result = results.get('rule_results', {}).get(rule_name)
                
                if not rule_result:
                    continue
                
                query = """
                INSERT INTO dq_rule_execution_history 
                (session_id, rule_id, rule_name, total_records, passed_records, failed_records, 
                 success_rate, dq_score, execution_time_seconds, error_count, status, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                total_records = results.get('data_stats', {}).get('total_rows', 0)
                violations = rule_result.get('violations', [])
                failed_records = len(violations)
                passed_records = total_records - failed_records
                success_rate = (passed_records / total_records * 100) if total_records > 0 else 0
                
                # Determine status
                if failed_records == 0:
                    status = 'success'
                elif success_rate >= 80:
                    status = 'partial'
                else:
                    status = 'failed'
                
                details = json.dumps({
                    'rule_type': rule_result.get('rule_type'),
                    'stats': rule_result.get('stats', {}),
                    'dq_score': results.get('dq_score', 0)
                }, default=str)
                
                values = (
                    self.session_id,
                    rule_id,
                    rule_name,
                    total_records,
                    passed_records,
                    failed_records,
                    round(success_rate, 2),
                    results.get('dq_score', 0),  # <-- Added dq_score
                    results.get('execution_time_seconds', 0),
                    failed_records,
                    status,
                    details
                )
                
                cursor.execute(query, values)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Logged {len(rules)} rule executions to database")
            
        except Exception as e:
            logger.error(f"Error logging rule execution: {str(e)}")
    
    def log_rule_violations(self, violations: List[Dict]):
        """Log rule violations to database"""
        if not violations or not self.error_logger:
            return
        
        try:
            # Convert violations to error log format
            error_data_list = []
            
            for violation in violations:
                error_data = {
                    'session_id': self.session_id,
                    'check_type': 'business_rules',
                    'column_name': violation.get('column_name', ''),
                    'row_index': violation.get('row_index'),
                    'excel_row': violation.get('excel_row'),
                    'actual_value': violation.get('actual_value', ''),
                    'expected_value': violation.get('expected_value', ''),
                    'error_type': violation.get('violation_type', 'rule_violation'),
                    'error_description': violation.get('details', ''),
                    'severity': violation.get('severity', 'medium'),
                    'comparison_context': f"Rule: {violation.get('rule_name', 'unknown')}",
                    'difference_summary': violation.get('details', '')
                }
                
                # Add source/target values for text comparison
                if violation.get('source_text') is not None:
                    error_data['source_actual_value'] = violation.get('source_text')
                if violation.get('target_text') is not None:
                    error_data['target_actual_value'] = violation.get('target_text')
                
                # Add numeric values for threshold/range violations
                if violation.get('numeric_actual') is not None:
                    error_data['source_actual_value'] = str(violation.get('numeric_actual'))
                if violation.get('threshold') is not None:
                    error_data['target_actual_value'] = str(violation.get('threshold'))
                
                error_data_list.append(error_data)
            
            # Batch log errors
            logged_count = self.error_logger.log_batch_errors(
                self.session_id, 'business_rules', 'rules_engine', error_data_list
            )
            
            self.insert_into_rule_violations(violations)
            
            logger.info(f"Logged {logged_count} rule violations to error logs")
            
        except Exception as e:
            logger.error(f"Error logging rule violations: {str(e)}")
    
    def log_audit_record(self, results: Dict, source_type: str, source_name: str):
        """Log to audit database"""
        try:
            if not self.audit_logger:
                return
            
            audit_data = {
                'session_id': self.session_id,
                'check_type': 'business_rules',
                'source_type': source_type,
                'source_name': source_name,
                'overall_score': float(results.get('dq_score', 0)),
                'total_rules_executed': results.get('summary', {}).get('total_rules_executed', 0),
                'rules_passed': results.get('summary', {}).get('rules_passed', 0),
                'rules_failed': results.get('summary', {}).get('rules_failed', 0),
                'compliance_score': float(results.get('dq_score', 0)),
                'assessment_category': self.get_assessment_category(results.get('dq_score', 0)),
                'issues_summary': f"Rules: {results.get('summary', {}).get('total_rules_executed')} total, "
                                f"{results.get('summary', {}).get('rules_passed')} passed, "
                                f"{results.get('summary', {}).get('rules_failed')} failed, "
                                f"Violations: {results.get('summary', {}).get('total_violations')}"
            }
            
            self.audit_logger.log_audit_record(audit_data)
            logger.info(f"Audit record logged for business rules session: {self.session_id}")
            
        except Exception as e:
            logger.warning(f"Audit logging failed: {e}")
    
    def get_assessment_category(self, score: float) -> str:
        """Get assessment category based on DQ score"""
        if score >= 95:
            return "EXCELLENT"
        elif score >= 80:
            return "GOOD"
        elif score >= 60:
            return "FAIR"
        else:
            return "POOR"

# ============================================================================
# MAIN ENTRY POINTS
# ============================================================================

def run_rules_analysis_ui(ui_data: Dict = None) -> Dict[str, Any]:
    """
    Run business rules analysis in UI/API mode.
    
    Args:
        ui_data: Dictionary containing configuration
        
    Returns:
        Dictionary with analysis results
    """
    logger.info("Starting business rules analysis in UI/API mode")
    
    try:
        # Initialize input handler if available
        if ui_data and HAS_INPUT_HANDLER:
            init_input_handler(mode='ui', data=ui_data)
        
        # Setup rule tables if needed
        setup_rule_tables()
        
        # FIX: Load data - check if database source
        if ui_data and ui_data.get('source_type') == 'database':
            db_type = ui_data.get('db_type', 'mysql')
            host = ui_data.get('host', 'localhost')
            port = int(ui_data.get('port', 3306))
            database = ui_data.get('database', '')
            table = ui_data.get('table', '')
            user = ui_data.get('user', 'root')
            password = ui_data.get('password', '')

            logger.info(f"Loading database data: {database}.{table}")

            try:
                import pandas as pd

                if db_type == 'mysql':
                    import mysql.connector

                    conn = mysql.connector.connect(
                        host=host,
                        port=port,
                        user=user,
                        password=password,
                        database=database
                    )
                    query = f"SELECT * FROM `{table}`"

                elif db_type in ['postgresql', 'postgres']:
                    import psycopg2

                    schema = ui_data.get('schema', 'public')

                    conn = psycopg2.connect(
                        host=host,
                        port=port,
                        user=user,
                        password=password,
                        dbname=database
                    )
                    query = f'SELECT * FROM "{schema}"."{table}"'

                else:
                    return {'error': f'Unsupported database type: {db_type}'}

                df = pd.read_sql(query, conn)
                conn.close()

                source_info = f"Database: {database}.{table}"
                source_file = table

            except Exception as e:
                logger.error(f"Database loading error: {str(e)}")
                return {'error': f'Database loading failed: {str(e)}'}

        else:
            df, source_info, source_file = load_data_for_rules_analysis(ui_data)

        if df is None or df.empty:
            return {'error': 'No data loaded or dataset is empty'}
        
        # Initialize rule engine
        rule_engine = BusinessRuleEngine()
        
        # Generate session ID
        session_id = f"RULES_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{np.random.randint(1000, 9999)}"
        rule_engine.set_session_id(session_id)
        logger.info(f"Session ID: {session_id}")
        
        # Get rules to execute
        rules_to_execute = []
        if ui_data and 'rule_ids' in ui_data:
            # Use specific rule IDs from UI
            rule_ids = ui_data.get('rule_ids', [])
            all_rules = rule_engine.get_all_rules(active_only=True)
            
            if rule_ids:  # If specific IDs provided
                # Filter to only selected rules 
                rules_to_execute = [rule for rule in all_rules 
                                if rule['rule_id'] in rule_ids]
                logger.info(f"Executing specific rules: {rule_ids}")
            else:
                # If no IDs specified, use all active rules
                rules_to_execute = all_rules
                logger.info("Executing all active rules")

        elif ui_data and 'selected_rules' in ui_data:
            # Use selected rules from UI
            selected_rules = ui_data['selected_rules']
            all_rules = rule_engine.get_all_rules(active_only=True)
            
            if selected_rules == 'all':
                rules_to_execute = all_rules
            else:
                # Filter selected rules
                rules_to_execute = [rule for rule in all_rules 
                                  if rule['rule_id'] in selected_rules or 
                                     rule['rule_name'] in selected_rules]
        
        # elif ui_data and 'rule_type' in ui_data:
        #     # Create rule from UI configuration
        #     rule_data = create_rule_from_ui_data(ui_data)
        #     if rule_data:
        #         rules_to_execute = [rule_data]
        
        else:
            # Get all active rules
            rules_to_execute = rule_engine.get_all_rules(active_only=True)
        
        if not rules_to_execute:
            return {'error': 'No rules to execute. Please create or select rules.'}
        
        # Execute rules
        results = rule_engine.execute_rules_on_dataframe(df, rules_to_execute, source_info)
        
        # Determine source type for audit logging
        if 'csv' in source_info.lower():
            source_type = 'csv'
        elif 'excel' in source_info.lower():
            source_type = 'excel'
        else:
            source_type = 'database'
        
        # Log audit record
        rule_engine.log_audit_record(results, source_type, source_info)
        
        # Generate report
        final_results = generate_rules_report(results, mode='ui')
        
        # Add additional information for API response
        final_results['rule_definitions'] = rules_to_execute
        final_results['columns_available'] = list(df.columns)
        final_results['data_preview'] = df.head(10).to_dict('records')
        
        # Add error logs for API response (limited)
        try:
            if rule_engine.error_logger:
                final_results['error_logs'] = rule_engine.error_logger.get_error_logs_for_session(session_id, limit=100)
                final_results['error_summary'] = rule_engine.error_logger.get_error_summary_for_session(session_id)
        except Exception as e:
            logger.warning(f"Could not get error logs: {e}")
            final_results['error_logs'] = []
            final_results['error_summary'] = {'total_errors': 0}
        
        # Add audit logs if available
        try:
            final_results['audit_logs'] = rule_engine.audit_logger.get_audit_logs_for_session(session_id)
        except:
            final_results['audit_logs'] = []
        
            # Filter violations to only include selected rules
        if 'rule_ids' in ui_data and ui_data['rule_ids']:
            selected_rule_ids = set(ui_data['rule_ids'])
            # Filter violations
            filtered_violations = [
                violation for violation in final_results.get('violations', [])
                if violation.get('rule_id') in selected_rule_ids or 
                violation.get('rule_name') in [r.get('rule_name') for r in rules_to_execute]
            ]
            final_results['violations'] = filtered_violations
            
            # Filter rule results
            filtered_rule_results = {}
            for rule_name, rule_result in final_results.get('rule_results', {}).items():
                # Check if this rule was in our selected rules
                for rule in rules_to_execute:
                    if rule['rule_name'] == rule_name:
                        filtered_rule_results[rule_name] = rule_result
                        break
            final_results['rule_results'] = filtered_rule_results
            
            # Update summary counts
            final_results['summary']['total_rules_executed'] = len(filtered_rule_results)
            final_results['summary']['rules_passed'] = sum(
                1 for r in filtered_rule_results.values() if r.get('passed', False)
            )
            final_results['summary']['rules_failed'] = len(filtered_rule_results) - final_results['summary']['rules_passed']
            final_results['summary']['total_violations'] = len(filtered_violations)
            
        logger.info(f"Business rules analysis completed for session {session_id}")
        return final_results
        
    except Exception as e:
        logger.error(f"Error in business rules analysis: {str(e)}", exc_info=True)
        return {'error': str(e)}

def load_data_for_rules_analysis(ui_data=None):
    """Load data from any source for rules analysis"""
    logger.info("Loading data for rules analysis")
    
    # Determine mode
    is_ui_mode = False
    if ui_data and HAS_INPUT_HANDLER:
        init_input_handler(mode='ui', data=ui_data)
        is_ui_mode = True
    
    # Show loading message
    if not is_ui_mode and not ui_data:
        print("\n🔍 LOADING DATA FOR RULES ANALYSIS...")
    
    # Load data using existing select_data_source
    if ui_data and HAS_INPUT_HANDLER:
        source_type, source_config = select_data_source(ui_data)
    else:
        source_type, source_config = select_data_source()
    
    if source_config is None:
        logger.error("No data source configuration available")
        return None, None, None
    
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

def create_rule_from_ui_data(ui_data: Dict) -> Dict:
    """Create rule configuration from UI data"""
    rule_type = ui_data.get('rule_type')
    
    if not rule_type:
        return None
    
    rule_data = {
        'rule_name': ui_data.get('rule_name', f'Rule_{datetime.now().strftime("%H%M%S")}'),
        'rule_type': rule_type,
        'description': ui_data.get('rule_description', ''),
        'severity': ui_data.get('severity', 'medium'),
        'is_active': True,
        'created_by': 'ui_user'
    }
    
    # Add type-specific fields
    if rule_type in ['mandatory', 'threshold', 'pattern', 'range', 'kpi', 'aggregate']:
        rule_data['source_column'] = ui_data.get('source_column')
    
    if rule_type in ['threshold', 'kpi', 'aggregate']:
        rule_data['threshold_value'] = ui_data.get('threshold_value')
        rule_data['operator'] = ui_data.get('operator', '≤')
    
    if rule_type in ['cross_column', 'text_comparison']:
        rule_data['source_column'] = ui_data.get('source_column')
        rule_data['target_column'] = ui_data.get('target_column')
        if rule_type == 'text_comparison':
            rule_data['comparison_type'] = ui_data.get('comparison_type', 'exact')
            rule_data['case_sensitive'] = ui_data.get('case_sensitive', False)
        else:
            rule_data['operator'] = ui_data.get('operator', '>')
    
    if rule_type == 'range':
        rule_data['min_value'] = ui_data.get('min_value')
        rule_data['max_value'] = ui_data.get('max_value')
    
    if rule_type == 'pattern':
        rule_data['pattern_type'] = ui_data.get('pattern_type', 'email')
    
    if rule_type == 'kpi':
        rule_data['kpi_type'] = ui_data.get('kpi_type', 'count')
    
    if rule_type == 'aggregate':
        rule_data['aggregation'] = ui_data.get('aggregation', 'sum')
    
    if rule_type == 'custom':
        rule_data['rule_logic'] = ui_data.get('rule_logic', '')
    
    return rule_data

def generate_rules_report(results: Dict, mode: str = 'cli') -> Dict[str, Any]:
    """Generate comprehensive rules report"""
    report = results.copy()
    
    if mode == 'cli':
        print("\n" + "="*70)
        print("📊 BUSINESS RULES VALIDATION REPORT")
        print("="*70)
        
        # Header
        print(f"Session ID: {results['session_id']}")
        print(f"Source: {results['source_name']}")
        print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-"*70)
        
        # Data Statistics
        stats = results['data_stats']
        print(f"\n📈 DATA STATISTICS:")
        print(f"   • Total rows: {stats['total_rows']:,}")
        print(f"   • Total columns: {stats['total_columns']}")
        print(f"   • Columns checked: {stats['columns_checked']}")
        print("-"*70)
        
        # Summary
        summary = results['summary']
        print(f"\n📊 RULES EXECUTION SUMMARY:")
        print(f"   • Total rules executed: {summary['total_rules_executed']}")
        print(f"   • Rules passed: {summary['rules_passed']}")
        print(f"   • Rules failed: {summary['rules_failed']}")
        print(f"   • Total violations: {summary['total_violations']:,}")
        print(f"   • DQ Score: {results.get('dq_score', 0):.1f}%")
        print("-"*70)
        
        # Rule Details
        if results['rule_results']:
            print(f"\n🔍 RULE DETAILS:")
            for rule_name, rule_result in results['rule_results'].items():
                passed = rule_result.get('passed', False)
                violations = rule_result.get('violations', [])
                rule_type = rule_result.get('rule_type', 'unknown')
                
                status_icon = "✅" if passed else "❌"
                print(f"   {status_icon} {rule_name} ({rule_type}):")
                print(f"     - Status: {'PASSED' if passed else 'FAILED'}")
                print(f"     - Violations: {len(violations)}")
                
                if rule_result.get('stats'):
                    stats = rule_result['stats']
                    if 'compliance_rate' in stats:
                        print(f"     - Compliance rate: {stats['compliance_rate']:.1f}%")
        
        # Violations Summary
        if results['violations']:
            print(f"\n🚨 VIOLATIONS SUMMARY:")
            
            # Group by rule type
            violations_by_type = defaultdict(list)
            for violation in results['violations']:
                rule_name = violation.get('rule_name', 'unknown')
                violations_by_type[rule_name].append(violation)
            
            for rule_name, rule_violations in violations_by_type.items():
                print(f"   • {rule_name}: {len(rule_violations):,} violations")
                
                # Show sample violations
                if rule_violations and len(rule_violations) <= 3:
                    for i, violation in enumerate(rule_violations[:3], 1):
                        print(f"     {i}. Row {violation.get('excel_row')}: {violation.get('details', '')}")
        
        # Recommendations
        if results['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(results['recommendations'], 1):
                print(f"   {i}. {rec}")
        
        print("\n📋 DETAILED LOGS:")
        print(f"   • Session ID for query: {results['session_id']}")
        print(f"   • Rule violations logged to: dq_error_logs")
        print(f"   • Execution history in: dq_rule_execution_history")
        
        print("\n" + "="*70)
    
    # Always include these in results for UI/API mode
    report['report_generated'] = datetime.now().isoformat()
    
    return report

def display_rule_management_menu():
    """Display rule management menu in CLI mode"""
    print("\n📋 RULE MANAGEMENT MENU:")
    print("1. View all rules")
    print("2. Create new rule")
    print("3. Edit existing rule")
    print("4. Delete (deactivate) rule")
    print("5. Execute rules on data")
    print("6. View rule execution history")
    print("7. Back to main menu")
    
    choice = input("\nEnter choice (1-7): ").strip()
    return choice

def create_rule_interactive():
    """Interactive rule creation in CLI mode"""
    print("\n📝 CREATE NEW BUSINESS RULE")
    print("-" * 40)
    
    rule_data = {}
    
    # Basic information
    rule_data['rule_name'] = input("Rule name: ").strip()
    if not rule_data['rule_name']:
        print("❌ Rule name is required")
        return None
    
    rule_data['description'] = input("Description (optional): ").strip()
    
    # Rule type
    print("\nSelect rule type:")
    print("1. Mandatory field validation")
    print("2. Threshold validation (value ≤ X)")
    print("3. Cross-column validation (A > B)")
    print("4. Text comparison (exact/substring/fuzzy)")
    print("5. Pattern validation (email, phone, etc.)")
    print("6. Range validation (min ≤ value ≤ max)")
    print("7. KPI validation (count, sum, avg, etc.)")
    print("8. Aggregate validation (sum ≤ X)")
    print("9. Custom rule (Python/SQL)")
    
    type_choice = input("\nEnter choice (1-9): ").strip()
    type_map = {
        '1': 'mandatory',
        '2': 'threshold',
        '3': 'cross_column',
        '4': 'text_comparison',
        '5': 'pattern',
        '6': 'range',
        '7': 'kpi',
        '8': 'aggregate',
        '9': 'custom'
    }
    
    rule_data['rule_type'] = type_map.get(type_choice, 'custom')
    
    # Severity
    print("\nSelect severity:")
    print("1. Low (informational)")
    print("2. Medium (warning)")
    print("3. High (error)")
    print("4. Critical (blocker)")
    
    severity_choice = input("Enter choice (1-4, default=2): ").strip() or '2'
    severity_map = {'1': 'low', '2': 'medium', '3': 'high', '4': 'critical'}
    rule_data['severity'] = severity_map.get(severity_choice, 'medium')
    
    # Type-specific configuration
    if rule_data['rule_type'] == 'mandatory':
        rule_data['source_column'] = input("Column name: ").strip()
        
    elif rule_data['rule_type'] == 'threshold':
        rule_data['source_column'] = input("Column name: ").strip()
        rule_data['threshold_value'] = float(input("Threshold value: ").strip())
        
        print("\nSelect operator:")
        print("1. ≤ (less than or equal)")
        print("2. ≥ (greater than or equal)")
        print("3. < (less than)")
        print("4. > (greater than)")
        print("5. = (equal to)")
        print("6. ≠ (not equal to)")
        
        op_choice = input("Enter choice (1-6, default=1): ").strip() or '1'
        op_map = {'1': '≤', '2': '≥', '3': '<', '4': '>', '5': '=', '6': '≠'}
        rule_data['operator'] = op_map.get(op_choice, '≤')
        
    elif rule_data['rule_type'] == 'cross_column':
        rule_data['source_column'] = input("First column: ").strip()
        rule_data['target_column'] = input("Second column: ").strip()
        
        print("\nSelect comparison:")
        print("1. > (greater than)")
        print("2. < (less than)")
        print("3. = (equal to)")
        print("4. ≥ (greater than or equal)")
        print("5. ≤ (less than or equal)")
        
        comp_choice = input("Enter choice (1-5, default=1): ").strip() or '1'
        comp_map = {'1': '>', '2': '<', '3': '=', '4': '≥', '5': '≤'}
        rule_data['operator'] = comp_map.get(comp_choice, '>')
        
    elif rule_data['rule_type'] == 'text_comparison':
        rule_data['source_column'] = input("First column: ").strip()
        rule_data['target_column'] = input("Second column: ").strip()
        
        print("\nSelect comparison type:")
        print("1. Exact match")
        print("2. Substring match (A contains B or vice versa)")
        print("3. Fuzzy match (similarity threshold)")
        
        comp_type_choice = input("Enter choice (1-3, default=1): ").strip() or '1'
        comp_type_map = {'1': 'exact', '2': 'substring', '3': 'fuzzy'}
        rule_data['comparison_type'] = comp_type_map.get(comp_type_choice, 'exact')
        
        if rule_data['comparison_type'] == 'fuzzy':
            threshold = input("Similarity threshold (0.0-1.0, default=0.8): ").strip()
            rule_data['similarity_threshold'] = float(threshold) if threshold else 0.8
        
        case_sensitive = input("Case sensitive? (y/n, default=n): ").strip().lower()
        rule_data['case_sensitive'] = (case_sensitive == 'y')
        
    elif rule_data['rule_type'] == 'pattern':
        rule_data['source_column'] = input("Column name: ").strip()
        
        print("\nSelect pattern:")
        print("1. Email address")
        print("2. US Phone number")
        print("3. International phone number")
        print("4. Date (ISO format: YYYY-MM-DD)")
        print("5. Social Security Number")
        print("6. US ZIP code")
        print("7. URL")
        print("8. IP Address")
        print("9. Currency")
        print("10. Percentage")
        print("11. Custom regex pattern")
        
        pattern_choice = input("Enter choice (1-11): ").strip()
        pattern_map = {
            '1': 'email', '2': 'phone_us', '3': 'phone_international',
            '4': 'date_iso', '5': 'ssn', '6': 'zip_code',
            '7': 'url', '8': 'ip_address', '9': 'currency',
            '10': 'percentage'
        }
        
        if pattern_choice == '11':
            rule_data['pattern_type'] = input("Enter custom regex pattern: ").strip()
        else:
            rule_data['pattern_type'] = pattern_map.get(pattern_choice, 'email')
        
    elif rule_data['rule_type'] == 'range':
        rule_data['source_column'] = input("Column name: ").strip()
        
        min_val = input("Minimum value (leave empty for no minimum): ").strip()
        max_val = input("Maximum value (leave empty for no maximum): ").strip()
        
        if min_val:
            rule_data['min_value'] = float(min_val)
        if max_val:
            rule_data['max_value'] = float(max_val)
        
    elif rule_data['rule_type'] == 'kpi':
        print("\nSelect KPI type:")
        print("1. Row count")
        print("2. Sum of column")
        print("3. Average of column")
        print("4. Minimum value")
        print("5. Maximum value")
        print("6. Null count")
        print("7. Distinct count")
        
        kpi_choice = input("Enter choice (1-7): ").strip()
        kpi_map = {
            '1': 'count', '2': 'sum', '3': 'average',
            '4': 'min', '5': 'max', '6': 'null_count',
            '7': 'distinct_count'
        }
        rule_data['kpi_type'] = kpi_map.get(kpi_choice, 'count')
        
        if rule_data['kpi_type'] != 'count':
            rule_data['source_column'] = input("Column name: ").strip()
        
        rule_data['threshold_value'] = float(input("Threshold value: ").strip())
        
        print("\nSelect operator:")
        print("1. ≤ (less than or equal)")
        print("2. ≥ (greater than or equal)")
        print("3. < (less than)")
        print("4. > (greater than)")
        print("5. = (equal to)")
        print("6. ≠ (not equal to)")
        
        op_choice = input("Enter choice (1-6, default=1): ").strip() or '1'
        op_map = {'1': '≤', '2': '≥', '3': '<', '4': '>', '5': '=', '6': '≠'}
        rule_data['operator'] = op_map.get(op_choice, '≤')
        
    elif rule_data['rule_type'] == 'aggregate':
        rule_data['source_column'] = input("Column name: ").strip()
        
        print("\nSelect aggregation:")
        print("1. Sum")
        print("2. Average")
        print("3. Count")
        print("4. Minimum")
        print("5. Maximum")
        print("6. Median")
        print("7. Standard deviation")
        
        agg_choice = input("Enter choice (1-7): ").strip()
        agg_map = {
            '1': 'sum', '2': 'average', '3': 'count',
            '4': 'min', '5': 'max', '6': 'median',
            '7': 'std'
        }
        rule_data['aggregation'] = agg_map.get(agg_choice, 'sum')
        
        rule_data['threshold_value'] = float(input("Threshold value: ").strip())
        
        print("\nSelect operator:")
        print("1. ≤ (less than or equal)")
        print("2. ≥ (greater than or equal)")
        print("3. < (less than)")
        print("4. > (greater than)")
        print("5. = (equal to)")
        
        op_choice = input("Enter choice (1-5, default=1): ").strip() or '1'
        op_map = {'1': '≤', '2': '≥', '3': '<', '4': '>', '5': '='}
        rule_data['operator'] = op_map.get(op_choice, '≤')
        
    elif rule_data['rule_type'] == 'custom':
        print("\nEnter rule logic (Python expression):")
        print("Examples:")
        print("  - df['age'] > 18")
        print("  - df['end_date'] > df['start_date']")
        print("  - df['price'].sum() <= 100000")
        
        rule_data['rule_logic'] = input("\nRule logic: ").strip()
    
    rule_data['is_active'] = True
    rule_data['created_by'] = 'cli_user'
    
    return rule_data

def main(ui_data: Dict = None):
    """Main function for CLI mode"""
    logger.info("Starting business rules engine")
    
    try:
        # If UI data provided, run in UI mode
        if ui_data and HAS_INPUT_HANDLER:
            return run_rules_analysis_ui(ui_data)
        
        # CLI Mode
        print("\n" + "="*60)
        print("📈 BUSINESS RULES ENGINE")
        print("="*60)
        print("ℹ️  This engine allows you to:")
        print("   • Define and manage business rules")
        print("   • Validate data against rules")
        print("   • Track KPI compliance")
        print("   • Generate comprehensive reports")
        print("   • Log violations to database")
        print("="*60)
        
        # Setup tables
        print("\n🔧 Setting up database tables...")
        if setup_rule_tables():
            print("✅ Tables verified/created successfully")
        else:
            print("⚠️  Could not setup tables. Some features may not work.")
        
        # Initialize rule engine
        rule_engine = BusinessRuleEngine()
        
        while True:
            choice = display_rule_management_menu()
            
            if choice == '1':
                # View all rules
                rules = rule_engine.get_all_rules(active_only=True)
                if rules:
                    print(f"\n📋 ACTIVE RULES ({len(rules)}):")
                    print("-" * 50)
                    for rule in rules:
                        status = "✅ ACTIVE" if rule['is_active'] else "❌ INACTIVE"
                        print(f"{status} [{rule['rule_id']}] {rule['rule_name']} ({rule['rule_type']})")
                        if rule['description']:
                            print(f"     Description: {rule['description']}")
                        print()
                else:
                    print("\n📭 No rules found. Create some rules first!")
                
                input("\nPress Enter to continue...")
            
            elif choice == '2':
                # Create new rule
                rule_data = create_rule_interactive()
                if rule_data:
                    rule_id = rule_engine.create_rule(rule_data)
                    if rule_id:
                        print(f"✅ Rule created successfully (ID: {rule_id})")
                    else:
                        print("❌ Failed to create rule")
                
                input("\nPress Enter to continue...")
            
            elif choice == '3':
                # Edit existing rule
                rules = rule_engine.get_all_rules(active_only=True)
                if not rules:
                    print("\n📭 No rules to edit")
                    input("\nPress Enter to continue...")
                    continue
                
                print("\n📋 Select rule to edit:")
                for rule in rules:
                    print(f"[{rule['rule_id']}] {rule['rule_name']} ({rule['rule_type']})")
                
                try:
                    rule_id = int(input("\nEnter rule ID to edit: ").strip())
                    selected_rule = next((r for r in rules if r['rule_id'] == rule_id), None)
                    
                    if selected_rule:
                        print(f"\nEditing rule: {selected_rule['rule_name']}")
                        print("Leave field empty to keep current value")
                        
                        updates = {}
                        new_name = input(f"New name [{selected_rule['rule_name']}]: ").strip()
                        if new_name:
                            updates['rule_name'] = new_name
                        
                        new_desc = input(f"New description [{selected_rule['description']}]: ").strip()
                        if new_desc:
                            updates['description'] = new_desc
                        
                        new_severity = input(f"New severity (low/medium/high/critical) [{selected_rule['severity']}]: ").strip().lower()
                        if new_severity in ['low', 'medium', 'high', 'critical']:
                            updates['severity'] = new_severity
                        
                        if updates:
                            success = rule_engine.update_rule(rule_id, updates)
                            if success:
                                print("✅ Rule updated successfully")
                            else:
                                print("❌ Failed to update rule")
                        else:
                            print("⚠️  No changes made")
                    else:
                        print("❌ Rule not found")
                
                except ValueError:
                    print("❌ Invalid rule ID")
                
                input("\nPress Enter to continue...")
            
            elif choice == '4':
                # Delete rule
                rules = rule_engine.get_all_rules(active_only=True)
                if not rules:
                    print("\n📭 No rules to delete")
                    input("\nPress Enter to continue...")
                    continue
                
                print("\n📋 Select rule to delete (deactivate):")
                for rule in rules:
                    print(f"[{rule['rule_id']}] {rule['rule_name']} ({rule['rule_type']})")
                
                try:
                    rule_id = int(input("\nEnter rule ID to delete: ").strip())
                    confirm = input(f"Are you sure you want to deactivate rule {rule_id}? (y/n): ").strip().lower()
                    
                    if confirm == 'y':
                        success = rule_engine.delete_rule(rule_id)
                        if success:
                            print("✅ Rule deactivated successfully")
                        else:
                            print("❌ Failed to deactivate rule")
                    else:
                        print("⚠️  Deletion cancelled")
                
                except ValueError:
                    print("❌ Invalid rule ID")
                
                input("\nPress Enter to continue...")
            
            elif choice == '5':
                # Execute rules on data
                print("\n🔍 LOADING DATA FOR RULES ANALYSIS...")
                
                # Load data
                df, source_info, source_file = load_data_for_rules_analysis()
                
                if df is None or df.empty:
                    print("❌ No data loaded or dataset is empty")
                    input("\nPress Enter to continue...")
                    continue
                
                print(f"✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
                
                # Select rules to execute
                rules = rule_engine.get_all_rules(active_only=True)
                if not rules:
                    print("❌ No active rules found. Create rules first!")
                    input("\nPress Enter to continue...")
                    continue
                
                print("\n📋 SELECT RULES TO EXECUTE:")
                print("all - Execute all active rules")
                print("OR enter rule IDs separated by commas")
                
                for rule in rules:
                    print(f"[{rule['rule_id']}] {rule['rule_name']} ({rule['rule_type']})")
                
                rule_selection = input("\nEnter selection: ").strip().lower()
                
                if rule_selection == 'all':
                    rules_to_execute = rules
                else:
                    try:
                        selected_ids = [int(id_str.strip()) for id_str in rule_selection.split(',')]
                        rules_to_execute = [r for r in rules if r['rule_id'] in selected_ids]
                    except ValueError:
                        print("❌ Invalid selection. Using all rules.")
                        rules_to_execute = rules
                
                if not rules_to_execute:
                    print("❌ No rules selected")
                    input("\nPress Enter to continue...")
                    continue
                
                print(f"\n🔧 Executing {len(rules_to_execute)} rules...")
                
                # Generate session ID
                session_id = f"RULES_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                rule_engine.set_session_id(session_id)
                
                # Execute rules
                results = rule_engine.execute_rules_on_dataframe(df, rules_to_execute, source_info)
                
                # Generate and display report
                generate_rules_report(results, mode='cli')
                
                # Determine source type for audit logging
                if 'csv' in source_info.lower():
                    source_type = 'csv'
                elif 'excel' in source_info.lower():
                    source_type = 'excel'
                else:
                    source_type = 'database'
                
                # Log audit record
                rule_engine.log_audit_record(results, source_type, source_info)
                
                print(f"\n✅ Business rules analysis completed!")
                print(f"📋 Session ID: {session_id}")
                
                input("\nPress Enter to continue...")
            
            elif choice == '6':
                # View rule execution history
                try:
                    conn = rule_engine.connect_to_db()
                    if conn:
                        cursor = conn.cursor(dictionary=True)
                        cursor.execute("""
                            SELECT session_id, rule_name, execution_timestamp, 
                                   total_records, passed_records, failed_records,
                                   success_rate, status
                            FROM dq_rule_execution_history
                            ORDER BY execution_timestamp DESC
                            LIMIT 20
                        """)
                        history = cursor.fetchall()
                        
                        if history:
                            print(f"\n📜 RECENT RULE EXECUTIONS ({len(history)}):")
                            print("-" * 80)
                            for record in history:
                                timestamp = record['execution_timestamp'].strftime('%Y-%m-%d %H:%M') if hasattr(record['execution_timestamp'], 'strftime') else str(record['execution_timestamp'])
                                print(f"📅 {timestamp} - {record['session_id']}")
                                print(f"   Rule: {record['rule_name']}")
                                print(f"   Status: {record['status']} - Success rate: {record['success_rate']:.1f}%")
                                print(f"   Records: {record['passed_records']}/{record['total_records']} passed")
                                print()
                        else:
                            print("\n📭 No execution history found")
                        
                        cursor.close()
                        conn.close()
                except Exception as e:
                    print(f"❌ Error loading history: {str(e)}")
                
                input("\nPress Enter to continue...")
            
            elif choice == '7':
                # Back to main menu
                print("\nReturning to main menu...")
                break
            
            else:
                print("❌ Invalid choice. Please try again.")
        
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        logger.info("Business rules engine cancelled by user")
    except Exception as e:
        logger.error(f"Error in business rules engine: {str(e)}", exc_info=True)
        print(f"\n❌ Error in business rules engine: {e}")
        print("Please check the log file for details.")

if __name__ == "__main__":
    main()