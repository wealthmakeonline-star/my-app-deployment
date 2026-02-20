# dq_error_log.py - PROFESSIONAL VERSION WITH LOGGING
import mysql.connector
from datetime import datetime
import logging
from db_config import MYSQL_CONFIG
import pandas as pd
import os
import json

logger = logging.getLogger(__name__)

class ErrorLogger:
    def __init__(self, db_config=None):
        self.db_config = db_config or MYSQL_CONFIG
        logger.info(f"ErrorLogger initialized with database: {self.db_config['database']}")
    
    def _get_connection(self):
        """Get a fresh database connection with proper settings"""
        try:
            conn = mysql.connector.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                port=self.db_config.get('port', 3306),
                buffered=True,
                autocommit=False
            )
            return conn
        except mysql.connector.Error as e:
            logger.error(f"Connection error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected connection error: {e}")
            return None
    
    def log_error(self, error_data):
        """Log validation errors with actual vs expected values"""
        logger.debug(f"Logging error: {error_data.get('error_type', 'unknown')}")
        
        # DEBUG: Check what's being passed
        logger.debug(f"Error data received: {error_data}")

        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                logger.error("Cannot connect to database")
                self._fallback_error_log_to_file(error_data)
                return None
            
            cursor = conn.cursor()
            
            # DEBUG: Check what's being passed - ADD THIS
            import traceback
            logger.debug(f"Logging error: {error_data.get('error_type', 'unknown')}")

            # DEBUG: Check if any value is a slice object
            for key, value in error_data.items():
                if isinstance(value, slice):
                    logger.error(f"ERROR: Found slice object in error_data: {key} = {value}")
                    # Convert slice to string
                    error_data[key] = f"slice({value.start}, {value.stop}, {value.step})"

            # Prepare data for insertion
            session_id = error_data.get('session_id', 'UNKNOWN')
            check_type = error_data.get('check_type', 'unknown')
            source_name = str(error_data.get('source_name', ''))[:200]
            target_name = str(error_data.get('target_name', ''))[:200]
            column_name = str(error_data.get('column_name', ''))[:100]
            row_index = error_data.get('row_index')
            excel_row = error_data.get('excel_row', row_index + 2 if row_index else 0)
            
            # Truncate values but preserve important info
            actual_value = str(error_data.get('actual_value', ''))[:1000]
            expected_value = str(error_data.get('expected_value', ''))[:1000]
            
            source_actual_value = str(error_data.get('source_actual_value', ''))[:1000]  # NEW
            target_actual_value = str(error_data.get('target_actual_value', ''))[:1000]  # NEW
            comparison_context = str(error_data.get('comparison_context', ''))[:100]    # NEW
            difference_summary = str(error_data.get('difference_summary', ''))[:2000]   # NEW
            
            error_type = str(error_data.get('error_type', ''))[:50]
            error_description = str(error_data.get('error_description', ''))[:2000]
            source_file = str(error_data.get('source_file', ''))[:200]
            target_file = str(error_data.get('target_file', ''))[:200]
            severity = str(error_data.get('severity', 'medium'))[:20]
            
            query = """
            INSERT INTO dq_error_logs (
                session_id, check_type, source_name, target_name,
                column_name, row_index, excel_row, actual_value,
                source_actual_value, target_actual_value,
                expected_value, error_type, error_description,
                check_timestamp, source_file, target_file, severity,comparison_context, difference_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)
            """
            
            values = (
                session_id,
                check_type,
                source_name,
                target_name,
                column_name,
                row_index,
                excel_row,
                actual_value,
                source_actual_value,
                target_actual_value,
                expected_value,
                error_type,
                error_description,
                datetime.now(),
                source_file,
                target_file,
                severity,
                comparison_context,
                difference_summary   
            )
            
            cursor.execute(query, values)
            conn.commit()
            
            error_id = cursor.lastrowid
            logger.debug(f"Error logged (ID: {error_id}): {error_type}")
            return error_id
            
        except mysql.connector.Error as e:
            logger.error(f"MySQL Error logging error: {e}")
            self._fallback_error_log_to_file(error_data)
            return None
        except Exception as e:
            # Get the actual error message, not the slice object
            error_msg = str(e) if not isinstance(e, slice) else f"Slice object error: {e}"
            logger.error(f"General Error logging error: {error_msg}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            self._fallback_error_log_to_file(error_data)
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass

    def log_batch_errors(self, session_id, check_type, source_name, errors):
        """Log multiple errors in batch for ANY dataset size"""
        if not errors:
            return 0
        
        total_logged = 0
        batch_size = 500  # Safe for most datasets
        
        logger.info(f"Starting batch logging for {len(errors)} errors, session: {session_id}")
        
        # Process in batches
        for batch_num, i in enumerate(range(0, len(errors), batch_size), 1):
            batch = errors[i:i+batch_size]
            batch_logged = 0
            
            try:
                # Get fresh connection for each batch
                conn = self._get_connection()
                if not conn:
                    logger.error(f"Batch {batch_num}: Cannot connect to database")
                    continue
                
                cursor = conn.cursor()
                
                query = """
                INSERT INTO dq_error_logs (
                    session_id, check_type, source_name, column_name, 
                    row_index, excel_row, actual_value, expected_value,
                    source_actual_value, target_actual_value,
                    error_type, error_description, check_timestamp, severity,
                    comparison_context, difference_summary
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                batch_values = []
                timestamp = datetime.now()
                
                for error in batch:
                    # Truncate values to avoid MySQL errors
                    column_name = (error.get('column_name', '') or '')[:100]
                    row_index = error.get('row_index')
                    excel_row = error.get('excel_row', row_index + 2 if row_index is not None else 0)
                    actual_value = str(error.get('actual_value', ''))[:500]  # Reduced from 1000
                    expected_value = str(error.get('expected_value', ''))[:500]  # Reduced from 1000

                    source_actual_value = str(error.get('source_actual_value', ''))[:500]  # NEW
                    target_actual_value = str(error.get('target_actual_value', ''))[:500]  # NEW
                    comparison_context = str(error.get('comparison_context', ''))[:100]    # NEW
                    difference_summary = str(error.get('difference_summary', ''))[:1000]   # NEW
                    
                    error_type = (error.get('error_type', '') or '')[:50]
                    error_description = (error.get('error_description', '') or '')[:1000]  # Reduced from 2000
                    severity = (error.get('severity', 'medium') or 'medium')[:20]
                    
                    # Ensure row_index is valid
                    if row_index is None:
                        row_index = 0
                    
                    batch_values.append((
                        session_id, check_type, source_name, column_name,
                        row_index, excel_row, actual_value, expected_value,source_actual_value, target_actual_value,
                        error_type, error_description, timestamp, severity,comparison_context, difference_summary
                    ))
                
                # Log batch attempt
                logger.debug(f"Batch {batch_num}: Attempting to insert {len(batch_values)} rows")
                
                # Execute with error handling
                try:
                    cursor.executemany(query, batch_values)
                    conn.commit()
                    batch_logged = cursor.rowcount
                    total_logged += batch_logged
                    
                    logger.info(f"Batch {batch_num}: Successfully logged {batch_logged} errors")
                    
                except mysql.connector.Error as e:
                    logger.error(f"Batch {batch_num}: MySQL error: {e}")
                    conn.rollback()
                    
                    # Fallback: Try individual inserts
                    logger.info(f"Batch {batch_num}: Falling back to individual inserts")
                    individual_success = 0
                    for error_data in batch:
                        try:
                            self.log_error({
                                'session_id': session_id,
                                'check_type': check_type,
                                'source_name': source_name,
                                'column_name': error.get('column_name', ''),
                                'row_index': error.get('row_index'),
                                'excel_row': error.get('excel_row'),
                                'actual_value': error.get('actual_value', ''),
                                'expected_value': error.get('expected_value', ''),
                                'source_actual_value': error.get('source_actual_value', ''),  
                                'target_actual_value': error.get('target_actual_value', ''),  
                                'comparison_context': error.get('comparison_context', ''),   
                                'difference_summary': error.get('difference_summary', ''),   
                                'error_type': error.get('error_type', ''),
                                'error_description': error.get('error_description', ''),
                                'severity': error.get('severity', 'medium')
                            })
                            individual_success += 1
                        except:
                            pass
                    
                    total_logged += individual_success
                    logger.info(f"Batch {batch_num}: Individual fallback logged {individual_success} errors")
                
                except Exception as e:
                    logger.error(f"Batch {batch_num}: General error: {e}")
                    conn.rollback()
            
            except Exception as e:
                logger.error(f"Batch {batch_num}: Setup error: {e}")
            
            finally:
                # Always clean up
                try:
                    if 'cursor' in locals() and cursor:
                        cursor.close()
                except:
                    pass
                try:
                    if 'conn' in locals() and conn:
                        conn.close()
                except:
                    pass
            
            # Progress logging for large datasets
            if batch_num % 10 == 0 or i + batch_size >= len(errors):
                logger.info(f"Progress: {min(i + batch_size, len(errors))}/{len(errors)} errors processed")
        
        # Final summary
        logger.info(f"COMPLETE: Logged {total_logged}/{len(errors)} errors for session {session_id}")
        
        if total_logged < len(errors):
            logger.warning(f"Missed {len(errors) - total_logged} errors due to failures")
        
        return total_logged    
    def _fallback_error_log_to_file(self, error_data):
        """Fallback to CSV file for error logging"""
        try:
            log_file = 'dq_error_logs_fallback.csv'
            
            # Add metadata
            error_data['fallback_timestamp'] = datetime.now().isoformat()
            error_data['logged_via'] = 'fallback'
            
            # Ensure all values are strings
            for key, value in error_data.items():
                if not isinstance(value, (str, int, float)):
                    error_data[key] = str(value)
            
            df = pd.DataFrame([error_data])
            
            if not os.path.exists(log_file):
                df.to_csv(log_file, index=False, encoding='utf-8')
                logger.info(f"Created fallback file: {log_file}")
            else:
                df.to_csv(log_file, mode='a', header=False, index=False, encoding='utf-8')
                
        except Exception as e:
            logger.error(f"Fallback logging failed: {e}")
            # Last resort: write to text file
            try:
                with open('dq_error_logs_fallback.txt', 'a', encoding='utf-8') as f:
                    f.write(f"{datetime.now()}: {json.dumps(error_data)}\n")
            except:
                pass
    
    def log_comparison_mismatch_immediate(self, session_id, mismatch_data, source_name, target_name):
        """
        Log a single comparison mismatch immediately when found
        
        Args:
            session_id: Session ID
            mismatch_data: Dictionary with mismatch details (from compare_rows_detailed)
            source_name: Source dataset name
            target_name: Target dataset name
        
        Returns:
            int: Error ID or None
        """
        try:
            conn = self._get_connection()
            if not conn:
                logger.error("Cannot connect to database for immediate mismatch logging")
                return None
            
            cursor = conn.cursor()
            
            # Extract data from mismatch_data
            row_index = mismatch_data.get('row_index', 0)
            excel_row = mismatch_data.get('excel_row', row_index + 2)
            
            mismatch_summary = mismatch_data.get('mismatch_summary')
            if not mismatch_summary:
                # Create a simple summary from differences
                differences = mismatch_data.get('differences', [])
                if differences:
                    # Take first difference only
                    diff = differences[0]
                    mismatch_summary = f"{diff['column']}: '{diff['source']}' ≠ '{diff['target']}'"
                    if len(differences) > 1:
                        mismatch_summary += f" ... (+{len(differences)-1} more)"
                else:
                    mismatch_summary = "No mismatch summary available"
            
            # Convert source and target data to JSON strings
            import json
            source_data_json = json.dumps(mismatch_data.get('source_data', {}), default=str)
            target_data_json = json.dumps(mismatch_data.get('target_data', {}), default=str)
            
            # Convert differences to JSON
            differences_json = json.dumps(mismatch_data.get('differences', []), default=str)
            
            # Format for actual_value field (like your screenshot's Mismatch Summary)
            actual_value = f"Mismatch: {mismatch_summary}"
            
            query = """
            INSERT INTO dq_error_logs (
                session_id, check_type, source_name, target_name,
                column_name, row_index, excel_row, 
                actual_value, expected_value,
                source_actual_value, target_actual_value,
                error_type, error_description, check_timestamp,
                severity, comparison_context, difference_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                session_id,
                'comparison',
                source_name,
                target_name,
                'ALL_COLUMNS',
                row_index,
                excel_row,
                actual_value,  # Mismatch summary
                "Rows should match exactly",  # Expected value
                source_data_json,  # Complete source row data as JSON
                target_data_json,  # Complete target row data as JSON
                'row_data_mismatch',
                f"Row {excel_row}: Detailed data mismatch detected",
                datetime.now(),
                'high',
                f"Row {excel_row} detailed comparison",
                differences_json  # Which specific columns differ
            )
            
            cursor.execute(query, values)
            conn.commit()
            
            error_id = cursor.lastrowid
            logger.debug(f"Logged detailed mismatch (ID: {error_id}) for row {excel_row}")
            
            cursor.close()
            conn.close()
            
            return error_id
            
        except Exception as e:
            logger.error(f"Error logging immediate mismatch: {e}")
            
            # Fallback: Log basic error
            try:
                self.log_error({
                    'session_id': session_id,
                    'check_type': 'comparison',
                    'source_name': source_name,
                    'target_name': target_name,
                    'column_name': 'ALL_COLUMNS',
                    'row_index': mismatch_data.get('row_index', 0),
                    'excel_row': mismatch_data.get('excel_row', 0),
                    'actual_value': f"Mismatch: {mismatch_summary[:200]}",
                    'expected_value': "Rows should match exactly",
                    'error_type': 'row_data_mismatch',
                    'error_description': f"Row {mismatch_data.get('excel_row', '?')}: Data mismatch",
                    'severity': 'high'
                })
            except:
                pass
            
            return None
    
    def get_errors_by_session(self, session_id, limit=100):
        """Get errors for a specific session"""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                logger.error("Cannot connect to database")
                return []
            
            cursor = conn.cursor(dictionary=True, buffered=True)
            
            query = """
            SELECT error_id, session_id, check_type, column_name, row_index, excel_row, 
                   actual_value, expected_value, error_type, error_description,
                   check_timestamp, source_file, target_file, severity
            FROM dq_error_logs 
            WHERE session_id = %s
            ORDER BY excel_row, column_name
            LIMIT %s
            """
            
            cursor.execute(query, (session_id, limit))
            results = cursor.fetchall()
            
            logger.info(f"Retrieved {len(results)} errors for session {session_id}")
            return results
            
        except Exception as e:
            logger.error(f"Error getting errors: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def get_error_summary_by_session(self, session_id):
        """Get error summary for dashboard"""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                return {
                    'session_id': session_id, 
                    'total_errors': 0, 
                    'errors_by_type': [], 
                    'errors_by_column': [],
                    'status': 'error',
                    'message': 'Database connection failed'
                }
            
            cursor = conn.cursor(dictionary=True, buffered=True)
            
            # Get total errors
            query_total = "SELECT COUNT(*) as total FROM dq_error_logs WHERE session_id = %s"
            cursor.execute(query_total, (session_id,))
            total_result = cursor.fetchone()
            total_errors = total_result['total'] if total_result else 0
            
            # Get error count by type
            query_type = """
            SELECT error_type, COUNT(*) as error_count
            FROM dq_error_logs 
            WHERE session_id = %s
            GROUP BY error_type
            ORDER BY error_count DESC
            LIMIT 10
            """
            cursor.execute(query_type, (session_id,))
            by_type = cursor.fetchall()
            
            # Get error count by column
            query_column = """
            SELECT column_name, COUNT(*) as error_count
            FROM dq_error_logs 
            WHERE session_id = %s AND column_name != '' AND column_name != 'ALL' 
            GROUP BY column_name
            ORDER BY error_count DESC
            LIMIT 10
            """
            cursor.execute(query_column, (session_id,))
            by_column = cursor.fetchall()
            
            # Get severity breakdown
            query_severity = """
            SELECT severity, COUNT(*) as error_count
            FROM dq_error_logs 
            WHERE session_id = %s
            GROUP BY severity
            ORDER BY severity
            """
            cursor.execute(query_severity, (session_id,))
            by_severity = cursor.fetchall()
            
            logger.info(f"Retrieved error summary for session {session_id}: {total_errors} total errors")
            
            return {
                'session_id': session_id,
                'total_errors': total_errors,
                'errors_by_type': by_type,
                'errors_by_column': by_column,
                'errors_by_severity': by_severity,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error getting error summary: {e}")
            return {
                'session_id': session_id, 
                'total_errors': 0, 
                'errors_by_type': [], 
                'errors_by_column': [],
                'status': 'error',
                'message': str(e)
            }
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def test_connection(self):
        """Test database connection"""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                return False
            
            cursor = conn.cursor(buffered=True)
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            
            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'dq_error_logs'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                logger.info("Database connection successful")
                return True
            else:
                logger.warning("Table 'dq_error_logs' doesn't exist")
                return False
                
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def clear_session_errors(self, session_id):
        """Clear errors for a specific session"""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                return 0
            
            cursor = conn.cursor()
            query = "DELETE FROM dq_error_logs WHERE session_id = %s"
            cursor.execute(query, (session_id,))
            conn.commit()
            
            deleted_count = cursor.rowcount
            logger.info(f"Cleared {deleted_count} errors for session {session_id}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error clearing session errors: {e}")
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
        
    def get_error_logs_for_session(self, session_id, limit=200):
        """Get errors for a specific session"""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                logger.warning(f"Could not connect to database for session {session_id}")
                return []
            
            logger.debug(f"Looking for session_id='{session_id}'")
            
            cursor = conn.cursor(dictionary=True, buffered=True)
            
            # FIX: Use proper integer for limit, not slice object
            if isinstance(limit, slice):
                logger.warning(f"WARNING: limit parameter is a slice object: {limit}")
                # Extract the stop value from slice(None, 200, None)
                if limit.stop is not None:
                    fetch_limit = limit.stop
                else:
                    fetch_limit = 200
            else:
                fetch_limit = int(limit) if limit > 0 else 200
            
            logger.debug(f"Using fetch limit: {fetch_limit}")
            
            # Get ALL columns and ensure we fetch ALL records
            query = """
            SELECT 
                error_id, session_id, check_type, 
                source_name, target_name, column_name, 
                row_index, excel_row, 
                actual_value, expected_value,
                source_actual_value, target_actual_value,
                error_type, error_description,
                check_timestamp, source_file, target_file, 
                severity, comparison_context, difference_summary
            FROM dq_error_logs 
            WHERE session_id = %s
            ORDER BY excel_row ASC, column_name ASC, error_id ASC
            LIMIT %s
            """
            
            cursor.execute(query, (session_id, fetch_limit))
            results = cursor.fetchall()
            
            logger.debug(f"Retrieved {len(results)} rows from query")
            
            # Convert datetime objects to strings for JSON serialization
            for result in results:
                if result.get('check_timestamp') and isinstance(result['check_timestamp'], datetime):
                    result['check_timestamp'] = result['check_timestamp'].isoformat()
            
            logger.info(f"Retrieved {len(results)} error logs for session {session_id}")
            return results
            
        except mysql.connector.Error as e:
            logger.error(f"MySQL Error getting error logs: {e}")
            return []
        except Exception as e:
            logger.error(f"General Error getting error logs: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    def get_error_summary_for_session(self, session_id):
        """Get error summary for a session"""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                logger.warning(f"Could not connect to database for session summary {session_id}")
                return {'total_errors': 0, 'errors_by_type': [], 'errors_by_column': []}
            
            cursor = conn.cursor(dictionary=True, buffered=True)
            
            # Get total errors
            query_total = "SELECT COUNT(*) as total FROM dq_error_logs WHERE session_id = %s"
            cursor.execute(query_total, (session_id,))
            total_result = cursor.fetchone()
            total_errors = total_result['total'] if total_result else 0
            
            # Get error count by type
            query_type = """
            SELECT error_type, COUNT(*) as error_count
            FROM dq_error_logs 
            WHERE session_id = %s
            GROUP BY error_type
            ORDER BY error_count DESC
            LIMIT 10
            """
            cursor.execute(query_type, (session_id,))
            by_type = cursor.fetchall()
            
            # Get error count by column
            query_column = """
            SELECT column_name, COUNT(*) as error_count
            FROM dq_error_logs 
            WHERE session_id = %s AND column_name != '' AND column_name IS NOT NULL
            GROUP BY column_name
            ORDER BY error_count DESC
            LIMIT 10
            """
            cursor.execute(query_column, (session_id,))
            by_column = cursor.fetchall()
            
            logger.info(f"Retrieved summary: {total_errors} total errors for session {session_id}")
            
            return {
                'total_errors': total_errors,
                'errors_by_type': by_type,
                'errors_by_column': by_column
            }
            
        except Exception as e:
            logger.error(f"Error getting error summary: {e}")
            return {'total_errors': 0, 'errors_by_type': [], 'errors_by_column': []}
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass