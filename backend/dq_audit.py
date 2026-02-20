# dq_audit.py - PROFESSIONAL VERSION WITH LOGGING
import mysql.connector
import pandas as pd
import os
import traceback
from datetime import datetime
import logging
from db_config import MYSQL_CONFIG
from app_config import APP_SETTINGS, FILE_PATHS

logger = logging.getLogger(__name__)

class DataQualityAudit:
    def __init__(self, db_config=None):
        # Use config from file or override with custom config
        self.db_config = db_config or MYSQL_CONFIG
        self.session_id = f"DQ_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Audit class initialized with database: {self.db_config['database']}")
    
    def log_audit_record(self, audit_data):
        """Store key metrics in audit database"""
        logger.info(f"Starting database insertion for {audit_data.get('check_type', 'unknown')}...")
        logger.debug(f"Audit data keys: {list(audit_data.keys())}")
        
        conn = None
        cursor = None
        try:
            logger.debug(f"Connecting to MySQL at {self.db_config['host']}...")
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(buffered=True)
            logger.debug("MySQL connection successful")
            
            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'dq_audit_logs'")
            table_exists = cursor.fetchone()
            if not table_exists:
                logger.warning("Table 'dq_audit_logs' does not exist! Creating...")
                self._create_audit_table(cursor, conn)
            
            query = """
            INSERT INTO dq_audit_logs (
                session_id, check_type, source_type, source_name, target_type, target_name,
                check_timestamp, duration_seconds, source_row_count, target_row_count,
                common_row_count, match_rate, value_mismatch_count, structure_issues_count,
                total_null_count, duplicate_row_count, mandatory_field_issues, quality_score,
                whitespace_issues, formatting_issues, numeric_issues, total_rules_executed,
                rules_passed, rules_failed, compliance_score, overall_score, assessment_category, issues_summary
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Convert numpy types to Python native types for MySQL compatibility
            values = (
                audit_data.get('session_id', self.session_id),
                audit_data.get('check_type', 'unknown'),
                audit_data.get('source_type', 'unknown'),
                audit_data.get('source_name', 'unknown'),
                audit_data.get('target_type', ''),
                audit_data.get('target_name', ''),
                audit_data.get('check_timestamp', datetime.now()),
                float(audit_data.get('duration_seconds', 0)) if audit_data.get('duration_seconds') is not None else 0.0,
                int(audit_data.get('source_row_count', 0)) if audit_data.get('source_row_count') is not None else 0,
                int(audit_data.get('target_row_count', 0)) if audit_data.get('target_row_count') is not None else 0,
                int(audit_data.get('common_row_count', 0)) if audit_data.get('common_row_count') is not None else 0,
                float(audit_data.get('match_rate', 0.0)) if audit_data.get('match_rate') is not None else 0.0,
                int(audit_data.get('value_mismatch_count', 0)) if audit_data.get('value_mismatch_count') is not None else 0,
                int(audit_data.get('structure_issues_count', 0)) if audit_data.get('structure_issues_count') is not None else 0,
                int(audit_data.get('total_null_count', 0)) if audit_data.get('total_null_count') is not None else 0,
                int(audit_data.get('duplicate_row_count', 0)) if audit_data.get('duplicate_row_count') is not None else 0,
                int(audit_data.get('mandatory_field_issues', 0)) if audit_data.get('mandatory_field_issues') is not None else 0,
                float(audit_data.get('quality_score', 0.0)) if audit_data.get('quality_score') is not None else 0.0,
                int(audit_data.get('whitespace_issues', 0)) if audit_data.get('whitespace_issues') is not None else 0,
                int(audit_data.get('formatting_issues', 0)) if audit_data.get('formatting_issues') is not None else 0,
                int(audit_data.get('numeric_issues', 0)) if audit_data.get('numeric_issues') is not None else 0,
                int(audit_data.get('total_rules_executed', 0)) if audit_data.get('total_rules_executed') is not None else 0,
                int(audit_data.get('rules_passed', 0)) if audit_data.get('rules_passed') is not None else 0,
                int(audit_data.get('rules_failed', 0)) if audit_data.get('rules_failed') is not None else 0,
                float(audit_data.get('compliance_score', 0.0)) if audit_data.get('compliance_score') is not None else 0.0,
                float(audit_data.get('overall_score', 0.0)) if audit_data.get('overall_score') is not None else 0.0,
                audit_data.get('assessment_category', 'UNKNOWN'),
                audit_data.get('issues_summary', '')
            )
            
            logger.debug(f"Executing SQL insert for {audit_data.get('check_type', 'unknown')}...")
            cursor.execute(query, values)
            conn.commit()
            
            audit_id = cursor.lastrowid
            logger.debug(f"Database insert successful - ID: {audit_id}, {cursor.rowcount} row(s) affected")
            logger.info(f"Inserted {audit_data.get('check_type', 'unknown')} record with ID: {audit_id}")
            
            return audit_id
            
        except mysql.connector.Error as e:
            logger.error(f"MySQL Error storing audit record: {str(e)}")
            if APP_SETTINGS.get('fallback_logging', True):
                self._fallback_log_to_file(audit_data)
            return None
        except Exception as e:
            logger.error(f"Failed to store audit record: {str(e)}")
            if APP_SETTINGS.get('fallback_logging', True):
                self._fallback_log_to_file(audit_data)
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
    
    def _create_audit_table(self, cursor, conn):
        """Create the audit table if it doesn't exist"""
        try:
            logger.info("Creating dq_audit_logs table...")
            create_table_query = """
            CREATE TABLE IF NOT EXISTS dq_audit_logs (
                audit_id INT AUTO_INCREMENT PRIMARY KEY,
                session_id VARCHAR(50) NOT NULL,
                check_type VARCHAR(20) NOT NULL,
                source_type VARCHAR(10) NOT NULL,
                source_name VARCHAR(255),
                target_type VARCHAR(10),
                target_name VARCHAR(255),
                check_timestamp DATETIME NOT NULL,
                duration_seconds DECIMAL(8,2),
                source_row_count INT,
                target_row_count INT,
                common_row_count INT,
                match_rate DECIMAL(5,2),
                value_mismatch_count INT,
                structure_issues_count INT,
                total_null_count INT,
                duplicate_row_count INT,
                mandatory_field_issues INT,
                quality_score DECIMAL(5,2),
                whitespace_issues INT,
                formatting_issues INT,
                numeric_issues INT,
                total_rules_executed INT,
                rules_passed INT,
                rules_failed INT,
                compliance_score DECIMAL(5,2),
                overall_score DECIMAL(5,2),
                assessment_category VARCHAR(20),
                issues_summary TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            logger.info("dq_audit_logs table created successfully")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def _fallback_log_to_file(self, audit_data):
        """Fallback to CSV file if database is unavailable"""
        try:
            log_file = FILE_PATHS.get('fallback_audit_file', 'dq_audit_fallback.csv')
            
            # Create directory if it doesn't exist
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            # Add timestamp for fallback
            audit_data['fallback_timestamp'] = datetime.now().isoformat()
            audit_data['logged_via'] = 'fallback'
            
            df = pd.DataFrame([audit_data])
            
            # Write header only if file doesn't exist
            if not os.path.exists(log_file):
                df.to_csv(log_file, index=False)
                logger.info(f"Created fallback file: {log_file}")
            else:
                df.to_csv(log_file, mode='a', header=False, index=False)
                logger.info(f"Appended to fallback file: {log_file}")
                
            logger.info(f"Audit record saved to fallback file: {log_file}")
            return True
        except Exception as e:
            logger.error(f"Fallback logging also failed: {str(e)}")
            return False

    def _get_connection(self):
        """Get database connection for KPI history"""
        try:
            return mysql.connector.connect(**self.db_config)
        except Exception as e:
            logger.warning(f"Could not establish database connection: {e}")
            return None
    
    def get_audit_logs_for_session(self, session_id):
        """Get audit logs for a specific session"""
        logger.debug(f"Entering get_audit_logs_for_session() for session_id: '{session_id}'")
        
        conn = None
        cursor = None
        try:
            logger.debug("Connecting to database...")
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True, buffered=True)
            
            # Check if ANY records exist
            cursor.execute("SELECT COUNT(*) as total FROM dq_audit_logs")
            total_count = cursor.fetchone()['total']
            logger.debug(f"Total records in table: {total_count}")
            
            # Try exact match
            query_exact = "SELECT * FROM dq_audit_logs WHERE session_id = %s"
            cursor.execute(query_exact, (session_id,))
            results = cursor.fetchall()
            logger.debug(f"Results found: {len(results)}")
            
            if len(results) > 0:
                logger.debug(f"First record details available")
            
            # Convert datetime to string for JSON serialization
            for result in results:
                for key, value in result.items():
                    if isinstance(value, datetime):
                        result[key] = value.isoformat()
                        logger.debug(f"Converted {key} to ISO format")
                    elif isinstance(value, (int, float)):
                        result[key] = float(value) if isinstance(value, float) else int(value)
            
            logger.info(f"Retrieved {len(results)} audit logs for session {session_id}")
            return results
            
        except mysql.connector.Error as e:
            logger.error(f"MySQL Error: {e}")
            return []
        except Exception as e:
            logger.error(f"General Error: {e}")
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
            cursor.execute("SHOW TABLES LIKE 'dq_audit_logs'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                logger.info("Database connection successful and audit table exists")
                return True
            else:
                logger.warning("Table 'dq_audit_logs' doesn't exist")
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

    def get_recent_sessions(self, limit=10):
        """Get recent audit sessions"""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                return []
            
            cursor = conn.cursor(dictionary=True, buffered=True)
            
            query = """
            SELECT session_id, check_type, source_type, source_name, target_type, target_name,
                check_timestamp, overall_score, assessment_category
            FROM dq_audit_logs 
            ORDER BY check_timestamp DESC
            LIMIT %s
            """
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            
            # Convert datetime to string
            for result in results:
                if result['check_timestamp']:
                    result['check_timestamp'] = result['check_timestamp'].isoformat()
            
            logger.info(f"Retrieved {len(results)} recent sessions")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching recent sessions: {e}")
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
    
    def get_session_summary(self, session_id):
        """Get summary for a specific session"""
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                return None
            
            cursor = conn.cursor(dictionary=True, buffered=True)
            
            query = """
            SELECT session_id, check_type, source_type, source_name, target_type, target_name,
                check_timestamp, overall_score, assessment_category, source_row_count, target_row_count,
                common_row_count, match_rate, value_mismatch_count, issues_summary
            FROM dq_audit_logs 
            WHERE session_id = %s
            ORDER BY check_timestamp DESC
            LIMIT 1
            """
            cursor.execute(query, (session_id,))
            result = cursor.fetchone()
            
            if result:
                # Convert datetime to string
                if result['check_timestamp']:
                    result['check_timestamp'] = result['check_timestamp'].isoformat()
            
            logger.info(f"Retrieved session summary for {session_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching session summary: {e}")
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