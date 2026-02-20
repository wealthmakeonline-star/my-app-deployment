
-- Run this SQL file in your MySQL database before starting the application

--create database
create database dq_checks;

--use database
use dq_checks

-- Create error logs table
CREATE TABLE IF NOT EXISTS dq_error_logs (
    error_id INT AUTO_INCREMENT PRIMARY KEY,
    session_id VARCHAR(50) NOT NULL,
    check_type VARCHAR(20) NOT NULL,
    source_name VARCHAR(255),
    target_name VARCHAR(255),
    column_name VARCHAR(100),
    row_index INT,
    excel_row INT,
    actual_value TEXT,
    expected_value TEXT,
    error_type VARCHAR(50),
    error_description TEXT,
    check_timestamp DATETIME NOT NULL,
    source_file VARCHAR(255),
    target_file VARCHAR(255),
    severity VARCHAR(20) DEFAULT 'medium',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_session (session_id),
    INDEX idx_check_type (check_type),
    INDEX idx_error_type (error_type),
    INDEX idx_timestamp (check_timestamp)
);

-- Create audit logs table
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_session (session_id),
    INDEX idx_check_type (check_type),
    INDEX idx_timestamp (check_timestamp)
);

CREATE TABLE kpi_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    session_id VARCHAR(50) NOT NULL,
    kpi_name VARCHAR(100) NOT NULL,
    kpi_value DECIMAL(15,2) NOT NULL,
    threshold_value DECIMAL(15,2),
    check_timestamp DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_session_id (session_id),
    INDEX idx_kpi_name (kpi_name),
    INDEX idx_timestamp (check_timestamp)
);

-- Show tables created
SHOW TABLES;