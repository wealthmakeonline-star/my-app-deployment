import React, { useState, useEffect } from 'react';
import './AnalysisResults.css';

const AnalysisResults = ({ analysisResult, onReset }) => {
  // 1. Basic State
  const [activeTab, setActiveTab] = useState('nulls');
  
  // 2. Pagination State
  const [tableData, setTableData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [pagination, setPagination] = useState({
    page: 1,
    pageSize: 50,
    totalPages: 1,
    totalItems: 0
  });

  if (!analysisResult || !analysisResult.data) {
    return <div className="error-msg">No analysis data available.</div>;
  }

  const d = analysisResult.data;
  const sessionId = d.session_id || analysisResult.session_id;
  const totalRows = d.total_rows || 0;
  const qualityScore = d.quality_metrics?.quality_score || 0;
  const sourceInfo = d.source_info || "Unknown Source";
  const columnDataTypes = d.column_data_types || {};

  // Summary counts
  const nullCount = d.results?.nulls?.total_null_cells || 0;
  const duplicateCount = d.results?.duplicates?.total_duplicates || 0;
  const formatCount = d.results?.formats?.total_format_issues || 0;

  // --- Calculate Passed/Failed Rows ---
  const failedRowIndices = new Set();
  
  // 1. Collect Null indices
  if (d.results?.nulls?.issue_rows) {
    Object.values(d.results.nulls.issue_rows).forEach(colIssues => {
      colIssues.forEach(issue => failedRowIndices.add(issue.row_index));
    });
  }
  // 2. Collect Format indices
  if (d.results?.formats?.format_issue_details) {
    d.results.formats.format_issue_details.forEach(issue => 
      failedRowIndices.add(issue.row_index)
    );
  }
  // 3. Collect Duplicate indices
  if (d.results?.duplicates?.duplicate_details) {
    d.results.duplicates.duplicate_details.forEach(issue => 
      failedRowIndices.add(issue.row_index)
    );
  }

  const failedRowsCount = failedRowIndices.size;
  const passedRowsCount = Math.max(0, totalRows - failedRowsCount);

  // 3. Mapping Tabs
  const tabToErrorType = {
    'nulls': 'null',
    'duplicates': 'duplicate',
    'format': 'format'
  };

  // 4. Fetch Data Effect
  useEffect(() => {
    if (!sessionId) return;

    const fetchData = async () => {
      setLoading(true);
      try {
        const errorType = tabToErrorType[activeTab];
        const response = await fetch(
          `/api/single/${sessionId}/errors?page=${pagination.page}&page_size=${pagination.pageSize}&error_type=${errorType}`
        );
        
        if (response.ok) {
          const result = await response.json();
          const apiData = result.data;
          
          setTableData(apiData.errors || []);
          
          if (apiData.pagination) {
            setPagination(prev => ({
              ...prev,
              totalPages: apiData.pagination.total_pages,
              totalItems: apiData.pagination.total_errors
            }));
          }
        } else {
          console.error("Failed to fetch data");
          setTableData([]);
        }
      } catch (error) {
        console.error("Error fetching data:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId, activeTab, pagination.page]);

  const handleTabChange = (tab) => {
    setActiveTab(tab);
    setPagination(prev => ({ ...prev, page: 1 }));
  };

  const handlePageChange = (newPage) => {
    if (newPage >= 1 && newPage <= pagination.totalPages) {
      setPagination(prev => ({ ...prev, page: newPage }));
    }
  };

  return (
    <div className="results-container">
      <div className="results-header-section">
        <div>
          <h3 className="results-header">Data Quality Report</h3>
          <p className="results-subtitle">Source: <strong>{sourceInfo}</strong></p>
          <div className="quality-badge">
            DQ Score: {qualityScore.toFixed(1)}%
          </div>
        </div>
      </div>

      <div className="summary-grid">
        <div className="metric-card total">
          <span className="metric-label">Total Records</span>
          <span className="metric-value">{totalRows.toLocaleString()}</span>
        </div>
        <div className="metric-card passed">
          <span className="metric-label">Passed</span>
          <span className="metric-value">{passedRowsCount.toLocaleString()}</span>
        </div>
        <div className="metric-card failed">
           <span className="metric-label">Failed (Rows)</span>
           <span className="metric-value">{failedRowsCount.toLocaleString()}</span>
        </div>
      </div>

      <div className="datatypes-section">
        <h4 className="section-title">Data Types</h4>
        <div className="datatypes-grid">
          {Object.entries(columnDataTypes).map(([colName, type]) => (
            <div key={colName} className="datatype-pill">
              <span className="col-key">{colName}</span>
              <span className={`type-val type-${type}`}>{type}</span>
            </div>
          ))}
        </div>
      </div>

      {/* NEW LOOK: Clean Grid Layout */}
      <div className="issue-breakdown-row">
        <div className="mini-card">
          <span className="mini-label">Null Values</span>
          <span className="mini-value">{nullCount}</span>
        </div>
        <div className="mini-card">
          <span className="mini-label">Duplicates</span>
          <span className="mini-value">{duplicateCount}</span>
        </div>
        <div className="mini-card">
          <span className="mini-label">Format Issues</span>
          <span className="mini-value">{formatCount}</span>
        </div>
      </div>

      <div className="details-section">
        <div className="tabs-header">
          <button 
            className={`tab-btn ${activeTab === 'nulls' ? 'active' : ''}`} 
            onClick={() => handleTabChange('nulls')}
          >
            Null Values
          </button>
          <button 
            className={`tab-btn ${activeTab === 'duplicates' ? 'active' : ''}`} 
            onClick={() => handleTabChange('duplicates')}
          >
            Duplicates
          </button>
          <button 
            className={`tab-btn ${activeTab === 'format' ? 'active' : ''}`} 
            onClick={() => handleTabChange('format')}
          >
            Format Issues
          </button>
        </div>

        <div className="tab-content">
          {loading ? (
            <div className="loading-spinner">Loading data...</div>
          ) : (
            <>
              {activeTab === 'nulls' && (
                <TableDisplay 
                  data={tableData} 
                  columns={[
                    { key: 'excel_row', label: 'Row' }, 
                    { key: 'column', label: 'Column' }, 
                    { key: 'identifier', label: 'Identifier' }
                  ]} 
                  emptyMsg="No null values found."
                />
              )}

              {activeTab === 'duplicates' && (
                <TableDisplay 
                  data={tableData} 
                  columns={[
                    { key: 'excel_row', label: 'Row' }, 
                    { key: 'column', label: 'Column' },
                    { key: 'description', label: 'Description' }
                  ]} 
                  emptyMsg="No duplicates found."
                />
              )}

              {activeTab === 'format' && (
                <TableDisplay 
                  data={tableData} 
                  columns={[
                    { key: 'excel_row', label: 'Row' }, 
                    { key: 'column', label: 'Column' }, 
                    { key: 'description', label: 'Description' }
                  ]} 
                  emptyMsg="No formatting issues found."
                />
              )}
            </>
          )}

          {/* PAGINATION CONTROLS */}
          {tableData.length > 0 && (
            <div className="pagination-controls">
              <button 
                className="page-btn" 
                disabled={pagination.page === 1 || loading}
                onClick={() => handlePageChange(pagination.page - 1)}
              >
                Previous
              </button>
              <span className="page-info">
                Page {pagination.page} of {pagination.totalPages || 1}
              </span>
              <button 
                className="page-btn" 
                disabled={pagination.page >= pagination.totalPages || loading}
                onClick={() => handlePageChange(pagination.page + 1)}
              >
                Next
              </button>
            </div>
          )}
        </div>
      </div>

      <div className="results-actions">
        <button className="reset-btn" onClick={onReset}>New Analysis</button>
      </div>
    </div>
  );
};

const TableDisplay = ({ data, columns, emptyMsg }) => (
  data && data.length > 0 ? (
    <div className="table-wrapper">
      <table className="failures-table">
        <thead>
          <tr>
            {columns.map(col => <th key={col.key}>{col.label.toUpperCase()}</th>)}
          </tr>
        </thead>
        <tbody>
          {data.map((item, idx) => (
            <tr key={idx}>
              {columns.map(col => (
                <td key={col.key} title={String(item[col.key] || '')}>
                  {String(item[col.key] || '')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ) : <p className="empty-msg">{emptyMsg}</p>
);

export default AnalysisResults;