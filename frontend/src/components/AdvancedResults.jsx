import React, { useState, useEffect } from 'react';
import './AdvancedResults.css';

const AdvancedResults = ({ analysisResult, onReset }) => {
  const [activeTab, setActiveTab] = useState('all');
  
// 1. Keep your current initialization
const [paginatedErrors, setPaginatedErrors] = useState(analysisResult?.data?.logs?.error_logs || []);
const [currentPage, setCurrentPage] = useState(1);
const [isLoading, setIsLoading] = useState(false);

// 2. Initialize with the static data from the report
const [paginationInfo, setPaginationInfo] = useState({
  total_pages: analysisResult?.data?.pagination_info?.error_logs_pages || 1,
  total_items: analysisResult?.data?.pagination_info?.error_logs_total || 0
});

const session_id = analysisResult?.session_id;

useEffect(() => {
  const fetchPaginatedData = async () => {
    if (!session_id) return;

    // FIX: Remove the 'if (currentPage === 1 && ...)' block entirely.
    // Instead, ALWAYS fetch to sync the real database count, 
    // but only show the loading spinner if we don't have data yet.
    if (currentPage > 1) setIsLoading(true);

    try {
      const response = await fetch(
        `/api/advanced/${session_id}?data_type=errors&page=${currentPage}&page_size=50`
      );
      const result = await response.json();
      
      if (result.status === 200) {
        setPaginatedErrors(result.data.data);
        // This update fixes the "2 to 88" jump by getting real DB counts immediately
        setPaginationInfo(result.data.pagination);
      }
    } catch (error) {
      console.error("Error fetching paginated data:", error);
    } finally {
      setIsLoading(false);
    }
  };

  fetchPaginatedData();
}, [session_id, currentPage]);

  if (!analysisResult || !analysisResult.data) {
    return <div className="adv-error-msg">No analysis data available.</div>;
  }

  const d = analysisResult.data;
  const metrics = d.validation_summary || {};
  const columnDataTypes = d.column_data_types || {};
  const audit = d.logs?.audit_logs?.[0] || {};
  const recommendations = d.recommendations || [];
  
  // Restore Issue Count Logic from check_breakdown
  const breakdown = d.check_breakdown || {};
  const whitespaceFailures = breakdown.whitespace?.total_errors || 0;
  const formatFailures = breakdown.format?.total_errors || 0;
  const numericFailures = breakdown.numeric?.total_errors || 0;
  // const datatypeFailures = breakdown.data_type?.total_errors || 0;
  const zeroPaddingFailures = breakdown.zero_padding?.total_errors || 0;

  const getFilteredLogs = () => {
    if (activeTab === 'all') return paginatedErrors;
    const normalizedTab = activeTab.toLowerCase().replace(/\s+/g, '_');
    return paginatedErrors.filter(log => 
      log.error_type.toLowerCase().includes(normalizedTab)
    );
  };

  const handleTabChange = (tab) => {
    setActiveTab(tab);
    setCurrentPage(1);
  };

  return (
    <div className="adv-results-container">
      <div className="adv-header-section">
        <div className="adv-title-group">
          <h3 className="adv-header">Data Quality Report</h3>
          <p className="adv-subtitle">Source: <strong>{audit.source_name || "Advanced Source"}</strong></p>
        </div>
        <div className="adv-score-hexagon">
          <span className="adv-score-value">{(parseFloat(metrics.quality_score) || 0).toFixed(1)}%</span>
          <span className="adv-score-label">DQ Score</span>
        </div>
      </div>

      <div className="adv-summary-grid">
        <div className="adv-metric-card adv-total">
          <span className="adv-metric-label">Total Records</span>
          <span className="adv-metric-value">{metrics.total_records || 0}</span>
        </div>
        <div className="adv-metric-card adv-passed">
          <span className="adv-metric-label">Total Passed</span>
          <span className="adv-metric-value">{metrics.good_records || 0}</span>
        </div>
        <div className="adv-metric-card adv-failed">
          <span className="adv-metric-label">Total Failed</span>
          <span className="adv-metric-value">{metrics.bad_records || 0}</span>
        </div>
      </div>

      <div className="adv-datatypes-section">
        <h4 className="adv-section-title">Data Types</h4>
        <div className="adv-datatypes-grid">
          {Object.entries(columnDataTypes).map(([colName, type]) => (
            <div key={colName} className="adv-datatype-pill">
              <span className="adv-col-key">{colName}</span>
              <span className={`adv-type-val type-${type}`}>{type}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="adv-issue-strip">
        <div className="adv-mini-stat">
          <strong>{whitespaceFailures}</strong> Whitespace Issue(s)
        </div>
        <div className="adv-mini-stat">
          <strong>{formatFailures}</strong> Format Issue(s)
        </div>
        <div className="adv-mini-stat">
          <strong>{numericFailures}</strong> Numeric Issue(s)
        </div>
        <div className="adv-mini-stat">
          <strong>{zeroPaddingFailures}</strong> Zero Padding Issue(s)
        </div>
        {/* <div className="adv-mini-stat">
          <strong>{datatypeFailures}</strong> Data Type Issue(s)
        </div> */}
      </div>

      <div className="adv-details-box">
        <div className="adv-tabs">
          {['all', 'whitespace', 'format', 'numeric', 'zero_padding'].map(tab => (
            <button 
              key={tab}
              className={`adv-tab-btn ${activeTab === tab ? 'active' : ''}`} 
              onClick={() => handleTabChange(tab)}
            >
              {tab.toUpperCase().replace('_', ' ')}
            </button>
          ))}
        </div>

        <div className="adv-table-wrapper" style={{ opacity: isLoading ? 0.6 : 1 }}>
          <table className="adv-table">
            <thead>
              <tr>
                <th>Row</th>
                <th>Column</th>
                <th>Description</th>
                <th>Actual Value</th>
              </tr>
            </thead>
            <tbody>
              {getFilteredLogs().map((err, i) => (
                <tr key={i}>
                  <td>{err.excel_row}</td>
                  <td className="adv-col-name">{err.column_name}</td>
                  <td>{err.error_description}</td>
                  <td><code className="adv-code">{String(err.actual_value)}</code></td>
                </tr>
              ))}
            </tbody>
          </table>
          {isLoading && <div className="adv-loading-overlay">Loading Page...</div>}
          {!isLoading && getFilteredLogs().length === 0 && <p className="adv-empty">No records found.</p>}
        </div>

        <div className="adv-pagination-footer">
          <button 
            className="adv-page-btn"
            disabled={currentPage === 1 || isLoading}
            onClick={() => setCurrentPage(prev => prev - 1)}
          >
            Previous
          </button>
          <span className="adv-page-info">
            Page <strong>{currentPage}</strong> of {paginationInfo.total_pages}
          </span>
          <button 
            className="adv-page-btn"
            disabled={currentPage === paginationInfo.total_pages || isLoading}
            onClick={() => setCurrentPage(prev => prev + 1)}
          >
            Next
          </button>
        </div>
      </div>

      {recommendations.length > 0 && (
        <div className="adv-recommendations-box">
          <h4 className="adv-rec-title">💡 Recommended Actions</h4>
          <ul className="adv-rec-list">
            {recommendations.map((rec, index) => (
              <li key={index} className="adv-rec-item">{rec}</li>
            ))}
          </ul>
        </div>
      )}

      <div className="adv-actions">
        <button className="adv-reset-btn" onClick={onReset}>Start New Advanced Session</button>
      </div>
    </div>
  );
};

export default AdvancedResults;