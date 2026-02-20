import React, { useState, useEffect } from 'react';
import './ComparisonResults.css';

const MetricCard = ({ label, value, status }) => (
  <div className="metric-card">
    <label>{label}</label>
    {/* <div className={`value ${status}`}>{typeof value === 'number' ? value.toLocaleString() : value}</div> */}
    <div className={`value ${status}`}>
      {typeof value === 'number' || !isNaN(parseFloat(value)) ? parseFloat(value).toLocaleString() : value}
    </div>
  </div>
);

const ComparisonResults = ({ analysisResult, onReset }) => {
  const d = analysisResult?.data || {};
  const sessionId = analysisResult?.session_id || d.session_id;

  // Pagination State
  const [mismatches, setMismatches] = useState([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [isLoading, setIsLoading] = useState(false);
  const [totalCount, setTotalCount] = useState(0);

  // Existing Metrics Logic
  // const score = d.overall_score || d.comparison_score || 0;
  const score = parseFloat(d.overall_score || d.comparison_score || 0);
  const commonRows = d.common_rows || 0;
  const sourceStats = d.source_stats || {};
  const targetStats = d.target_stats || {};
  const metadata = d.table_metadata_comparison || {};
  
  const sourceTypes = metadata.source_metadata?.data_types || {};
  const targetTypes = metadata.target_metadata?.data_types || {};
  const allColumns = Array.from(new Set([...Object.keys(sourceTypes), ...Object.keys(targetTypes)]));

  const missingInTargetCount = d.summary?.rows_only_in_source || d.unique_to_source || 0;
  const missingInSourceCount = d.summary?.rows_only_in_target || d.unique_to_target || 0;

  // Fetch Mismatches from the paginated API endpoint
  const fetchMismatches = async (page) => {
    if (!sessionId) return;
    setIsLoading(true);
    try {
      const response = await fetch(`/api/compare/${sessionId}/mismatches?page=${page}&page_size=50`);
      const result = await response.json();
      if (result.status === 200) {
        setMismatches(result.data.mismatches);
        setTotalPages(result.data.pagination.total_pages);
        setCurrentPage(result.data.pagination.page);
        setTotalCount(result.data.pagination.total_rows);
      }
    } catch (error) {
      console.error("Failed to fetch mismatches:", error);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchMismatches(1);
  }, [sessionId]);

  const formatJsonValue = (jsonStr) => {
    if (!jsonStr || jsonStr === "{}") return <span className="text-muted">No data available</span>;
    try {
      const obj = typeof jsonStr === 'string' ? JSON.parse(jsonStr) : jsonStr;
      return <pre className="json-display">{JSON.stringify(obj, null, 2)}</pre>;
    } catch (e) {
      return jsonStr;
    }
  };

  const formatDifference = (diffStr) => {
    try {
      // Handles both your original key (difference_summary) and paginated key (summary/difference)
      const diffs = typeof diffStr === 'string' ? JSON.parse(diffStr) : diffStr;
      if (!Array.isArray(diffs)) return <div className="diff-item">{diffStr}</div>;
      return diffs.map((df, idx) => (
        <div key={idx} className="diff-item">
          <strong>{df.column}:</strong> {df.source} ≠ {df.target}
        </div>
      ));
    } catch (e) {
      return <div className="diff-item">{diffStr}</div>;
    }
  };

  return (
    <div className="dashboard-container">
      <header className="report-header">
        <div className="header-left">
          <h2>Data Quality Report</h2>
        </div>
      </header>

      <section className="dashboard-section overview-grid">
        <div className="score-main">
          <h1>{score}%</h1>
          <p className="comparison-header">DQ Score</p>
        </div>
      </section>

      <section className="dashboard-section source-target-bar">
        <div className="source-box">
          <span className="chip">Source</span>
          <p className="info-text">{d.source_info || 'Source System'}</p>
          <small>{sourceStats.rows?.toLocaleString() || 0} Rows | {sourceStats.columns || 0} Cols</small>
        </div>
        <div className="vs-divider">VS</div>
        <div className="source-box">
          <span className="chip target">Target</span>
          <p className="info-text">{d.target_info || 'Target System'}</p>
          <small>{targetStats.rows?.toLocaleString() || 0} Rows | {targetStats.columns || 0} Cols</small>
        </div>
      </section>

      <div className="metrics-grid">
        <MetricCard label="Perfect Matches" value={commonRows} status="good" />
        <MetricCard label="Records Missing in Target" value={missingInTargetCount} status={missingInTargetCount > 0 ? 'bad' : 'good'} />
        <MetricCard label="Records Missing in Source" value={missingInSourceCount} status={missingInSourceCount > 0 ? 'warning' : 'good'} />
      </div>

      <div className="main-content-split">
        <div className="left-panel">
          <div className="content-card">
            <h3>Structure & Data Types</h3>
            <div className={`status-banner ${d.column_structure_match ? 'success' : 'warning'}`}>
              {d.column_structure_match ? '✓ Column Structure Matches' : '⚠ Structure Mismatch Found'}
            </div>
            <table className="mini-table">
              <thead>
                <tr>
                  <th>Source Column</th>
                  <th>Source Type</th>
                  <th>Target Column</th>
                  <th>Target Type</th>
                </tr>
              </thead>
              <tbody>
                {allColumns.length > 0 ? (
                  allColumns.map((col, i) => (
                    <tr key={i}>
                      <td>{sourceTypes[col] ? col : <span className="text-muted">-</span>}</td>
                      <td>{sourceTypes[col] || <span className="text-muted">missing</span>}</td>
                      <td>{targetTypes[col] ? col : <span className="text-muted">-</span>}</td>
                      <td className={sourceTypes[col] !== targetTypes[col] ? "text-danger" : ""}>
                        {targetTypes[col] || <span className="text-muted">missing</span>}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr><td colSpan="4" className="empty-state">No metadata available</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="right-panel">
          <div className="content-card">
            <h3>Dataset Alignment</h3>
            <table className="mini-table">
              <thead>
                <tr><th>Metric</th><th>Source</th><th>Target</th><th>Diff</th></tr>
              </thead>
              <tbody>
                {metadata.differences?.length > 0 ? metadata.differences.map((diff, i) => (
                  <tr key={i}>
                    <td>{diff.type?.replace(/_/g, ' ').toUpperCase()}</td>
                    <td>{diff.source_value?.toLocaleString()}</td>
                    <td>{diff.target_value?.toLocaleString()}</td>
                    <td className={diff.difference !== 0 ? 'text-danger fw-bold' : ''}>{diff.difference?.toLocaleString()}</td>
                  </tr>
                )) : (
                  <tr><td colSpan="4" className="empty-state">Metadata consistent</td></tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="content-card">
            <h3>Primary Key Integrity</h3>
            <div className={`status-banner ${d.primary_key_validation?.validation_passed ? 'success' : 'warning'}`}>
              {d.primary_key_validation?.validation_passed ? '✓ PK Validation Passed' : '⚠ Key Issue Found'}
            </div>
            <p className="small-text">
              <strong>Key Fields:</strong> {d.primary_key_validation?.key_fields_found?.length > 0 ? d.primary_key_validation.key_fields_found.map(k => k.source_column || k).join(', ') : (d.primary_key_validation?.status_message || 'None')}
            </p>
          </div>
        </div>
      </div>

      <section className="dashboard-section">
        <div className="section-header-flex">
          <div className="title-group">
            <h3>Row-Level Value Mismatches ({totalCount})</h3>
            <p className="section-desc">Comparison of specific row data where differences were detected.</p>
          </div>
          <div className="pagination-controls">
            <button disabled={currentPage === 1 || isLoading} onClick={() => fetchMismatches(currentPage - 1)}>Prev</button>
            <span className="page-indicator"> Page {currentPage} of {totalPages} </span>
            <button disabled={currentPage === totalPages || isLoading} onClick={() => fetchMismatches(currentPage + 1)}>Next</button>
          </div>
        </div>
        
        <div className="table-wrapper">
          <table className="failures-table comparison-viewer">
            <thead>
              <tr>
                <th style={{ width: '80px' }}>Row</th>
                <th style={{ width: '220px' }}>Mismatch Summary</th>
                <th>Source Actual Value</th>
                <th>Target Actual Value</th>
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                <tr><td colSpan="4" className="empty-state">Loading page {currentPage}...</td></tr>
              ) : mismatches.length > 0 ? (
                mismatches.map((m, i) => (
                  <tr key={i}>
                    <td className="text-center fw-bold">{m.row || m.excel_row}</td>
                    <td>
                      <div className="diff-summary-box">
                        {formatDifference(m.difference || m.summary || m.difference_summary)}
                      </div>
                    </td>
                    <td className="data-cell source-cell">
                      <div className="cell-content">
                        {formatJsonValue(m.source_data || m.source_actual_value)}
                      </div>
                    </td>
                    <td className="data-cell target-cell">
                      <div className="cell-content">
                        {formatJsonValue(m.target_data || m.target_actual_value)}
                      </div>
                    </td>
                  </tr>
                ))
              ) : (
                <tr><td colSpan="4" className="empty-state">No data mismatches found.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <div className="footer-actions">
        <button className="reset-btn" onClick={onReset}>Perform New Comparison</button>
      </div>
    </div>
  );
};

export default ComparisonResults;