import React, { useState, useEffect } from 'react';
import './BusinessResults.css';

const API = {
  GET_RULES: '/api/rules?active_only=true',
  CREATE_RULE: '/api/rules',
  EXECUTE_RULES: '/api/rules/execute',
  GET_VIOLATIONS: '/api/rules/violations'
};

const RULE_TYPES = [
  { id: 'mandatory', label: 'Mandatory Field Validation' },
  { id: 'threshold', label: 'Threshold Validation (≤ X)' },
  { id: 'cross_column', label: 'Cross-column Validation (A > B)' },
  { id: 'text_comparison', label: 'Text Comparison' },
  { id: 'pattern', label: 'Pattern Validation' },
  { id: 'range', label: 'Range Validation' },
  { id: 'kpi', label: 'KPI Validation' },
  { id: 'aggregate', label: 'Aggregate Validation' },
  { id: 'custom', label: 'Custom Rule (Python/SQL)' },
];

const BusinessResults = ({ analysisResult, formData, onReset }) => {
  const [step, setStep] = useState('CHOICE');
  const [loading, setLoading] = useState(false);
  const [notification, setNotification] = useState(null);

  // Data State
  const [availableRules, setAvailableRules] = useState([]);
  const [executionResult, setExecutionResult] = useState(null);
  const [isEditing, setIsEditing] = useState(false);

  // Pagination State
  const [pagination, setPagination] = useState({
    page: 1,
    pageSize: 50,
    total: 0,
    totalPages: 1
  });

  const INITIAL_RULE_STATE = {
    rule_name: '',
    description: '',
    rule_type: 'mandatory',
    severity: 'medium',
    source_column: '',
    threshold_value: '',
    operator: '',
    target_column: '',
    pattern_type: '',
    min_value: '',
    max_value: '',
    custom_pattern: '',
    operator_type: '',
    comparison_type: 'exact',  // FIXED: Added with default value 'exact'
    is_case_sensitive: 'n',
    rule_logic: ''
  };

  const [newRule, setNewRule] = useState(INITIAL_RULE_STATE);

  // Selection State
  const [selectedRuleIds, setSelectedRuleIds] = useState([]);

  const filePath = analysisResult?.filepath || '';
  const sourceType = formData?.selectedSource?.toLowerCase() || 'csv';
  const fileName = formData?.selectedFile?.name || formData?.dbConnection?.table || "Unknown Source";
  const columns = analysisResult?.columns || [];

  useEffect(() => {
    fetchRules();
  }, []);

  const fetchRules = async () => {
    try {
      const res = await fetch(API.GET_RULES);
      const data = await res.json();
      setAvailableRules(data.rules || []);
    } catch (e) {
      showNotify('error', "Failed to fetch rules");
    }
  };

  const showNotify = (type, message) => {
    setNotification({ type, message });
    setTimeout(() => setNotification(null), 3000);
  };

  const resetForm = () => {
    setNewRule(INITIAL_RULE_STATE);
    setIsEditing(false);
  };

  // --- PAGINATION HANDLER ---
  const fetchViolations = async (page, sessionIdOverride = null) => {
    const currentSessionId = sessionIdOverride || executionResult?.session_id;
    if (!currentSessionId) return;

    setLoading(true);
    try {
      const res = await fetch(`${API.GET_VIOLATIONS}/${currentSessionId}?page=${page}&page_size=${pagination.pageSize}`);
      const data = await res.json();

      if (res.ok) {
        setExecutionResult(prev => ({
          ...prev,
          violations: data.data || []
        }));

        if (data.pagination) {
          setPagination({
            page: data.pagination.page,
            pageSize: data.pagination.page_size,
            total: data.pagination.total,
            totalPages: data.pagination.total_pages
          });
        }
      } else {
        showNotify('error', "Failed to fetch violations page");
      }
    } catch (err) {
      showNotify('error', "Network error fetching page");
    } finally {
      setLoading(false);
    }
  };

  const handleSaveRule = async (e) => {
    e.preventDefault();
    setLoading(true);
    
    try {
      // FIXED: Match backend expected schema with comparison_type
      const payload = {
        rule_name: newRule.rule_name,
        rule_type: newRule.rule_type,
        source_column: newRule.source_column || null,
        target_column: newRule.target_column || null,
        threshold_value: newRule.threshold_value !== "" ? parseFloat(newRule.threshold_value) : null,
        operator: newRule.operator || null,
        pattern_type: newRule.pattern_type || null,
        min_value: newRule.min_value !== "" ? parseFloat(newRule.min_value) : null,
        max_value: newRule.max_value !== "" ? parseFloat(newRule.max_value) : null,
        custom_pattern: newRule.custom_pattern || null,
        operator_type: newRule.operator_type || null,
        comparison_type: newRule.comparison_type || 'exact', // FIXED: Critical for text_comparison
        description: newRule.description || null,
        severity: newRule.severity,
        is_active: 1,
        created_by: 'web_user',
        case_sensitive: newRule.is_case_sensitive === 'y' ? 1 : 0,
        rule_logic: newRule.rule_logic || null
      };

      // Remove undefined values
      Object.keys(payload).forEach(key => {
        if (payload[key] === undefined) {
          delete payload[key];
        }
      });

      console.log('Saving rule with payload:', payload);

      const url = isEditing ? `${API.CREATE_RULE}/${newRule.rule_id}` : API.CREATE_RULE;
      const method = isEditing ? 'PUT' : 'POST';

      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      const data = await res.json();

      if (res.ok) {
        showNotify('success', `Rule ${isEditing ? 'updated' : 'created'} successfully!`);
        fetchRules();
        setStep('CHOICE');
        resetForm();
      } else {
        showNotify('error', data.message || "Failed to save rule");
        console.error('Server error:', data);
      }
    } catch (err) {
      console.error('Save rule error:', err);
      showNotify('error', "A network error occurred. Check backend logs.");
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteRule = async (e, id) => {
    e.stopPropagation();
    if (!window.confirm("Are you sure you want to deactivate this rule?")) return;
    try {
      const res = await fetch(`${API.CREATE_RULE}/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ is_active: 0 })
      });
      if (res.ok) {
        setAvailableRules(prev => prev.filter(r => r.rule_id !== id));
        showNotify('success', "Rule deactivated");
      }
    } catch (err) {
      showNotify('error', "Deactivation failed");
    }
  };

  const handleExecuteRules = async () => {
    setLoading(true);
    try {
      const payload = {
        rule_ids: selectedRuleIds,
        source_type: sourceType,
        file_path: filePath,
        mandatory_fields: columns,
        source: {
          source_type: sourceType,
          file_path: filePath
        }
      };

      if (sourceType === 'database' && formData?.dbConnection) {
        const dbConfig = {
          db_type: formData.dbConnection.dbType?.toLowerCase() || 'mysql',
          host: formData.dbConnection.host,
          port: parseInt(formData.dbConnection.port) || 3306,
          database: formData.dbConnection.database,
          table: formData.dbConnection.table,
          user: formData.dbConnection.user,
          password: formData.dbConnection.password
        };
        
        payload.db_config = dbConfig;
        payload.source.db_config = dbConfig;
        
        // Also include flattened for backward compatibility
        Object.assign(payload, {
          db_type: dbConfig.db_type,
          host: dbConfig.host,
          port: dbConfig.port,
          database: dbConfig.database,
          table: dbConfig.table,
          user: dbConfig.user,
          password: dbConfig.password
        });
      }

      console.log('Executing rules with payload:', payload);

      const res = await fetch(API.EXECUTE_RULES, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      const data = await res.json();
      
      if (!res.ok) {
        throw new Error(data.message || 'Execution failed');
      }

      const finalResult = data.data || data;

      setExecutionResult({ 
        ...finalResult, 
        session_id: data.session_id || finalResult.session_id, 
        violations: []
      });

      setStep('REPORT');

      if (data.session_id || finalResult.session_id) {
        await fetchViolations(1, data.session_id || finalResult.session_id);
      }

    } catch (err) {
      console.error('Execution error:', err);
      showNotify('error', err.message || 'Execution Failed');
    } finally {
      setLoading(false);
    }
  };

  const renderDynamicFields = () => {
    switch (newRule.rule_type) {
      case 'mandatory':
        return (
          <div className="br-dynamic-section animate-fade-in">
            <div className="br-input-group">
              <label>Column Name</label>
              <input type="text" required placeholder="e.g. email_address"
                value={newRule.source_column} onChange={e => setNewRule({ ...newRule, source_column: e.target.value })} />
            </div>
          </div>
        );

      case 'threshold':
        return (
          <div className="br-dynamic-section animate-fade-in">
            <div className="br-input-group">
              <label>Column Name</label>
              <input type="text" required placeholder="e.g. transaction_amount"
                value={newRule.source_column} onChange={e => setNewRule({ ...newRule, source_column: e.target.value })} />
            </div>
            <div className="br-row-split">
              <div className="br-input-group">
                <label>Operator</label>
                <select required value={newRule.operator} onChange={e => setNewRule({ ...newRule, operator: e.target.value })}>
                  <option value="">Select</option>
                  <option value="<">{'<'}</option><option value=">">{'>'}</option>
                  <option value="<=">{'<='}</option><option value=">=">{'>='}</option>
                  <option value="=">=</option><option value="!=">!=</option>
                </select>
              </div>
              <div className="br-input-group">
                <label>Threshold Value</label>
                <input type="number" step="any" required value={newRule.threshold_value} 
                  onChange={e => setNewRule({ ...newRule, threshold_value: e.target.value })} />
              </div>
            </div>
          </div>
        );

      case 'cross_column':
        return (
          <div className="br-dynamic-section animate-fade-in">
            <div className="br-row-split">
              <div className="br-input-group">
                <label>First Column (A)</label>
                <input type="text" required value={newRule.source_column} 
                  onChange={e => setNewRule({ ...newRule, source_column: e.target.value })} />
              </div>
              <div className="br-input-group">
                <label>Comparison</label>
                <select required value={newRule.operator} onChange={e => setNewRule({ ...newRule, operator: e.target.value })}>
                  <option value="">Select</option>
                  <option value="<">{'<'}</option><option value=">">{'>'}</option>
                  <option value="<=">{'<='}</option><option value=">=">{'>='}</option>
                  <option value="=">=</option><option value="!=">!=</option>
                </select>
              </div>
              <div className="br-input-group">
                <label>Second Column (B)</label>
                <input type="text" required value={newRule.target_column} 
                  onChange={e => setNewRule({ ...newRule, target_column: e.target.value })} />
              </div>
            </div>
          </div>
        );

      case 'text_comparison':
        return (
          <div className="br-dynamic-section animate-fade-in">
            <div className="br-row-split">
              <div className="br-input-group">
                <label>First Column</label>
                <input type="text" required value={newRule.source_column} 
                  onChange={e => setNewRule({ ...newRule, source_column: e.target.value })} />
              </div>
              <div className="br-input-group">
                <label>Second Column</label>
                <input type="text" required value={newRule.target_column} 
                  onChange={e => setNewRule({ ...newRule, target_column: e.target.value })} />
              </div>
            </div>
            <div className="br-row-split">
              <div className="br-input-group">
                <label>Comparison Type</label>
                <select 
                  required 
                  value={newRule.comparison_type} 
                  onChange={e => setNewRule({ ...newRule, comparison_type: e.target.value })}
                >
                  <option value="exact">Exact Match</option>
                  <option value="substring">Substring Match</option>
                  <option value="fuzzy">Fuzzy Match</option>
                </select>
              </div>
              <div className="br-input-group">
                <label>Case Sensitive?</label>
                <select value={newRule.is_case_sensitive} onChange={e => setNewRule({ ...newRule, is_case_sensitive: e.target.value })}>
                  <option value="y">Yes</option><option value="n">No</option>
                </select>
              </div>
            </div>
            {newRule.comparison_type === 'fuzzy' && (
              <div className="br-input-group">
                <label>Similarity Threshold (0.0 - 1.0)</label>
                <input type="number" step="0.1" min="0" max="1" value={newRule.threshold_value} 
                  onChange={e => setNewRule({ ...newRule, threshold_value: e.target.value })} />
              </div>
            )}
          </div>
        );

      case 'pattern':
        return (
          <div className="br-dynamic-section animate-fade-in">
            <div className="br-input-group">
              <label>Column Name</label>
              <input type="text" required value={newRule.source_column} 
                onChange={e => setNewRule({ ...newRule, source_column: e.target.value })} />
            </div>
            <div className="br-input-group">
              <label>Pattern Type</label>
              <select required value={newRule.pattern_type} onChange={e => setNewRule({ ...newRule, pattern_type: e.target.value })}>
                <option value="">Select Pattern</option>
                <option value="email">Email Address</option>
                <option value="us_phone">US Phone Number</option>
                <option value="intl_phone">International Phone Number</option>
                <option value="date_iso">Date (yyyy-mm-dd)</option>
                <option value="ssn">Social Security Number</option>
                <option value="zip">US Zip Code</option>
                <option value="url">URL</option>
                <option value="ip">IP Address</option>
                <option value="currency">Currency</option>
                <option value="percentage">Percentage</option>
                <option value="custom_regex">Custom Regex Pattern</option>
              </select>
            </div>
            {newRule.pattern_type === 'custom_regex' && (
              <div className="br-input-group">
                <label>Regex Expression</label>
                <input type="text" required placeholder="^([a-z0-9_\.-]+)@([\da-z\.-]+)\.([a-z\.]{2,6})$"
                  value={newRule.custom_pattern} onChange={e => setNewRule({ ...newRule, custom_pattern: e.target.value })} />
              </div>
            )}
          </div>
        );

      case 'range':
        return (
          <div className="br-dynamic-section animate-fade-in">
            <div className="br-input-group">
              <label>Column Name</label>
              <input type="text" required value={newRule.source_column} 
                onChange={e => setNewRule({ ...newRule, source_column: e.target.value })} />
            </div>
            <div className="br-row-split">
              <div className="br-input-group">
                <label>Minimum Value</label>
                <input type="number" step="any" placeholder="No minimum"
                  value={newRule.min_value} onChange={e => setNewRule({ ...newRule, min_value: e.target.value })} />
              </div>
              <div className="br-input-group">
                <label>Maximum Value</label>
                <input type="number" step="any" placeholder="No maximum"
                  value={newRule.max_value} onChange={e => setNewRule({ ...newRule, max_value: e.target.value })} />
              </div>
            </div>
          </div>
        );

      case 'kpi':
      case 'aggregate':
        return (
          <div className="br-dynamic-section animate-fade-in">
            <div className="br-input-group">
              <label>Column Name</label>
              <input type="text" required value={newRule.source_column} 
                onChange={e => setNewRule({ ...newRule, source_column: e.target.value })} />
            </div>
            <div className="br-row-split">
              <div className="br-input-group">
                <label>{newRule.rule_type === 'kpi' ? 'KPI Type' : 'Aggregation'}</label>
                <select required value={newRule.operator_type} onChange={e => setNewRule({ ...newRule, operator_type: e.target.value })}>
                  <option value="">Select</option>
                  <option value="sum">Sum</option>
                  <option value="avg">Average</option>
                  <option value="count">Count</option>
                  <option value="min">Minimum</option>
                  <option value="max">Maximum</option>
                  {newRule.rule_type === 'kpi' && <option value="null_count">Null Count</option>}
                  {newRule.rule_type === 'kpi' && <option value="distinct_count">Distinct Count</option>}
                  {newRule.rule_type === 'aggregate' && <option value="median">Median</option>}
                  {newRule.rule_type === 'aggregate' && <option value="stddev">Std Deviation</option>}
                </select>
              </div>
              <div className="br-input-group">
                <label>Comparison</label>
                <select required value={newRule.operator} onChange={e => setNewRule({ ...newRule, operator: e.target.value })}>
                  <option value="">Operator</option>
                  <option value="<">{'<'}</option><option value=">">{'>'}</option>
                  <option value="=">=</option><option value="!=">!=</option>
                  <option value="<=">{'<='}</option><option value=">=">{'>='}</option>
                </select>
              </div>
              <div className="br-input-group">
                <label>Threshold</label>
                <input type="number" step="any" required value={newRule.threshold_value} 
                  onChange={e => setNewRule({ ...newRule, threshold_value: e.target.value })} />
              </div>
            </div>
          </div>
        );

      case 'custom':
        return (
          <div className="br-dynamic-section animate-fade-in">
            <div className="br-input-group">
              <label>Rule Logic (SQL or Python)</label>
              <textarea 
                required 
                placeholder="SELECT * FROM table WHERE..."
                value={newRule.rule_logic} 
                onChange={e => setNewRule({ ...newRule, rule_logic: e.target.value })} 
                rows={5}
              />
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  const renderChoiceMenu = () => (
    <div className="br-menu-card animate-fade-in">
      <h3>Business Logic Configuration</h3>
      <p className="br-subtitle">Source: <strong>{fileName}</strong></p>
      <div className="br-action-row">
        <div className="br-action-box" onClick={() => { resetForm(); setStep('CREATE_FORM'); }}>
          <div className="br-icon">+</div>
          <h4>Create New Rule</h4>
          <p>Define custom validation logic.</p>
        </div>
        <div className="br-action-box" onClick={() => setStep('MAPPING')}>
          <div className="br-icon">⚡</div>
          <h4>Run Existing Rules</h4>
          <p>Apply saved templates to this data.</p>
        </div>
      </div>
    </div>
  );

  const renderCreateRuleForm = () => (
    <div className="br-form-card animate-fade-in">
      <h4>{isEditing ? 'Edit Business Rule' : 'Define New Business Rule'}</h4>
      <form onSubmit={handleSaveRule}>
        <div className="br-input-group">
          <label>Rule Name *</label>
          <input type="text" required value={newRule.rule_name}
            onChange={e => setNewRule({ ...newRule, rule_name: e.target.value })} />
        </div>

        <div className="br-input-group">
          <label>Description (Optional)</label>
          <textarea value={newRule.description}
            onChange={e => setNewRule({ ...newRule, description: e.target.value })} 
            rows={2}
          />
        </div>

        <div className="br-row-split">
          <div className="br-input-group">
            <label>Rule Type *</label>
            <select value={newRule.rule_type} onChange={e => setNewRule({ ...newRule, rule_type: e.target.value })}>
              {RULE_TYPES.map(t => <option key={t.id} value={t.id}>{t.label}</option>)}
            </select>
          </div>
          <div className="br-input-group">
            <label>Severity *</label>
            <select value={newRule.severity} onChange={e => setNewRule({ ...newRule, severity: e.target.value })}>
              <option value="low">Low</option>
              <option value="medium">Medium</option>
              <option value="high">High</option>
              <option value="critical">Critical</option>
            </select>
          </div>
        </div>

        <hr className="br-divider" />

        {renderDynamicFields()}

        <div className="br-form-actions">
          <button type="button" className="br-btn-secondary" onClick={() => setStep('CHOICE')}>Cancel</button>
          <button type="submit" className="br-btn-primary" disabled={loading}>
            {loading ? 'Saving...' : 'Save Rule Configuration'}
          </button>
        </div>
      </form>
    </div>
  );

  const renderMappingScreen = () => (
    <div className="br-mapping-card animate-fade-in">
      <div className="br-header-split">
        <h4>Select Rules to Execute</h4>
        <button className="br-btn-text" onClick={() => setSelectedRuleIds(selectedRuleIds.length === availableRules.length ? [] : availableRules.map(r => r.rule_id))}>
          {selectedRuleIds.length === availableRules.length ? 'Deselect All' : 'Select All'}
        </button>
      </div>
      <div className="br-scroll-list">
        {availableRules.map(rule => (
          <div key={rule.rule_id} className={`br-check-item ${selectedRuleIds.includes(rule.rule_id) ? 'selected' : ''}`}
            onClick={() => setSelectedRuleIds(prev => prev.includes(rule.rule_id) ? prev.filter(id => id !== rule.rule_id) : [...prev, rule.rule_id])}>
            <div className="br-rule-info">
              <input type="checkbox" checked={selectedRuleIds.includes(rule.rule_id)} readOnly />
              <div className="br-rule-label">
                <strong>{rule.rule_name}</strong>
                <span className="br-rule-meta">{rule.rule_type} • {rule.source_column || 'Any'}</span>
              </div>
            </div>
            <div className="br-rule-actions">
              <button className="br-icon-btn" onClick={(e) => { e.stopPropagation(); setNewRule(rule); setIsEditing(true); setStep('CREATE_FORM'); }}>✎</button>
              <button className="br-icon-btn del" onClick={(e) => handleDeleteRule(e, rule.rule_id)}>×</button>
            </div>
          </div>
        ))}
      </div>
      <div className="br-form-actions">
        <button className="br-btn-secondary" onClick={() => setStep('CHOICE')}>Back</button>
        <button className="br-btn-primary" onClick={handleExecuteRules} disabled={loading || selectedRuleIds.length === 0}>
          {loading ? 'Processing...' : `Execute ${selectedRuleIds.length} Rules`}
        </button>
      </div>
    </div>
  );

  const renderReport = () => {
    if (!executionResult) return null;
    const { dq_score = 0, violations = [], summary = {}, session_id = 'N/A' } = executionResult;
    const scoreClass = dq_score > 80 ? 'good' : dq_score > 50 ? 'avg' : 'poor';

    return (
      <div className="br-results-container animate-fade-in">
        <div className="br-score-header">
          <div className="br-header-titles">
            <h3>Compliance Analysis Report</h3>
            <p className="br-mono-sub">Session ID: {session_id}</p>
            <span className="br-tag-file">Source: {fileName}</span>
          </div>
          <div className={`br-score-circle ${scoreClass}`}>
            <strong>{parseFloat(dq_score).toFixed(1)}%</strong>
            <small>DQ Score</small>
          </div>
        </div>

        <div className="br-stats-grid">
          <div className="stat-card">
            <span>Total Rows</span>
            <strong>{summary?.total_records_checked || '0'}</strong>
          </div>
          <div className="stat-card fail">
            <span>Violations Found</span>
            <strong>{pagination.total || summary?.total_violations || violations.length}</strong>
          </div>
          <div className="stat-card">
            <span>Rules Applied</span>
            <strong>{selectedRuleIds.length}</strong>
          </div>
        </div>

        {/* Rules Applied Section */}
        <div className="br-rules-applied-section">
          <h4>Rules Applied in this Check</h4>
          <div className="br-rules-pill-container">
            {availableRules
              .filter(r => selectedRuleIds.includes(r.rule_id))
              .map(rule => (
                <div key={rule.rule_id} className="br-rule-pill">
                  <span className="pill-name">{rule.rule_name}</span>
                  <span className="pill-type">{rule.rule_type}</span>
                </div>
              ))}
          </div>
        </div>
        
        <div className="br-violations-section">
          <h4>Violation Details</h4>
          <div className="br-table-wrap">
            <table className="br-table">
              <thead>
                <tr>
                  <th>Row</th>
                  <th>Column</th>
                  <th>Rule</th>
                  <th>Actual Value</th>
                  <th>Expected/Comparison</th>
                  <th>Severity</th>
                </tr>
              </thead>
              <tbody>
                {violations.length > 0 ? violations.map((v, i) => (
                  <tr key={i} className={`severity-${v.severity || 'medium'}`}>
                    <td>{v.excel_row || v.row_index || 'N/A'}</td>
                    <td>{v.column_name || 'N/A'}</td>
                    <td>{v.rule_name || 'N/A'}</td>
                    <td>{String(v.actual_value ?? 'NULL')}</td>
                    <td>{v.comparison_type || v.expected_value || v.operator || 'N/A'}</td>
                    <td>
                      <span className={`severity-badge ${v.severity || 'medium'}`}>
                        {v.severity || 'medium'}
                      </span>
                    </td>
                  </tr>
                )) : (
                  <tr>
                    <td colSpan="6" style={{ textAlign: 'center', padding: '20px' }}>
                      ✅ No violations found on this page.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {/* Pagination Controls */}
          {pagination.totalPages > 1 && (
            <div className="br-pagination-controls">
              <button 
                disabled={pagination.page === 1 || loading} 
                onClick={() => fetchViolations(pagination.page - 1)}
                className="br-btn-secondary"
              >
                Previous
              </button>
              <span className="br-page-info">
                Page <strong>{pagination.page}</strong> of {pagination.totalPages} 
                <small>({pagination.total} total violations)</small>
              </span>
              <button 
                disabled={pagination.page === pagination.totalPages || loading} 
                onClick={() => fetchViolations(pagination.page + 1)}
                className="br-btn-secondary"
              >
                Next
              </button>
            </div>
          )}
        </div>
        
        <div className="br-form-actions" style={{ marginTop: '20px' }}>
          <button className="br-btn-secondary" onClick={() => setStep('MAPPING')}>
            ← Back to Rules
          </button>
          <button className="br-reset-btn" onClick={onReset}>
            Return to Dashboard
          </button>
        </div>
      </div>
    );
  };

  return (
    <div className="business-results-wrapper">
      {notification && <div className={`br-notification ${notification.type}`}>{notification.message}</div>}
      {step === 'CHOICE' && renderChoiceMenu()}
      {step === 'CREATE_FORM' && renderCreateRuleForm()}
      {step === 'MAPPING' && renderMappingScreen()}
      {step === 'REPORT' && renderReport()}
    </div>
  );
};

export default BusinessResults;