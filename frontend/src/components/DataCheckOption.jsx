import React, { useState } from 'react';
import AnalysisResultsDisplay from './AnalysisResults'; 
import ComparisonResults from './ComparisonResults'; 
import AdvancedResults from './AdvancedResults';
import BusinessResults from './BusinessResults';
import './DataCheckOption.css'; 

const DATABASE_OPTIONS = ['MySQL', 'PostgreSQL', 'Oracle', 'SQL Server', 'SQLite'];
const DB_HIERARCHY_API = '/api/database-hierarchy';

const API_ENDPOINTS = {
  SINGLE_SOURCE_ANALYSIS: '/api/check-single',
  SOURCE_TARGET_COMPARISON: '/api/compare',
  ADVANCED_DATA_CHECKS: '/api/advanced',
  BUSINESS_RULES_ENGINE: '/api/rules',
};

const OPTION_DETAILS = {
  SINGLE_SOURCE_ANALYSIS: { title: "INSTANT DATA PROFILING", bullets: ["CSV, excel or database checks", "Find nulls, duplicates, format issues", "Show exact problem rows"] },
  SOURCE_TARGET_COMPARISON: { title: "DATA MIGRATION GUARDIAN", bullets: ["Compare source vs target", "Verify row counts & data integrity", "Identify mismatched records"] },
  ADVANCED_DATA_CHECKS: { title: "SMART FIELD VALIDATION", bullets: ["Cross-field validation", "Run complex aggregation checks", "Numeric range checks"] },
  BUSINESS_RULES_ENGINE: { title: "DYNAMIC POLICY AUTOMATION", bullets: ["Define custom business logic", "Ensure compliance", "Generate violation reports"] },
};

const RESET_STATE = {
  selectedSource: 'CSV',
  selectedFile: null,
  sourceFileName: null,
  dbConnection: { dbType: 'MySQL', host: '', port: '', user: '', password: '', database: '', schema: '', table: '' },
  targetSource: 'CSV',
  targetFile: null,
  targetFileName: null,
  targetDbConnection: { dbType: 'MySQL', host: '', port: '', user: '', password: '', database: '', schema: '', table: '' }
};

const DataCheckOption = ({ type, formData, updateFormData }) => {
  const details = OPTION_DETAILS[type] || { title: "", bullets: [] };
  const isComparisonMode = type === 'SOURCE_TARGET_COMPARISON';
  
  const { 
    selectedSource, selectedFile, dbConnection, 
    targetSource, targetFile, targetDbConnection 
  } = formData;

  const [analysisResult, setAnalysisResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const [hierarchies, setHierarchies] = useState({
    source: { databases: [], schemas: [], tables: [], isConnected: false },
    target: { databases: [], schemas: [], tables: [], isConnected: false }
  });
  const [isDbConnecting, setIsDbConnecting] = useState(false);

  const buildHierarchyPayload = (overrides = {}, conn) => ({
    db_type: conn.dbType?.toLowerCase() || "mysql",
    db_config: {
      host: conn.host,
      port: parseInt(conn.port) || 3306,
      user: conn.user,
      password: conn.password,
      ...(conn.dbType === 'Oracle' && { service_name: conn.serviceName })
    },
    ...overrides
  });

  const handleConnect = async (isTarget) => {
    const side = isTarget ? 'target' : 'source';
    const conn = isTarget ? targetDbConnection : dbConnection;
    setError(null); setIsDbConnecting(true);
    try {
      const res = await fetch(DB_HIERARCHY_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildHierarchyPayload({}, conn)),
      });
      const result = await res.json();
      setHierarchies(prev => ({
        ...prev,
        [side]: { ...prev[side], databases: result.data?.databases || [], isConnected: true }
      }));
    } catch (e) { setError("Connection failed"); } finally { setIsDbConnecting(false); }
  };

  const handleDatabaseChange = async (dbName, isTarget) => {
    const side = isTarget ? 'target' : 'source';
    const connKey = isTarget ? 'targetDbConnection' : 'dbConnection';
    updateFormData({ [connKey]: { ...formData[connKey], database: dbName, schema: '', table: '' } });
    try {
      const res = await fetch(DB_HIERARCHY_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildHierarchyPayload({ database: dbName }, formData[connKey])),
      });
      const result = await res.json();
      setHierarchies(prev => ({ ...prev, [side]: { ...prev[side], schemas: result.data?.schemas || [], tables: [] } }));
    } catch (e) { setError("Failed to fetch schemas"); }
  };

  const handleSchemaChange = async (schemaName, isTarget) => {
    const side = isTarget ? 'target' : 'source';
    const connKey = isTarget ? 'targetDbConnection' : 'dbConnection';
    updateFormData({ [connKey]: { ...formData[connKey], schema: schemaName, table: '' } });
    try {
      const res = await fetch(DB_HIERARCHY_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildHierarchyPayload({ database: formData[connKey].database, schema: schemaName }, formData[connKey])),
      });
      const result = await res.json();
      setHierarchies(prev => ({ ...prev, [side]: { ...prev[side], tables: result.data?.tables || [] } }));
    } catch (e) { setError("Failed to fetch tables"); }
  };

  const handleComparisonReset = () => {
    // 1. Clear the analysis report (Visual reset)
    setAnalysisResult(null);
    setError(null);

    // 2. Clear the form data (Input reset)
    // We use the constant we defined to wipe both Source and Target fields
    updateFormData(RESET_STATE);

    // 3. Reset the internal database dropdowns (Hierarchies)
    // This ensures that if they select Database again, the old table list is gone
    setHierarchies({
      source: { databases: [], schemas: [], tables: [], isConnected: false },
      target: { databases: [], schemas: [], tables: [], isConnected: false }
    });
  };

const handleRunAnalysis = async () => {
    setLoading(true);
    setError(null);
    
    try {
      // 1. UPLOAD FILES HELPER
      const upload = async (file, isTarget) => {
        if (!file) return null;
        const d = new FormData();
        d.append('file', file);
        const r = await fetch('/api/upload', { method: 'POST', body: d });
        if (!r.ok) throw new Error(`Upload failed for ${isTarget ? 'Target' : 'Source'}`);
        
        const j = await r.json();
        
        // Update UI
        if (isTarget) {
          updateFormData({ targetFileName: j.filename });
        } else {
          updateFormData({ sourceFileName: j.filename });
        }
        
        // --- FIX 1: FORCE FORWARD SLASHES ---
        // Converts "C:\Path\To\File" -> "C:/Path/To/File"
        // This prevents "\t" in "..._target_data" from being read as a TAB character
        return j.full_path ? j.full_path.replace(/\\/g, '/') : null;
      };

      const sPath = await upload(selectedFile, false);
      const tPath = await upload(targetFile, true);

      // 2. SHORT-CIRCUIT FOR BUSINESS RULES
      if (type === 'BUSINESS_RULES_ENGINE') {
        setAnalysisResult({ 
          isReadyForWorkflow: true, 
          filepath: sPath,
          sourceType: selectedSource,
          dbConnection: dbConnection 
        });
        setLoading(false);
        return;
      }

      // 3. BUILD UNIFIED PAYLOAD
      let payload = {};

      if (!isComparisonMode) {
        // --- SINGLE SOURCE ANALYSIS ---
        payload = {
            analysis_type: type === 'ADVANCED_DATA_CHECKS' ? 'advanced' : 'basic',
            source_type: selectedSource.toLowerCase(),
            ...(selectedSource === 'Database' 
              ? {
                  db_type: dbConnection.dbType?.toLowerCase(),
                  host: dbConnection.host,
                  port: parseInt(dbConnection.port),
                  user: dbConnection.user,
                  password: dbConnection.password,
                  database: dbConnection.database,
                  schema: dbConnection.schema,
                  table: dbConnection.table
                }
              : { 
                  file_path: sPath,
                  sheet_name: formData.sourceSheetName || null, 
                  mandatory_fields: 'all'
                }
            )
        };
      } else {
        // --- COMPARISON MODE ---
        payload = {
          // Source Object
          source: {
            source_type: selectedSource.toLowerCase(),
            ...(selectedSource === 'Database' 
              ? {
                  db_type: dbConnection.dbType?.toLowerCase(),
                  host: dbConnection.host,
                  port: parseInt(dbConnection.port) || 3306,
                  database: dbConnection.database,
                  schema: dbConnection.schema,
                  table: dbConnection.table,
                  user: dbConnection.user,
                  password: dbConnection.password,
                  ...(dbConnection.dbType === 'Oracle' && { service_name: dbConnection.serviceName })
                }
              : { 
                  source_file_path: sPath, 
                  // file_path: sPath,
                  // source_sheet_name: formData.sourceSheetName || null, 
                  // mandatory_fields: 'all'
                }
            )
          },
          
          // Target Object (Nested) - Populated for dq_comparison.py
          target: {
            source_type: targetSource.toLowerCase(),
            ...(targetSource === 'Database' 
              ? {
                  db_type: targetDbConnection.dbType?.toLowerCase(),
                  host: targetDbConnection.host,
                  port: parseInt(targetDbConnection.port) || 5432,
                  database: targetDbConnection.database,
                  schema: targetDbConnection.schema,
                  table: targetDbConnection.table,
                  user: targetDbConnection.user,
                  password: targetDbConnection.password,
                  ...(targetDbConnection.dbType === 'Oracle' && { service_name: targetDbConnection.serviceName })
                }
              : { 
                  target_file_path: tPath,
                  // file_path: tPath,  
                  // target_sheet_name: formData.targetSheetName || null,
                  // mandatory_fields: 'all'
                }
            )
          },

          // target_source_type: targetSource.toLowerCase(),
          // source_type: selectedSource.toLowerCase(),
          
          // ...(targetSource === 'Database' ? {} : {
          //   target_file_path: tPath,
          //   target_sheet_name: formData.targetSheetName || null
          // }),
          // ...(selectedSource === 'Database' ? {} : {
          //   file_path: sPath,
          //   source_sheet_name: formData.sourceSheetName || null
          // }), 

          selected_columns: "all"
        };
      }

      // 4. CALL API
      const res = await fetch(API_ENDPOINTS[type], {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!res.ok) {
        const errorData = await res.json();
        throw new Error(errorData.message || 'Analysis failed');
      }

      setAnalysisResult(await res.json());
    } catch (e) {
      setError(e.message);
      console.error("Analysis Error:", e);
    } finally {
      setLoading(false);
    }
  };

  const renderSide = (isTarget) => {
    const side = isTarget ? 'target' : 'source';
    const currSrc = isTarget ? targetSource : selectedSource;
    const currHie = hierarchies[side];

    const uploadedFileName = isTarget ? formData.targetFileName : formData.sourceFileName;
    const selectedRawFile = isTarget ? targetFile : selectedFile;

    return (
      <div className="input-column-box">
        <div className="source-options-row">
          {['CSV', 'Excel', 'Database'].map(s => (
            <button key={s} className={`source-button ${currSrc === s ? 'selected' : ''}`}
              onClick={() => updateFormData(isTarget ? { targetSource: s } : { selectedSource: s })}>
              {s}
            </button>
          ))}
        </div>
        {currSrc === 'Database' ? (
          <DbForm fields={isTarget ? targetDbConnection : dbConnection} isConnected={currHie.isConnected} isConnecting={isDbConnecting} 
            dbList={currHie.databases} schemaList={currHie.schemas} tableList={currHie.tables}
            onConnect={() => handleConnect(isTarget)} onDbChange={(v) => handleDatabaseChange(v, isTarget)}
            onSchemaChange={(v) => handleSchemaChange(v, isTarget)}
            update={(v) => updateFormData(isTarget ? { targetDbConnection: v } : { dbConnection: v })} />
        ) : (
        <div className="file-col">
                  <input 
                    type="file" 
                    id={isTarget ? "tf" : "sf"} 
                    hidden 
                    onChange={(e) => {
                      // When a new file is picked, clear the previous uploaded filename
                      const updateObj = isTarget 
                        ? { targetFile: e.target.files[0], targetFileName: null } 
                        : { selectedFile: e.target.files[0], sourceFileName: null };
                      updateFormData(updateObj);
                    }} 
                  />
                  <label htmlFor={isTarget ? "tf" : "sf"} className="drag-drop-label">
                    <p><strong>
                      {uploadedFileName 
                        ? `✅ ${uploadedFileName}` // Show the backend filename
                        : selectedRawFile 
                          ? `${selectedRawFile.name}` 
                          // ? `Selected: ${selectedRawFile.name}` 
                          : `Upload ${currSrc}`} 
                    </strong></p>
                  </label>
                </div>
              )}
            </div>
          );
        };

        const isFormValid = () => {
          // --- SOURCE VALIDATION ---
          const isSourceValid = selectedSource === 'Database' 
            ? (
                hierarchies.source.isConnected && 
                dbConnection.database && 
                dbConnection.schema && 
                dbConnection.table
              )
            : !!selectedFile;

          // If not in comparison mode, we only care about source
          if (!isComparisonMode) return isSourceValid;

          // --- TARGET VALIDATION (Only if Comparison Mode is active) ---
          const isTargetValid = targetSource === 'Database'
            ? (
                hierarchies.target.isConnected && 
                targetDbConnection.database && 
                targetDbConnection.schema && 
                targetDbConnection.table
              )
            : !!targetFile;

          return isSourceValid && isTargetValid;
        };

  return (
    <div className="data-option-page-container">
      <div className="data-option-box">
        <div className="dark-card">
          <h3 className="dark-card-title">{details.title}</h3>
          <ul className="dark-card-list">{details.bullets.map((b, i) => <li key={i}>{b}</li>)}</ul>
        </div>
        {!isComparisonMode ? (
          <div className="input-layout-wrapper">{renderSide(false)}</div>
        ) : (
          <div className="dual-input-layout">
            <div className="comparison-pane"><h4 className="pane-header">SOURCE</h4>{renderSide(false)}</div>
            <div className="comparison-pane"><h4 className="pane-header">TARGET</h4>{renderSide(true)}</div>
          </div>
        )}
        {error && <div className="error-text">❌ {error}</div>}
        <button 
          className={`option-page-run-btn ${!isFormValid() ? 'btn-disabled' : ''}`} 
          onClick={handleRunAnalysis} 
          disabled={loading || !isFormValid()}
        >
          {loading ? 'ANALYZING...' : 'RUN ANALYSIS'}
        </button>
        {analysisResult && (
          <div className="analysis-results-wrapper">
            {(() => {
              // 1. If it's the comparison API
              if (isComparisonMode) {
                return (
                  <ComparisonResults 
                    analysisResult={analysisResult} 
                    onReset={handleComparisonReset} 
                  />
                );
              }
              
              // 2. If it's the Advanced API
              if (type === 'ADVANCED_DATA_CHECKS') {
                return (
                  <AdvancedResults 
                    analysisResult={analysisResult} 
                    onReset={() => setAnalysisResult(null)} 
                  />
                );
              }

              if (type === 'BUSINESS_RULES_ENGINE') {
                  return (
                      <BusinessResults 
                          analysisResult={analysisResult} 
                          formData={formData}
                          onReset={() => setAnalysisResult(null)}
                      />
                  );
              }
              
              // 3. Default to Single Source Analysis (Check-Single)
              //is this below return statement required? It is...but what should the landing be here? But I also feel like it's not required. Idk.
              return (
                <AnalysisResultsDisplay 
                  analysisResult={analysisResult} 
                  onReset={() => setAnalysisResult(null)} 
                />
              );
            })()}
          </div>
        )}
        {/* {analysisResult && (
          <div className="analysis-results-wrapper">
             {isComparisonMode ? <ComparisonResults analysisResult={analysisResult} /> : <AnalysisResultsDisplay analysisResult={analysisResult} />}
          </div>
        )} */}
      </div>
    </div>
  );
};

const DbForm = ({ 
  fields, update, onConnect, onDbChange, onSchemaChange, 
  isConnecting, isConnected, dbList, schemaList, tableList 
}) => (
  <div className="db-form-wrapper">
    {/* STEP 1: Connection Details */}
    <select 
      className="db-select-dropdown" 
      value={fields.dbType || ''} 
      onChange={(e) => update({ ...fields, dbType: e.target.value })} 
      disabled={isConnected}
    >
      <option value="" disabled>Select Database Type</option>
      {DATABASE_OPTIONS.map(o => <option key={o} value={o}>{o}</option>)}
    </select>

    <div className="db-input-grid">
      {['host', 'port', 'user', 'password'].map(f => (
        <input 
          key={f} 
          placeholder={f.toUpperCase()} 
          className="db-input-field" 
          type={f === 'password' ? 'password' : 'text'}
          value={fields[f] || ''} 
          onChange={(e) => update({ ...fields, [f]: e.target.value })} 
          disabled={isConnected} 
        />
      ))}
    </div>

    {/* STEP 2: Connect Button (Disappears once connected) */}
    {!isConnected ? (
      <button className="db-connect-btn" onClick={onConnect} disabled={isConnecting}>
        {isConnecting ? 'CONNECTING...' : 'CONNECT & FETCH'}
      </button>
    ) : (
      <div className="db-selection-area animate-fade-in">
        
        {/* STEP 3: Database Dropdown (Visible after Connection) */}
        {/* <p className="dropdown-label">Select Database</p> */}
        <select 
          className="db-select-dropdown" 
          value={fields.database || ''} 
          onChange={(e) => onDbChange(e.target.value)}
        >
          <option value="">Choose Database</option>
          {dbList?.map(d => <option key={d} value={d}>{d}</option>)}
        </select>
        
        {/* STEP 4: Schema Dropdown (Only visible after Database is chosen) */}
        {fields.database && (
          <div className="animate-fade-in">
            {/* <p className="dropdown-label">Select Schema</p> */}
            <select 
              className="db-select-dropdown" 
              value={fields.schema || ''} 
              onChange={(e) => onSchemaChange(e.target.value)}
            >
              <option value="">Choose Schema</option>
              {schemaList?.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
        )}

        {/* STEP 5: Table Dropdown (Only visible after Schema is chosen) */}
        {fields.schema && (
          <div className="animate-fade-in">
            {/* <p className="dropdown-label">Select Table</p> */}
            <select 
              className="db-select-dropdown" 
              value={fields.table || ''} 
              onChange={(e) => update({ ...fields, table: e.target.value })}
            >
              <option value="">Choose Table</option>
              {tableList?.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
        )}
      </div>
    )}
  </div>
);

export default DataCheckOption;