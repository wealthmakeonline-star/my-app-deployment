import React, { useState } from 'react';
import DataCheckOption from '../components/DataCheckOption';
import './DataQualityCheck.css';
import DataCheckLogo from "../assets/DataCheck.png";

const OPTIONS = [
  { id: 'SINGLE_SOURCE_ANALYSIS', label: 'Instant Data Profiling' },
  { id: 'SOURCE_TARGET_COMPARISON', label: 'Data Migration Guardian' },
  { id: 'ADVANCED_DATA_CHECKS', label: 'Smart Field Validation' },
  { id: 'BUSINESS_RULES_ENGINE', label: 'Dynamic Policy Automation' },
];

const INITIAL_DB_CONNECTION = { dbType: '', host: '', port: '', database: '', schema: '', table: '', user: '', password: '', serviceName: '' };

const DataQualityCheck = () => {
  const [selectedOption, setSelectedOption] = useState(null);
  const [allFormsData, setAllFormsData] = useState({
    SINGLE_SOURCE_ANALYSIS: { selectedSource: 'CSV', selectedFile: null, dbConnection: { ...INITIAL_DB_CONNECTION } },
    SOURCE_TARGET_COMPARISON: { 
      selectedSource: 'CSV', selectedFile: null, dbConnection: { ...INITIAL_DB_CONNECTION },
      targetSource: 'CSV', targetFile: null, targetDbConnection: { ...INITIAL_DB_CONNECTION } 
    },
    ADVANCED_DATA_CHECKS: { selectedSource: 'CSV', selectedFile: null, dbConnection: { ...INITIAL_DB_CONNECTION } },
    BUSINESS_RULES_ENGINE: { selectedSource: 'CSV', selectedFile: null, dbConnection: { ...INITIAL_DB_CONNECTION } },
  });

  const renderContent = () => {
    if (selectedOption) {
      return (
        <DataCheckOption 
          key={selectedOption} 
          type={selectedOption} 
          formData={allFormsData[selectedOption]}
          updateFormData={(data) => setAllFormsData(prev => ({ ...prev, [selectedOption]: { ...prev[selectedOption], ...data } }))}
        />
      );
    }
    return <div className="default-content-area"><img src={DataCheckLogo} alt="Logo" className="data-check-logo" /></div>;
  };

  return (
    <div className="dq-app-container">
      <nav className="dq-sidebar">
        <h3 className="sidebar-title">Data Quality Check</h3>
        <div className="sidebar-links">
          {OPTIONS.map((opt) => (
            <button key={opt.id} className={`sidebar-link ${selectedOption === opt.id ? 'active' : ''}`}
              onClick={() => setSelectedOption(opt.id)}>{opt.label}</button>
          ))}
        </div>
      </nav>
      <main className="dq-main-content">{renderContent()}</main>
    </div>
  );
};

export default DataQualityCheck;