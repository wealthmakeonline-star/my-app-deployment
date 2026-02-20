import React from "react";
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';

import Header from "./components/Header";
import Homepage from "./components/Homepage";
import DataQualityCheck from "./components/DataQualityCheck"; 
import DataCheckOption from "./components/DataCheckOption";

// import bgLogo from "./assets/AutoEdge.png";
import "./App.css";



function Layout({ children }) {
  return (
    <>
      <Header />

      {/* Background Div (fixed, zIndex: -1) */}
      {/* <div
        className="App-bg"
        style={{
          backgroundImage: `url(${bgLogo})`,
          backgroundRepeat: "no-repeat",
          backgroundPosition: "center 250px",
          backgroundSize: "45%",
          opacity: 0.1,
          filter: "none",
          position: "fixed",
          inset: 0,
          zIndex: -1
        }}
      ></div> */}

      <main className="App-content">
        {children}
      </main>
    </>
  );
}

function App() {
  return (
    <Router>
      <Layout>
        <Routes>
          
          <Route 
            path="/" 
            element={<Homepage />} 
          />
          
          <Route 
            path="/data-quality-check" 
            element={<DataQualityCheck />} 
          />

          <Route 
            path="/data-quality-check/single-source-analysis" 
            element={<DataCheckOption type="SINGLE_SOURCE_ANALYSIS" />} 
          />
          <Route 
            path="/data-quality-check/source-target-comparison" 
            element={<DataCheckOption type="SOURCE_TARGET_COMPARISON" />} 
          />
          <Route 
            path="/data-quality-check/advanced-data-checks" 
            element={<DataCheckOption type="ADVANCED_DATA_CHECKS" />} 
          />
          <Route 
            path="/data-quality-check/business-rules-engine" 
            element={<DataCheckOption type="BUSINESS_RULES_ENGINE" />} 
          />

        </Routes>
      </Layout>
    </Router>
  );
}

export default App;