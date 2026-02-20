import React from "react";
import "./Homepage.css";
import { useNavigate } from "react-router-dom";

import AutoEdge from "../assets/TransparentAutoedge.png";
import DataCheck from "../assets/DataCheck.png";
import Ingestor from "../assets/Ingestor.png";
import NotifyIQ from "../assets/NotifyIQ.png";
import Securetag from "../assets/Securetag.png";
import Opsguard from "../assets/Opsguard.png";
import MigrateX from "../assets/MigrateX.png";
import FinOptima from "../assets/FinOptima.png";

const Homepage = () => {
  const navigate = useNavigate();

  return (
    <div className="homepage-container">
      <div className="arch-wrapper">

        {/* AUTOEDGE */}
        <div className="node autoedge autoedge-ring">
          <img src={AutoEdge} alt="AutoEdge" />
        </div>

        {/* TOP */}
        <div 
          className="node datacheck" 
          onClick={() => navigate("/data-quality-check")}
          style={{ cursor: "pointer" }}
        >
          <img src={DataCheck} alt="Data Quality"/>
        </div>

        <div className="node ingestor">
          <img src={Ingestor} alt="Ingestor" />
        </div>

        <div className="node opsguard">
          <img src={Opsguard} alt="OpsGuard" />
        </div>

        {/* RIGHT – MIGRATEX (FIXED: NOW RENDERED) */}
        <div className="node migratex">
          <img src={MigrateX} alt="MigrateX" />
        </div>

        {/* BOTTOM */}
        <div className="node notifyiq">
          <img src={NotifyIQ} alt="NotifyIQ" />
        </div>

        <div className="node finoptima">
          <img src={FinOptima} alt="FinOps" />
        </div>

        <div className="node securetag">
          <img src={Securetag} alt="SecureTag" />
        </div>

        <svg className="connectors" viewBox="0 0 1200 700">

  {/* ===== ARROWHEAD DEFINITION ===== */}
  <defs>
    <marker
      id="arrow-end"
      viewBox="0 0 10 10"
      refX="9"
      refY="5"
      markerWidth="7"
      markerHeight="7"
      orient="auto"
      markerUnits="strokeWidth"
    >
      <path d="M0 0 L10 5 L0 10 Z" fill="#163761" />
    </marker>
  </defs>

  {/* OPSGUARD */}
  <path
    d="
      M170 350
      C 420 330,
        650 370,
        910 220
    "
    markerEnd="url(#arrow-end)"
  />

  {/* INGESTOR */}
  <path
    d="
      M170 350
      C 360 305,
        220 255,
        400 245
    "
    markerEnd="url(#arrow-end)"
  />

  {/* DATA QUALITY */}
  <path
    d="
      M170 350
      C 360 245,
        220 165,
        490 110
    "
    markerEnd="url(#arrow-end)"
  />

  {/* NOTIFYIQ */}
  <path
    d="
      M170 350
      C 360 395,
        220 445,
        400 455
    "
    markerEnd="url(#arrow-end)"
  />

  {/* FINOPS */}
  <path
    d="
      M170 350
      C 420 370,
        650 330,
        910 480
    "
    markerEnd="url(#arrow-end)"
  />

  {/* SECURETAG */}
  <path
    d="
      M170 350
      C 360 455,
        220 535,
        490 590
    "
    markerEnd="url(#arrow-end)"
  />


  {/* MAIN HORIZONTAL*/}
  <path d="M170 350 L1050 350" markerEnd="url(#arrow-end)"/>

</svg>

      </div>
    </div>
  );
};

export default Homepage;
