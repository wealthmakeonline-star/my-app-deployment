import React from "react";
import { Link } from "react-router-dom";
import "./Header.css";
import autoEdgeLogo from "../assets/AutoEdge.png";

const Header = () => {
  return (
    <header className="header">
      <Link to="/" className="header-logo-link">
        <img src={autoEdgeLogo} alt="AutoEdge Logo" className="header-logo" />
      </Link>
      
      <nav className="header-nav">
        <Link to="/" className="nav-link">Home</Link>
        <Link to="/login" className="nav-link">Login</Link> 
      </nav>
    </header>
  );
};

export default Header;