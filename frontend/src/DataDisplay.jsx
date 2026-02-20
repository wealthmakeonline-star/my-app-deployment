import React, { useState, useEffect } from 'react';

function DataDisplay() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Because of the proxy setup in step 3, we can just use the path
    fetch('/api/data') 
      .then(response => response.json())
      .then(data => {
        setData(data);
        setLoading(false);
      })
      .catch(error => console.error('Error fetching data:', error));
  }, []); // The empty array ensures this runs only once after the initial render

  if (loading) return <div>Loading data from Python...</div>;

  return (
    <div>
      <h2>Data from Backend:</h2>
      <p>ID: **{data.id}**</p>
      <p>Status: **{data.status}**</p>
      <p>Source: **{data.source}**</p>
    </div>
  );
}
export default DataDisplay;