import React, { useState, useCallback } from 'react';
import ReactFlow, { 
  MiniMap, 
  Controls, 
  Background,
  useNodesState,
  useEdgesState 
} from 'reactflow';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import axios from 'axios';
import 'reactflow/dist/style.css';
import './App.css';

const API_BASE_URL = 'http://127.0.0.1:8000';

function App() {
  // States
  const [selectedFile, setSelectedFile] = useState(null);
  const [sourceDescription, setSourceDescription] = useState('');
  const [uploadStatus, setUploadStatus] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [pipeline, setPipeline] = useState(null);
  const [executionResult, setExecutionResult] = useState(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [activeTab, setActiveTab] = useState('upload'); // upload, analysis, pipeline, execution
  
  // ReactFlow states
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  // File upload handler
  const handleUpload = async () => {
    if (!selectedFile) {
      setUploadStatus('Please select a file');
      return;
    }

    setIsLoading(true);
    setUploadStatus('');
    setExecutionResult(null);

    try {
      // Upload file
      const formData = new FormData();
      formData.append('file', selectedFile);
      formData.append('source_description', sourceDescription);

      const uploadResponse = await axios.post(`${API_BASE_URL}/upload`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });

      setUploadStatus('File uploaded successfully!');

      // Generate pipeline
      const pipelineResponse = await axios.post(`${API_BASE_URL}/generate_pipeline`, null, {
        params: { filename: uploadResponse.data.filename }
      });

      const pipelineData = { ...pipelineResponse.data, filename: uploadResponse.data.filename };
      setPipeline(pipelineData);
      
      // Create flow visualization
      createFlowVisualization(pipelineData);
      
      setActiveTab('analysis');

    } catch (error) {
      console.error('Error:', error);
      setUploadStatus(`Error: ${error.response?.data?.detail || error.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  // Create flow visualization
  const createFlowVisualization = (pipelineData) => {
    const newNodes = [
      {
        id: '1',
        type: 'input',
        data: { label: `Source: ${pipelineData.data_analysis?.format_type || 'File'}` },
        position: { x: 100, y: 100 },
        style: { background: '#4CAF50', color: 'white', padding: 10, borderRadius: 8 }
      },
      {
        id: '2',
        data: { 
          label: `AI Analysis\n${pipelineData.data_analysis?.recommended_storage || 'Processing...'}` 
        },
        position: { x: 300, y: 100 },
        style: { background: '#2196F3', color: 'white', padding: 10, borderRadius: 8, whiteSpace: 'pre' }
      },
      {
        id: '3',
        data: { label: 'Transform' },
        position: { x: 500, y: 100 },
        style: { background: '#FF9800', color: 'white', padding: 10, borderRadius: 8 }
      },
      {
        id: '4',
        type: 'output',
        data: { label: `Target: ${pipelineData.data_analysis?.recommended_storage || 'Storage'}` },
        position: { x: 700, y: 100 },
        style: { background: '#9C27B0', color: 'white', padding: 10, borderRadius: 8 }
      },
    ];

    const newEdges = [
      { id: 'e1-2', source: '1', target: '2', animated: true },
      { id: 'e2-3', source: '2', target: '3', animated: true },
      { id: 'e3-4', source: '3', target: '4', animated: true },
    ];

    setNodes(newNodes);
    setEdges(newEdges);
  };

  // Execute pipeline
  const handleExecutePipeline = async () => {
    if (!pipeline || !pipeline.filename) return;

    setIsExecuting(true);
    setExecutionResult(null);

    try {
      const response = await axios.post(`${API_BASE_URL}/execute_pipeline/${pipeline.filename}`);
      setExecutionResult(response.data);
      setActiveTab('execution');
    } catch (error) {
      console.error('Execution error:', error);
      setExecutionResult({
        status: 'error',
        message: `Error: ${error.response?.data?.detail || error.message}`,
        error: true
      });
    } finally {
      setIsExecuting(false);
    }
  };

  return (
    <div className="App">
      {/* Header */}
      <header className="app-header">
        <h1>🚀 ETL Assistant - AI-Powered Data Engineering</h1>
        <p>Intelligent automation with AI + Hadoop HDFS + Airflow</p>
      </header>

      {/* Navigation Tabs */}
      <div className="tabs">
        <button 
          className={`tab ${activeTab === 'upload' ? 'active' : ''}`}
          onClick={() => setActiveTab('upload')}
        >
          📁 Upload
        </button>
        <button 
          className={`tab ${activeTab === 'analysis' ? 'active' : ''}`}
          onClick={() => setActiveTab('analysis')}
          disabled={!pipeline}
        >
          🤖 AI Analysis
        </button>
        <button 
          className={`tab ${activeTab === 'pipeline' ? 'active' : ''}`}
          onClick={() => setActiveTab('pipeline')}
          disabled={!pipeline}
        >
          📊 Pipeline
        </button>
        <button 
          className={`tab ${activeTab === 'execution' ? 'active' : ''}`}
          onClick={() => setActiveTab('execution')}
          disabled={!executionResult}
        >
          ✅ Execution
        </button>
      </div>

      {/* Content */}
      <div className="content">
        {/* Upload Tab */}
        {activeTab === 'upload' && (
          <div className="upload-section">
            <div className="card">
              <h2>📁 Upload Data File</h2>
              <div className="upload-form">
                <input
                  type="file"
                  accept=".csv,.json,.xml"
                  onChange={(e) => setSelectedFile(e.target.files[0])}
                  className="file-input"
                />
                
                <textarea
                  placeholder="Optional: Describe your data source..."
                  value={sourceDescription}
                  onChange={(e) => setSourceDescription(e.target.value)}
                  className="description-input"
                  rows="3"
                />
                
                <button
                  onClick={handleUpload}
                  disabled={!selectedFile || isLoading}
                  className="btn btn-primary"
                >
                  {isLoading ? '⏳ Processing...' : '🚀 Analyze with AI'}
                </button>
                
                {uploadStatus && (
                  <div className={`status ${uploadStatus.includes('Error') ? 'error' : 'success'}`}>
                    {uploadStatus}
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Analysis Tab */}
        {activeTab === 'analysis' && pipeline && (
          <div className="analysis-section">
            {/* AI Reasoning Card */}
            <div className="card ai-reasoning-card">
              <h2>🤖 AI Analysis & Reasoning</h2>
              
              <div className="reasoning-section">
                <h3>📊 Data Type:</h3>
                <p className="data-type-badge">
                  {pipeline.data_analysis?.data_type || 'Unknown'}
                </p>
              </div>

              <div className="reasoning-section">
                <h3>💾 Recommended Storage:</h3>
                <div className="storage-recommendation">
                  <span className="storage-icon">
                    {pipeline.data_analysis?.recommended_storage === 'ClickHouse' && '⚡'}
                    {pipeline.data_analysis?.recommended_storage === 'PostgreSQL' && '🐘'}
                    {pipeline.data_analysis?.recommended_storage === 'HDFS' && '🗄️'}
                  </span>
                  <span className="storage-name">
                    {pipeline.data_analysis?.recommended_storage || 'N/A'}
                  </span>
                  {pipeline.data_analysis?.confidence && (
                    <span className="confidence-badge">
                      {(pipeline.data_analysis.confidence * 100).toFixed(0)}% confidence
                    </span>
                  )}
                </div>
              </div>

              <div className="reasoning-section">
                <h3>💡 AI Reasoning:</h3>
                <div className="reasoning-text">
                  {pipeline.data_analysis?.reasoning || pipeline.data_analysis?.explanation || 
                   'No reasoning available'}
                </div>
              </div>

              {pipeline.data_analysis?.partitioning_strategy && (
                <div className="reasoning-section">
                  <h3>📂 Strategy:</h3>
                  <ul className="strategy-list">
                    <li><strong>Partitioning:</strong> {pipeline.data_analysis.partitioning_strategy}
                      {pipeline.data_analysis.partitioning_column && 
                       ` by ${pipeline.data_analysis.partitioning_column}`}
                    </li>
                    {pipeline.data_analysis.indexes && (
                      <li><strong>Indexes:</strong> {pipeline.data_analysis.indexes.join(', ')}</li>
                    )}
                    {pipeline.data_analysis.update_schedule && (
                      <li><strong>Schedule:</strong> {pipeline.data_analysis.update_schedule}</li>
                    )}
                  </ul>
                </div>
              )}

              {pipeline.data_analysis?.growth_estimation && (
                <div className="reasoning-section">
                  <h3>📈 Growth Estimation:</h3>
                  <p>{pipeline.data_analysis.growth_estimation}</p>
                </div>
              )}
            </div>

            {/* Statistics Card */}
            <div className="card">
              <h2>📊 Data Statistics</h2>
              <div className="stats-grid">
                <div className="stat-item">
                  <span className="stat-label">Rows:</span>
                  <span className="stat-value">{pipeline.data_analysis?.total_rows?.toLocaleString() || 'N/A'}</span>
                </div>
                <div className="stat-item">
                  <span className="stat-label">Columns:</span>
                  <span className="stat-value">{pipeline.data_analysis?.total_columns || 'N/A'}</span>
                </div>
                <div className="stat-item">
                  <span className="stat-label">Format:</span>
                  <span className="stat-value">{pipeline.data_analysis?.format_type || 'N/A'}</span>
                </div>
                <div className="stat-item">
                  <span className="stat-label">File Size:</span>
                  <span className="stat-value">
                    {pipeline.data_analysis?.file_size_mb 
                      ? `${pipeline.data_analysis.file_size_mb.toFixed(2)} MB`
                      : 'N/A'}
                  </span>
                </div>
              </div>
            </div>

            {/* Actions */}
            <div className="card">
              <h2>⚙️ Actions</h2>
              <div className="actions">
                <button
                  onClick={() => setActiveTab('pipeline')}
                  className="btn btn-secondary"
                >
                  📊 View Pipeline
                </button>
                <button
                  onClick={handleExecutePipeline}
                  disabled={isExecuting}
                  className="btn btn-primary"
                >
                  {isExecuting ? '⏳ Executing...' : '▶️ Execute Pipeline'}
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Pipeline Tab */}
        {activeTab === 'pipeline' && pipeline && (
          <div className="pipeline-section">
            {/* Flow Visualization */}
            <div className="card" style={{ height: '400px' }}>
              <h2>📊 Pipeline Visualization</h2>
              <div style={{ height: '350px' }}>
                <ReactFlow
                  nodes={nodes}
                  edges={edges}
                  onNodesChange={onNodesChange}
                  onEdgesChange={onEdgesChange}
                  fitView
                >
                  <MiniMap />
                  <Controls />
                  <Background />
                </ReactFlow>
              </div>
            </div>

            {/* Generated Code */}
            {pipeline.etl_code && (
              <div className="card">
                <h2>🐍 Generated Python ETL Code</h2>
                <SyntaxHighlighter 
                  language="python" 
                  style={vscDarkPlus}
                  showLineNumbers
                >
                  {pipeline.etl_code}
                </SyntaxHighlighter>
              </div>
            )}

            {pipeline.ddl && (
              <div className="card">
                <h2>🗄️ Generated SQL DDL</h2>
                <SyntaxHighlighter 
                  language="sql" 
                  style={vscDarkPlus}
                  showLineNumbers
                >
                  {pipeline.ddl}
                </SyntaxHighlighter>
              </div>
            )}

            {pipeline.airflow_dag && (
              <div className="card">
                <h2>🔄 Airflow DAG</h2>
                <SyntaxHighlighter 
                  language="python" 
                  style={vscDarkPlus}
                  showLineNumbers
                >
                  {pipeline.airflow_dag}
                </SyntaxHighlighter>
              </div>
            )}
          </div>
        )}

        {/* Execution Tab */}
        {activeTab === 'execution' && executionResult && (
          <div className="execution-section">
            <div className="card">
              <h2>✅ Execution Results</h2>
              <div className={`execution-status ${executionResult.error ? 'error' : 'success'}`}>
                <h3>{executionResult.error ? '❌ Error' : '✅ Success'}</h3>
                <p>{executionResult.message}</p>
                
                {executionResult.loaded_rows && (
                  <div className="execution-stats">
                    <p><strong>Rows Loaded:</strong> {executionResult.loaded_rows.toLocaleString()}</p>
                    <p><strong>Target:</strong> {executionResult.target_storage}</p>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Footer */}
      <footer className="app-footer">
        <p>🏆 ETL Assistant - ДИТ Москвы 2025 | Powered by AI & Hadoop HDFS & Airflow</p>
      </footer>
    </div>
  );
}

export default App;
