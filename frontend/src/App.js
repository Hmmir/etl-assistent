import React, { useState } from 'react';
import axios from 'axios';
import './App.css';

const API_BASE_URL = 'http://127.0.0.1:8000';

function App() {
  // Состояние компонента
  const [selectedFile, setSelectedFile] = useState(null);
  const [sourceDescription, setSourceDescription] = useState('');
  const [uploadStatus, setUploadStatus] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [pipeline, setPipeline] = useState(null);
  const [executionResult, setExecutionResult] = useState(null);
  const [isExecuting, setIsExecuting] = useState(false);
  
  // Состояния для управления инфраструктурой
  const [infrastructureStatus, setInfrastructureStatus] = useState(null);
  const [isDeploying, setIsDeploying] = useState(false);
  const [deploymentResult, setDeploymentResult] = useState(null);

  // Обработчик выбора файла
  const handleFileSelect = (event) => {
    const file = event.target.files[0];
    setSelectedFile(file);
    setUploadStatus('');
    setPipeline(null);
  };

  // Обработчик загрузки файла
  const handleUpload = async () => {
    if (!selectedFile) {
      setUploadStatus('Пожалуйста, выберите файл');
      return;
    }

    setIsLoading(true);
    setUploadStatus('');
    setExecutionResult(null);

    try {
      // Создание FormData для отправки файла
      const formData = new FormData();
      formData.append('file', selectedFile);
      formData.append('source_description', sourceDescription);

      // Загрузка файла
      const uploadResponse = await axios.post(`${API_BASE_URL}/upload`, formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });

      setUploadStatus('Файл загружен успешно!');

      // Генерация пайплайна
      const pipelineResponse = await axios.post(`${API_BASE_URL}/generate_pipeline`, null, {
        params: { filename: uploadResponse.data.filename }
      });

      setPipeline({...pipelineResponse.data, filename: uploadResponse.data.filename});

    } catch (error) {
      console.error('Ошибка:', error);
      setUploadStatus(`Ошибка: ${error.response?.data?.detail || error.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  // Обработчик выполнения пайплайна
  const handleExecutePipeline = async () => {
    if (!pipeline || !pipeline.filename) {
      return;
    }

    setIsExecuting(true);
    setExecutionResult(null);

    try {
      const response = await axios.post(`${API_BASE_URL}/execute_pipeline/${pipeline.filename}`);
      setExecutionResult(response.data);
    } catch (error) {
      console.error('Ошибка выполнения пайплайна:', error);
      setExecutionResult({
        status: 'error',
        message: `Ошибка выполнения: ${error.response?.data?.detail || error.message}`,
        error: true
      });
    } finally {
      setIsExecuting(false);
    }
  };

  // Обработчик редактирования пайплайна
  const handleEditPipeline = () => {
    const editWindow = window.open('about:blank', '_blank');
    editWindow.document.write(`
      <html>
        <head>
          <title>Редактирование ETL пайплайна</title>
          <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .section { margin: 20px 0; padding: 15px; border: 1px solid #ccc; border-radius: 5px; }
            .section h3 { margin-top: 0; color: #333; }
            textarea { width: 100%; height: 200px; font-family: monospace; padding: 10px; }
            button { background: #007bff; color: white; border: none; padding: 10px 15px; border-radius: 5px; cursor: pointer; margin: 5px; }
            button:hover { background: #0056b3; }
            .info { background: #e7f3ff; padding: 10px; border-radius: 5px; margin: 10px 0; }
          </style>
        </head>
        <body>
          <h1>🔧 Редактирование ETL пайплайна</h1>
          <div class="info">
            <strong>Файл:</strong> ${pipeline.filename}<br>
            <strong>Хранилище:</strong> ${pipeline.storage_recommendation}<br>
            <strong>Статус:</strong> Готов к редактированию
          </div>
          
          <div class="section">
            <h3>📝 DDL скрипт</h3>
            <textarea id="ddl">${pipeline.ddl || 'DDL код не найден'}</textarea>
            <button onclick="copyToClipboard('ddl')">📋 Копировать</button>
            <button onclick="downloadAsFile('ddl', 'create_table.sql')">💾 Скачать</button>
          </div>
          
          <div class="section">
            <h3>⚙️ ETL процесс</h3>
            <textarea id="etl">${pipeline.etl_steps ? pipeline.etl_steps.join('\\n') : 'ETL шаги не найдены'}</textarea>
            <button onclick="copyToClipboard('etl')">📋 Копировать</button>
            <button onclick="downloadAsFile('etl', 'etl_steps.txt')">💾 Скачать</button>
          </div>
          
          <div class="section">
            <h3>💡 ИИ Рекомендации</h3>
            <textarea id="explanation">${pipeline.explanation || 'Объяснение не найдено'}</textarea>
            <button onclick="copyToClipboard('explanation')">📋 Копировать</button>
          </div>
          
          <div class="section">
            <button onclick="applyChanges()" style="background: #28a745;">✅ Применить изменения</button>
            <button onclick="resetChanges()" style="background: #dc3545;">🔄 Сбросить</button>
            <button onclick="window.close()" style="background: #6c757d;">❌ Закрыть</button>
          </div>
          
          <script>
            function copyToClipboard(elementId) {
              const element = document.getElementById(elementId);
              element.select();
              document.execCommand('copy');
              alert('📋 Скопировано в буфер обмена!');
            }
            
            function downloadAsFile(elementId, filename) {
              const content = document.getElementById(elementId).value;
              const blob = new Blob([content], { type: 'text/plain' });
              const url = window.URL.createObjectURL(blob);
              const a = document.createElement('a');
              a.href = url;
              a.download = filename;
              a.click();
              window.URL.revokeObjectURL(url);
              alert('💾 Файл скачан!');
            }
            
            function applyChanges() {
              alert('✅ Изменения применены! (В production версии здесь будет API вызов)');
            }
            
            function resetChanges() {
              if (confirm('🔄 Сбросить все изменения?')) {
                location.reload();
              }
            }
          </script>
        </body>
      </html>
    `);
  };

  // === INFRASTRUCTURE MANAGEMENT FUNCTIONS ===

  // Развертывание инфраструктуры
  const handleDeployInfrastructure = async () => {
    setIsDeploying(true);
    setDeploymentResult(null);

    try {
      const response = await axios.post(`${API_BASE_URL}/infrastructure/deploy`);
      setDeploymentResult(response.data);
      
      // Обновляем статус инфраструктуры
      checkInfrastructureStatus();
      
    } catch (error) {
      setDeploymentResult({
        success: false,
        error: error.response?.data?.detail || error.message
      });
    } finally {
      setIsDeploying(false);
    }
  };

  // Проверка статуса инфраструктуры
  const checkInfrastructureStatus = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/infrastructure/status`);
      setInfrastructureStatus(response.data);
    } catch (error) {
      console.error('Ошибка получения статуса инфраструктуры:', error);
    }
  };

  // Остановка инфраструктуры
  const handleStopInfrastructure = async () => {
    try {
      const response = await axios.post(`${API_BASE_URL}/infrastructure/stop`);
      setDeploymentResult(response.data);
      setInfrastructureStatus(null);
    } catch (error) {
      setDeploymentResult({
        success: false,
        error: error.response?.data?.detail || error.message
      });
    }
  };

  // Развертывание DAG в Airflow
  const handleDeployDAG = async () => {
    if (!executionResult?.filename) {
      alert('Сначала выполните пайплайн для генерации DAG');
      return;
    }

    try {
      const response = await axios.post(
        `${API_BASE_URL}/infrastructure/deploy_dag?dag_name=${executionResult.filename}`
      );
      
      alert(response.data.message + '\n\n' + response.data.note);
      
    } catch (error) {
      alert('Ошибка развертывания DAG: ' + (error.response?.data?.detail || error.message));
    }
  };

  // Открытие Airflow UI
  const handleOpenAirflowUI = () => {
    window.open('http://localhost:8080', '_blank');
  };

  // Проверяем статус инфраструктуры при загрузке компонента
  React.useEffect(() => {
    checkInfrastructureStatus();
  }, []);

  return (
    <div className="App">
      <header className="App-header">
        <h1>🤖 ETL Assistant</h1>
        <p>Интеллектуальный цифровой инженер данных</p>
      </header>

      <main className="App-main">
        <div className="upload-section">
          <h2>📁 Загрузка данных</h2>
          
          {/* Форма загрузки файла */}
          <div className="upload-form">
            <div className="file-input-group">
              <label htmlFor="file-input" className="file-input-label">
                Выберите файл (CSV, JSON, XML):
              </label>
              <input
                id="file-input"
                type="file"
                accept=".csv,.json,.xml"
                onChange={handleFileSelect}
                className="file-input"
              />
            </div>

            {selectedFile && (
              <div className="file-info">
                <p><strong>Выбранный файл:</strong> {selectedFile.name}</p>
                <p><strong>Размер:</strong> {(selectedFile.size / 1024 / 1024).toFixed(2)} MB</p>
              </div>
            )}

            <div className="description-input-group">
              <label htmlFor="source-description">
                Описание источника данных (опционально):
              </label>
              <textarea
                id="source-description"
                value={sourceDescription}
                onChange={(e) => setSourceDescription(e.target.value)}
                placeholder="Например: Данные посещений музеев Москвы за 2021-2025"
                rows="3"
                className="description-input"
              />
            </div>

            <button 
              onClick={handleUpload}
              disabled={!selectedFile || isLoading}
              className="upload-button"
            >
              {isLoading ? '⏳ Анализируем данные...' : '🚀 Сгенерировать пайплайн'}
            </button>
          </div>

          {/* Статус загрузки */}
          {uploadStatus && (
            <div className={`status-message ${uploadStatus.includes('Ошибка') ? 'error' : 'success'}`}>
              {uploadStatus}
            </div>
          )}
        </div>

        {/* Отображение сгенерированного пайплайна */}
        {pipeline && (
          <div className="pipeline-section">
            <h2>📊 Сгенерированный пайплайн</h2>
            
            <div className="pipeline-card">
              <h3>💾 Рекомендуемое хранилище</h3>
              <div className="storage-recommendation">
                <strong>{pipeline.storage_recommendation}</strong>
                <p>{pipeline.reason}</p>
              </div>
            </div>

            <div className="pipeline-card">
              <h3>🏗️ DDL скрипт</h3>
              <pre className="code-block">
                <code>{pipeline.ddl}</code>
              </pre>
            </div>

            <div className="pipeline-card">
              <h3>⚙️ ETL процесс</h3>
              <ol className="etl-steps">
                {pipeline.etl_steps.map((step, index) => (
                  <li key={index}>{step}</li>
                ))}
              </ol>
            </div>

            <div className="pipeline-card">
              <h3>💡 Пояснение ИИ</h3>
              <p className="explanation">{pipeline.explanation}</p>
            </div>

            <div className="pipeline-actions">
              <button 
                className="action-button primary"
                onClick={handleExecutePipeline}
                disabled={isExecuting}
              >
                {isExecuting ? '⏳ Выполняется...' : '▶️ Выполнить пайплайн'}
              </button>
              <button 
                className="action-button secondary"
                onClick={handleEditPipeline}
              >
                ✏️ Редактировать
              </button>
            </div>
          </div>
        )}

        {/* Результаты выполнения пайплайна */}
        {executionResult && (
          <div className="execution-section">
            <h2>🚀 Результат выполнения</h2>
            
            {executionResult.error ? (
              <div className="pipeline-card error">
                <h3>❌ Ошибка</h3>
                <p>{executionResult.message}</p>
              </div>
            ) : (
              <>
                <div className="pipeline-card success">
                  <h3>✅ {executionResult.message}</h3>
                  <div className="execution-stats">
                    <div className="stat">
                      <span className="stat-label">Обработано строк:</span>
                      <span className="stat-value">{executionResult.pipeline_stats?.total_rows?.toLocaleString()}</span>
                    </div>
                    <div className="stat">
                      <span className="stat-label">Колонок:</span>
                      <span className="stat-value">{executionResult.pipeline_stats?.total_columns}</span>
                    </div>
                    <div className="stat">
                      <span className="stat-label">Размер файла:</span>
                      <span className="stat-value">{executionResult.pipeline_stats?.file_size_mb} MB</span>
                    </div>
                    <div className="stat">
                      <span className="stat-label">Время выполнения:</span>
                      <span className="stat-value">{executionResult.pipeline_stats?.execution_time}</span>
                    </div>
                    <div className="stat">
                      <span className="stat-label">Хранилище:</span>
                      <span className="stat-value">{executionResult.pipeline_stats?.recommended_storage}</span>
                    </div>
                  </div>
                </div>

                <div className="pipeline-card">
                  <h3>📋 Этапы выполнения</h3>
                  <div className="execution-stages">
                    {Object.entries(executionResult.pipeline_stages || {}).map(([stage, status]) => (
                      <div key={stage} className="stage-item">
                        <span className="stage-name">{stage.toUpperCase()}:</span>
                        <span className="stage-status">{status}</span>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="pipeline-card">
                  <h3>📁 Сгенерированные файлы</h3>
                  <div className="generated-files">
                    {executionResult.generated_files?.map((file, index) => (
                      <div key={index} className="file-item">
                        📄 {file}
                      </div>
                    ))}
                  </div>
                </div>

                <div className="pipeline-card">
                  <h3>🔄 Интерактивные следующие шаги</h3>
                  
                  {/* Статус инфраструктуры */}
                  <div className="infrastructure-status">
                    <h4>📊 Статус инфраструктуры</h4>
                    {infrastructureStatus ? (
                      <div className="services-grid">
                        {Object.entries(infrastructureStatus.services || {}).map(([service, info]) => (
                          <div key={service} className={`service-status ${info.status}`}>
                            <span className="service-name">{service.toUpperCase()}</span>
                            <span className={`status-badge ${info.status}`}>
                              {info.status === 'ready' ? '✅ Готов' : info.status === 'starting' ? '⏳ Запуск' : '❓ Неизвестно'}
                            </span>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="no-infrastructure">🔴 Инфраструктура не развернута</p>
                    )}
                  </div>

                  {/* Интерактивные действия */}
                  <div className="action-steps">
                    <div className="step-item">
                      <div className="step-content">
                        <h4>1. 🚀 Развернуть инфраструктуру</h4>
                        <p>Автоматическое развертывание ClickHouse + PostgreSQL + Airflow через Docker</p>
                      </div>
                      <button 
                        className="step-button primary"
                        onClick={handleDeployInfrastructure}
                        disabled={isDeploying || (infrastructureStatus?.services && Object.values(infrastructureStatus.services).some(s => s.status === 'ready'))}
                      >
                        {isDeploying ? '⏳ Развертывание...' : '🚀 Развернуть'}
                      </button>
                    </div>

                    <div className="step-item">
                      <div className="step-content">
                        <h4>2. 🛩️ Загрузить DAG в Airflow</h4>
                        <p>Развертывание сгенерированного DAG в Airflow для автоматизации</p>
                      </div>
                      <button 
                        className="step-button secondary"
                        onClick={handleDeployDAG}
                        disabled={!infrastructureStatus?.services?.airflow || infrastructureStatus.services.airflow.status !== 'ready'}
                      >
                        🛩️ Загрузить DAG
                      </button>
                    </div>

                    <div className="step-item">
                      <div className="step-content">
                        <h4>3. 📊 Открыть Airflow UI</h4>
                        <p>Мониторинг и управление ETL пайплайнами через веб-интерфейс</p>
                      </div>
                      <button 
                        className="step-button success"
                        onClick={handleOpenAirflowUI}
                        disabled={!infrastructureStatus?.services?.airflow || infrastructureStatus.services.airflow.status !== 'ready'}
                      >
                        📊 Открыть UI
                      </button>
                    </div>

                    <div className="step-item">
                      <div className="step-content">
                        <h4>4. 🛑 Остановить инфраструктуру</h4>
                        <p>Полная остановка всех сервисов и освобождение ресурсов</p>
                      </div>
                      <button 
                        className="step-button danger"
                        onClick={handleStopInfrastructure}
                        disabled={!infrastructureStatus?.services}
                      >
                        🛑 Остановить
                      </button>
                    </div>
                  </div>

                  {/* Результат развертывания */}
                  {deploymentResult && (
                    <div className={`deployment-result ${deploymentResult.success ? 'success' : 'error'}`}>
                      <h4>{deploymentResult.success ? '✅ Успешно!' : '❌ Ошибка'}</h4>
                      <p>{deploymentResult.message || deploymentResult.error}</p>
                      
                      {deploymentResult.success && deploymentResult.endpoints && (
                        <div className="endpoints-info">
                          <h5>🔗 Доступные endpoints:</h5>
                          <ul>
                            <li><strong>ClickHouse:</strong> {deploymentResult.endpoints.clickhouse_http}</li>
                            <li><strong>PostgreSQL:</strong> {deploymentResult.endpoints.postgres}</li>
                            <li><strong>Airflow UI:</strong> {deploymentResult.endpoints.airflow_ui}</li>
                          </ul>
                        </div>
                      )}
                    </div>
                  )}
                </div>

                {executionResult.deployment_ready && (
                  <div className="deployment-status">
                    🎉 <strong>Готово к развертыванию!</strong> Все файлы сгенерированы и готовы к использованию.
                  </div>
                )}
              </>
            )}
          </div>
        )}
      </main>

      <footer className="App-footer">
        <p>MVP для хакатона ДИТ Москвы • Powered by YandexGPT & Yandex Cloud</p>
      </footer>
    </div>
  );
}

export default App;