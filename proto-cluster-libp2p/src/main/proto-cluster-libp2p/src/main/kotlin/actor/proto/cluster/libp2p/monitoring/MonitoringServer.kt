package actor.proto.cluster.libp2p.monitoring

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.features.CORS
import io.ktor.features.ContentNegotiation
import io.ktor.gson.gson
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.io.File
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * MonitoringServer 提供 Web 界面和 API 来监控集群状态
 */
class MonitoringServer(
    private val monitor: ClusterMonitor,
    private val port: Int = 8080
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var server: io.ktor.server.engine.ApplicationEngine? = null
    private val gson: Gson = GsonBuilder().setPrettyPrinting().create()
    
    /**
     * 启动监控服务器
     */
    fun start() {
        logger.info { "Starting monitoring server on port $port" }
        
        scope.launch {
            try {
                server = embeddedServer(Netty, port = port) {
                    configureServer()
                }
                server?.start(wait = false)
                
                logger.info { "Monitoring server started on http://localhost:$port" }
            } catch (e: Exception) {
                logger.error(e) { "Error starting monitoring server" }
            }
        }
    }
    
    /**
     * 停止监控服务器
     */
    fun stop() {
        logger.info { "Stopping monitoring server" }
        
        server?.stop(1000, 5000)
        server = null
    }
    
    /**
     * 配置服务器
     */
    private fun Application.configureServer() {
        // 安装 CORS
        install(CORS) {
            anyHost()
        }
        
        // 安装 ContentNegotiation
        install(ContentNegotiation) {
            gson {
                setPrettyPrinting()
            }
        }
        
        // 配置路由
        routing {
            // 首页
            get("/") {
                val html = getIndexHtml()
                call.respondText(html, ContentType.Text.Html)
            }
            
            // 集群状态 API
            get("/api/cluster/status") {
                val status = monitor.getClusterStatus()
                call.respond(status)
            }
            
            // 所有指标 API
            get("/api/metrics") {
                val metrics = monitor.getAllMetrics()
                call.respond(metrics)
            }
            
            // 单个指标 API
            get("/api/metrics/{name}") {
                val name = call.parameters["name"]
                if (name != null) {
                    val value = monitor.getMetric(name)
                    call.respond(mapOf("name" to name, "value" to value))
                } else {
                    call.respond(HttpStatusCode.BadRequest, "Metric name is required")
                }
            }
            
            // 指标历史数据 API
            get("/api/metrics/{name}/history") {
                val name = call.parameters["name"]
                if (name != null) {
                    val history = monitor.getMetricHistory(name)
                    call.respond(history)
                } else {
                    call.respond(HttpStatusCode.BadRequest, "Metric name is required")
                }
            }
        }
    }
    
    /**
     * 获取首页 HTML
     */
    private fun getIndexHtml(): String {
        return """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Protoactor Cluster Monitor</title>
                <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
                <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
                <style>
                    body {
                        padding: 20px;
                    }
                    .card {
                        margin-bottom: 20px;
                    }
                    .metric-value {
                        font-size: 24px;
                        font-weight: bold;
                    }
                    .chart-container {
                        height: 300px;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1 class="mb-4">Protoactor Cluster Monitor</h1>
                    
                    <div class="row">
                        <div class="col-md-6">
                            <div class="card">
                                <div class="card-header">
                                    <h5>Cluster Status</h5>
                                </div>
                                <div class="card-body">
                                    <div id="cluster-status">Loading...</div>
                                </div>
                            </div>
                        </div>
                        
                        <div class="col-md-6">
                            <div class="card">
                                <div class="card-header">
                                    <h5>Key Metrics</h5>
                                </div>
                                <div class="card-body">
                                    <div id="key-metrics">Loading...</div>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="row">
                        <div class="col-md-6">
                            <div class="card">
                                <div class="card-header">
                                    <h5>Members</h5>
                                </div>
                                <div class="card-body">
                                    <div id="members-table">Loading...</div>
                                </div>
                            </div>
                        </div>
                        
                        <div class="col-md-6">
                            <div class="card">
                                <div class="card-header">
                                    <h5>Message Metrics</h5>
                                </div>
                                <div class="card-body">
                                    <div class="chart-container">
                                        <canvas id="messages-chart"></canvas>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="row">
                        <div class="col-md-6">
                            <div class="card">
                                <div class="card-header">
                                    <h5>DHT Metrics</h5>
                                </div>
                                <div class="card-body">
                                    <div class="chart-container">
                                        <canvas id="dht-chart"></canvas>
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <div class="col-md-6">
                            <div class="card">
                                <div class="card-header">
                                    <h5>Failure Detection</h5>
                                </div>
                                <div class="card-body">
                                    <div class="chart-container">
                                        <canvas id="failure-chart"></canvas>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                <script>
                    // 图表实例
                    let messagesChart = null;
                    let dhtChart = null;
                    let failureChart = null;
                    
                    // 刷新间隔（毫秒）
                    const refreshInterval = 5000;
                    
                    // 初始化页面
                    function initPage() {
                        // 初始化图表
                        initCharts();
                        
                        // 加载数据
                        refreshData();
                        
                        // 定期刷新数据
                        setInterval(refreshData, refreshInterval);
                    }
                    
                    // 初始化图表
                    function initCharts() {
                        // 消息图表
                        messagesChart = new Chart(document.getElementById('messages-chart'), {
                            type: 'line',
                            data: {
                                labels: [],
                                datasets: [
                                    {
                                        label: 'Messages Sent',
                                        data: [],
                                        borderColor: 'rgba(75, 192, 192, 1)',
                                        backgroundColor: 'rgba(75, 192, 192, 0.2)',
                                        tension: 0.1
                                    },
                                    {
                                        label: 'Messages Received',
                                        data: [],
                                        borderColor: 'rgba(153, 102, 255, 1)',
                                        backgroundColor: 'rgba(153, 102, 255, 0.2)',
                                        tension: 0.1
                                    }
                                ]
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                scales: {
                                    y: {
                                        beginAtZero: true
                                    }
                                }
                            }
                        });
                        
                        // DHT 图表
                        dhtChart = new Chart(document.getElementById('dht-chart'), {
                            type: 'line',
                            data: {
                                labels: [],
                                datasets: [
                                    {
                                        label: 'DHT Operations',
                                        data: [],
                                        borderColor: 'rgba(255, 159, 64, 1)',
                                        backgroundColor: 'rgba(255, 159, 64, 0.2)',
                                        tension: 0.1
                                    },
                                    {
                                        label: 'Cache Size',
                                        data: [],
                                        borderColor: 'rgba(255, 99, 132, 1)',
                                        backgroundColor: 'rgba(255, 99, 132, 0.2)',
                                        tension: 0.1
                                    }
                                ]
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                scales: {
                                    y: {
                                        beginAtZero: true
                                    }
                                }
                            }
                        });
                        
                        // 故障检测图表
                        failureChart = new Chart(document.getElementById('failure-chart'), {
                            type: 'line',
                            data: {
                                labels: [],
                                datasets: [
                                    {
                                        label: 'Heartbeats',
                                        data: [],
                                        borderColor: 'rgba(54, 162, 235, 1)',
                                        backgroundColor: 'rgba(54, 162, 235, 0.2)',
                                        tension: 0.1
                                    },
                                    {
                                        label: 'Suspected Nodes',
                                        data: [],
                                        borderColor: 'rgba(255, 206, 86, 1)',
                                        backgroundColor: 'rgba(255, 206, 86, 0.2)',
                                        tension: 0.1
                                    }
                                ]
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                scales: {
                                    y: {
                                        beginAtZero: true
                                    }
                                }
                            }
                        });
                    }
                    
                    // 刷新数据
                    function refreshData() {
                        // 获取集群状态
                        fetch('/api/cluster/status')
                            .then(response => response.json())
                            .then(data => updateClusterStatus(data))
                            .catch(error => console.error('Error fetching cluster status:', error));
                        
                        // 获取所有指标
                        fetch('/api/metrics')
                            .then(response => response.json())
                            .then(data => updateMetrics(data))
                            .catch(error => console.error('Error fetching metrics:', error));
                        
                        // 获取消息指标历史数据
                        fetch('/api/metrics/remote.messages.sent/history')
                            .then(response => response.json())
                            .then(data => {
                                if (messagesChart) {
                                    updateChart(messagesChart, data, 0);
                                }
                            })
                            .catch(error => console.error('Error fetching message history:', error));
                        
                        fetch('/api/metrics/remote.messages.received/history')
                            .then(response => response.json())
                            .then(data => {
                                if (messagesChart) {
                                    updateChart(messagesChart, data, 1);
                                }
                            })
                            .catch(error => console.error('Error fetching message history:', error));
                        
                        // 获取 DHT 指标历史数据
                        fetch('/api/metrics/dht.operations.get/history')
                            .then(response => response.json())
                            .then(data => {
                                if (dhtChart) {
                                    updateChart(dhtChart, data, 0);
                                }
                            })
                            .catch(error => console.error('Error fetching DHT history:', error));
                        
                        fetch('/api/metrics/dht.cache.size/history')
                            .then(response => response.json())
                            .then(data => {
                                if (dhtChart) {
                                    updateChart(dhtChart, data, 1);
                                }
                            })
                            .catch(error => console.error('Error fetching DHT history:', error));
                        
                        // 获取故障检测指标历史数据
                        fetch('/api/metrics/failure_detector.heartbeats.received/history')
                            .then(response => response.json())
                            .then(data => {
                                if (failureChart) {
                                    updateChart(failureChart, data, 0);
                                }
                            })
                            .catch(error => console.error('Error fetching failure detection history:', error));
                        
                        fetch('/api/metrics/failure_detector.nodes.suspected_count/history')
                            .then(response => response.json())
                            .then(data => {
                                if (failureChart) {
                                    updateChart(failureChart, data, 1);
                                }
                            })
                            .catch(error => console.error('Error fetching failure detection history:', error));
                    }
                    
                    // 更新集群状态
                    function updateClusterStatus(data) {
                        const statusHtml = `
                            <div class="row">
                                <div class="col-md-6">
                                    <div class="mb-3">
                                        <div class="text-muted">Total Members</div>
                                        <div class="metric-value">${data.totalMembers}</div>
                                    </div>
                                </div>
                                <div class="col-md-6">
                                    <div class="mb-3">
                                        <div class="text-muted">Alive Members</div>
                                        <div class="metric-value">${data.aliveMembers}</div>
                                    </div>
                                </div>
                            </div>
                            <div class="row">
                                <div class="col-md-6">
                                    <div class="mb-3">
                                        <div class="text-muted">Leaving Members</div>
                                        <div class="metric-value">${data.leavingMembers}</div>
                                    </div>
                                </div>
                                <div class="col-md-6">
                                    <div class="mb-3">
                                        <div class="text-muted">Unavailable Members</div>
                                        <div class="metric-value">${data.unavailableMembers}</div>
                                    </div>
                                </div>
                            </div>
                        `;
                        
                        document.getElementById('cluster-status').innerHTML = statusHtml;
                        
                        // 更新成员表格
                        let membersHtml = `
                            <table class="table table-striped">
                                <thead>
                                    <tr>
                                        <th>ID</th>
                                        <th>Host</th>
                                        <th>Port</th>
                                        <th>Status</th>
                                    </tr>
                                </thead>
                                <tbody>
                        `;
                        
                        data.memberList.forEach(member => {
                            membersHtml += `
                                <tr>
                                    <td>${member.id}</td>
                                    <td>${member.host}</td>
                                    <td>${member.port}</td>
                                    <td>${member.status}</td>
                                </tr>
                            `;
                        });
                        
                        membersHtml += `
                                </tbody>
                            </table>
                        `;
                        
                        document.getElementById('members-table').innerHTML = membersHtml;
                    }
                    
                    // 更新指标
                    function updateMetrics(data) {
                        const keyMetrics = [
                            { name: 'Messages Sent', value: data['remote.messages.sent'] || 0 },
                            { name: 'Messages Received', value: data['remote.messages.received'] || 0 },
                            { name: 'DHT Cache Size', value: data['dht.cache.size'] || 0 },
                            { name: 'DHT Hit Ratio', value: data['dht.cache.hit_ratio'] || 0 },
                            { name: 'Suspected Nodes', value: data['failure_detector.nodes.suspected_count'] || 0 },
                            { name: 'Compression Ratio', value: data['remote.compression.ratio'] || 0 }
                        ];
                        
                        let metricsHtml = `
                            <div class="row">
                        `;
                        
                        keyMetrics.forEach(metric => {
                            metricsHtml += `
                                <div class="col-md-6">
                                    <div class="mb-3">
                                        <div class="text-muted">${metric.name}</div>
                                        <div class="metric-value">${metric.value}</div>
                                    </div>
                                </div>
                            `;
                        });
                        
                        metricsHtml += `
                            </div>
                        `;
                        
                        document.getElementById('key-metrics').innerHTML = metricsHtml;
                    }
                    
                    // 更新图表
                    function updateChart(chart, data, datasetIndex) {
                        if (!data || data.length === 0) return;
                        
                        // 提取时间戳和值
                        const timestamps = data.map(point => new Date(point.timestamp).toLocaleTimeString());
                        const values = data.map(point => point.value);
                        
                        // 更新图表数据
                        chart.data.labels = timestamps;
                        chart.data.datasets[datasetIndex].data = values;
                        
                        // 更新图表
                        chart.update();
                    }
                    
                    // 页面加载完成后初始化
                    document.addEventListener('DOMContentLoaded', initPage);
                </script>
            </body>
            </html>
        """.trimIndent()
    }
}
