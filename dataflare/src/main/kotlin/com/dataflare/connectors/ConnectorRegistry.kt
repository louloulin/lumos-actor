package com.dataflare.connectors

import com.dataflare.connectors.database.PostgresConfig
import com.dataflare.connectors.database.PostgresConnectorFactory
import com.dataflare.connectors.database.MySQLConfig
import com.dataflare.connectors.database.MySQLConnectorFactory
import com.dataflare.connectors.queue.RedisConfig
import com.dataflare.connectors.queue.RedisConnectorFactory
import com.dataflare.processors.FilterConfig
import com.dataflare.processors.FilterProcessorFactory
import com.dataflare.processors.MappingConfig
import com.dataflare.processors.MappingProcessorFactory
import com.dataflare.processors.Processor
import com.dataflare.processors.ProcessorConfig
import com.dataflare.processors.ProcessorFactory
import com.dataflare.processors.aggregation.AggregationConfig
import com.dataflare.processors.aggregation.AggregationProcessorFactory
import com.dataflare.processors.json.JSONProcessorConfig
import com.dataflare.processors.json.JSONProcessorFactory
import com.dataflare.processors.script.JavaScriptConfig
import com.dataflare.processors.script.JavaScriptProcessorFactory
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * 连接器注册表 - 管理所有可用的连接器和处理器
 */
class ConnectorRegistry(private val system: actor.proto.ActorSystem? = null) {
    private val inputFactories = mutableMapOf<String, ConnectorFactory>()
    private val outputFactories = mutableMapOf<String, ConnectorFactory>()
    private val processorFactories = mutableMapOf<String, ProcessorFactory>()

    init {
        // 注册默认连接器
        registerConnector("file", FileConnectorFactory())
        registerConnector("postgres", PostgresConnectorFactory())
        registerConnector("mysql", MySQLConnectorFactory())
        registerConnector("redis", RedisConnectorFactory())

        // 注册默认处理器
        registerProcessor("mapping", MappingProcessorFactory())
        registerProcessor("filter", FilterProcessorFactory())
        registerProcessor("javascript", JavaScriptProcessorFactory())
        registerProcessor("json", JSONProcessorFactory())
        registerProcessor("aggregation", AggregationProcessorFactory())
    }

    /**
     * 初始化连接器注册表
     */
    fun initialize() {
        // 已在init中注册了默认连接器和处理器
        logger.info { "Connector registry initialized" }
    }

    /**
     * 注册连接器工厂
     */
    fun registerConnector(type: String, factory: ConnectorFactory) {
        inputFactories[type] = factory
        outputFactories[type] = factory
        logger.info { "Registered connector: $type" }
    }

    /**
     * 注册处理器工厂
     */
    fun registerProcessor(type: String, factory: ProcessorFactory) {
        processorFactories[type] = factory
        logger.info { "Registered processor: $type" }
    }

    /**
     * 创建输入连接器
     */
    fun createInput(type: String, config: Map<String, Any>): Input {
        val factory = inputFactories[type] ?: throw IllegalArgumentException("Unknown input type: $type")

        val connectorConfig = when (type) {
            "file" -> FileConfig(
                type = type,
                path = config["path"] as String,
                format = config["format"] as? String ?: "json",
                append = config["append"] as? Boolean ?: false
            )
            "postgres" -> PostgresConfig(
                type = type,
                connectionString = config["connectionString"] as String,
                table = config["table"] as String,
                columns = config["columns"] as? List<String> ?: emptyList(),
                query = config["query"] as? String ?: "",
                batchSize = config["batchSize"] as? Int ?: 100
            )
            "mysql" -> MySQLConfig(
                type = type,
                connectionString = config["connectionString"] as String,
                table = config["table"] as String,
                columns = config["columns"] as? List<String> ?: emptyList(),
                query = config["query"] as? String ?: "",
                batchSize = config["batchSize"] as? Int ?: 100,
                useSSL = config["useSSL"] as? Boolean ?: false,
                allowPublicKeyRetrieval = config["allowPublicKeyRetrieval"] as? Boolean ?: false
            )
            "redis" -> RedisConfig(
                type = type,
                host = config["host"] as? String ?: "localhost",
                port = config["port"] as? Int ?: 6379,
                password = config["password"] as? String,
                database = config["database"] as? Int ?: 0,
                key = config["key"] as String,
                listMode = config["listMode"] as? Boolean ?: true,
                channelMode = config["channelMode"] as? Boolean ?: false,
                batchSize = config["batchSize"] as? Int ?: 100,
                timeout = config["timeout"] as? Int ?: 2000
            )
            else -> throw IllegalArgumentException("Unknown input type: $type")
        }

        return factory.createInput(connectorConfig)
    }

    /**
     * 创建输出连接器
     */
    fun createOutput(type: String, config: Map<String, Any>): Output {
        val factory = outputFactories[type] ?: throw IllegalArgumentException("Unknown output type: $type")

        val connectorConfig = when (type) {
            "file" -> FileConfig(
                type = type,
                path = config["path"] as String,
                format = config["format"] as? String ?: "json",
                append = config["append"] as? Boolean ?: false
            )
            "postgres" -> PostgresConfig(
                type = type,
                connectionString = config["connectionString"] as String,
                table = config["table"] as String,
                columns = config["columns"] as? List<String> ?: emptyList(),
                query = config["query"] as? String ?: "",
                batchSize = config["batchSize"] as? Int ?: 100
            )
            "mysql" -> MySQLConfig(
                type = type,
                connectionString = config["connectionString"] as String,
                table = config["table"] as String,
                columns = config["columns"] as? List<String> ?: emptyList(),
                query = config["query"] as? String ?: "",
                batchSize = config["batchSize"] as? Int ?: 100,
                useSSL = config["useSSL"] as? Boolean ?: false,
                allowPublicKeyRetrieval = config["allowPublicKeyRetrieval"] as? Boolean ?: false
            )
            "redis" -> RedisConfig(
                type = type,
                host = config["host"] as? String ?: "localhost",
                port = config["port"] as? Int ?: 6379,
                password = config["password"] as? String,
                database = config["database"] as? Int ?: 0,
                key = config["key"] as String,
                listMode = config["listMode"] as? Boolean ?: true,
                channelMode = config["channelMode"] as? Boolean ?: false,
                batchSize = config["batchSize"] as? Int ?: 100,
                timeout = config["timeout"] as? Int ?: 2000
            )
            else -> throw IllegalArgumentException("Unknown output type: $type")
        }

        return factory.createOutput(connectorConfig)
    }

    /**
     * 创建处理器
     */
    fun createProcessor(type: String, config: Map<String, Any>): Processor {
        val factory = processorFactories[type] ?: throw IllegalArgumentException("Unknown processor type: $type")
        val processorConfig: ProcessorConfig = when (type) {
            "mapping" -> MappingConfig(
                mapping = config["mapping"] as String
            )
            "filter" -> FilterConfig(
                condition = config["condition"] as String
            )
            "javascript" -> JavaScriptConfig(
                script = config["script"] as String,
                functionName = config["functionName"] as? String ?: "process",
                initFunctionName = config["initFunctionName"] as? String ?: "init",
                engineName = config["engineName"] as? String ?: "nashorn"
            )
            "json" -> JSONProcessorConfig(
                operation = config["operation"] as? String ?: "parse",
                field = config["field"] as? String ?: "content",
                targetField = config["targetField"] as? String ?: "",
                pretty = config["pretty"] as? Boolean ?: false,
                arrayAsItems = config["arrayAsItems"] as? Boolean ?: true
            )
            "aggregation" -> AggregationConfig(
                operation = config["operation"] as String,
                field = config["field"] as String,
                groupBy = config["groupBy"] as? String,
                windowSize = config["windowSize"] as? Int ?: 10,
                windowType = config["windowType"] as? String ?: "count",
                outputField = config["outputField"] as? String
            )
            else -> throw IllegalArgumentException("Unknown processor type: $type")
        }
        return factory.create(processorConfig)
    }

    /**
     * 关闭注册表
     */
    suspend fun shutdown() {
        logger.info { "Shutting down connector registry" }
        // 清空所有工厂
        inputFactories.clear()
        outputFactories.clear()
        processorFactories.clear()
    }
}
