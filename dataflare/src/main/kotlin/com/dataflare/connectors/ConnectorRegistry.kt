package com.dataflare.connectors

import com.dataflare.processors.FilterConfig
import com.dataflare.processors.FilterProcessorFactory
import com.dataflare.processors.MappingConfig
import com.dataflare.processors.MappingProcessorFactory
import com.dataflare.processors.Processor
import com.dataflare.processors.ProcessorConfig
import com.dataflare.processors.ProcessorFactory
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

        // 注册默认处理器
        registerProcessor("mapping", MappingProcessorFactory())
        registerProcessor("filter", FilterProcessorFactory())
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
        val fileConfig = FileConfig(
            type = type,
            path = config["path"] as String,
            format = config["format"] as? String ?: "json",
            append = config["append"] as? Boolean ?: false
        )
        return factory.createInput(fileConfig)
    }

    /**
     * 创建输出连接器
     */
    fun createOutput(type: String, config: Map<String, Any>): Output {
        val factory = outputFactories[type] ?: throw IllegalArgumentException("Unknown output type: $type")
        val fileConfig = FileConfig(
            type = type,
            path = config["path"] as String,
            format = config["format"] as? String ?: "json",
            append = config["append"] as? Boolean ?: false
        )
        return factory.createOutput(fileConfig)
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
