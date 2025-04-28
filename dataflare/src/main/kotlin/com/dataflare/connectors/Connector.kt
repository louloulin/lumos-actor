package com.dataflare.connectors

import com.dataflare.core.Message
import com.dataflare.core.ProcessingResult
import kotlinx.serialization.Serializable

/**
 * 连接器上下文，提供连接器运行时所需的上下文信息
 */
class Context(
    val workflowId: String,
    val connectorId: String,
    val metadata: Map<String, String> = emptyMap()
)

/**
 * 连接器配置基类
 */
@Serializable
abstract class Config {
    abstract val type: String
}

/**
 * 写入结果
 */
data class WriteResult(
    val success: Boolean,
    val recordsWritten: Int,
    val errors: List<String> = emptyList()
)

/**
 * 输入连接器接口，用于从外部系统读取数据
 */
interface Input {
    /**
     * 配置连接器
     */
    suspend fun configure(config: Config)

    /**
     * 连接到数据源
     */
    suspend fun connect(ctx: Context): Boolean

    /**
     * 读取数据
     */
    suspend fun read(ctx: Context): Message?

    /**
     * 关闭连接
     */
    suspend fun close(ctx: Context)
}

/**
 * 输出连接器接口，用于将数据写入外部系统
 */
interface Output {
    /**
     * 配置连接器
     */
    suspend fun configure(config: Config)

    /**
     * 连接到目标系统
     */
    suspend fun connect(ctx: Context): Boolean

    /**
     * 写入数据
     */
    suspend fun write(ctx: Context, batch: List<Message>): WriteResult

    /**
     * 关闭连接
     */
    suspend fun close(ctx: Context)
}

/**
 * 连接器注册表，管理所有可用的连接器
 */
class ConnectorRegistry(private val system: actor.proto.ActorSystem) {
    private val inputs = mutableMapOf<String, (Config) -> Input>()
    private val outputs = mutableMapOf<String, (Config) -> Output>()

    /**
     * 初始化连接器注册表
     */
    fun initialize() {
        // 注册内置连接器
    }

    /**
     * 注册输入连接器
     */
    fun registerInput(type: String, factory: (Config) -> Input) {
        inputs[type] = factory
    }

    /**
     * 注册输出连接器
     */
    fun registerOutput(type: String, factory: (Config) -> Output) {
        outputs[type] = factory
    }

    /**
     * 创建输入连接器
     */
    fun createInput(type: String, config: Config): Input {
        val factory = inputs[type] ?: throw IllegalArgumentException("Unknown input type: $type")
        return factory(config)
    }

    /**
     * 创建输出连接器
     */
    fun createOutput(type: String, config: Config): Output {
        val factory = outputs[type] ?: throw IllegalArgumentException("Unknown output type: $type")
        return factory(config)
    }

    /**
     * 关闭连接器注册表
     */
    suspend fun shutdown() {
        // 清理资源
    }
}
