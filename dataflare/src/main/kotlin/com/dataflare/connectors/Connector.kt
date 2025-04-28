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
 * 连接器工厂接口
 */
interface ConnectorFactory {
    fun createInput(config: Config): Input
    fun createOutput(config: Config): Output
}

