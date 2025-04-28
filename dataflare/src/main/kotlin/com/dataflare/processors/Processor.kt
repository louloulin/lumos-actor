package com.dataflare.processors

import com.dataflare.connectors.Context
import com.dataflare.core.Message
import kotlinx.serialization.Serializable

/**
 * 处理器工厂接口
 */
interface ProcessorFactory {
    fun create(config: ProcessorConfig): Processor
}

/**
 * 处理器接口，用于处理和转换消息
 */
interface Processor {
    /**
     * 处理消息
     */
    suspend fun process(ctx: Context, message: Message): List<Message>

    /**
     * 关闭处理器
     */
    suspend fun close(ctx: Context)
}

/**
 * 处理器配置基类
 */
@Serializable
abstract class ProcessorConfig {
    abstract val type: String
}

/**
 * 映射处理器配置
 */
@Serializable
data class MappingConfig(
    override val type: String = "mapping",
    val mapping: String
) : ProcessorConfig()

/**
 * 过滤处理器配置
 */
@Serializable
data class FilterConfig(
    override val type: String = "filter",
    val condition: String
) : ProcessorConfig()

/**
 * HTTP处理器配置
 */
@Serializable
data class HttpConfig(
    override val type: String = "http",
    val url: String,
    val method: String = "GET",
    val headers: Map<String, String> = emptyMap(),
    val timeout: Long = 30000
) : ProcessorConfig()


