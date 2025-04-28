package com.dataflare.processors

import com.dataflare.connectors.Context
import com.dataflare.core.Message
import kotlinx.serialization.Serializable

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

/**
 * 映射处理器 - 使用表达式语言转换数据
 */
class MappingProcessor(val mapping: String) : Processor {
    override suspend fun process(ctx: Context, message: Message): List<Message> {
        // 在实际实现中，这里会解析和执行映射表达式
        // 这里简单模拟一下转换过程
        val transformedPayload = message.payload.toMutableMap()
        transformedPayload["processed_by"] = "MappingProcessor"
        transformedPayload["mapping_expression"] = mapping
        
        return listOf(
            Message(
                id = message.id,
                timestamp = message.timestamp,
                payload = transformedPayload,
                metadata = message.metadata + mapOf("processor" to "mapping")
            )
        )
    }
    
    override suspend fun close(ctx: Context) {
        // 清理资源
    }
}

/**
 * 过滤处理器 - 根据条件过滤消息
 */
class FilterProcessor(val condition: String) : Processor {
    override suspend fun process(ctx: Context, message: Message): List<Message> {
        // 在实际实现中，这里会解析和执行条件表达式
        // 这里简单模拟一下过滤过程
        val shouldKeep = evaluateCondition(message)
        
        return if (shouldKeep) {
            listOf(
                Message(
                    id = message.id,
                    timestamp = message.timestamp,
                    payload = message.payload,
                    metadata = message.metadata + mapOf("filtered" to "false")
                )
            )
        } else {
            emptyList()
        }
    }
    
    private fun evaluateCondition(message: Message): Boolean {
        // 简单模拟条件评估
        // 在实际实现中，这里会解析和执行条件表达式
        return condition.contains("true") || 
               message.payload.any { (_, value) -> value.toString().contains("keep") }
    }
    
    override suspend fun close(ctx: Context) {
        // 清理资源
    }
}

/**
 * HTTP处理器 - 调用外部HTTP服务
 */
class HttpProcessor(val config: HttpConfig) : Processor {
    override suspend fun process(ctx: Context, message: Message): List<Message> {
        // 在实际实现中，这里会发送HTTP请求并处理响应
        // 这里简单模拟一下HTTP调用
        val responsePayload = message.payload.toMutableMap()
        responsePayload["http_url"] = config.url
        responsePayload["http_method"] = config.method
        responsePayload["http_response"] = "Simulated HTTP response"
        
        return listOf(
            Message(
                id = message.id,
                timestamp = message.timestamp,
                payload = responsePayload,
                metadata = message.metadata + mapOf("processor" to "http")
            )
        )
    }
    
    override suspend fun close(ctx: Context) {
        // 清理资源
    }
}
