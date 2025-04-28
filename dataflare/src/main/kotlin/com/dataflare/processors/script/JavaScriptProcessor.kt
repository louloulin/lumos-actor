package com.dataflare.processors.script

import com.dataflare.connectors.Context
import com.dataflare.core.Message
import com.dataflare.processors.Processor
import com.dataflare.processors.ProcessorConfig
import com.dataflare.processors.ProcessorFactory
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import javax.script.Invocable
import javax.script.ScriptEngine
import javax.script.ScriptEngineManager

private val logger = KotlinLogging.logger {}

/**
 * JavaScript 处理器配置
 */
@Serializable
data class JavaScriptConfig(
    override val type: String = "javascript",
    val script: String,
    val functionName: String = "process",
    val initFunctionName: String = "init",
    val engineName: String = "nashorn"
) : ProcessorConfig()

/**
 * JavaScript 脚本执行处理器
 */
class JavaScriptProcessor(private val config: JavaScriptConfig) : Processor {
    private val engine: ScriptEngine
    private var initialized = false

    init {
        // 创建 JavaScript 引擎
        val manager = ScriptEngineManager()
        engine = manager.getEngineByName(config.engineName)
            ?: throw IllegalStateException("JavaScript engine '${config.engineName}' not found")

        // 评估脚本
        try {
            engine.eval(config.script)
            logger.info { "JavaScript script evaluated successfully" }
        } catch (e: Exception) {
            logger.error(e) { "Error evaluating JavaScript script: ${e.message}" }
            throw e
        }
    }

    override suspend fun process(ctx: Context, message: Message): List<Message> {
        // 确保初始化
        if (!initialized) {
            initialize(ctx)
        }

        try {
            // 将消息转换为 JavaScript 对象
            val jsMessage = message.payload

            // 调用处理函数
            val result = (engine as Invocable).invokeFunction(config.functionName, jsMessage, ctx.properties)

            // 处理结果
            return when (result) {
                null -> emptyList()
                is Map<*, *> -> {
                    // 安全地转换 Map<*, *> 到 Map<String, Any?>
                    val safeMap = result.entries.associate { entry ->
                        val key = entry.key?.toString() ?: ""
                        val value = entry.value
                        key to value
                    }
                    listOf(Message.create(safeMap))
                }
                is List<*> -> {
                    // 过滤并安全地转换 List 中的 Map 元素
                    result.filterIsInstance<Map<*, *>>()
                        .map { map ->
                            val safeMap = map.entries.associate { entry ->
                                val key = entry.key?.toString() ?: ""
                                val value = entry.value
                                key to value
                            }
                            Message.create(safeMap)
                        }
                }
                else -> listOf(Message.create(mapOf("result" to result)))
            }
        } catch (e: Exception) {
            logger.error(e) { "Error executing JavaScript function '${config.functionName}': ${e.message}" }
            throw e
        }
    }

    override suspend fun close(ctx: Context) {
        logger.info { "Closing JavaScript processor" }
        // JavaScript 引擎不需要显式关闭
    }

    private fun initialize(ctx: Context) {
        try {
            // 检查是否存在初始化函数
            if (config.initFunctionName.isNotEmpty()) {
                (engine as Invocable).invokeFunction(config.initFunctionName, ctx.properties)
                logger.info { "JavaScript initialization function '${config.initFunctionName}' executed successfully" }
            }
            initialized = true
        } catch (e: Exception) {
            logger.error(e) { "Error executing JavaScript initialization function '${config.initFunctionName}': ${e.message}" }
            // 即使初始化失败，也继续处理
        }
    }
}

/**
 * JavaScript 处理器工厂
 */
class JavaScriptProcessorFactory : ProcessorFactory {
    override fun create(config: ProcessorConfig): Processor {
        if (config !is JavaScriptConfig) {
            throw IllegalArgumentException("Expected JavaScriptConfig, got ${config::class.simpleName}")
        }
        return JavaScriptProcessor(config)
    }
}
