package com.dataflare.connectors

import com.dataflare.core.Message
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.nio.file.Paths

private val logger = KotlinLogging.logger {}

/**
 * 文件连接器配置
 */
@Serializable
data class FileConfig(
    override val type: String = "file",
    val path: String,
    val format: String = "json",
    val append: Boolean = false
) : Config()

/**
 * 文件输入连接器
 */
class FileInputConnector : Input {
    private lateinit var config: FileConfig
    private var isConnected = false
    
    override suspend fun configure(config: Config) {
        if (config !is FileConfig) {
            throw IllegalArgumentException("Expected FileConfig, got ${config::class.simpleName}")
        }
        this.config = config
    }
    
    override suspend fun connect(ctx: Context): Boolean {
        logger.info { "Connecting to file: ${config.path} (format: ${config.format})" }
        
        val file = resolveFile(config.path)
        if (!file.exists()) {
            logger.warn { "File not found: ${config.path}" }
            return false
        }
        
        isConnected = true
        return true
    }
    
    override suspend fun read(ctx: Context): Message? {
        if (!isConnected) {
            throw IllegalStateException("Not connected to file")
        }
        
        val file = resolveFile(config.path)
        if (!file.exists()) {
            logger.warn { "File not found: ${config.path}" }
            return null
        }
        
        val content = file.readText()
        
        return when (config.format.lowercase()) {
            "json" -> {
                // 简单处理，将整个JSON作为一条消息
                Message.create(mapOf("content" to content, "path" to config.path))
            }
            "text" -> {
                // 按行处理文本文件，但这里只返回第一行
                val line = file.readLines().firstOrNull() ?: return null
                Message.create(mapOf("line" to line, "path" to config.path))
            }
            else -> {
                logger.warn { "Unsupported format: ${config.format}" }
                Message.create(mapOf("content" to content, "path" to config.path))
            }
        }
    }
    
    override suspend fun close(ctx: Context) {
        logger.info { "Closing file input connector" }
        isConnected = false
    }
    
    private fun resolveFile(path: String): File {
        // 如果是相对路径，则相对于资源目录
        if (!path.startsWith("/")) {
            val resourceUrl = javaClass.classLoader.getResource(path)
            if (resourceUrl != null) {
                return File(resourceUrl.toURI())
            }
        }
        return File(path)
    }
}

/**
 * 文件输出连接器
 */
class FileOutputConnector : Output {
    private lateinit var config: FileConfig
    private var isConnected = false
    
    override suspend fun configure(config: Config) {
        if (config !is FileConfig) {
            throw IllegalArgumentException("Expected FileConfig, got ${config::class.simpleName}")
        }
        this.config = config
    }
    
    override suspend fun connect(ctx: Context): Boolean {
        logger.info { "Connecting to file: ${config.path} (append: ${config.append})" }
        
        val file = File(config.path)
        
        // 确保目录存在
        file.parentFile?.mkdirs()
        
        // 清空文件（如果不是追加模式）
        if (!config.append && file.exists()) {
            file.writeText("")
        }
        
        isConnected = true
        return true
    }
    
    override suspend fun write(ctx: Context, batch: List<Message>): WriteResult {
        if (!isConnected) {
            throw IllegalStateException("Not connected to file")
        }
        
        val file = File(config.path)
        var recordsWritten = 0
        val errors = mutableListOf<String>()
        
        try {
            // 写入消息
            batch.forEach { message ->
                val content = when {
                    message.payload.containsKey("content") -> message.payload["content"] as String
                    message.payload.containsKey("line") -> message.payload["line"] as String
                    else -> message.payload.toString()
                }
                
                if (config.append) {
                    file.appendText("$content\n")
                } else {
                    file.writeText("$content\n")
                }
                recordsWritten++
            }
        } catch (e: Exception) {
            logger.error(e) { "Error writing to file: ${config.path}" }
            errors.add("Error writing to file: ${e.message}")
        }
        
        return WriteResult(
            success = errors.isEmpty(),
            recordsWritten = recordsWritten,
            errors = errors
        )
    }
    
    override suspend fun close(ctx: Context) {
        logger.info { "Closing file output connector" }
        isConnected = false
    }
}

/**
 * 文件连接器工厂
 */
class FileConnectorFactory : ConnectorFactory {
    override fun createInput(config: Config): Input {
        return FileInputConnector().apply { 
            kotlinx.coroutines.runBlocking { configure(config) }
        }
    }
    
    override fun createOutput(config: Config): Output {
        return FileOutputConnector().apply {
            kotlinx.coroutines.runBlocking { configure(config) }
        }
    }
}
