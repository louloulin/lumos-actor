package com.dataflare.processors.json

import com.dataflare.connectors.Context
import com.dataflare.core.Message
import com.dataflare.processors.Processor
import com.dataflare.processors.ProcessorConfig
import com.dataflare.processors.ProcessorFactory
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import org.json.JSONArray
import org.json.JSONObject
import org.json.JSONException

private val logger = KotlinLogging.logger {}

/**
 * JSON 处理器配置
 */
@Serializable
data class JSONProcessorConfig(
    override val type: String = "json",
    val operation: String = "parse",
    val field: String = "content",
    val targetField: String = "",
    val pretty: Boolean = false,
    val arrayAsItems: Boolean = true
) : ProcessorConfig()

/**
 * JSON 处理器 - 处理 JSON 数据
 */
class JSONProcessor(private val config: JSONProcessorConfig) : Processor {

    override suspend fun process(ctx: Context, message: Message): List<Message> {
        logger.info { "Processing message with JSON processor: operation=${config.operation}, field=${config.field}" }

        return when (config.operation.lowercase()) {
            "parse" -> parseJSON(message)
            "stringify" -> stringifyJSON(message)
            "validate" -> validateJSON(message)
            "select" -> selectJSON(message)
            "merge" -> mergeJSON(message)
            else -> {
                logger.warn { "Unknown JSON operation: ${config.operation}" }
                listOf(message)
            }
        }
    }

    override suspend fun close(ctx: Context) {
        // 清理资源
    }

    /**
     * 解析 JSON 字符串为结构化数据
     */
    private fun parseJSON(message: Message): List<Message> {
        if (!message.payload.containsKey(config.field) || message.payload[config.field] !is String) {
            logger.warn { "Field ${config.field} not found or not a string" }
            return listOf(message)
        }

        val jsonContent = message.payload[config.field] as String
        try {
            // 尝试解析 JSON
            val jsonData = if (jsonContent.trim().startsWith("[")) {
                // JSON 数组
                val jsonArray = JSONArray(jsonContent)
                if (config.arrayAsItems) {
                    // 将 JSON 数组转换为 items 列表
                    val result = mutableListOf<Map<String, Any>>()
                    for (i in 0 until jsonArray.length()) {
                        val obj = jsonArray.getJSONObject(i)
                        val map = mutableMapOf<String, Any>()
                        obj.keys().forEach { key ->
                            map[key] = jsonToKotlin(obj.get(key))
                        }
                        result.add(map)
                    }
                    mapOf("items" to result)
                } else {
                    // 将 JSON 数组转换为列表
                    val result = mutableListOf<Any>()
                    for (i in 0 until jsonArray.length()) {
                        result.add(jsonToKotlin(jsonArray.get(i)))
                    }
                    mapOf(config.targetField.ifEmpty { "array" } to result)
                }
            } else {
                // JSON 对象
                val jsonObject = JSONObject(jsonContent)
                val map = mutableMapOf<String, Any>()
                jsonObject.keys().forEach { key ->
                    map[key] = jsonToKotlin(jsonObject.get(key))
                }
                map
            }

            // 创建新消息
            val newPayload = if (config.targetField.isNotEmpty() && !jsonContent.trim().startsWith("[")) {
                val result = message.payload.toMutableMap()
                result[config.targetField] = jsonData
                result
            } else {
                if (config.field == "content" && !config.targetField.isNotEmpty()) {
                    // 如果字段是 content 且没有指定目标字段，则替换整个 payload
                    @Suppress("UNCHECKED_CAST")
                    jsonData as Map<String, Any>
                } else {
                    // 否则合并到现有 payload
                    val result = message.payload.toMutableMap()
                    @Suppress("UNCHECKED_CAST")
                    result.putAll(jsonData as Map<String, Any>)
                    result
                }
            }

            return listOf(Message.create(newPayload, message.metadata))
        } catch (e: JSONException) {
            logger.error(e) { "Failed to parse JSON: ${e.message}" }
            // 添加错误信息到消息
            val newPayload = message.payload.toMutableMap()
            newPayload["json_error"] = e.message ?: "Unknown JSON parsing error"
            return listOf(Message.create(newPayload, message.metadata))
        }
    }

    /**
     * 将结构化数据转换为 JSON 字符串
     */
    private fun stringifyJSON(message: Message): List<Message> {
        val field = config.field
        val targetField = config.targetField.ifEmpty { "${field}_json" }

        try {
            val value = if (field == "*") {
                // 将整个 payload 转换为 JSON
                message.payload
            } else if (message.payload.containsKey(field)) {
                // 将特定字段转换为 JSON
                message.payload[field]
            } else {
                logger.warn { "Field $field not found" }
                return listOf(message)
            }

            // 转换为 JSON 字符串
            val jsonString = when (value) {
                is Map<*, *> -> JSONObject(value.mapKeys { it.key.toString() }).toString(if (config.pretty) 2 else 0)
                is List<*> -> JSONArray(value).toString(if (config.pretty) 2 else 0)
                else -> JSONObject().put("value", value).toString(if (config.pretty) 2 else 0)
            }

            // 创建新消息
            val newPayload = message.payload.toMutableMap()
            newPayload[targetField] = jsonString
            return listOf(Message.create(newPayload, message.metadata))
        } catch (e: Exception) {
            logger.error(e) { "Failed to stringify to JSON: ${e.message}" }
            // 添加错误信息到消息
            val newPayload = message.payload.toMutableMap()
            newPayload["json_error"] = e.message ?: "Unknown JSON stringification error"
            return listOf(Message.create(newPayload, message.metadata))
        }
    }

    /**
     * 验证 JSON 字符串是否有效
     */
    private fun validateJSON(message: Message): List<Message> {
        if (!message.payload.containsKey(config.field) || message.payload[config.field] !is String) {
            logger.warn { "Field ${config.field} not found or not a string" }
            return listOf(message)
        }

        val jsonContent = message.payload[config.field] as String
        val targetField = config.targetField.ifEmpty { "json_valid" }

        try {
            // 尝试解析 JSON 以验证其有效性
            if (jsonContent.trim().startsWith("[")) {
                JSONArray(jsonContent)
            } else {
                JSONObject(jsonContent)
            }

            // JSON 有效
            val newPayload = message.payload.toMutableMap()
            newPayload[targetField] = true
            return listOf(Message.create(newPayload, message.metadata))
        } catch (e: JSONException) {
            logger.info { "Invalid JSON: ${e.message}" }
            // JSON 无效
            val newPayload = message.payload.toMutableMap()
            newPayload[targetField] = false
            newPayload["json_error"] = e.message ?: "Unknown JSON validation error"
            return listOf(Message.create(newPayload, message.metadata))
        }
    }

    /**
     * 从 JSON 对象中选择特定字段
     */
    private fun selectJSON(message: Message): List<Message> {
        // 字段路径，例如 "user.address.city"
        val fieldPath = config.field.split(".")
        val targetField = config.targetField.ifEmpty { fieldPath.last() }

        try {
            // 递归查找字段
            var current: Any? = message.payload

            for (field in fieldPath) {
                when (current) {
                    is Map<*, *> -> {
                        current = current[field]
                    }
                    is List<*> -> {
                        if (field.toIntOrNull() != null) {
                            val index = field.toInt()
                            if (index >= 0 && index < current.size) {
                                current = current[index]
                            } else {
                                logger.warn { "Index $index out of bounds for list of size ${current.size}" }
                                return listOf(message)
                            }
                        } else {
                            logger.warn { "Expected integer index for list, got $field" }
                            return listOf(message)
                        }
                    }
                    else -> {
                        logger.warn { "Cannot navigate through non-container type: ${current?.javaClass?.simpleName}" }
                        return listOf(message)
                    }
                }
            }

            if (current == null) {
                logger.warn { "Field path $fieldPath resolved to null" }
                return listOf(message)
            }

            // 创建新消息
            val newPayload = message.payload.toMutableMap()
            newPayload[targetField] = current
            return listOf(Message.create(newPayload, message.metadata))
        } catch (e: Exception) {
            logger.error(e) { "Failed to select JSON field: ${e.message}" }
            // 添加错误信息到消息
            val newPayload = message.payload.toMutableMap()
            newPayload["json_error"] = e.message ?: "Unknown JSON selection error"
            return listOf(Message.create(newPayload, message.metadata))
        }
    }

    /**
     * 合并多个 JSON 对象
     */
    private fun mergeJSON(message: Message): List<Message> {
        val fields = config.field.split(",").map { it.trim() }
        val targetField = config.targetField.ifEmpty { "merged" }

        try {
            val result = mutableMapOf<String, Any>()

            // 合并所有指定的字段
            for (field in fields) {
                if (message.payload.containsKey(field)) {
                    val value = message.payload[field]
                    if (value is Map<*, *>) {
                        @Suppress("UNCHECKED_CAST")
                        result.putAll(value as Map<String, Any>)
                    } else {
                        logger.warn { "Field $field is not a map, skipping" }
                    }
                } else {
                    logger.warn { "Field $field not found, skipping" }
                }
            }

            // 创建新消息
            val newPayload = message.payload.toMutableMap()
            newPayload[targetField] = result
            return listOf(Message.create(newPayload, message.metadata))
        } catch (e: Exception) {
            logger.error(e) { "Failed to merge JSON objects: ${e.message}" }
            // 添加错误信息到消息
            val newPayload = message.payload.toMutableMap()
            newPayload["json_error"] = e.message ?: "Unknown JSON merge error"
            return listOf(Message.create(newPayload, message.metadata))
        }
    }

    /**
     * 将 JSON 对象转换为 Kotlin 对象
     */
    private fun jsonToKotlin(value: Any): Any {
        return when (value) {
            is JSONObject -> {
                val map = mutableMapOf<String, Any>()
                value.keys().forEach { key ->
                    val jsonValue = value.get(key)
                    if (jsonValue != JSONObject.NULL) {
                        map[key] = jsonToKotlin(jsonValue)
                    } else {
                        map[key] = ""
                    }
                }
                map
            }
            is JSONArray -> {
                val list = mutableListOf<Any>()
                for (i in 0 until value.length()) {
                    val jsonValue = value.get(i)
                    if (jsonValue != JSONObject.NULL) {
                        list.add(jsonToKotlin(jsonValue))
                    } else {
                        list.add("")
                    }
                }
                list
            }
            JSONObject.NULL -> ""
            else -> value
        }
    }
}

/**
 * JSON 处理器工厂
 */
class JSONProcessorFactory : ProcessorFactory {
    override fun create(config: ProcessorConfig): Processor {
        if (config !is JSONProcessorConfig) {
            throw IllegalArgumentException("Expected JSONProcessorConfig, got ${config::class.simpleName}")
        }
        return JSONProcessor(config)
    }
}
