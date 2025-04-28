package com.dataflare.processors

import com.dataflare.connectors.Context
import com.dataflare.core.Message
import mu.KotlinLogging
import org.json.JSONArray
import org.json.JSONObject

private val logger = KotlinLogging.logger {}

/**
 * 映射处理器 - 对消息进行简单的转换
 */
class MappingProcessor(private val mapping: String) : Processor {
    
    override suspend fun process(ctx: Context, message: Message): List<Message> {
        logger.info { "Processing message with mapping: $mapping" }
        
        // 处理映射表达式
        return when {
            mapping.startsWith(".") -> {
                // 简单的属性添加，例如 ".processed = true"
                val parts = mapping.substring(1).split("=").map { it.trim() }
                if (parts.size == 2) {
                    val key = parts[0]
                    val value = parseValue(parts[1])
                    val newPayload = message.payload.toMutableMap()
                    newPayload[key] = value
                    listOf(Message.create(newPayload, message.metadata))
                } else {
                    listOf(message)
                }
            }
            mapping.contains("=>") -> {
                // 转换映射，例如 "price => cost"
                val parts = mapping.split("=>").map { it.trim() }
                if (parts.size == 2) {
                    val sourceKey = parts[0]
                    val targetKey = parts[1]
                    if (message.payload.containsKey(sourceKey)) {
                        val newPayload = message.payload.toMutableMap()
                        newPayload[targetKey] = message.payload[sourceKey]!!
                        listOf(Message.create(newPayload, message.metadata))
                    } else {
                        listOf(message)
                    }
                } else {
                    listOf(message)
                }
            }
            mapping.startsWith("json.") -> {
                // JSON处理，例如 "json.parse(content)"
                val command = mapping.substring(5)
                if (command.startsWith("parse(") && command.endsWith(")")) {
                    val field = command.substring(6, command.length - 1)
                    if (message.payload.containsKey(field) && message.payload[field] is String) {
                        val jsonContent = message.payload[field] as String
                        try {
                            // 尝试解析JSON
                            val jsonData = if (jsonContent.trim().startsWith("[")) {
                                // JSON数组
                                val jsonArray = JSONArray(jsonContent)
                                val result = mutableListOf<Map<String, Any>>()
                                for (i in 0 until jsonArray.length()) {
                                    val obj = jsonArray.getJSONObject(i)
                                    val map = mutableMapOf<String, Any>()
                                    obj.keys().forEach { key ->
                                        map[key] = obj.get(key)
                                    }
                                    result.add(map)
                                }
                                mapOf("items" to result)
                            } else {
                                // JSON对象
                                val jsonObject = JSONObject(jsonContent)
                                val map = mutableMapOf<String, Any>()
                                jsonObject.keys().forEach { key ->
                                    map[key] = jsonObject.get(key)
                                }
                                map
                            }
                            listOf(Message.create(jsonData, message.metadata))
                        } catch (e: Exception) {
                            logger.error(e) { "Failed to parse JSON: $jsonContent" }
                            listOf(message)
                        }
                    } else {
                        listOf(message)
                    }
                } else {
                    listOf(message)
                }
            }
            else -> {
                // 默认不做任何处理
                listOf(message)
            }
        }
    }
    
    override suspend fun close(ctx: Context) {
        // 清理资源
    }
    
    private fun parseValue(valueStr: String): Any {
        return when {
            valueStr == "true" -> true
            valueStr == "false" -> false
            valueStr.toIntOrNull() != null -> valueStr.toInt()
            valueStr.toDoubleOrNull() != null -> valueStr.toDouble()
            valueStr.startsWith("\"") && valueStr.endsWith("\"") -> 
                valueStr.substring(1, valueStr.length - 1)
            else -> valueStr
        }
    }
}

/**
 * 映射处理器工厂
 */
class MappingProcessorFactory : ProcessorFactory {
    override fun create(config: ProcessorConfig): Processor {
        if (config !is MappingConfig) {
            throw IllegalArgumentException("Expected MappingConfig, got ${config::class.simpleName}")
        }
        return MappingProcessor(config.mapping)
    }
}
