package com.dataflare.processors

import com.dataflare.connectors.Context
import com.dataflare.core.Message
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * 过滤处理器 - 根据条件过滤消息
 */
class FilterProcessor(private val condition: String) : Processor {

    override suspend fun process(ctx: Context, message: Message): List<Message> {
        logger.info { "Filtering message with condition: $condition" }

        // 解析条件表达式
        val shouldKeep = when {
            condition.contains("==") -> {
                val parts = condition.split("==").map { it.trim() }
                if (parts.size == 2) {
                    val key = parts[0]
                    val value = parseValue(parts[1])

                    if (key.startsWith("items.") && message.payload.containsKey("items")) {
                        // 处理数组项
                        val itemsKey = key.substring(6)
                        val items = message.payload["items"] as? List<Map<String, Any>> ?: emptyList()
                        // 过滤出符合条件的项
                        val filteredItems = items.filter { item -> item[itemsKey] == value }
                        if (filteredItems.isNotEmpty()) {
                            // 创建新的消息，只包含符合条件的项
                            val newPayload = message.payload.toMutableMap()
                            newPayload["items"] = filteredItems
                            message.copy(payload = newPayload)
                            true
                        } else {
                            false
                        }
                    } else {
                        // 处理普通属性
                        message.payload[key] == value
                    }
                } else {
                    true
                }
            }
            condition.contains(">") -> {
                val parts = condition.split(">").map { it.trim() }
                if (parts.size == 2) {
                    val key = parts[0]
                    val value = parseValue(parts[1])

                    if (key.startsWith("items.") && message.payload.containsKey("items")) {
                        // 处理数组项
                        val itemsKey = key.substring(6)
                        val items = message.payload["items"] as? List<Map<String, Any>> ?: emptyList()
                        // 过滤出符合条件的项
                        val filteredItems = items.filter { item ->
                            val itemValue = item[itemsKey]
                            when {
                                itemValue is Int && value is Int -> itemValue > value
                                itemValue is Double && value is Double -> itemValue > value
                                itemValue is Int && value is Double -> itemValue > value
                                itemValue is Double && value is Int -> itemValue > value.toDouble()
                                else -> false
                            }
                        }
                        if (filteredItems.isNotEmpty()) {
                            // 创建新的消息，只包含符合条件的项
                            val newPayload = message.payload.toMutableMap()
                            newPayload["items"] = filteredItems
                            message.copy(payload = newPayload)
                            true
                        } else {
                            false
                        }
                    } else {
                        // 处理普通属性
                        val messageValue = message.payload[key]
                        when {
                            messageValue is Int && value is Int -> messageValue > value
                            messageValue is Double && value is Double -> messageValue > value
                            messageValue is Int && value is Double -> messageValue > value
                            messageValue is Double && value is Int -> messageValue > value.toDouble()
                            else -> false
                        }
                    }
                } else {
                    true
                }
            }
            condition.contains("<") -> {
                val parts = condition.split("<").map { it.trim() }
                if (parts.size == 2) {
                    val key = parts[0]
                    val value = parseValue(parts[1])

                    if (key.startsWith("items.") && message.payload.containsKey("items")) {
                        // 处理数组项
                        val itemsKey = key.substring(6)
                        val items = message.payload["items"] as? List<Map<String, Any>> ?: emptyList()
                        // 过滤出符合条件的项
                        val filteredItems = items.filter { item ->
                            val itemValue = item[itemsKey]
                            when {
                                itemValue is Int && value is Int -> itemValue < value
                                itemValue is Double && value is Double -> itemValue < value
                                itemValue is Int && value is Double -> itemValue < value
                                itemValue is Double && value is Int -> itemValue < value.toDouble()
                                else -> false
                            }
                        }
                        if (filteredItems.isNotEmpty()) {
                            // 创建新的消息，只包含符合条件的项
                            val newPayload = message.payload.toMutableMap()
                            newPayload["items"] = filteredItems
                            message.copy(payload = newPayload)
                            true
                        } else {
                            false
                        }
                    } else {
                        // 处理普通属性
                        val messageValue = message.payload[key]
                        when {
                            messageValue is Int && value is Int -> messageValue < value
                            messageValue is Double && value is Double -> messageValue < value
                            messageValue is Int && value is Double -> messageValue < value
                            messageValue is Double && value is Int -> messageValue < value.toDouble()
                            else -> false
                        }
                    }
                } else {
                    true
                }
            }
            condition.contains("contains") -> {
                val parts = condition.split("contains").map { it.trim() }
                if (parts.size == 2) {
                    val key = parts[0]
                    val value = parseValue(parts[1])

                    if (key.startsWith("items.") && message.payload.containsKey("items")) {
                        // 处理数组项
                        val itemsKey = key.substring(6)
                        val items = message.payload["items"] as? List<Map<String, Any>> ?: emptyList()
                        // 过滤出符合条件的项
                        val filteredItems = items.filter { item ->
                            val itemValue = item[itemsKey]
                            when {
                                itemValue is String && value is String -> itemValue.contains(value)
                                else -> false
                            }
                        }
                        if (filteredItems.isNotEmpty()) {
                            // 创建新的消息，只包含符合条件的项
                            val newPayload = message.payload.toMutableMap()
                            newPayload["items"] = filteredItems
                            message.copy(payload = newPayload)
                            true
                        } else {
                            false
                        }
                    } else {
                        // 处理普通属性
                        val messageValue = message.payload[key]
                        when {
                            messageValue is String && value is String -> messageValue.contains(value)
                            else -> false
                        }
                    }
                } else {
                    true
                }
            }
            else -> {
                // 默认不过滤
                true
            }
        }

        return if (shouldKeep) {
            listOf(message)
        } else {
            emptyList()
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
 * 过滤处理器工厂
 */
class FilterProcessorFactory : ProcessorFactory {
    override fun create(config: ProcessorConfig): Processor {
        if (config !is FilterConfig) {
            throw IllegalArgumentException("Expected FilterConfig, got ${config::class.simpleName}")
        }
        return FilterProcessor(config.condition)
    }
}
