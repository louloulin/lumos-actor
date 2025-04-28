package com.dataflare.core

import kotlinx.serialization.Serializable
import java.time.Instant

/**
 * 表示系统中处理的基本消息单元
 */
@Serializable
data class Message(
    val id: String,
    val timestamp: Long = Instant.now().toEpochMilli(),
    val payload: Map<String, Any?>,
    val metadata: Map<String, String> = emptyMap()
) {
    companion object {
        fun create(payload: Map<String, Any?>, metadata: Map<String, String> = emptyMap()): Message {
            return Message(
                id = java.util.UUID.randomUUID().toString(),
                payload = payload,
                metadata = metadata
            )
        }
    }
}

/**
 * 消息批处理，用于批量处理多个消息
 */
@Serializable
data class MessageBatch(
    val messages: List<Message>,
    val metadata: Map<String, String> = emptyMap()
) {
    companion object {
        fun create(messages: List<Message>, metadata: Map<String, String> = emptyMap()): MessageBatch {
            return MessageBatch(
                messages = messages,
                metadata = metadata
            )
        }
    }
}

/**
 * 表示处理结果的状态
 */
enum class ProcessingStatus {
    SUCCESS,
    PARTIAL_SUCCESS,
    FAILURE
}

/**
 * 表示消息处理的结果
 */
@Serializable
data class ProcessingResult(
    val status: ProcessingStatus,
    val messages: List<Message>,
    val errors: List<String> = emptyList(),
    val metadata: Map<String, String> = emptyMap()
) {
    companion object {
        fun success(messages: List<Message>, metadata: Map<String, String> = emptyMap()): ProcessingResult {
            return ProcessingResult(
                status = ProcessingStatus.SUCCESS,
                messages = messages,
                metadata = metadata
            )
        }

        fun failure(errors: List<String>, metadata: Map<String, String> = emptyMap()): ProcessingResult {
            return ProcessingResult(
                status = ProcessingStatus.FAILURE,
                messages = emptyList(),
                errors = errors,
                metadata = metadata
            )
        }

        fun partialSuccess(
            messages: List<Message>,
            errors: List<String>,
            metadata: Map<String, String> = emptyMap()
        ): ProcessingResult {
            return ProcessingResult(
                status = ProcessingStatus.PARTIAL_SUCCESS,
                messages = messages,
                errors = errors,
                metadata = metadata
            )
        }
    }
}
