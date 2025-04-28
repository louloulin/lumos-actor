package com.dataflare.workflow

import com.dataflare.core.Message

/**
 * 工作流执行结果
 */
data class WorkflowResult(
    val status: String,
    val messages: List<Message> = emptyList(),
    val errors: List<String> = emptyList()
)

/**
 * 工作流引擎接口
 */
interface WorkflowEngine {
    /**
     * 执行工作流
     */
    suspend fun execute(config: WorkflowConfig): WorkflowResult
}
