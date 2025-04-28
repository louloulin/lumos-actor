package com.dataflare.workflow

import kotlinx.serialization.Serializable

/**
 * 工作流配置
 */
@Serializable
data class WorkflowConfig(
    val name: String,
    val inputs: Map<String, InputConfig>,
    val processors: Map<String, ProcessorConfig>,
    val outputs: Map<String, OutputConfig>,
    val connections: List<Connection>,
    val engineName: String = "flow" // 默认使用 Flow 执行引擎
)

/**
 * 输入配置
 */
@Serializable
data class InputConfig(
    val type: String,
    val config: Map<String, Any>
)

/**
 * 处理器配置
 */
@Serializable
data class ProcessorConfig(
    val type: String,
    val inputs: List<String>,
    val config: Map<String, Any>
)

/**
 * 输出配置
 */
@Serializable
data class OutputConfig(
    val type: String,
    val inputs: List<String>,
    val config: Map<String, Any>
)

/**
 * 连接
 */
@Serializable
data class Connection(
    val from: String,
    val to: String
)

/**
 * 工作流句柄
 */
@Serializable
data class WorkflowHandle(
    val id: String,
    val name: String,
    val engineName: String = "flow" // 默认使用 Flow 执行引擎
)

/**
 * 编译后的工作流
 */
@Serializable
data class CompiledWorkflow(
    val config: WorkflowConfig
)
