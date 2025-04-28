package com.dataflare.demo

import com.dataflare.config.YamlConfigLoader
import com.dataflare.workflow.WorkflowEngine
import com.dataflare.workflow.flow.FlowWorkflowEngine
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.io.File

private val logger = KotlinLogging.logger {}

/**
 * JSON 处理器演示程序
 */
fun main() = runBlocking {
    logger.info { "Starting JSON Processor Demo" }

    // 确保输出目录存在
    File("dataflare/data/output").mkdirs()

    // 加载工作流配置
    val configLoader = YamlConfigLoader()
    val workflowConfig = configLoader.loadWorkflowConfig("dataflare/src/main/resources/workflows/json-workflow.yaml")

    logger.info { "Loaded workflow: ${workflowConfig.name}" }

    // 创建工作流引擎
    val engine = FlowWorkflowEngine()

    // 执行工作流
    logger.info { "Executing workflow..." }
    val result = engine.execute(workflowConfig)

    logger.info { "Workflow execution completed with status: ${result.status}" }
    if (result.errors.isNotEmpty()) {
        logger.error { "Errors: ${result.errors}" }
    }

    // 检查输出文件
    val outputFile = File("dataflare/data/output/expensive_products.json")
    if (outputFile.exists()) {
        logger.info { "Output file created: ${outputFile.absolutePath}" }
        logger.info { "Output content: ${outputFile.readText()}" }
    } else {
        logger.error { "Output file not created" }
    }
}
