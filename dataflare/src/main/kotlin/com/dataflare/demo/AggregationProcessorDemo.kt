package com.dataflare.demo

import com.dataflare.config.YamlConfigLoader
import com.dataflare.workflow.flow.FlowWorkflowEngine
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.io.File

private val logger = KotlinLogging.logger {}

/**
 * 聚合处理器演示程序
 */
fun main() = runBlocking {
    logger.info { "Starting Aggregation Processor Demo" }
    
    // 确保输出目录存在
    File("dataflare/data/output").mkdirs()
    
    // 加载工作流配置
    val configLoader = YamlConfigLoader()
    val workflowConfig = configLoader.loadWorkflowConfig("dataflare/src/main/resources/workflows/aggregation-workflow.yaml")
    
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
    val sumOutputFile = File("dataflare/data/output/sales_sum_by_category.json")
    val avgOutputFile = File("dataflare/data/output/sales_avg_by_category.json")
    
    if (sumOutputFile.exists()) {
        logger.info { "Sum output file created: ${sumOutputFile.absolutePath}" }
        logger.info { "Sum output content: ${sumOutputFile.readText()}" }
    } else {
        logger.error { "Sum output file not created" }
    }
    
    if (avgOutputFile.exists()) {
        logger.info { "Average output file created: ${avgOutputFile.absolutePath}" }
        logger.info { "Average output content: ${avgOutputFile.readText()}" }
    } else {
        logger.error { "Average output file not created" }
    }
}
