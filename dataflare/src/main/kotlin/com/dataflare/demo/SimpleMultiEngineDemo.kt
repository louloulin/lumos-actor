package com.dataflare.demo

import com.dataflare.core.DataProcessingSystem
import com.dataflare.engine.ExecutionEngineFactory
import com.dataflare.engine.FlowExecutionEngine
import com.dataflare.engine.ProtoActorExecutionEngine


import com.dataflare.workflow.Connection
import com.dataflare.workflow.InputConfig
import com.dataflare.workflow.OutputConfig
import com.dataflare.workflow.ProcessorConfig
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.io.File

private val logger = KotlinLogging.logger {}

/**
 * 简单多引擎模式演示
 */
object SimpleMultiEngineDemo {

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        logger.info { "Starting SimpleMultiEngineDemo" }

        // 注册执行引擎
        ExecutionEngineFactory.registerEngine(FlowExecutionEngine())
        ExecutionEngineFactory.registerEngine(ProtoActorExecutionEngine(actor.proto.ActorSystem("demo-system")))

        // 创建示例数据
        createSampleData()

        // 创建数据处理系统
        val dataProcessingSystem = DataProcessingSystem("simple-multi-engine-system")

        try {
            // 启动数据处理系统
            dataProcessingSystem.start()

            // 创建 Flow 引擎工作流
            val flowWorkflowConfig = createWorkflowConfig("flow-workflow", "flow")

            // 部署 Flow 工作流
            val flowWorkflowHandle = dataProcessingSystem.createWorkflow(flowWorkflowConfig, "flow")

            // 启动 Flow 工作流
            dataProcessingSystem.startWorkflow(flowWorkflowHandle)

            // 等待 Flow 工作流执行完成
            logger.info { "Waiting for Flow workflow to complete..." }
            delay(2000)

            // 停止 Flow 工作流
            dataProcessingSystem.stopWorkflow(flowWorkflowHandle)

            // 显示 Flow 工作流处理结果
            displayResults("flow")

            // 创建 ProtoActor 引擎工作流
            val protoActorWorkflowConfig = createWorkflowConfig("protoactor-workflow", "protoactor")

            // 部署 ProtoActor 工作流
            val protoActorWorkflowHandle = dataProcessingSystem.createWorkflow(protoActorWorkflowConfig, "protoactor")

            // 启动 ProtoActor 工作流
            dataProcessingSystem.startWorkflow(protoActorWorkflowHandle)

            // 等待 ProtoActor 工作流执行完成
            logger.info { "Waiting for ProtoActor workflow to complete..." }
            delay(5000) // 增加等待时间，确保 ProtoActor 引擎有足够时间处理消息

            // 停止 ProtoActor 工作流
            dataProcessingSystem.stopWorkflow(protoActorWorkflowHandle)

            // 显示 ProtoActor 工作流处理结果
            displayResults("protoactor")

            // 停止数据处理系统
            dataProcessingSystem.stop()

        } catch (e: Exception) {
            logger.error(e) { "Error in SimpleMultiEngineDemo" }
        } finally {
            // 数据处理系统已经在 try 块中关闭
        }

        logger.info { "SimpleMultiEngineDemo completed" }
    }

    /**
     * 创建示例数据
     */
    private fun createSampleData() {
        logger.info { "Creating sample data" }

        val productsJson = """
            [
              {"id": 1, "name": "Smartphone X", "price": 799.99, "category": "Electronics", "stock": 120},
              {"id": 2, "name": "Coffee Maker", "price": 49.99, "category": "Home", "stock": 35}
            ]
        """.trimIndent()

        // 确保目录存在
        File("dataflare/data").mkdirs()
        File("dataflare/data/flow").mkdirs()
        File("dataflare/data/protoactor").mkdirs()

        // 写入示例数据
        File("dataflare/data/products.json").writeText(productsJson)

        logger.info { "Sample data created at dataflare/data/products.json" }
    }

    /**
     * 创建工作流配置
     */
    private fun createWorkflowConfig(name: String, engineName: String): WorkflowConfig {
        logger.info { "Creating workflow config: $name with engine: $engineName" }

        val outputDir = "dataflare/data/$engineName"

        return WorkflowConfig(
            name = name,
            inputs = mapOf(
                "products" to InputConfig(
                    "file",
                    mapOf(
                        "path" to "dataflare/data/products.json",
                        "format" to "json"
                    )
                )
            ),
            processors = mapOf(
                "parse" to ProcessorConfig(
                    "mapping",
                    listOf("products"),
                    mapOf("mapping" to "json.parse(content)")
                )
            ),
            outputs = mapOf(
                "output" to OutputConfig(
                    "file",
                    listOf("parse"),
                    mapOf(
                        "path" to "$outputDir/output.json",
                        "format" to "json"
                    )
                )
            ),
            connections = listOf(
                Connection("products", "parse"),
                Connection("parse", "output")
            ),
            engineName = engineName
        )
    }

    /**
     * 显示处理结果
     */
    private fun displayResults(engineName: String) {
        logger.info { "Processing results for $engineName engine:" }

        val outputDir = "dataflare/data/$engineName"

        try {
            val outputFile = File("$outputDir/output.json")

            if (outputFile.exists()) {
                logger.info { "$engineName - Output:" }
                logger.info { outputFile.readText() }
            } else {
                logger.warn { "$engineName - Output file not found" }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error displaying results for $engineName" }
        }
    }
}
