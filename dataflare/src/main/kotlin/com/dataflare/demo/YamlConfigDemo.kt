package com.dataflare.demo

import com.dataflare.config.YamlConfigLoader
import com.dataflare.core.DataProcessingSystem
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.io.File

private val logger = KotlinLogging.logger {}

/**
 * YAML 配置演示
 * 
 * 本演示展示了如何从 YAML 文件加载工作流配置
 */
object YamlConfigDemo {

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        logger.info { "Starting YamlConfigDemo" }

        // 创建示例数据
        createSampleData()

        // 创建数据处理系统
        val dataProcessingSystem = DataProcessingSystem("yaml-config-demo")

        try {
            // 启动数据处理系统
            dataProcessingSystem.start()

            // 加载 YAML 配置
            val configLoader = YamlConfigLoader()
            val workflowConfig = configLoader.loadWorkflowConfig("dataflare/src/main/resources/workflows/product-workflow.yaml")

            // 打印工作流配置
            logger.info { "Loaded workflow config: ${workflowConfig.name}" }
            logger.info { "Inputs: ${workflowConfig.inputs.keys}" }
            logger.info { "Processors: ${workflowConfig.processors.keys}" }
            logger.info { "Outputs: ${workflowConfig.outputs.keys}" }
            logger.info { "Connections: ${workflowConfig.connections.size}" }

            // 确保输出目录存在
            File("dataflare/data/output").mkdirs()

            // 部署工作流
            val workflowHandle = dataProcessingSystem.createWorkflow(workflowConfig)

            // 启动工作流
            dataProcessingSystem.startWorkflow(workflowHandle)

            // 等待工作流执行完成
            logger.info { "Waiting for workflow to complete..." }
            delay(5000)

            // 停止工作流
            dataProcessingSystem.stopWorkflow(workflowHandle)

            // 显示处理结果
            displayResults()

            // 停止数据处理系统
            dataProcessingSystem.stop()

        } catch (e: Exception) {
            logger.error(e) { "Error in YamlConfigDemo" }
        }

        logger.info { "YamlConfigDemo completed" }
    }

    /**
     * 创建示例数据
     */
    private fun createSampleData() {
        logger.info { "Creating sample data" }

        val productsJson = """
            [
              {"id": 1, "name": "Smartphone", "price": 799.99, "category": "Electronics", "stock": 120},
              {"id": 2, "name": "Laptop", "price": 1299.99, "category": "Electronics", "stock": 45},
              {"id": 3, "name": "Coffee Maker", "price": 89.99, "category": "Home", "stock": 30},
              {"id": 4, "name": "Headphones", "price": 199.99, "category": "Electronics", "stock": 75},
              {"id": 5, "name": "Smart Watch", "price": 299.99, "category": "Electronics", "stock": 25},
              {"id": 6, "name": "Blender", "price": 79.99, "category": "Home", "stock": 50},
              {"id": 7, "name": "Microwave", "price": 149.99, "category": "Home", "stock": 15},
              {"id": 8, "name": "TV", "price": 899.99, "category": "Electronics", "stock": 35},
              {"id": 9, "name": "Desk Lamp", "price": 24.99, "category": "Home", "stock": 65},
              {"id": 10, "name": "Tablet", "price": 349.99, "category": "Electronics", "stock": 30}
            ]
        """.trimIndent()

        // 确保目录存在
        File("dataflare/data").mkdirs()

        // 写入示例数据
        File("dataflare/data/products.json").writeText(productsJson)

        logger.info { "Sample data created at dataflare/data/products.json" }
    }

    /**
     * 显示处理结果
     */
    private fun displayResults() {
        logger.info { "Processing results:" }

        try {
            val electronicsFile = File("dataflare/data/output/electronics.json")
            val expensiveFile = File("dataflare/data/output/expensive.json")
            val lowStockFile = File("dataflare/data/output/low-stock.json")

            if (electronicsFile.exists()) {
                logger.info { "Electronics products:" }
                logger.info { electronicsFile.readText() }
            } else {
                logger.warn { "Electronics file not found" }
            }

            if (expensiveFile.exists()) {
                logger.info { "Expensive products:" }
                logger.info { expensiveFile.readText() }
            } else {
                logger.warn { "Expensive file not found" }
            }

            if (lowStockFile.exists()) {
                logger.info { "Low stock products:" }
                logger.info { lowStockFile.readText() }
            } else {
                logger.warn { "Low stock file not found" }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error displaying results" }
        }
    }
}
