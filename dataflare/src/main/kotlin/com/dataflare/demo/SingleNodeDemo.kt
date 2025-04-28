package com.dataflare.demo

import com.dataflare.core.DataProcessingSystem
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
 * 单机模式演示
 */
object SingleNodeDemo {

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        logger.info { "Starting SingleNodeDemo" }

        // 创建示例数据
        createSampleData()

        // 创建数据处理系统
        val dataProcessingSystem = DataProcessingSystem("single-node-system")

        try {
            // 启动数据处理系统
            dataProcessingSystem.start()

            // 创建工作流
            val workflowConfig = createWorkflowConfig()

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
            logger.error(e) { "Error in SingleNodeDemo" }
        } finally {
            // 数据处理系统已经在 try 块中关闭
        }

        logger.info { "SingleNodeDemo completed" }
    }

    /**
     * 创建示例数据
     */
    private fun createSampleData() {
        logger.info { "Creating sample data" }

        val productsJson = """
            [
              {"id": 1, "name": "Smartphone X", "price": 799.99, "category": "Electronics", "stock": 120},
              {"id": 2, "name": "Coffee Maker", "price": 49.99, "category": "Home", "stock": 35},
              {"id": 3, "name": "Wireless Earbuds", "price": 129.99, "category": "Electronics", "stock": 78},
              {"id": 4, "name": "Running Shoes", "price": 89.99, "category": "Clothing", "stock": 42},
              {"id": 5, "name": "Smart TV", "price": 549.99, "category": "Electronics", "stock": 15},
              {"id": 6, "name": "Blender", "price": 39.99, "category": "Home", "stock": 53},
              {"id": 7, "name": "Laptop", "price": 1299.99, "category": "Electronics", "stock": 22},
              {"id": 8, "name": "T-shirt", "price": 19.99, "category": "Clothing", "stock": 150},
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
     * 创建工作流配置
     */
    private fun createWorkflowConfig(): WorkflowConfig {
        logger.info { "Creating workflow config" }

        return WorkflowConfig(
            name = "product-workflow",
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
                ),
                "filter-electronics" to ProcessorConfig(
                    "filter",
                    listOf("parse"),
                    mapOf("condition" to "items.category == \"Electronics\"")
                ),
                "filter-expensive" to ProcessorConfig(
                    "filter",
                    listOf("parse"),
                    mapOf("condition" to "items.price > 100")
                ),
                "filter-low-stock" to ProcessorConfig(
                    "filter",
                    listOf("parse"),
                    mapOf("condition" to "items.stock < 50")
                ),
                "add-metadata" to ProcessorConfig(
                    "mapping",
                    listOf("filter-electronics"),
                    mapOf("mapping" to ".processed_time = ${System.currentTimeMillis()}")
                )
            ),
            outputs = mapOf(
                "electronics-output" to OutputConfig(
                    "file",
                    listOf("add-metadata"),
                    mapOf(
                        "path" to "dataflare/data/electronics.json",
                        "format" to "json"
                    )
                ),
                "expensive-output" to OutputConfig(
                    "file",
                    listOf("filter-expensive"),
                    mapOf(
                        "path" to "dataflare/data/expensive.json",
                        "format" to "json"
                    )
                ),
                "low-stock-output" to OutputConfig(
                    "file",
                    listOf("filter-low-stock"),
                    mapOf(
                        "path" to "dataflare/data/low-stock.json",
                        "format" to "json"
                    )
                )
            ),
            connections = listOf(
                Connection("products", "parse"),
                Connection("parse", "filter-electronics"),
                Connection("parse", "filter-expensive"),
                Connection("parse", "filter-low-stock"),
                Connection("filter-electronics", "add-metadata"),
                Connection("add-metadata", "electronics-output"),
                Connection("filter-expensive", "expensive-output"),
                Connection("filter-low-stock", "low-stock-output")
            )
        )
    }

    /**
     * 显示处理结果
     */
    private fun displayResults() {
        logger.info { "Processing results:" }

        try {
            val electronicsFile = File("dataflare/data/electronics.json")
            val expensiveFile = File("dataflare/data/expensive.json")
            val lowStockFile = File("dataflare/data/low-stock.json")

            if (electronicsFile.exists()) {
                logger.info { "Electronics products:" }
                logger.info { electronicsFile.readText() }
            } else {
                logger.warn { "Electronics products file not found" }
            }

            if (expensiveFile.exists()) {
                logger.info { "Expensive products:" }
                logger.info { expensiveFile.readText() }
            } else {
                logger.warn { "Expensive products file not found" }
            }

            if (lowStockFile.exists()) {
                logger.info { "Low stock products:" }
                logger.info { lowStockFile.readText() }
            } else {
                logger.warn { "Low stock products file not found" }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error displaying results" }
        }
    }
}
