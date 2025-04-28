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
 * 高级功能演示
 *
 * 本演示展示了如何使用 PostgreSQL 连接器和 JavaScript 处理器
 */
object AdvancedFeaturesDemo {

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        logger.info { "Starting AdvancedFeaturesDemo" }

        // 创建示例数据
        createSampleData()

        // 创建数据处理系统
        val dataProcessingSystem = DataProcessingSystem("advanced-features-system")

        try {
            // 启动数据处理系统
            dataProcessingSystem.start()

            // 创建工作流配置
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
            logger.error(e) { "Error in AdvancedFeaturesDemo" }
        }

        logger.info { "AdvancedFeaturesDemo completed" }
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
              {"id": 3, "name": "Wireless Headphones", "price": 159.99, "category": "Electronics", "stock": 89},
              {"id": 4, "name": "Smart Watch", "price": 249.99, "category": "Electronics", "stock": 42},
              {"id": 5, "name": "Blender", "price": 79.99, "category": "Home", "stock": 28}
            ]
        """.trimIndent()

        // 确保目录存在
        File("dataflare/data").mkdirs()
        File("dataflare/data/advanced").mkdirs()

        // 写入示例数据
        File("dataflare/data/advanced/products.json").writeText(productsJson)

        logger.info { "Sample data created at dataflare/data/advanced/products.json" }
    }

    /**
     * 创建工作流配置
     */
    private fun createWorkflowConfig(): WorkflowConfig {
        logger.info { "Creating workflow config" }

        return WorkflowConfig(
            name = "advanced-features-workflow",
            inputs = mapOf(
                "products" to InputConfig(
                    "file",
                    mapOf(
                        "path" to "dataflare/data/advanced/products.json",
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
                "enrich" to ProcessorConfig(
                    "javascript",
                    listOf("parse"),
                    mapOf(
                        "script" to """
                            // 初始化函数
                            function init(context) {
                                context.processingStart = new Date();
                                context.processedCount = 0;
                                return "JavaScript processor initialized";
                            }

                            // 处理函数
                            function process(message, context) {
                                // 增加处理计数
                                context.processedCount++;

                                // 计算折扣价格
                                let discount = 0;
                                if (message.category === "Electronics") {
                                    discount = 0.1; // 电子产品10%折扣
                                } else if (message.category === "Home") {
                                    discount = 0.05; // 家居产品5%折扣
                                }

                                // 计算库存价值
                                const stockValue = message.price * message.stock;

                                // 创建丰富的消息
                                return {
                                    id: message.id,
                                    name: message.name,
                                    original_price: message.price,
                                    discounted_price: message.price * (1 - discount),
                                    discount_percentage: discount * 100,
                                    category: message.category,
                                    stock: message.stock,
                                    stock_value: stockValue,
                                    low_stock: message.stock < 50,
                                    processing_id: context.processedCount,
                                    processed_at: new Date().toISOString()
                                };
                            }
                        """.trimIndent(),
                        "engineName" to "nashorn"
                    )
                ),
                "categorize" to ProcessorConfig(
                    "filter",
                    listOf("enrich"),
                    mapOf("condition" to ".category == 'Electronics'")
                )
            ),
            outputs = mapOf(
                "all_products" to OutputConfig(
                    "file",
                    listOf("enrich"),
                    mapOf(
                        "path" to "dataflare/data/advanced/all_products.json",
                        "format" to "json"
                    )
                ),
                "electronics" to OutputConfig(
                    "file",
                    listOf("categorize"),
                    mapOf(
                        "path" to "dataflare/data/advanced/electronics.json",
                        "format" to "json"
                    )
                )
            ),
            connections = listOf(
                Connection("products", "parse"),
                Connection("parse", "enrich"),
                Connection("enrich", "categorize"),
                Connection("enrich", "all_products"),
                Connection("categorize", "electronics")
            )
        )
    }

    /**
     * 显示处理结果
     */
    private fun displayResults() {
        logger.info { "Processing results:" }

        try {
            val allProductsFile = File("dataflare/data/advanced/all_products.json")
            val electronicsFile = File("dataflare/data/advanced/electronics.json")

            if (allProductsFile.exists()) {
                logger.info { "All Products:" }
                logger.info { allProductsFile.readText() }
            } else {
                logger.warn { "All products file not found" }
            }

            if (electronicsFile.exists()) {
                logger.info { "Electronics Products:" }
                logger.info { electronicsFile.readText() }
            } else {
                logger.warn { "Electronics file not found" }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error displaying results" }
        }
    }
}
