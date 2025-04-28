package com.dataflare

import com.dataflare.connectors.ConnectorRegistry
import com.dataflare.connectors.Context
import com.dataflare.connectors.FileConfig
import com.dataflare.core.DataProcessingSystem
import com.dataflare.core.Message
import com.dataflare.processors.FilterConfig
import com.dataflare.processors.MappingConfig
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import actor.proto.ActorSystem
import org.json.JSONArray
import org.json.JSONObject
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
        
        // 创建 Actor 系统
        val system = ActorSystem.create("single-node-system")
        
        try {
            // 创建连接器注册表
            val registry = ConnectorRegistry(system)
            
            // 创建数据处理系统
            val dataProcessingSystem = DataProcessingSystem(
                name = "single-node-system",
                system = system,
                registry = registry
            )
            
            // 启动数据处理系统
            dataProcessingSystem.start()
            
            // 创建工作流
            val workflowConfig = createWorkflowConfig()
            
            // 部署工作流
            val workflowId = dataProcessingSystem.createWorkflow("product-workflow", workflowConfig)
            
            // 启动工作流
            dataProcessingSystem.startWorkflow(workflowId)
            
            // 等待工作流执行完成
            delay(5000)
            
            // 停止工作流
            dataProcessingSystem.stopWorkflow(workflowId)
            
            // 显示处理结果
            displayResults()
            
            // 停止数据处理系统
            dataProcessingSystem.stop()
            
        } catch (e: Exception) {
            logger.error(e) { "Error in SingleNodeDemo" }
        } finally {
            // 关闭 Actor 系统
            system.shutdown()
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
    }
    
    /**
     * 创建工作流配置
     */
    private fun createWorkflowConfig(): WorkflowConfig {
        logger.info { "Creating workflow config" }
        
        return WorkflowConfig(
            name = "product-workflow",
            inputs = mapOf(
                "products" to com.dataflare.workflow.InputConfig(
                    "file", 
                    mapOf(
                        "path" to "dataflare/data/products.json",
                        "format" to "json"
                    )
                )
            ),
            processors = mapOf(
                "parse" to com.dataflare.workflow.ProcessorConfig(
                    "mapping", 
                    listOf("products"), 
                    mapOf("mapping" to "json.parse(content)")
                ),
                "filter-electronics" to com.dataflare.workflow.ProcessorConfig(
                    "filter", 
                    listOf("parse"), 
                    mapOf("condition" to "items.category == \"Electronics\"")
                ),
                "filter-expensive" to com.dataflare.workflow.ProcessorConfig(
                    "filter", 
                    listOf("parse"), 
                    mapOf("condition" to "items.price > 100")
                ),
                "filter-low-stock" to com.dataflare.workflow.ProcessorConfig(
                    "filter", 
                    listOf("parse"), 
                    mapOf("condition" to "items.stock < 50")
                ),
                "add-metadata" to com.dataflare.workflow.ProcessorConfig(
                    "mapping", 
                    listOf("filter-electronics"), 
                    mapOf("mapping" to ".processed_time = ${System.currentTimeMillis()}")
                )
            ),
            outputs = mapOf(
                "electronics-output" to com.dataflare.workflow.OutputConfig(
                    "file", 
                    listOf("add-metadata"), 
                    mapOf("path" to "dataflare/data/electronics.json")
                ),
                "expensive-output" to com.dataflare.workflow.OutputConfig(
                    "file", 
                    listOf("filter-expensive"), 
                    mapOf("path" to "dataflare/data/expensive.json")
                ),
                "low-stock-output" to com.dataflare.workflow.OutputConfig(
                    "file", 
                    listOf("filter-low-stock"), 
                    mapOf("path" to "dataflare/data/low-stock.json")
                )
            ),
            connections = listOf(
                com.dataflare.workflow.Connection("products", "parse"),
                com.dataflare.workflow.Connection("parse", "filter-electronics"),
                com.dataflare.workflow.Connection("parse", "filter-expensive"),
                com.dataflare.workflow.Connection("parse", "filter-low-stock"),
                com.dataflare.workflow.Connection("filter-electronics", "add-metadata"),
                com.dataflare.workflow.Connection("add-metadata", "electronics-output"),
                com.dataflare.workflow.Connection("filter-expensive", "expensive-output"),
                com.dataflare.workflow.Connection("filter-low-stock", "low-stock-output")
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
            }
            
            if (expensiveFile.exists()) {
                logger.info { "Expensive products:" }
                logger.info { expensiveFile.readText() }
            }
            
            if (lowStockFile.exists()) {
                logger.info { "Low stock products:" }
                logger.info { lowStockFile.readText() }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error displaying results" }
        }
    }
}
