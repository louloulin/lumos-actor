package com.dataflare.demo

import com.dataflare.config.YamlConfigLoader
import com.dataflare.core.DataProcessingSystem
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import redis.clients.jedis.Jedis
import java.io.File

private val logger = KotlinLogging.logger {}

/**
 * 高级 YAML 配置演示
 * 
 * 本演示展示了如何使用更复杂的 YAML 配置，包括 Redis 和 PostgreSQL 连接器
 * 注意：需要有运行中的 Redis 和 PostgreSQL 服务器
 */
object AdvancedYamlConfigDemo {

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        logger.info { "Starting AdvancedYamlConfigDemo" }

        // Redis 配置
        val redisHost = "localhost"
        val redisPort = 6379
        val redisInputKey = "dataflare-input"
        val redisOutputKey = "dataflare-output"

        try {
            // 准备测试数据
            createSampleData(redisHost, redisPort, redisInputKey)

            // 创建数据处理系统
            val dataProcessingSystem = DataProcessingSystem("advanced-yaml-config-demo")

            // 启动数据处理系统
            dataProcessingSystem.start()

            // 加载 YAML 配置
            val configLoader = YamlConfigLoader()
            val workflowConfig = configLoader.loadWorkflowConfig("dataflare/src/main/resources/workflows/advanced-workflow.yaml")

            // 打印工作流配置
            logger.info { "Loaded workflow config: ${workflowConfig.name}" }
            logger.info { "Inputs: ${workflowConfig.inputs.keys}" }
            logger.info { "Processors: ${workflowConfig.processors.keys}" }
            logger.info { "Outputs: ${workflowConfig.outputs.keys}" }
            logger.info { "Connections: ${workflowConfig.connections.size}" }
            logger.info { "Engine: ${workflowConfig.engineName}" }

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
            displayResults(redisHost, redisPort, redisOutputKey)

            // 停止数据处理系统
            dataProcessingSystem.stop()

        } catch (e: Exception) {
            logger.error(e) { "Error in AdvancedYamlConfigDemo" }
        }

        logger.info { "AdvancedYamlConfigDemo completed" }
    }

    /**
     * 准备测试数据
     */
    private fun createSampleData(host: String, port: Int, key: String) {
        logger.info { "Preparing test data" }

        // 创建产品数据
        val productsJson = """
            [
              {"id": 1, "name": "Smartphone", "price": 799.99, "category": "Electronics", "stock": 120, "source": "redis"},
              {"id": 2, "name": "Laptop", "price": 1299.99, "category": "Electronics", "stock": 45, "source": "redis"},
              {"id": 3, "name": "Coffee Maker", "price": 89.99, "category": "Home", "stock": 30, "source": "redis"}
            ]
        """.trimIndent()

        // 创建客户数据
        val customersJson = """
            [
              {"id": 101, "name": "John Doe", "email": "john@example.com", "customer_value": "high", "source": "file"},
              {"id": 102, "name": "Jane Smith", "email": "jane@example.com", "customer_value": "medium", "source": "file"},
              {"id": 103, "name": "Bob Johnson", "email": "bob@example.com", "customer_value": "low", "source": "file"}
            ]
        """.trimIndent()

        // 确保目录存在
        File("dataflare/data").mkdirs()

        // 写入客户数据
        File("dataflare/data/customers.json").writeText(customersJson)
        logger.info { "Customer data created at dataflare/data/customers.json" }

        try {
            // 写入 Redis 数据
            Jedis(host, port).use { jedis ->
                // 清空之前的数据
                jedis.del(key)

                // 添加产品数据
                val products = productsJson.split("\n")
                    .filter { it.trim().startsWith("{") }
                    .map { it.trim().removeSuffix(",") }

                products.forEach { product ->
                    jedis.rpush(key, product)
                }

                logger.info { "Product data added to Redis key: $key" }
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to prepare Redis data: ${e.message}" }
            logger.warn { "Continuing without Redis data" }
        }
    }

    /**
     * 显示处理结果
     */
    private fun displayResults(host: String, port: Int, key: String) {
        logger.info { "Processing results:" }

        try {
            // 显示文件输出
            val joinedDataFile = File("dataflare/data/output/joined_data.json")
            if (joinedDataFile.exists()) {
                logger.info { "Joined data:" }
                logger.info { joinedDataFile.readText() }
            } else {
                logger.warn { "Joined data file not found" }
            }

            // 显示 Redis 输出
            try {
                Jedis(host, port).use { jedis ->
                    val listLength = jedis.llen(key)
                    logger.info { "Redis output list length: $listLength" }

                    val results = jedis.lrange(key, 0, -1)
                    results.forEachIndexed { index, item ->
                        logger.info { "Redis item $index: $item" }
                    }
                }
            } catch (e: Exception) {
                logger.error(e) { "Failed to read Redis output: ${e.message}" }
            }

            // 注意：PostgreSQL 输出需要数据库连接，这里省略
            logger.info { "PostgreSQL output: Check database table 'high_value_items'" }

        } catch (e: Exception) {
            logger.error(e) { "Error displaying results: ${e.message}" }
        }
    }
}
