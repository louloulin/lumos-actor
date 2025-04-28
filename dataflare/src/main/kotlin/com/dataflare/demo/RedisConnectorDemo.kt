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
import redis.clients.jedis.Jedis
import java.io.File

private val logger = KotlinLogging.logger {}

/**
 * Redis 连接器演示
 * 
 * 本演示展示了如何使用 Redis 连接器
 * 注意：需要有一个运行中的 Redis 服务器
 */
object RedisConnectorDemo {

    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        logger.info { "Starting RedisConnectorDemo" }

        // Redis 配置
        val redisHost = "localhost"
        val redisPort = 6379
        val redisInputKey = "dataflare-input"
        val redisOutputKey = "dataflare-output"

        try {
            // 准备测试数据
            prepareTestData(redisHost, redisPort, redisInputKey)

            // 创建数据处理系统
            val dataProcessingSystem = DataProcessingSystem("redis-connector-demo")

            // 启动数据处理系统
            dataProcessingSystem.start()

            // 创建工作流配置
            val workflowConfig = createWorkflowConfig(redisHost, redisPort, redisInputKey, redisOutputKey)

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
            logger.error(e) { "Error in RedisConnectorDemo" }
        }

        logger.info { "RedisConnectorDemo completed" }
    }

    /**
     * 准备测试数据
     */
    private fun prepareTestData(host: String, port: Int, key: String) {
        logger.info { "Preparing test data in Redis" }

        try {
            Jedis(host, port).use { jedis ->
                // 清空之前的数据
                jedis.del(key)

                // 添加测试数据
                jedis.rpush(key, 
                    """{"id": 1, "name": "Product A", "price": 99.99, "category": "Electronics"}""",
                    """{"id": 2, "name": "Product B", "price": 49.99, "category": "Home"}""",
                    """{"id": 3, "name": "Product C", "price": 149.99, "category": "Electronics"}"""
                )

                logger.info { "Added test data to Redis key: $key" }
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to prepare test data: ${e.message}" }
            throw e
        }
    }

    /**
     * 创建工作流配置
     */
    private fun createWorkflowConfig(
        redisHost: String,
        redisPort: Int,
        inputKey: String,
        outputKey: String
    ): WorkflowConfig {
        logger.info { "Creating workflow config" }

        return WorkflowConfig(
            name = "redis-workflow",
            inputs = mapOf(
                "redis-input" to InputConfig(
                    "redis",
                    mapOf(
                        "host" to redisHost,
                        "port" to redisPort,
                        "key" to inputKey,
                        "listMode" to true
                    )
                )
            ),
            processors = mapOf(
                "parse" to ProcessorConfig(
                    "mapping",
                    listOf("redis-input"),
                    mapOf("mapping" to "json.parse(content)")
                ),
                "transform" to ProcessorConfig(
                    "javascript",
                    listOf("parse"),
                    mapOf(
                        "script" to """
                            function process(message, context) {
                                // 添加处理时间戳
                                message.processed_at = new Date().toISOString();
                                
                                // 计算折扣价格
                                let discount = 0;
                                if (message.category === "Electronics") {
                                    discount = 0.1; // 电子产品10%折扣
                                } else {
                                    discount = 0.05; // 其他产品5%折扣
                                }
                                
                                message.discounted_price = message.price * (1 - discount);
                                message.discount_percentage = discount * 100;
                                
                                return message;
                            }
                        """.trimIndent(),
                        "engineName" to "nashorn"
                    )
                ),
                "filter-electronics" to ProcessorConfig(
                    "filter",
                    listOf("transform"),
                    mapOf("condition" to ".category == \"Electronics\"")
                )
            ),
            outputs = mapOf(
                "redis-output" to OutputConfig(
                    "redis",
                    listOf("filter-electronics"),
                    mapOf(
                        "host" to redisHost,
                        "port" to redisPort,
                        "key" to outputKey,
                        "listMode" to true
                    )
                )
            ),
            connections = listOf(
                Connection("redis-input", "parse"),
                Connection("parse", "transform"),
                Connection("transform", "filter-electronics"),
                Connection("filter-electronics", "redis-output")
            )
        )
    }

    /**
     * 显示处理结果
     */
    private fun displayResults(host: String, port: Int, key: String) {
        logger.info { "Processing results:" }

        try {
            Jedis(host, port).use { jedis ->
                val listLength = jedis.llen(key)
                logger.info { "Output list length: $listLength" }

                val results = jedis.lrange(key, 0, -1)
                results.forEachIndexed { index, item ->
                    logger.info { "Item $index: $item" }
                }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error displaying results: ${e.message}" }
        }
    }
}
