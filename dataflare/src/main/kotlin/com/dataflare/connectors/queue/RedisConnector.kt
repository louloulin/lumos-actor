package com.dataflare.connectors.queue

import com.dataflare.connectors.*
import com.dataflare.core.Message
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.exceptions.JedisConnectionException
import java.time.Duration
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * Redis 连接器配置
 */
@Serializable
data class RedisConfig(
    override val type: String = "redis",
    val host: String = "localhost",
    val port: Int = 6379,
    val password: String? = null,
    val database: Int = 0,
    val key: String,
    val listMode: Boolean = true,
    val channelMode: Boolean = false,
    val batchSize: Int = 100,
    val timeout: Int = 2000
) : Config()

/**
 * Redis 输入连接器
 */
class RedisInputConnector : Input {
    private lateinit var config: RedisConfig
    private var jedisPool: JedisPool? = null
    private var isConnected = false

    override suspend fun configure(config: Config) {
        if (config !is RedisConfig) {
            throw IllegalArgumentException("Expected RedisConfig, got ${config::class.simpleName}")
        }
        this.config = config
    }

    override suspend fun connect(ctx: Context): Boolean = withContext(Dispatchers.IO) {
        try {
            logger.info { "Connecting to Redis: ${config.host}:${config.port}" }

            // 创建连接池配置
            val poolConfig = JedisPoolConfig()
            poolConfig.maxTotal = 10
            poolConfig.maxIdle = 5
            poolConfig.minIdle = 1
            poolConfig.testOnBorrow = true
            poolConfig.testOnReturn = true
            poolConfig.testWhileIdle = true

            jedisPool = JedisPool(
                poolConfig,
                config.host,
                config.port,
                config.timeout,
                config.password,
                config.database
            )

            // 测试连接
            jedisPool?.resource?.use { jedis ->
                jedis.ping()
                logger.info { "Connected to Redis successfully" }
            }

            isConnected = true
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to connect to Redis: ${e.message}" }
            false
        }
    }

    override suspend fun read(ctx: Context): Message? = withContext(Dispatchers.IO) {
        if (!isConnected || jedisPool == null) {
            throw IllegalStateException("Not connected to Redis")
        }

        try {
            jedisPool?.resource?.use { jedis ->
                when {
                    config.listMode -> {
                        // 从列表中读取数据
                        val value = jedis.lpop(config.key)
                        if (value != null) {
                            Message.create(mapOf("content" to value))
                        } else {
                            null
                        }
                    }
                    config.channelMode -> {
                        // 从发布/订阅模式中读取数据
                        // 注意：这种模式通常需要单独的订阅线程
                        // 这里简化实现，实际使用时应该使用 JedisPubSub
                        null
                    }
                    else -> {
                        // 从键值对中读取数据
                        val value = jedis.get(config.key)
                        if (value != null) {
                            Message.create(mapOf("content" to value))
                        } else {
                            null
                        }
                    }
                }
            }
        } catch (e: JedisConnectionException) {
            logger.error(e) { "Redis connection lost: ${e.message}" }
            isConnected = false
            null
        } catch (e: Exception) {
            logger.error(e) { "Error reading from Redis: ${e.message}" }
            null
        }
    }

    override suspend fun close(ctx: Context): Unit = withContext(Dispatchers.IO) {
        logger.info { "Closing Redis input connector" }
        try {
            jedisPool?.close()
        } catch (e: Exception) {
            logger.error(e) { "Error closing Redis connection: ${e.message}" }
        } finally {
            jedisPool = null
            isConnected = false
        }
    }
}

/**
 * Redis 输出连接器
 */
class RedisOutputConnector : Output {
    private lateinit var config: RedisConfig
    private var jedisPool: JedisPool? = null
    private var isConnected = false

    override suspend fun configure(config: Config) {
        if (config !is RedisConfig) {
            throw IllegalArgumentException("Expected RedisConfig, got ${config::class.simpleName}")
        }
        this.config = config
    }

    override suspend fun connect(ctx: Context): Boolean = withContext(Dispatchers.IO) {
        try {
            logger.info { "Connecting to Redis: ${config.host}:${config.port}" }

            // 创建连接池配置
            val poolConfig = JedisPoolConfig()
            poolConfig.maxTotal = 10
            poolConfig.maxIdle = 5
            poolConfig.minIdle = 1
            poolConfig.testOnBorrow = true
            poolConfig.testOnReturn = true
            poolConfig.testWhileIdle = true

            jedisPool = JedisPool(
                poolConfig,
                config.host,
                config.port,
                config.timeout,
                config.password,
                config.database
            )

            // 测试连接
            jedisPool?.resource?.use { jedis ->
                jedis.ping()
                logger.info { "Connected to Redis successfully" }
            }

            isConnected = true
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to connect to Redis: ${e.message}" }
            false
        }
    }

    override suspend fun write(ctx: Context, batch: List<Message>): WriteResult = withContext(Dispatchers.IO) {
        if (!isConnected || jedisPool == null) {
            throw IllegalStateException("Not connected to Redis")
        }

        var recordsWritten = 0
        val errors = mutableListOf<String>()

        try {
            jedisPool?.resource?.use { jedis ->
                batch.forEach { message ->
                    try {
                        val content = message.payload["content"]?.toString() ?: message.toString()

                        when {
                            config.listMode -> {
                                // 写入列表
                                jedis.rpush(config.key, content)
                            }
                            config.channelMode -> {
                                // 发布到频道
                                jedis.publish(config.key, content)
                            }
                            else -> {
                                // 写入键值对
                                val messageId = UUID.randomUUID().toString()
                                jedis.set("${config.key}:$messageId", content)
                            }
                        }

                        recordsWritten++
                    } catch (e: Exception) {
                        logger.error(e) { "Error writing message to Redis: ${e.message}" }
                        errors.add("Error writing message: ${e.message}")
                    }
                }
            }

            WriteResult(
                success = errors.isEmpty(),
                recordsWritten = recordsWritten,
                errors = errors
            )
        } catch (e: JedisConnectionException) {
            logger.error(e) { "Redis connection lost: ${e.message}" }
            isConnected = false

            WriteResult(
                success = false,
                recordsWritten = recordsWritten,
                errors = listOf("Redis connection lost: ${e.message}")
            )
        } catch (e: Exception) {
            logger.error(e) { "Error writing batch to Redis: ${e.message}" }

            WriteResult(
                success = false,
                recordsWritten = recordsWritten,
                errors = listOf("Error writing batch: ${e.message}")
            )
        }
    }

    override suspend fun close(ctx: Context): Unit = withContext(Dispatchers.IO) {
        logger.info { "Closing Redis output connector" }
        try {
            jedisPool?.close()
        } catch (e: Exception) {
            logger.error(e) { "Error closing Redis connection: ${e.message}" }
        } finally {
            jedisPool = null
            isConnected = false
        }
    }
}

/**
 * Redis 连接器工厂
 */
class RedisConnectorFactory : ConnectorFactory {
    override fun createInput(config: Config): Input {
        return RedisInputConnector().apply {
            kotlinx.coroutines.runBlocking { configure(config) }
        }
    }

    override fun createOutput(config: Config): Output {
        return RedisOutputConnector().apply {
            kotlinx.coroutines.runBlocking { configure(config) }
        }
    }
}
