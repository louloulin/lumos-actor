package com.dataflare.connectors.queue

import com.dataflare.connectors.ConnectorContext
import com.dataflare.core.Message
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import redis.clients.jedis.Jedis
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@Testcontainers
@Disabled("Requires Docker for TestContainers")
class RedisConnectorTest {

    companion object {
        @Container
        val redis = GenericContainer<Nothing>(DockerImageName.parse("redis:7-alpine")).apply {
            withExposedPorts(6379)
        }
    }

    private lateinit var jedis: Jedis
    private lateinit var inputConnector: RedisInputConnector
    private lateinit var outputConnector: RedisOutputConnector
    private lateinit var ctx: ConnectorContext

    @BeforeEach
    fun setup() {
        // 启动容器
        redis.start()

        // 创建 Jedis 客户端
        jedis = Jedis(redis.host, redis.getMappedPort(6379))

        // 初始化连接器
        inputConnector = RedisInputConnector()
        outputConnector = RedisOutputConnector()
        ctx = ConnectorContext("test-workflow", "test-connector")
    }

    @AfterEach
    fun tearDown() {
        // 关闭 Jedis 客户端
        jedis.close()

        // 停止容器
        redis.stop()
    }

    @Test
    fun `test Redis input connector with list mode`() = runBlocking {
        // 准备测试数据
        val testKey = "test-list"
        jedis.rpush(testKey, "value1", "value2", "value3")

        // 配置输入连接器
        val config = RedisConfig(
            host = redis.host,
            port = redis.getMappedPort(6379),
            key = testKey,
            listMode = true
        )
        inputConnector.configure(config)

        // 连接
        val connected = inputConnector.connect(ctx)
        assertTrue(connected, "Should connect successfully")

        // 读取数据
        val message1 = inputConnector.read(ctx)
        assertNotNull(message1, "Should read first message")
        assertEquals("value1", message1.payload["content"], "First message should be 'value1'")

        val message2 = inputConnector.read(ctx)
        assertNotNull(message2, "Should read second message")
        assertEquals("value2", message2.payload["content"], "Second message should be 'value2'")

        val message3 = inputConnector.read(ctx)
        assertNotNull(message3, "Should read third message")
        assertEquals("value3", message3.payload["content"], "Third message should be 'value3'")

        // 关闭连接器
        inputConnector.close(ctx)
    }

    @Test
    fun `test Redis output connector with list mode`() = runBlocking {
        // 配置输出连接器
        val testKey = "output-list"
        val config = RedisConfig(
            host = redis.host,
            port = redis.getMappedPort(6379),
            key = testKey,
            listMode = true
        )
        outputConnector.configure(config)

        // 连接
        val connected = outputConnector.connect(ctx)
        assertTrue(connected, "Should connect successfully")

        // 准备测试消息
        val messages = listOf(
            Message.create(mapOf("content" to "output1")),
            Message.create(mapOf("content" to "output2"))
        )

        // 写入数据
        val result = outputConnector.write(ctx, messages)
        assertTrue(result.success, "Write should be successful")
        assertEquals(2, result.recordsWritten, "Should write 2 records")

        // 验证数据已写入
        val listLength = jedis.llen(testKey)
        assertEquals(2, listLength, "List should have 2 items")

        val listItems = jedis.lrange(testKey, 0, -1)
        assertTrue(listItems.contains("output1"), "List should contain 'output1'")
        assertTrue(listItems.contains("output2"), "List should contain 'output2'")

        // 关闭连接器
        outputConnector.close(ctx)
    }

    @Test
    fun `test Redis connector factory`() = runBlocking {
        // 创建工厂
        val factory = RedisConnectorFactory()

        // 创建配置
        val config = RedisConfig(
            host = redis.host,
            port = redis.getMappedPort(6379),
            key = "test-key"
        )

        // 创建输入连接器
        val input = factory.createInput(config)
        assertTrue(input is RedisInputConnector, "Should create RedisInputConnector")

        // 创建输出连接器
        val output = factory.createOutput(config)
        assertTrue(output is RedisOutputConnector, "Should create RedisOutputConnector")
    }
}
