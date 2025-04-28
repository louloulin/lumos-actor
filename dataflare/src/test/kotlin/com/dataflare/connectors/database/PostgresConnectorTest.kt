package com.dataflare.connectors.database

import com.dataflare.connectors.ConnectorContext
import com.dataflare.core.Message
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@Testcontainers
@Disabled("Requires Docker for TestContainers")
class PostgresConnectorTest {

    companion object {
        @Container
        val postgres = PostgreSQLContainer<Nothing>("postgres:14-alpine").apply {
            withDatabaseName("testdb")
            withUsername("test")
            withPassword("test")
        }
    }

    private lateinit var connection: Connection
    private lateinit var inputConnector: PostgresInputConnector
    private lateinit var outputConnector: PostgresOutputConnector
    private lateinit var ctx: ConnectorContext

    @BeforeEach
    fun setup() {
        // 启动容器
        postgres.start()

        // 创建连接
        connection = DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)

        // 创建测试表
        connection.createStatement().use { statement ->
            statement.execute("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    value INTEGER
                )
            """)
        }

        // 初始化连接器
        inputConnector = PostgresInputConnector()
        outputConnector = PostgresOutputConnector()
        ctx = ConnectorContext("test-workflow", "test-connector")
    }

    @AfterEach
    fun tearDown() {
        // 关闭连接
        connection.close()

        // 停止容器
        postgres.stop()
    }

    @Test
    fun `test PostgreSQL input connector`() = runBlocking {
        // 准备测试数据
        connection.createStatement().use { statement ->
            statement.execute("""
                INSERT INTO test_table (name, value) VALUES
                ('test1', 100),
                ('test2', 200),
                ('test3', 300)
            """)
        }

        // 配置输入连接器
        val config = PostgresConfig(
            connectionString = postgres.jdbcUrl,
            table = "test_table",
            columns = listOf("id", "name", "value")
        )
        inputConnector.configure(config)

        // 连接
        val connected = inputConnector.connect(ctx)
        assertTrue(connected, "Should connect successfully")

        // 读取数据
        val message1 = inputConnector.read(ctx)
        assertNotNull(message1, "Should read first message")
        assertEquals("test1", message1.payload["name"], "First message should have name 'test1'")
        assertEquals(100, message1.payload["value"], "First message should have value 100")

        val message2 = inputConnector.read(ctx)
        assertNotNull(message2, "Should read second message")
        assertEquals("test2", message2.payload["name"], "Second message should have name 'test2'")
        assertEquals(200, message2.payload["value"], "Second message should have value 200")

        val message3 = inputConnector.read(ctx)
        assertNotNull(message3, "Should read third message")
        assertEquals("test3", message3.payload["name"], "Third message should have name 'test3'")
        assertEquals(300, message3.payload["value"], "Third message should have value 300")

        // 关闭连接器
        inputConnector.close(ctx)
    }

    @Test
    fun `test PostgreSQL output connector`() = runBlocking {
        // 配置输出连接器
        val config = PostgresConfig(
            connectionString = postgres.jdbcUrl,
            table = "test_table",
            columns = listOf("name", "value")
        )
        outputConnector.configure(config)

        // 连接
        val connected = outputConnector.connect(ctx)
        assertTrue(connected, "Should connect successfully")

        // 准备测试消息
        val messages = listOf(
            Message.create(mapOf("name" to "output1", "value" to 1000)),
            Message.create(mapOf("name" to "output2", "value" to 2000))
        )

        // 写入数据
        val result = outputConnector.write(ctx, messages)
        assertTrue(result.success, "Write should be successful")
        assertEquals(2, result.recordsWritten, "Should write 2 records")

        // 验证数据已写入
        connection.createStatement().use { statement ->
            val resultSet = statement.executeQuery("SELECT * FROM test_table WHERE name LIKE 'output%'")
            var count = 0
            while (resultSet.next()) {
                count++
                val name = resultSet.getString("name")
                val value = resultSet.getInt("value")
                when (name) {
                    "output1" -> assertEquals(1000, value, "output1 should have value 1000")
                    "output2" -> assertEquals(2000, value, "output2 should have value 2000")
                }
            }
            assertEquals(2, count, "Should have 2 records in database")
        }

        // 关闭连接器
        outputConnector.close(ctx)
    }

    @Test
    fun `test PostgreSQL connector factory`() = runBlocking {
        // 创建工厂
        val factory = PostgresConnectorFactory()

        // 创建配置
        val config = PostgresConfig(
            connectionString = postgres.jdbcUrl,
            table = "test_table"
        )

        // 创建输入连接器
        val input = factory.createInput(config)
        assertTrue(input is PostgresInputConnector, "Should create PostgresInputConnector")

        // 创建输出连接器
        val output = factory.createOutput(config)
        assertTrue(output is PostgresOutputConnector, "Should create PostgresOutputConnector")
    }
}
