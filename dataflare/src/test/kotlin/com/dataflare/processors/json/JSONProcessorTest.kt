package com.dataflare.processors.json

import com.dataflare.connectors.ConnectorContext
import com.dataflare.core.Message
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class JSONProcessorTest {

    private val ctx = ConnectorContext("test-workflow", "test-processor")

    @Test
    fun `test JSON parse operation with object`() = runBlocking {
        // 创建配置
        val config = JSONProcessorConfig(
            operation = "parse",
            field = "content"
        )

        // 创建处理器
        val processor = JSONProcessor(config)

        // 创建测试消息
        val jsonContent = """{"name":"John","age":30,"city":"New York"}"""
        val message = Message.create(mapOf("content" to jsonContent))

        // 处理消息
        val result = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, result.size, "Should return one message")
        val resultMessage = result[0]

        assertEquals("John", resultMessage.payload["name"], "Should parse name correctly")
        assertEquals(30, resultMessage.payload["age"], "Should parse age correctly")
        assertEquals("New York", resultMessage.payload["city"], "Should parse city correctly")
    }

    @Test
    fun `test JSON parse operation with array`() = runBlocking {
        // 创建配置
        val config = JSONProcessorConfig(
            operation = "parse",
            field = "content",
            arrayAsItems = true
        )

        // 创建处理器
        val processor = JSONProcessor(config)

        // 创建测试消息
        val jsonContent = """[
            {"id":1,"name":"Product 1","price":10.99},
            {"id":2,"name":"Product 2","price":20.99},
            {"id":3,"name":"Product 3","price":30.99}
        ]"""
        val message = Message.create(mapOf("content" to jsonContent))

        // 处理消息
        val result = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, result.size, "Should return one message")
        val resultMessage = result[0]

        assertTrue(resultMessage.payload.containsKey("items"), "Should have items key")
        val items = resultMessage.payload["items"] as List<*>
        assertEquals(3, items.size, "Should have 3 items")

        val item1 = items[0] as Map<*, *>
        assertEquals(1, item1["id"], "First item should have id 1")
        assertEquals("Product 1", item1["name"], "First item should have name 'Product 1'")
        assertEquals(10.99, (item1["price"] as Number).toDouble(), "First item should have price 10.99")
    }

    @Test
    fun `test JSON parse operation with array as list`() = runBlocking {
        // 创建配置
        val config = JSONProcessorConfig(
            operation = "parse",
            field = "content",
            arrayAsItems = false,
            targetField = "products"
        )

        // 创建处理器
        val processor = JSONProcessor(config)

        // 创建测试消息
        val jsonContent = """[
            {"id":1,"name":"Product 1","price":10.99},
            {"id":2,"name":"Product 2","price":20.99},
            {"id":3,"name":"Product 3","price":30.99}
        ]"""
        val message = Message.create(mapOf("content" to jsonContent))

        // 处理消息
        val result = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, result.size, "Should return one message")
        val resultMessage = result[0]

        assertTrue(resultMessage.payload.containsKey("products"), "Should have products key")
        val products = resultMessage.payload["products"] as List<*>
        assertEquals(3, products.size, "Should have 3 products")
    }

    @Test
    fun `test JSON stringify operation`() = runBlocking {
        // 创建配置
        val config = JSONProcessorConfig(
            operation = "stringify",
            field = "data",
            targetField = "json_string",
            pretty = true
        )

        // 创建处理器
        val processor = JSONProcessor(config)

        // 创建测试消息
        val data = mapOf(
            "name" to "John",
            "age" to 30,
            "address" to mapOf(
                "city" to "New York",
                "zip" to "10001"
            )
        )
        val message = Message.create(mapOf("data" to data))

        // 处理消息
        val result = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, result.size, "Should return one message")
        val resultMessage = result[0]

        assertTrue(resultMessage.payload.containsKey("json_string"), "Should have json_string key")
        val jsonString = resultMessage.payload["json_string"] as String
        assertTrue(jsonString.contains("\"name\": \"John\""), "JSON string should contain name")
        assertTrue(jsonString.contains("\"age\": 30"), "JSON string should contain age")
        assertTrue(jsonString.contains("\"city\": \"New York\""), "JSON string should contain city")
    }

    @Test
    fun `test JSON validate operation with valid JSON`() = runBlocking {
        // 创建配置
        val config = JSONProcessorConfig(
            operation = "validate",
            field = "content"
        )

        // 创建处理器
        val processor = JSONProcessor(config)

        // 创建测试消息
        val jsonContent = """{"name":"John","age":30,"city":"New York"}"""
        val message = Message.create(mapOf("content" to jsonContent))

        // 处理消息
        val result = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, result.size, "Should return one message")
        val resultMessage = result[0]

        assertTrue(resultMessage.payload.containsKey("json_valid"), "Should have json_valid key")
        assertTrue(resultMessage.payload["json_valid"] as Boolean, "JSON should be valid")
    }

    @Test
    fun `test JSON validate operation with invalid JSON`() = runBlocking {
        // 创建配置
        val config = JSONProcessorConfig(
            operation = "validate",
            field = "content"
        )

        // 创建处理器
        val processor = JSONProcessor(config)

        // 创建测试消息
        val jsonContent = """{"name":"John","age":30,"city":"New York",,}"""
        val message = Message.create(mapOf("content" to jsonContent))

        // 处理消息
        val result = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, result.size, "Should return one message")
        val resultMessage = result[0]

        assertTrue(resultMessage.payload.containsKey("json_valid"), "Should have json_valid key")
        assertFalse(resultMessage.payload["json_valid"] as Boolean, "JSON should be invalid")
        assertTrue(resultMessage.payload.containsKey("json_error"), "Should have json_error key")
    }

    @Test
    fun `test JSON select operation`() = runBlocking {
        // 创建配置
        val config = JSONProcessorConfig(
            operation = "select",
            field = "user.address.city",
            targetField = "user_city"
        )

        // 创建处理器
        val processor = JSONProcessor(config)

        // 创建测试消息
        val data = mapOf(
            "user" to mapOf(
                "name" to "John",
                "age" to 30,
                "address" to mapOf(
                    "city" to "New York",
                    "zip" to "10001"
                )
            )
        )
        val message = Message.create(data)

        // 处理消息
        val result = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, result.size, "Should return one message")
        val resultMessage = result[0]

        assertTrue(resultMessage.payload.containsKey("user_city"), "Should have user_city key")
        assertEquals("New York", resultMessage.payload["user_city"], "Should extract city correctly")
    }

    @Test
    fun `test JSON merge operation`() = runBlocking {
        // 创建配置
        val config = JSONProcessorConfig(
            operation = "merge",
            field = "user_info,address_info",
            targetField = "complete_user"
        )

        // 创建处理器
        val processor = JSONProcessor(config)

        // 创建测试消息
        val data = mapOf(
            "user_info" to mapOf(
                "name" to "John",
                "age" to 30
            ),
            "address_info" to mapOf(
                "city" to "New York",
                "zip" to "10001"
            )
        )
        val message = Message.create(data)

        // 处理消息
        val result = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, result.size, "Should return one message")
        val resultMessage = result[0]

        assertTrue(resultMessage.payload.containsKey("complete_user"), "Should have complete_user key")
        val completeUser = resultMessage.payload["complete_user"] as Map<*, *>

        assertEquals("John", completeUser["name"], "Should merge name correctly")
        assertEquals(30, completeUser["age"], "Should merge age correctly")
        assertEquals("New York", completeUser["city"], "Should merge city correctly")
        assertEquals("10001", completeUser["zip"], "Should merge zip correctly")
    }

    @Test
    fun `test JSON processor factory`() {
        // 创建配置
        val config = JSONProcessorConfig(
            operation = "parse",
            field = "content"
        )

        // 创建工厂
        val factory = JSONProcessorFactory()

        // 创建处理器
        val processor = factory.create(config)

        // 验证结果
        assertNotNull(processor, "Should create processor")
        assertTrue(processor is JSONProcessor, "Should create JSONProcessor")
    }
}
