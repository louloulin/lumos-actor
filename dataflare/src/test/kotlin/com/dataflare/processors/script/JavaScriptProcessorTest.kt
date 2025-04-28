package com.dataflare.processors.script

import com.dataflare.connectors.ConnectorContext
import com.dataflare.core.Message
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class JavaScriptProcessorTest {

    @Test
    @Disabled("JavaScript engine not available in test environment")
    fun `test JavaScript processor with simple transformation`() = runBlocking {
        // 创建简单的 JavaScript 脚本
        val script = """
            function process(message, context) {
                // 简单转换：将所有数字值加倍
                const result = {};
                for (const key in message) {
                    if (typeof message[key] === 'number') {
                        result[key] = message[key] * 2;
                    } else {
                        result[key] = message[key];
                    }
                }

                // 添加处理时间戳
                result.processed_at = new Date().toISOString();

                return result;
            }
        """.trimIndent()

        // 创建处理器
        val config = JavaScriptConfig(script = script, engineName = "nashorn")
        val processor = JavaScriptProcessor(config)

        // 创建测试消息
        val message = Message.create(mapOf(
            "id" to 1,
            "name" to "test",
            "value" to 100,
            "active" to true
        ))

        // 创建上下文
        val ctx = ConnectorContext("test-workflow", "test-processor")

        // 处理消息
        val results = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, results.size, "Should return one message")

        val result = results.first()
        assertEquals(2, result.payload["id"], "ID should be doubled")
        assertEquals("test", result.payload["name"], "Name should remain unchanged")
        assertEquals(200, result.payload["value"], "Value should be doubled")
        assertEquals(true, result.payload["active"], "Active should remain unchanged")
        assertNotNull(result.payload["processed_at"], "Should add processed_at timestamp")

        // 关闭处理器
        processor.close(ctx)
    }

    @Test
    @Disabled("JavaScript engine not available in test environment")
    fun `test JavaScript processor with filtering`() = runBlocking {
        // 创建过滤脚本
        val script = """
            function process(message, context) {
                // 只保留 value > 50 的消息
                if (message.value > 50) {
                    return message;
                }
                return null; // 返回 null 表示过滤掉该消息
            }
        """.trimIndent()

        // 创建处理器
        val config = JavaScriptConfig(script = script, engineName = "nashorn")
        val processor = JavaScriptProcessor(config)

        // 创建上下文
        val ctx = ConnectorContext("test-workflow", "test-processor")

        // 测试应该保留的消息
        val keepMessage = Message.create(mapOf(
            "id" to 1,
            "name" to "keep",
            "value" to 100
        ))

        val keepResults = processor.process(ctx, keepMessage)
        assertEquals(1, keepResults.size, "Should keep message with value > 50")

        // 测试应该过滤掉的消息
        val filterMessage = Message.create(mapOf(
            "id" to 2,
            "name" to "filter",
            "value" to 30
        ))

        val filterResults = processor.process(ctx, filterMessage)
        assertEquals(0, filterResults.size, "Should filter out message with value <= 50")

        // 关闭处理器
        processor.close(ctx)
    }

    @Test
    @Disabled("JavaScript engine not available in test environment")
    fun `test JavaScript processor with initialization`() = runBlocking {
        // 创建带初始化函数的脚本
        val script = """
            // 全局计数器
            let counter = 0;

            // 初始化函数
            function init(context) {
                counter = 1000;
                context.initialized = true;
                return "Initialization complete";
            }

            // 处理函数
            function process(message, context) {
                counter++;
                return {
                    original: message,
                    counter: counter,
                    initialized: context.initialized
                };
            }
        """.trimIndent()

        // 创建处理器
        val config = JavaScriptConfig(
            script = script,
            functionName = "process",
            initFunctionName = "init",
            engineName = "nashorn"
        )
        val processor = JavaScriptProcessor(config)

        // 创建上下文
        val ctx = ConnectorContext("test-workflow", "test-processor")

        // 处理消息
        val message = Message.create(mapOf("test" to true))
        val results = processor.process(ctx, message)

        // 验证结果
        assertEquals(1, results.size, "Should return one message")
        val result = results.first()

        // 验证初始化状态
        assertEquals(true, result.payload["initialized"], "Context should be initialized")
        assertEquals(1001, result.payload["counter"], "Counter should be initialized and incremented")

        // 处理另一条消息，验证状态保持
        val results2 = processor.process(ctx, message)
        assertEquals(1002, results2.first().payload["counter"], "Counter should be incremented again")

        // 关闭处理器
        processor.close(ctx)
    }

    @Test
    @Disabled("JavaScript engine not available in test environment")
    fun `test JavaScript processor with multiple output messages`() = runBlocking {
        // 创建返回多条消息的脚本
        val script = """
            function process(message, context) {
                // 将一条消息拆分为多条
                const results = [];

                // 为每个字段创建一条消息
                for (const key in message) {
                    results.push({
                        field: key,
                        value: message[key],
                        original_message_id: message.id
                    });
                }

                return results;
            }
        """.trimIndent()

        // 创建处理器
        val config = JavaScriptConfig(script = script, engineName = "nashorn")
        val processor = JavaScriptProcessor(config)

        // 创建上下文
        val ctx = ConnectorContext("test-workflow", "test-processor")

        // 创建测试消息
        val message = Message.create(mapOf(
            "id" to 1,
            "name" to "test",
            "value" to 100
        ))

        // 处理消息
        val results = processor.process(ctx, message)

        // 验证结果
        assertEquals(3, results.size, "Should return three messages (one for each field)")

        // 验证每条消息都包含正确的字段
        val fields = results.map { it.payload["field"] }.toSet()
        assertTrue(fields.contains("id"), "Should have a message for 'id' field")
        assertTrue(fields.contains("name"), "Should have a message for 'name' field")
        assertTrue(fields.contains("value"), "Should have a message for 'value' field")

        // 验证所有消息都引用原始消息ID
        results.forEach { result ->
            assertEquals(1, result.payload["original_message_id"], "All messages should reference original ID")
        }

        // 关闭处理器
        processor.close(ctx)
    }

    @Test
    @Disabled("JavaScript engine not available in test environment")
    fun `test JavaScript processor factory`() = runBlocking {
        // 创建工厂
        val factory = JavaScriptProcessorFactory()

        // 创建配置
        val config = JavaScriptConfig(
            script = "function process(message) { return message; }",
            engineName = "nashorn"
        )

        // 创建处理器
        val processor = factory.create(config)

        // 验证处理器类型
        assertTrue(processor is JavaScriptProcessor, "Factory should create JavaScriptProcessor")
    }
}
