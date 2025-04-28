package com.dataflare.processors.aggregation

import com.dataflare.connectors.ConnectorContext
import com.dataflare.core.Message
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AggregationProcessorTest {

    private val ctx = ConnectorContext("test-workflow", "test-processor")

    @Test
    fun `test count aggregation with count window`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "count",
            field = "value",
            windowSize = 3,
            windowType = "count"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("value" to 10))
        val message2 = Message.create(mapOf("value" to 20))
        val message3 = Message.create(mapOf("value" to 30))
        
        // 处理消息
        val result1 = processor.process(ctx, message1)
        val result2 = processor.process(ctx, message2)
        val result3 = processor.process(ctx, message3)
        
        // 验证结果
        assertTrue(result1.isEmpty(), "First message should not produce result")
        assertTrue(result2.isEmpty(), "Second message should not produce result")
        assertEquals(1, result3.size, "Third message should produce one result")
        
        val resultMessage = result3[0]
        assertEquals(3, resultMessage.payload["count_value"], "Count should be 3")
    }
    
    @Test
    fun `test sum aggregation with count window`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "sum",
            field = "value",
            windowSize = 3,
            windowType = "count"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("value" to 10))
        val message2 = Message.create(mapOf("value" to 20))
        val message3 = Message.create(mapOf("value" to 30))
        
        // 处理消息
        val result1 = processor.process(ctx, message1)
        val result2 = processor.process(ctx, message2)
        val result3 = processor.process(ctx, message3)
        
        // 验证结果
        assertTrue(result1.isEmpty(), "First message should not produce result")
        assertTrue(result2.isEmpty(), "Second message should not produce result")
        assertEquals(1, result3.size, "Third message should produce one result")
        
        val resultMessage = result3[0]
        assertEquals(60.0, resultMessage.payload["sum_value"], "Sum should be 60.0")
    }
    
    @Test
    fun `test avg aggregation with count window`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "avg",
            field = "value",
            windowSize = 3,
            windowType = "count"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("value" to 10))
        val message2 = Message.create(mapOf("value" to 20))
        val message3 = Message.create(mapOf("value" to 30))
        
        // 处理消息
        val result1 = processor.process(ctx, message1)
        val result2 = processor.process(ctx, message2)
        val result3 = processor.process(ctx, message3)
        
        // 验证结果
        assertTrue(result1.isEmpty(), "First message should not produce result")
        assertTrue(result2.isEmpty(), "Second message should not produce result")
        assertEquals(1, result3.size, "Third message should produce one result")
        
        val resultMessage = result3[0]
        assertEquals(20.0, resultMessage.payload["avg_value"], "Average should be 20.0")
    }
    
    @Test
    fun `test min aggregation with count window`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "min",
            field = "value",
            windowSize = 3,
            windowType = "count"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("value" to 30))
        val message2 = Message.create(mapOf("value" to 10))
        val message3 = Message.create(mapOf("value" to 20))
        
        // 处理消息
        val result1 = processor.process(ctx, message1)
        val result2 = processor.process(ctx, message2)
        val result3 = processor.process(ctx, message3)
        
        // 验证结果
        assertTrue(result1.isEmpty(), "First message should not produce result")
        assertTrue(result2.isEmpty(), "Second message should not produce result")
        assertEquals(1, result3.size, "Third message should produce one result")
        
        val resultMessage = result3[0]
        assertEquals(10.0, resultMessage.payload["min_value"], "Min should be 10.0")
    }
    
    @Test
    fun `test max aggregation with count window`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "max",
            field = "value",
            windowSize = 3,
            windowType = "count"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("value" to 10))
        val message2 = Message.create(mapOf("value" to 30))
        val message3 = Message.create(mapOf("value" to 20))
        
        // 处理消息
        val result1 = processor.process(ctx, message1)
        val result2 = processor.process(ctx, message2)
        val result3 = processor.process(ctx, message3)
        
        // 验证结果
        assertTrue(result1.isEmpty(), "First message should not produce result")
        assertTrue(result2.isEmpty(), "Second message should not produce result")
        assertEquals(1, result3.size, "Third message should produce one result")
        
        val resultMessage = result3[0]
        assertEquals(30.0, resultMessage.payload["max_value"], "Max should be 30.0")
    }
    
    @Test
    fun `test first aggregation with count window`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "first",
            field = "value",
            windowSize = 3,
            windowType = "count"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("value" to 10))
        val message2 = Message.create(mapOf("value" to 20))
        val message3 = Message.create(mapOf("value" to 30))
        
        // 处理消息
        val result1 = processor.process(ctx, message1)
        val result2 = processor.process(ctx, message2)
        val result3 = processor.process(ctx, message3)
        
        // 验证结果
        assertTrue(result1.isEmpty(), "First message should not produce result")
        assertTrue(result2.isEmpty(), "Second message should not produce result")
        assertEquals(1, result3.size, "Third message should produce one result")
        
        val resultMessage = result3[0]
        assertEquals(10.0, resultMessage.payload["first_value"], "First should be 10.0")
    }
    
    @Test
    fun `test last aggregation with count window`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "last",
            field = "value",
            windowSize = 3,
            windowType = "count"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("value" to 10))
        val message2 = Message.create(mapOf("value" to 20))
        val message3 = Message.create(mapOf("value" to 30))
        
        // 处理消息
        val result1 = processor.process(ctx, message1)
        val result2 = processor.process(ctx, message2)
        val result3 = processor.process(ctx, message3)
        
        // 验证结果
        assertTrue(result1.isEmpty(), "First message should not produce result")
        assertTrue(result2.isEmpty(), "Second message should not produce result")
        assertEquals(1, result3.size, "Third message should produce one result")
        
        val resultMessage = result3[0]
        assertEquals(30.0, resultMessage.payload["last_value"], "Last should be 30.0")
    }
    
    @Test
    fun `test aggregation with group by`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "sum",
            field = "value",
            groupBy = "category",
            windowSize = 4,
            windowType = "count"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("value" to 10, "category" to "A"))
        val message2 = Message.create(mapOf("value" to 20, "category" to "B"))
        val message3 = Message.create(mapOf("value" to 30, "category" to "A"))
        val message4 = Message.create(mapOf("value" to 40, "category" to "B"))
        
        // 处理消息
        val results = mutableListOf<Message>()
        results.addAll(processor.process(ctx, message1))
        results.addAll(processor.process(ctx, message2))
        results.addAll(processor.process(ctx, message3))
        results.addAll(processor.process(ctx, message4))
        
        // 验证结果
        assertEquals(2, results.size, "Should produce two results")
        
        // 找到分组 A 的结果
        val resultA = results.find { it.payload["group_value"] == "A" }
        assertEquals(40.0, resultA?.payload?.get("sum_value"), "Sum for group A should be 40.0")
        
        // 找到分组 B 的结果
        val resultB = results.find { it.payload["group_value"] == "B" }
        assertEquals(60.0, resultB?.payload?.get("sum_value"), "Sum for group B should be 60.0")
    }
    
    @Test
    fun `test aggregation with custom output field`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "sum",
            field = "value",
            windowSize = 3,
            windowType = "count",
            outputField = "total"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("value" to 10))
        val message2 = Message.create(mapOf("value" to 20))
        val message3 = Message.create(mapOf("value" to 30))
        
        // 处理消息
        val result1 = processor.process(ctx, message1)
        val result2 = processor.process(ctx, message2)
        val result3 = processor.process(ctx, message3)
        
        // 验证结果
        assertTrue(result1.isEmpty(), "First message should not produce result")
        assertTrue(result2.isEmpty(), "Second message should not produce result")
        assertEquals(1, result3.size, "Third message should produce one result")
        
        val resultMessage = result3[0]
        assertEquals(60.0, resultMessage.payload["total"], "Output field 'total' should be 60.0")
    }
    
    @Test
    fun `test aggregation with nested field`() = runBlocking {
        // 创建配置
        val config = AggregationConfig(
            operation = "sum",
            field = "data.value",
            windowSize = 3,
            windowType = "count"
        )
        
        // 创建处理器
        val processor = AggregationProcessor(config)
        
        // 创建测试消息
        val message1 = Message.create(mapOf("data" to mapOf("value" to 10)))
        val message2 = Message.create(mapOf("data" to mapOf("value" to 20)))
        val message3 = Message.create(mapOf("data" to mapOf("value" to 30)))
        
        // 处理消息
        val result1 = processor.process(ctx, message1)
        val result2 = processor.process(ctx, message2)
        val result3 = processor.process(ctx, message3)
        
        // 验证结果
        assertTrue(result1.isEmpty(), "First message should not produce result")
        assertTrue(result2.isEmpty(), "Second message should not produce result")
        assertEquals(1, result3.size, "Third message should produce one result")
        
        val resultMessage = result3[0]
        assertEquals(60.0, resultMessage.payload["sum_data.value"], "Sum should be 60.0")
    }
    
    @Test
    fun `test aggregation processor factory`() {
        // 创建配置
        val config = AggregationConfig(
            operation = "sum",
            field = "value",
            windowSize = 3,
            windowType = "count"
        )
        
        // 创建工厂
        val factory = AggregationProcessorFactory()
        
        // 创建处理器
        val processor = factory.create(config)
        
        // 验证结果
        assertTrue(processor is AggregationProcessor, "Factory should create AggregationProcessor")
    }
}
