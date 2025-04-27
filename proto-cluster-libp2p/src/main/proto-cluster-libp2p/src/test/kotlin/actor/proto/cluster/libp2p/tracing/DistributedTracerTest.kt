package actor.proto.cluster.libp2p.tracing

import actor.proto.ActorSystem
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.libp2p.P2PClusterConfig
import actor.proto.cluster.libp2p.P2PClusterProvider
import actor.proto.cluster.libp2p.P2PIdentityLookup
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DistributedTracerTest {
    
    private lateinit var system: ActorSystem
    private lateinit var cluster: Cluster
    private lateinit var tracer: DistributedTracer
    private lateinit var testExporter: TestTraceExporter
    
    @BeforeEach
    fun setup() {
        // 创建 Actor 系统
        system = ActorSystem("test-system")
        
        // 创建 P2P 集群配置
        val p2pConfig = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4001,
            heartbeatInterval = Duration.ofSeconds(1),
            monitorInterval = Duration.ofSeconds(3)
        )
        
        // 创建集群提供者
        val clusterProvider = P2PClusterProvider(p2pConfig)
        
        // 创建身份查找服务
        val identityLookup = P2PIdentityLookup()
        
        // 创建集群配置
        val clusterConfig = ClusterConfig(
            name = "test-cluster",
            clusterProvider = clusterProvider,
            identityLookup = identityLookup
        )
        
        // 创建集群
        cluster = Cluster(system, clusterConfig)
        
        // 创建测试导出器
        testExporter = TestTraceExporter()
        
        // 创建分布式跟踪器
        tracer = DistributedTracer(
            cluster = cluster,
            samplingRate = 1.0, // 100% 采样率用于测试
            maxSpans = 1000,
            exportBatchSize = 100
        )
        
        // 设置导出器
        tracer.setExporter(testExporter)
        
        // 启动跟踪器
        tracer.start()
    }
    
    @AfterEach
    fun tearDown() = runBlocking {
        // 停止跟踪器
        tracer.stop()
        
        // 关闭集群和 Actor 系统
        if (::cluster.isInitialized) {
            cluster.shutdown(true)
        }
        
        system.shutdown()
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should create and complete trace`() = runBlocking {
        // 创建跟踪
        val context = tracer.startTrace("test-trace", mapOf("key" to "value"))
        
        // 验证跟踪 ID
        val traceId = context.getTraceId()
        assertNotNull(traceId, "Trace ID should not be null")
        assertTrue(traceId.isNotEmpty(), "Trace ID should not be empty")
        
        // 添加事件
        context.addEvent("test-event", mapOf("event-key" to "event-value"))
        
        // 添加属性
        context.addAttribute("another-key", "another-value")
        
        // 结束跟踪
        context.end()
        
        // 等待导出
        delay(1000)
        
        // 验证导出的跟踪
        val exportedSpans = testExporter.getExportedSpans()
        assertTrue(exportedSpans.isNotEmpty(), "Should have exported spans")
        
        val span = exportedSpans.find { it.traceId == traceId }
        assertNotNull(span, "Should find exported span with matching trace ID")
        
        assertEquals("test-trace", span.name, "Span name should match")
        assertEquals(2, span.attributes.size, "Span should have 2 attributes")
        assertEquals("value", span.attributes["key"], "Span should have correct attribute value")
        assertEquals("another-value", span.attributes["another-key"], "Span should have correct attribute value")
        
        assertEquals(1, span.events.size, "Span should have 1 event")
        assertEquals("test-event", span.events[0].name, "Event name should match")
        assertEquals("event-value", span.events[0].attributes["event-key"], "Event should have correct attribute value")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should create child traces`() = runBlocking {
        // 创建父跟踪
        val parentContext = tracer.startTrace("parent-trace")
        
        // 创建子跟踪
        val childContext = parentContext.createChildTrace("child-trace")
        
        // 验证跟踪 ID
        val parentTraceId = parentContext.getTraceId()
        val childTraceId = childContext.getTraceId()
        
        assertNotNull(parentTraceId, "Parent trace ID should not be null")
        assertNotNull(childTraceId, "Child trace ID should not be null")
        assertTrue(parentTraceId != childTraceId, "Parent and child trace IDs should be different")
        
        // 添加事件
        parentContext.addEvent("parent-event")
        childContext.addEvent("child-event")
        
        // 结束跟踪
        childContext.end()
        parentContext.end()
        
        // 等待导出
        delay(1000)
        
        // 验证导出的跟踪
        val exportedSpans = testExporter.getExportedSpans()
        assertTrue(exportedSpans.size >= 2, "Should have exported at least 2 spans")
        
        val parentSpan = exportedSpans.find { it.traceId == parentTraceId }
        val childSpan = exportedSpans.find { it.traceId == childTraceId }
        
        assertNotNull(parentSpan, "Should find exported parent span")
        assertNotNull(childSpan, "Should find exported child span")
        
        assertEquals("parent-trace", parentSpan.name, "Parent span name should match")
        assertEquals("child-trace", childSpan.name, "Child span name should match")
        
        assertEquals(parentTraceId, childSpan.parentId, "Child span should reference parent trace ID")
        
        assertEquals(1, parentSpan.events.size, "Parent span should have 1 event")
        assertEquals(1, childSpan.events.size, "Child span should have 1 event")
        
        assertEquals("parent-event", parentSpan.events[0].name, "Parent event name should match")
        assertEquals("child-event", childSpan.events[0].name, "Child event name should match")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should inject and extract trace context`() = runBlocking {
        // 创建跟踪
        val context = tracer.startTrace("test-trace")
        
        // 验证跟踪 ID
        val traceId = context.getTraceId()
        
        // 创建消息
        val originalMessage = "test-message"
        
        // 注入跟踪上下文
        val tracingMessage = tracer.injectTraceContext(originalMessage, context)
        
        // 验证注入的消息
        assertTrue(tracingMessage is TracingMessage, "Should be a TracingMessage")
        assertEquals(traceId, (tracingMessage as TracingMessage).traceId, "Tracing message should have correct trace ID")
        assertEquals(originalMessage, tracingMessage.message, "Tracing message should contain original message")
        
        // 提取跟踪上下文
        val extractedContext = tracer.extractTraceContext(tracingMessage)
        
        // 验证提取的上下文
        assertEquals(traceId, extractedContext.getTraceId(), "Extracted context should have correct trace ID")
        
        // 结束跟踪
        context.end()
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should handle multiple traces`() = runBlocking {
        // 创建多个跟踪
        val contexts = (1..100).map { tracer.startTrace("trace-$it") }
        
        // 添加事件
        contexts.forEachIndexed { index, context ->
            context.addEvent("event-$index")
        }
        
        // 结束跟踪
        contexts.forEach { it.end() }
        
        // 等待导出
        delay(2000)
        
        // 验证统计信息
        val stats = tracer.getStats()
        assertEquals(100, stats.tracesStarted, "Should have started 100 traces")
        assertEquals(100, stats.tracesCompleted, "Should have completed 100 traces")
        assertEquals(100, stats.spansCreated, "Should have created 100 spans")
        
        // 验证导出的跟踪
        val exportedSpans = testExporter.getExportedSpans()
        assertTrue(exportedSpans.size >= 100, "Should have exported at least 100 spans")
        
        // 验证每个跟踪都有一个事件
        contexts.forEachIndexed { index, context ->
            val traceId = context.getTraceId()
            val span = exportedSpans.find { it.traceId == traceId }
            
            assertNotNull(span, "Should find exported span for trace $traceId")
            assertEquals("trace-${index + 1}", span.name, "Span name should match")
            assertEquals(1, span.events.size, "Span should have 1 event")
            assertEquals("event-$index", span.events[0].name, "Event name should match")
        }
    }
}

/**
 * 测试跟踪导出器
 */
class TestTraceExporter : TraceExporter {
    private val exportedSpans = ConcurrentLinkedQueue<Span>()
    
    override fun exportSpans(spans: List<Span>) {
        exportedSpans.addAll(spans)
    }
    
    fun getExportedSpans(): List<Span> {
        return exportedSpans.toList()
    }
    
    fun clearExportedSpans() {
        exportedSpans.clear()
    }
}
