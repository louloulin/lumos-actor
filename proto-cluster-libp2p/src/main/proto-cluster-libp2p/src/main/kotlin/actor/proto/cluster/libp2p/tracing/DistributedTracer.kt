package actor.proto.cluster.libp2p.tracing

import actor.proto.PID
import actor.proto.cluster.Cluster
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

private val logger = KotlinLogging.logger {}

/**
 * DistributedTracer 负责分布式跟踪和日志聚合
 */
class DistributedTracer(
    private val cluster: Cluster,
    private val samplingRate: Double = 0.1, // 默认采样率 10%
    private val maxSpans: Int = 1000, // 最大跟踪数量
    private val exportBatchSize: Int = 100 // 导出批次大小
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var isRunning = false
    
    // 当前活跃的跟踪
    private val activeTraces = ConcurrentHashMap<String, Trace>()
    
    // 完成的跟踪
    private val completedSpans = ConcurrentLinkedQueue<Span>()
    
    // 统计信息
    private val tracesStarted = AtomicInteger(0)
    private val tracesCompleted = AtomicInteger(0)
    private val spansCreated = AtomicInteger(0)
    private val spansExported = AtomicInteger(0)
    
    // 跟踪导出器
    private var exporter: TraceExporter? = null
    
    /**
     * 启动跟踪器
     */
    fun start() {
        if (isRunning) return
        
        logger.info { "Starting distributed tracer" }
        
        isRunning = true
        
        // 启动跟踪导出任务
        startSpanExportTask()
    }
    
    /**
     * 停止跟踪器
     */
    fun stop() {
        if (!isRunning) return
        
        logger.info { "Stopping distributed tracer" }
        
        isRunning = false
        
        // 导出剩余的跟踪
        exportSpans()
        
        // 清理状态
        activeTraces.clear()
        completedSpans.clear()
    }
    
    /**
     * 设置跟踪导出器
     */
    fun setExporter(exporter: TraceExporter) {
        this.exporter = exporter
    }
    
    /**
     * 启动跟踪导出任务
     */
    private fun startSpanExportTask() {
        scope.launch {
            while (isRunning) {
                try {
                    // 导出跟踪
                    exportSpans()
                    
                    // 等待一段时间
                    kotlinx.coroutines.delay(5000) // 每5秒导出一次
                } catch (e: Exception) {
                    logger.error(e) { "Error exporting spans" }
                    kotlinx.coroutines.delay(1000) // 出错后等待一段时间再重试
                }
            }
        }
    }
    
    /**
     * 导出跟踪
     */
    private fun exportSpans() {
        val exporter = this.exporter ?: return
        
        // 收集要导出的跟踪
        val spans = mutableListOf<Span>()
        var count = 0
        
        while (count < exportBatchSize && !completedSpans.isEmpty()) {
            val span = completedSpans.poll() ?: break
            spans.add(span)
            count++
        }
        
        if (spans.isEmpty()) return
        
        // 导出跟踪
        try {
            exporter.exportSpans(spans)
            spansExported.addAndGet(spans.size)
        } catch (e: Exception) {
            logger.error(e) { "Error exporting spans" }
            
            // 重新入队未导出的跟踪
            spans.forEach { completedSpans.add(it) }
        }
    }
    
    /**
     * 开始新的跟踪
     */
    fun startTrace(name: String, attributes: Map<String, String> = emptyMap()): TraceContext {
        // 应用采样率
        if (Math.random() > samplingRate) {
            return NoopTraceContext
        }
        
        // 生成跟踪 ID
        val traceId = UUID.randomUUID().toString()
        
        // 创建跟踪
        val trace = Trace(
            id = traceId,
            name = name,
            startTime = Instant.now(),
            attributes = attributes
        )
        
        // 存储跟踪
        activeTraces[traceId] = trace
        
        // 更新统计信息
        tracesStarted.incrementAndGet()
        
        return TraceContextImpl(this, trace)
    }
    
    /**
     * 开始新的跟踪，继承父跟踪上下文
     */
    fun startTrace(name: String, parentContext: TraceContext, attributes: Map<String, String> = emptyMap()): TraceContext {
        // 如果父跟踪是空操作，返回空操作
        if (parentContext == NoopTraceContext) {
            return NoopTraceContext
        }
        
        // 获取父跟踪 ID
        val parentTraceId = parentContext.getTraceId()
        
        // 生成跟踪 ID
        val traceId = UUID.randomUUID().toString()
        
        // 创建跟踪
        val trace = Trace(
            id = traceId,
            name = name,
            startTime = Instant.now(),
            parentId = parentTraceId,
            attributes = attributes
        )
        
        // 存储跟踪
        activeTraces[traceId] = trace
        
        // 更新统计信息
        tracesStarted.incrementAndGet()
        
        return TraceContextImpl(this, trace)
    }
    
    /**
     * 结束跟踪
     */
    internal fun endTrace(traceId: String) {
        // 获取跟踪
        val trace = activeTraces.remove(traceId) ?: return
        
        // 设置结束时间
        trace.endTime = Instant.now()
        
        // 创建跟踪跨度
        val span = Span(
            traceId = trace.id,
            name = trace.name,
            startTime = trace.startTime,
            endTime = trace.endTime!!,
            parentId = trace.parentId,
            attributes = trace.attributes,
            events = trace.events
        )
        
        // 添加到完成的跟踪
        completedSpans.add(span)
        
        // 更新统计信息
        tracesCompleted.incrementAndGet()
        spansCreated.incrementAndGet()
        
        // 如果完成的跟踪太多，移除最旧的
        while (completedSpans.size > maxSpans) {
            completedSpans.poll()
        }
    }
    
    /**
     * 添加事件
     */
    internal fun addEvent(traceId: String, name: String, attributes: Map<String, String> = emptyMap()) {
        // 获取跟踪
        val trace = activeTraces[traceId] ?: return
        
        // 创建事件
        val event = Event(
            name = name,
            timestamp = Instant.now(),
            attributes = attributes
        )
        
        // 添加事件
        trace.events.add(event)
    }
    
    /**
     * 添加属性
     */
    internal fun addAttribute(traceId: String, key: String, value: String) {
        // 获取跟踪
        val trace = activeTraces[traceId] ?: return
        
        // 添加属性
        trace.attributes[key] = value
    }
    
    /**
     * 创建跟踪上下文
     */
    fun createTraceContext(traceId: String): TraceContext {
        // 获取跟踪
        val trace = activeTraces[traceId]
        
        return if (trace != null) {
            TraceContextImpl(this, trace)
        } else {
            NoopTraceContext
        }
    }
    
    /**
     * 从消息中提取跟踪上下文
     */
    fun extractTraceContext(message: Any): TraceContext {
        // 如果消息包含跟踪上下文
        if (message is TracingMessage) {
            return createTraceContext(message.traceId)
        }
        
        return NoopTraceContext
    }
    
    /**
     * 注入跟踪上下文到消息
     */
    fun <T> injectTraceContext(message: T, context: TraceContext): Any {
        // 如果上下文是空操作，直接返回原始消息
        if (context == NoopTraceContext) {
            return message as Any
        }
        
        // 创建包含跟踪上下文的消息
        return TracingMessage(
            traceId = context.getTraceId(),
            message = message as Any
        )
    }
    
    /**
     * 获取统计信息
     */
    fun getStats(): TracerStats {
        return TracerStats(
            activeTraces = activeTraces.size,
            completedSpans = completedSpans.size,
            tracesStarted = tracesStarted.get(),
            tracesCompleted = tracesCompleted.get(),
            spansCreated = spansCreated.get(),
            spansExported = spansExported.get()
        )
    }
}

/**
 * 跟踪
 */
data class Trace(
    val id: String,
    val name: String,
    val startTime: Instant,
    var endTime: Instant? = null,
    val parentId: String? = null,
    val attributes: MutableMap<String, String> = mutableMapOf(),
    val events: MutableList<Event> = mutableListOf()
)

/**
 * 跨度
 */
data class Span(
    val traceId: String,
    val name: String,
    val startTime: Instant,
    val endTime: Instant,
    val parentId: String? = null,
    val attributes: Map<String, String> = emptyMap(),
    val events: List<Event> = emptyList()
)

/**
 * 事件
 */
data class Event(
    val name: String,
    val timestamp: Instant,
    val attributes: Map<String, String> = emptyMap()
)

/**
 * 跟踪上下文
 */
interface TraceContext {
    /**
     * 获取跟踪 ID
     */
    fun getTraceId(): String
    
    /**
     * 添加事件
     */
    fun addEvent(name: String, attributes: Map<String, String> = emptyMap())
    
    /**
     * 添加属性
     */
    fun addAttribute(key: String, value: String)
    
    /**
     * 结束跟踪
     */
    fun end()
    
    /**
     * 创建子跟踪
     */
    fun createChildTrace(name: String, attributes: Map<String, String> = emptyMap()): TraceContext
}

/**
 * 跟踪上下文实现
 */
class TraceContextImpl(
    private val tracer: DistributedTracer,
    private val trace: Trace
) : TraceContext {
    override fun getTraceId(): String = trace.id
    
    override fun addEvent(name: String, attributes: Map<String, String>) {
        tracer.addEvent(trace.id, name, attributes)
    }
    
    override fun addAttribute(key: String, value: String) {
        tracer.addAttribute(trace.id, key, value)
    }
    
    override fun end() {
        tracer.endTrace(trace.id)
    }
    
    override fun createChildTrace(name: String, attributes: Map<String, String>): TraceContext {
        return tracer.startTrace(name, this, attributes)
    }
}

/**
 * 空操作跟踪上下文
 */
object NoopTraceContext : TraceContext {
    override fun getTraceId(): String = ""
    
    override fun addEvent(name: String, attributes: Map<String, String>) {
        // 空操作
    }
    
    override fun addAttribute(key: String, value: String) {
        // 空操作
    }
    
    override fun end() {
        // 空操作
    }
    
    override fun createChildTrace(name: String, attributes: Map<String, String>): TraceContext {
        return this
    }
}

/**
 * 跟踪导出器
 */
interface TraceExporter {
    /**
     * 导出跟踪
     */
    fun exportSpans(spans: List<Span>)
}

/**
 * 日志跟踪导出器
 */
class LoggingTraceExporter : TraceExporter {
    private val logger = KotlinLogging.logger {}
    
    override fun exportSpans(spans: List<Span>) {
        spans.forEach { span ->
            logger.info { "Trace: ${span.traceId}, Name: ${span.name}, Duration: ${span.endTime.toEpochMilli() - span.startTime.toEpochMilli()}ms" }
            
            span.events.forEach { event ->
                logger.info { "  Event: ${event.name}, Time: ${event.timestamp}" }
            }
        }
    }
}

/**
 * 包含跟踪上下文的消息
 */
data class TracingMessage(
    val traceId: String,
    val message: Any
)

/**
 * 跟踪器统计信息
 */
data class TracerStats(
    val activeTraces: Int,
    val completedSpans: Int,
    val tracesStarted: Int,
    val tracesCompleted: Int,
    val spansCreated: Int,
    val spansExported: Int
)
