package actor.proto.cluster.libp2p.resilience

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

private val logger = KotlinLogging.logger {}

/**
 * CircuitBreaker 实现断路器模式，用于增强系统在极端条件下的容错性
 */
class CircuitBreaker(
    private val name: String,
    private val failureThreshold: Int = 5,
    private val resetTimeout: Duration = Duration.ofSeconds(30),
    private val halfOpenMaxCalls: Int = 3
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    
    // 断路器状态
    private val state = AtomicReference(State.CLOSED)
    
    // 失败计数
    private val failureCount = AtomicInteger(0)
    
    // 半开状态下的调用计数
    private val halfOpenCallCount = AtomicInteger(0)
    
    // 半开状态下的成功计数
    private val halfOpenSuccessCount = AtomicInteger(0)
    
    // 上次状态变更时间
    private val lastStateChange = AtomicLong(System.currentTimeMillis())
    
    // 统计信息
    private val totalCalls = AtomicInteger(0)
    private val successfulCalls = AtomicInteger(0)
    private val failedCalls = AtomicInteger(0)
    private val shortCircuitedCalls = AtomicInteger(0)
    private val stateTransitions = AtomicInteger(0)
    
    /**
     * 执行受保护的操作
     */
    suspend fun <T> execute(operation: suspend () -> T): T {
        totalCalls.incrementAndGet()
        
        // 检查断路器状态
        when (state.get()) {
            State.OPEN -> {
                // 检查是否应该进入半开状态
                if (shouldAttemptReset()) {
                    transitionToHalfOpen()
                } else {
                    // 断路器开路，快速失败
                    shortCircuitedCalls.incrementAndGet()
                    throw CircuitBreakerOpenException("Circuit breaker '$name' is open")
                }
            }
            State.HALF_OPEN -> {
                // 检查是否允许调用
                if (halfOpenCallCount.incrementAndGet() > halfOpenMaxCalls) {
                    // 超过半开状态的最大调用次数，快速失败
                    shortCircuitedCalls.incrementAndGet()
                    throw CircuitBreakerOpenException("Circuit breaker '$name' is half-open and at capacity")
                }
            }
            State.CLOSED -> {
                // 断路器闭合，允许调用
            }
        }
        
        try {
            // 执行操作
            val result = operation()
            
            // 操作成功
            onSuccess()
            
            return result
        } catch (e: Exception) {
            // 操作失败
            onFailure(e)
            
            throw e
        }
    }
    
    /**
     * 处理成功调用
     */
    private fun onSuccess() {
        successfulCalls.incrementAndGet()
        
        when (state.get()) {
            State.CLOSED -> {
                // 重置失败计数
                failureCount.set(0)
            }
            State.HALF_OPEN -> {
                // 增加成功计数
                val successCount = halfOpenSuccessCount.incrementAndGet()
                
                // 如果成功次数达到阈值，关闭断路器
                if (successCount >= halfOpenMaxCalls) {
                    transitionToClosed()
                }
            }
            State.OPEN -> {
                // 不应该发生
            }
        }
    }
    
    /**
     * 处理失败调用
     */
    private fun onFailure(e: Exception) {
        failedCalls.incrementAndGet()
        
        when (state.get()) {
            State.CLOSED -> {
                // 增加失败计数
                val failures = failureCount.incrementAndGet()
                
                // 如果失败次数达到阈值，打开断路器
                if (failures >= failureThreshold) {
                    transitionToOpen()
                }
            }
            State.HALF_OPEN -> {
                // 半开状态下的失败，立即打开断路器
                transitionToOpen()
            }
            State.OPEN -> {
                // 不应该发生
            }
        }
    }
    
    /**
     * 检查是否应该尝试重置断路器
     */
    private fun shouldAttemptReset(): Boolean {
        val now = System.currentTimeMillis()
        val lastChange = lastStateChange.get()
        
        return now - lastChange >= resetTimeout.toMillis()
    }
    
    /**
     * 转换到开路状态
     */
    private fun transitionToOpen() {
        if (state.compareAndSet(State.CLOSED, State.OPEN) || 
            state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
            
            // 更新状态变更时间
            lastStateChange.set(System.currentTimeMillis())
            
            // 重置计数器
            halfOpenCallCount.set(0)
            halfOpenSuccessCount.set(0)
            
            // 更新统计信息
            stateTransitions.incrementAndGet()
            
            logger.info { "Circuit breaker '$name' transitioned to OPEN" }
            
            // 启动自动重置任务
            scheduleReset()
        }
    }
    
    /**
     * 转换到半开状态
     */
    private fun transitionToHalfOpen() {
        if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
            // 更新状态变更时间
            lastStateChange.set(System.currentTimeMillis())
            
            // 重置计数器
            halfOpenCallCount.set(0)
            halfOpenSuccessCount.set(0)
            
            // 更新统计信息
            stateTransitions.incrementAndGet()
            
            logger.info { "Circuit breaker '$name' transitioned to HALF_OPEN" }
        }
    }
    
    /**
     * 转换到闭合状态
     */
    private fun transitionToClosed() {
        if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
            // 更新状态变更时间
            lastStateChange.set(System.currentTimeMillis())
            
            // 重置计数器
            failureCount.set(0)
            
            // 更新统计信息
            stateTransitions.incrementAndGet()
            
            logger.info { "Circuit breaker '$name' transitioned to CLOSED" }
        }
    }
    
    /**
     * 调度自动重置任务
     */
    private fun scheduleReset() {
        scope.launch {
            delay(resetTimeout.toMillis())
            
            // 如果断路器仍然是开路状态，尝试转换到半开状态
            if (state.get() == State.OPEN) {
                transitionToHalfOpen()
            }
        }
    }
    
    /**
     * 获取当前状态
     */
    fun getState(): State {
        return state.get()
    }
    
    /**
     * 获取统计信息
     */
    fun getStats(): CircuitBreakerStats {
        return CircuitBreakerStats(
            name = name,
            state = state.get(),
            failureCount = failureCount.get(),
            halfOpenCallCount = halfOpenCallCount.get(),
            halfOpenSuccessCount = halfOpenSuccessCount.get(),
            totalCalls = totalCalls.get(),
            successfulCalls = successfulCalls.get(),
            failedCalls = failedCalls.get(),
            shortCircuitedCalls = shortCircuitedCalls.get(),
            stateTransitions = stateTransitions.get(),
            lastStateChangeTime = Instant.ofEpochMilli(lastStateChange.get())
        )
    }
    
    /**
     * 重置断路器
     */
    fun reset() {
        // 转换到闭合状态
        if (state.getAndSet(State.CLOSED) != State.CLOSED) {
            // 更新状态变更时间
            lastStateChange.set(System.currentTimeMillis())
            
            // 重置计数器
            failureCount.set(0)
            halfOpenCallCount.set(0)
            halfOpenSuccessCount.set(0)
            
            // 更新统计信息
            stateTransitions.incrementAndGet()
            
            logger.info { "Circuit breaker '$name' manually reset to CLOSED" }
        }
    }
    
    /**
     * 断路器状态
     */
    enum class State {
        CLOSED,     // 闭合状态，允许所有调用
        OPEN,       // 开路状态，拒绝所有调用
        HALF_OPEN   // 半开状态，允许有限的调用
    }
}

/**
 * 断路器开路异常
 */
class CircuitBreakerOpenException(message: String) : Exception(message)

/**
 * 断路器统计信息
 */
data class CircuitBreakerStats(
    val name: String,
    val state: CircuitBreaker.State,
    val failureCount: Int,
    val halfOpenCallCount: Int,
    val halfOpenSuccessCount: Int,
    val totalCalls: Int,
    val successfulCalls: Int,
    val failedCalls: Int,
    val shortCircuitedCalls: Int,
    val stateTransitions: Int,
    val lastStateChangeTime: Instant
)

/**
 * 断路器注册表
 */
object CircuitBreakerRegistry {
    private val circuitBreakers = ConcurrentHashMap<String, CircuitBreaker>()
    
    /**
     * 获取或创建断路器
     */
    fun getOrCreate(
        name: String,
        failureThreshold: Int = 5,
        resetTimeout: Duration = Duration.ofSeconds(30),
        halfOpenMaxCalls: Int = 3
    ): CircuitBreaker {
        return circuitBreakers.computeIfAbsent(name) {
            CircuitBreaker(name, failureThreshold, resetTimeout, halfOpenMaxCalls)
        }
    }
    
    /**
     * 获取断路器
     */
    fun get(name: String): CircuitBreaker? {
        return circuitBreakers[name]
    }
    
    /**
     * 获取所有断路器
     */
    fun getAll(): Map<String, CircuitBreaker> {
        return circuitBreakers.toMap()
    }
    
    /**
     * 重置所有断路器
     */
    fun resetAll() {
        circuitBreakers.values.forEach { it.reset() }
    }
}
