package actor.proto.cluster.libp2p.resilience

import kotlinx.coroutines.delay
import mu.KotlinLogging
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

private val logger = KotlinLogging.logger {}

/**
 * Retry 提供重试机制，用于增强系统的容错性
 */
object Retry {
    /**
     * 使用指定的重试策略执行操作
     */
    suspend fun <T> withRetry(
        retryPolicy: RetryPolicy,
        operation: suspend () -> T
    ): T {
        val maxAttempts = retryPolicy.maxAttempts
        var attempt = 0
        var lastException: Exception? = null
        
        while (attempt < maxAttempts) {
            try {
                // 执行操作
                return operation()
            } catch (e: Exception) {
                attempt++
                lastException = e
                
                // 检查是否应该重试
                if (!retryPolicy.shouldRetry(e, attempt)) {
                    throw e
                }
                
                // 计算延迟时间
                val delayMs = retryPolicy.calculateDelay(attempt)
                
                logger.debug { "Retry attempt $attempt/$maxAttempts after $delayMs ms due to: ${e.message}" }
                
                // 等待后重试
                delay(delayMs)
            }
        }
        
        // 所有重试都失败
        throw lastException ?: IllegalStateException("All retry attempts failed")
    }
}

/**
 * 重试策略
 */
interface RetryPolicy {
    /**
     * 最大重试次数
     */
    val maxAttempts: Int
    
    /**
     * 检查是否应该重试
     */
    fun shouldRetry(exception: Exception, attempt: Int): Boolean
    
    /**
     * 计算重试延迟
     */
    fun calculateDelay(attempt: Int): Long
}

/**
 * 固定延迟重试策略
 */
class FixedDelayRetryPolicy(
    override val maxAttempts: Int,
    private val delay: Duration,
    private val retryableExceptions: Set<Class<out Exception>> = emptySet()
) : RetryPolicy {
    override fun shouldRetry(exception: Exception, attempt: Int): Boolean {
        return attempt < maxAttempts && (retryableExceptions.isEmpty() || 
                retryableExceptions.any { it.isInstance(exception) })
    }
    
    override fun calculateDelay(attempt: Int): Long {
        return delay.toMillis()
    }
}

/**
 * 指数退避重试策略
 */
class ExponentialBackoffRetryPolicy(
    override val maxAttempts: Int,
    private val initialDelay: Duration,
    private val maxDelay: Duration,
    private val multiplier: Double = 2.0,
    private val jitter: Double = 0.2,
    private val retryableExceptions: Set<Class<out Exception>> = emptySet()
) : RetryPolicy {
    override fun shouldRetry(exception: Exception, attempt: Int): Boolean {
        return attempt < maxAttempts && (retryableExceptions.isEmpty() || 
                retryableExceptions.any { it.isInstance(exception) })
    }
    
    override fun calculateDelay(attempt: Int): Long {
        // 计算基本延迟
        val baseDelay = initialDelay.toMillis() * multiplier.pow(attempt - 1)
        
        // 应用最大延迟限制
        val cappedDelay = min(baseDelay, maxDelay.toMillis())
        
        // 应用抖动
        val jitterOffset = (cappedDelay * jitter * (Random.nextDouble() * 2 - 1)).toLong()
        
        return cappedDelay + jitterOffset
    }
}

/**
 * 重试统计信息
 */
class RetryStats(
    private val name: String
) {
    private val attempts = AtomicInteger(0)
    private val successes = AtomicInteger(0)
    private val failures = AtomicInteger(0)
    
    /**
     * 记录尝试
     */
    fun recordAttempt() {
        attempts.incrementAndGet()
    }
    
    /**
     * 记录成功
     */
    fun recordSuccess() {
        successes.incrementAndGet()
    }
    
    /**
     * 记录失败
     */
    fun recordFailure() {
        failures.incrementAndGet()
    }
    
    /**
     * 获取尝试次数
     */
    fun getAttempts(): Int = attempts.get()
    
    /**
     * 获取成功次数
     */
    fun getSuccesses(): Int = successes.get()
    
    /**
     * 获取失败次数
     */
    fun getFailures(): Int = failures.get()
    
    /**
     * 获取成功率
     */
    fun getSuccessRate(): Double {
        val total = attempts.get()
        return if (total > 0) successes.get().toDouble() / total else 0.0
    }
}

/**
 * 重试统计注册表
 */
object RetryStatsRegistry {
    private val stats = mutableMapOf<String, RetryStats>()
    
    /**
     * 获取或创建统计信息
     */
    fun getOrCreate(name: String): RetryStats {
        return stats.getOrPut(name) { RetryStats(name) }
    }
    
    /**
     * 获取所有统计信息
     */
    fun getAll(): Map<String, RetryStats> {
        return stats.toMap()
    }
    
    /**
     * 重置所有统计信息
     */
    fun resetAll() {
        stats.clear()
    }
}
