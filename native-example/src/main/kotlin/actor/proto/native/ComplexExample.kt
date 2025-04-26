package actor.proto.native

import actor.proto.*
import actor.proto.mailbox.newUnboundedMailbox
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * 复杂示例，展示 ProtoActor 在 Native Image 中的高级功能
 * 
 * 这个示例展示了：
 * 1. Actor 层次结构和监督
 * 2. 消息传递和状态管理
 * 3. 请求-响应模式
 * 4. Actor 重入
 * 5. 错误处理和恢复
 */

// 消息定义
data class Add(val value: Int)
data class Subtract(val value: Int)
data class GetValue(val replyTo: PID)
data class Result(val value: Int)
data class Divide(val value: Int)
object Reset
object Crash

// 计算器 Actor
class CalculatorActor : Actor {
    private var value = 0
    
    override suspend fun receive(context: Context) {
        when (val msg = context.message) {
            is Add -> value += msg.value
            is Subtract -> value -= msg.value
            is GetValue -> context.send(msg.replyTo, Result(value))
            is Reset -> value = 0
            is Divide -> {
                if (msg.value == 0) {
                    throw ArithmeticException("除以零")
                }
                value /= msg.value
            }
            is Crash -> throw RuntimeException("模拟崩溃")
            else -> println("计算器收到未知消息: ${msg.javaClass.name}")
        }
    }
}

// 监督者 Actor
class SupervisorActor : Actor {
    private val children = ConcurrentHashMap<String, PID>()
    private val restartCount = AtomicInteger(0)
    
    override suspend fun receive(context: Context) {
        when (val msg = context.message) {
            is Started -> println("监督者已启动")
            
            is String -> {
                // 创建新的计算器 Actor
                val props = Props.fromProducer { CalculatorActor() }
                    .withMailbox { newUnboundedMailbox() }
                    .withChildSupervisorStrategy(OneForOneStrategy(3, Duration.ofSeconds(1)) { _, _ -> SupervisorDirective.Restart })
                
                val child = context.spawnNamed(props, msg)
                children[msg] = child
                context.send(context.sender, child)
            }
            
            is Terminated -> {
                val childId = msg.who.id
                println("子 Actor $childId 已终止，原因: ${msg.reason}")
                children.remove(childId)
                restartCount.incrementAndGet()
            }
            
            is GetValue -> {
                // 获取所有计算器的值
                val results = mutableMapOf<String, Int>()
                val latch = CountDownLatch(children.size)
                
                for ((name, pid) in children) {
                    // 使用 Future 实现请求-响应模式
                    val future = context.actorSystem.requestFuture<Result>(pid, GetValue(context.self), Duration.ofSeconds(5))
                    
                    // 使用 Actor 重入，等待 Future 完成后继续处理
                    context.reenterAfter(future) { result ->
                        result.fold(
                            { response -> 
                                results[name] = response.value
                                latch.countDown()
                            },
                            { error -> 
                                println("获取 $name 的值时出错: ${error.message}")
                                latch.countDown()
                            }
                        )
                    }
                }
                
                // 等待所有请求完成
                latch.await(5, TimeUnit.SECONDS)
                
                // 发送结果
                context.send(msg.replyTo, results)
            }
            
            else -> println("监督者收到未知消息: ${msg.javaClass.name}")
        }
    }
}

// 主函数
fun main() {
    println("ProtoActor Native 复杂示例")
    println("==========================")
    
    // 创建 Actor 系统
    val system = ActorSystem("complex-example")
    
    // 创建监督者 Actor
    val supervisorProps = Props.fromProducer { SupervisorActor() }
    val supervisor = system.actorOf(supervisorProps, "supervisor")
    
    runBlocking {
        // 创建三个计算器 Actor
        val calc1Future = system.requestFuture<PID>(supervisor, "calculator1", Duration.ofSeconds(5))
        val calc2Future = system.requestFuture<PID>(supervisor, "calculator2", Duration.ofSeconds(5))
        val calc3Future = system.requestFuture<PID>(supervisor, "calculator3", Duration.ofSeconds(5))
        
        val calc1 = calc1Future.get()
        val calc2 = calc2Future.get()
        val calc3 = calc3Future.get()
        
        println("创建了三个计算器 Actor: ${calc1.id}, ${calc2.id}, ${calc3.id}")
        
        // 发送操作消息
        system.send(calc1, Add(10))
        system.send(calc1, Add(5))
        system.send(calc2, Add(20))
        system.send(calc2, Subtract(5))
        system.send(calc3, Add(30))
        
        // 等待操作完成
        delay(100)
        
        // 获取所有计算器的值
        val resultsFuture = system.requestFuture<Map<String, Int>>(supervisor, GetValue(system.deadLetter()), Duration.ofSeconds(5))
        val results = resultsFuture.get()
        
        println("\n计算器当前值:")
        results.forEach { (name, value) -> println("$name: $value") }
        
        // 测试错误处理和恢复
        println("\n测试错误处理和恢复...")
        system.send(calc1, Divide(0)) // 将触发异常
        
        // 等待重启
        delay(200)
        
        // 再次获取值
        val resultsAfterErrorFuture = system.requestFuture<Map<String, Int>>(supervisor, GetValue(system.deadLetter()), Duration.ofSeconds(5))
        val resultsAfterError = resultsAfterErrorFuture.get()
        
        println("\n错误后计算器值:")
        resultsAfterError.forEach { (name, value) -> println("$name: $value") }
        
        // 测试崩溃
        println("\n测试崩溃和恢复...")
        system.send(calc2, Crash)
        
        // 等待重启
        delay(200)
        
        // 停止所有 Actor
        system.stop(supervisor)
        
        println("\n示例完成!")
    }
}
