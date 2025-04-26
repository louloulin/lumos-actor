package actor.proto.simple

import actor.proto.*
import kotlinx.coroutines.runBlocking

/**
 * 简单的 Actor 示例，用于演示简化的 Native 编译
 */
class SimpleActor : Actor {
    override suspend fun Context.receive(msg: Any) {
        when (msg) {
            is Started -> println("Actor 已启动")
            is String -> {
                println("收到消息: $msg")
                stop(self)
            }
            is Stopping -> println("Actor 正在停止")
            is Stopped -> println("Actor 已停止")
            else -> println("未知消息: ${msg.javaClass.name}")
        }
    }
}

/**
 * 主函数
 */
fun main() {
    println("ProtoActor 简化 Native 示例")
    println("==========================")

    // 创建 Actor 系统
    val system = ActorSystem("simple-native-system")

    // 创建 Actor
    val props = fromProducer { SimpleActor() }
    val pid = system.actorOf(props)

    // 发送消息
    runBlocking {
        system.send(pid, "Hello, Native World!")

        // 等待消息处理完成
        Thread.sleep(100)
    }

    println("示例完成!")
}
