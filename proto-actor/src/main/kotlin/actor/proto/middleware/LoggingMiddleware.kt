package actor.proto.middleware

import actor.proto.Context
import actor.proto.Receive
import actor.proto.ReceiveMiddleware
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * 日志中间件，用于记录 Actor 接收到的消息
 * @return 接收中间件
 */
fun logReceive(): ReceiveMiddleware {
    return { next ->
        { ctx ->
            logger.info { "Actor ${ctx.self} received message: ${ctx.message}" }
            next(ctx)
        }
    }
}

/**
 * 日志中间件，用于记录 Actor 接收到的消息，带有自定义日志函数
 * @param logFn 自定义日志函数
 * @return 接收中间件
 */
fun logReceive(logFn: (Context) -> Unit): ReceiveMiddleware {
    return { next ->
        { ctx ->
            logFn(ctx)
            next(ctx)
        }
    }
}
