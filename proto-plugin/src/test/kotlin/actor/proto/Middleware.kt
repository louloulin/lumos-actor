package actor.proto

/**
 * 接收中间件类型
 */
typealias Receive = suspend (Context) -> Unit

/**
 * 接收中间件
 */
typealias ReceiveMiddleware = (next: Receive) -> Receive

/**
 * 发送函数类型
 */
typealias Send = suspend (context: SenderContext, target: TestPID, envelope: MessageEnvelope) -> Unit

/**
 * 发送中间件
 */
typealias SenderMiddleware = (next: Send) -> Send

/**
 * 发送者上下文
 */
interface SenderContext

/**
 * 消息信封
 */
data class MessageEnvelope(val message: Any)
