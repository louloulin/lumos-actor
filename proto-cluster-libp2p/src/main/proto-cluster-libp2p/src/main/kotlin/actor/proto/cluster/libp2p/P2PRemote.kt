package actor.proto.cluster.libp2p

import actor.proto.ActorSystem
import actor.proto.MessageEnvelope
import actor.proto.PID
import actor.proto.cluster.Cluster
import io.libp2p.core.Host
import io.libp2p.core.PeerId
import io.libp2p.core.Stream
import io.libp2p.core.multistream.ProtocolBinding
import io.libp2p.core.multistream.ProtocolId
import io.libp2p.protocol.ProtocolMessageHandler
import io.netty.buffer.ByteBuf
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * P2PRemote 负责远程通信
 */
class P2PRemote(
    private val actorSystem: ActorSystem,
    private val host: Host,
    private val cluster: Cluster
) {
    companion object {
        const val PROTOCOL_ID: ProtocolId = "/protoactor/remote/1.0.0"
    }
    
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val pendingRequests = ConcurrentHashMap<String, CompletableDeferred<Any>>()
    
    /**
     * 启动远程通信服务
     */
    fun start() {
        // 注册协议处理器
        host.addProtocolHandler(RemoteProtocol())
        
        logger.info { "P2P remote started" }
    }
    
    /**
     * 停止远程通信服务
     */
    fun stop() {
        // 取消所有未完成的请求
        pendingRequests.forEach { (_, deferred) ->
            if (!deferred.isCompleted) {
                deferred.completeExceptionally(Exception("Remote service shutting down"))
            }
        }
        pendingRequests.clear()
        
        logger.info { "P2P remote stopped" }
    }
    
    /**
     * 发送消息到远程 Actor
     */
    suspend fun send(pid: PID, message: Any) {
        try {
            // 获取目标节点的 PeerId
            val targetAddress = pid.address
            val peerId = getPeerIdFromAddress(targetAddress)
            
            // 创建消息信封
            val envelope = MessageEnvelope(message, null, null)
            
            // 序列化消息
            val serializedMessage = serializeMessage(RemoteMessage.Send(pid.id, envelope))
            
            // 打开到目标节点的流
            val stream = host.newStream(peerId, listOf(PROTOCOL_ID)).get()
            
            // 发送消息
            stream.writeAndFlush(serializedMessage)
            stream.close()
        } catch (e: Exception) {
            logger.error(e) { "Error sending message to ${pid.address}/${pid.id}" }
            throw e
        }
    }
    
    /**
     * 发送请求并等待响应
     */
    suspend fun request(pid: PID, message: Any): Any {
        try {
            // 创建请求 ID
            val requestId = UUID.randomUUID().toString()
            
            // 创建 CompletableDeferred 用于等待响应
            val deferred = CompletableDeferred<Any>()
            pendingRequests[requestId] = deferred
            
            // 获取目标节点的 PeerId
            val targetAddress = pid.address
            val peerId = getPeerIdFromAddress(targetAddress)
            
            // 创建消息信封
            val envelope = MessageEnvelope(message, actorSystem.address, requestId)
            
            // 序列化消息
            val serializedMessage = serializeMessage(RemoteMessage.Request(pid.id, envelope, requestId))
            
            // 打开到目标节点的流
            val stream = host.newStream(peerId, listOf(PROTOCOL_ID)).get()
            
            // 发送消息
            stream.writeAndFlush(serializedMessage)
            stream.close()
            
            // 等待响应
            return deferred.await()
        } catch (e: Exception) {
            logger.error(e) { "Error sending request to ${pid.address}/${pid.id}" }
            throw e
        }
    }
    
    /**
     * 处理接收到的响应
     */
    private fun handleResponse(requestId: String, message: Any) {
        val deferred = pendingRequests.remove(requestId)
        deferred?.complete(message)
    }
    
    /**
     * 从地址获取 PeerId
     */
    private fun getPeerIdFromAddress(address: String): PeerId {
        // 在实际实现中，可能需要从成员列表中查找 PeerId
        // 这里简单地假设地址就是 PeerId 的 Base58 表示
        return PeerId.fromBase58(address)
    }
    
    /**
     * 序列化消息
     */
    private fun serializeMessage(message: RemoteMessage): ByteBuffer {
        // 简单的序列化实现
        // 在实际实现中，应该使用更高效的序列化方式，如 Protocol Buffers
        val json = when (message) {
            is RemoteMessage.Send -> {
                """{"type":"send","target":"${message.target}","message":"${message.envelope.message}"}"""
            }
            is RemoteMessage.Request -> {
                """{"type":"request","target":"${message.target}","message":"${message.envelope.message}","requestId":"${message.requestId}"}"""
            }
            is RemoteMessage.Response -> {
                """{"type":"response","requestId":"${message.requestId}","message":"${message.message}"}"""
            }
        }
        
        val bytes = json.toByteArray(StandardCharsets.UTF_8)
        return ByteBuffer.wrap(bytes)
    }
    
    /**
     * 反序列化消息
     */
    private fun deserializeMessage(bytes: ByteArray): RemoteMessage {
        // 简单的反序列化实现
        // 在实际实现中，应该使用更高效的反序列化方式，如 Protocol Buffers
        val json = String(bytes, StandardCharsets.UTF_8)
        
        // 解析 JSON
        val type = json.substringAfter("\"type\":\"").substringBefore("\"")
        
        return when (type) {
            "send" -> {
                val target = json.substringAfter("\"target\":\"").substringBefore("\"")
                val message = json.substringAfter("\"message\":\"").substringBefore("\"")
                RemoteMessage.Send(target, MessageEnvelope(message, null, null))
            }
            "request" -> {
                val target = json.substringAfter("\"target\":\"").substringBefore("\"")
                val message = json.substringAfter("\"message\":\"").substringBefore("\"")
                val requestId = json.substringAfter("\"requestId\":\"").substringBefore("\"")
                RemoteMessage.Request(target, MessageEnvelope(message, actorSystem.address, requestId), requestId)
            }
            "response" -> {
                val requestId = json.substringAfter("\"requestId\":\"").substringBefore("\"")
                val message = json.substringAfter("\"message\":\"").substringBefore("\"")
                RemoteMessage.Response(requestId, message)
            }
            else -> throw IllegalArgumentException("Unknown message type: $type")
        }
    }
    
    /**
     * 远程协议处理器
     */
    inner class RemoteProtocol : ProtocolBinding<RemoteProtocol.Handler> {
        override fun getProtocolId(): ProtocolId = PROTOCOL_ID
        
        override fun initChannel(stream: Stream, isInitiator: Boolean): java.util.concurrent.CompletableFuture<Handler> {
            val handler = Handler()
            stream.pushHandler(handler)
            
            return java.util.concurrent.CompletableFuture.completedFuture(handler)
        }
        
        /**
         * 协议处理器
         */
        inner class Handler : ProtocolMessageHandler<ByteBuf> {
            private lateinit var stream: Stream
            
            override fun onActivated(stream: Stream) {
                this.stream = stream
            }
            
            override fun onMessage(stream: Stream, msg: ByteBuf) {
                try {
                    val bytes = ByteArray(msg.readableBytes())
                    msg.readBytes(bytes)
                    
                    // 处理消息
                    scope.launch {
                        handleIncomingMessage(bytes, stream)
                    }
                } catch (e: Exception) {
                    logger.error(e) { "Error handling message" }
                }
            }
        }
    }
    
    /**
     * 处理接收到的消息
     */
    private suspend fun handleIncomingMessage(bytes: ByteArray, stream: Stream) {
        try {
            val message = deserializeMessage(bytes)
            
            when (message) {
                is RemoteMessage.Send -> {
                    // 获取目标 Actor
                    val pid = PID(actorSystem.address, message.target)
                    
                    // 发送消息
                    actorSystem.root.send(pid, message.envelope.message)
                }
                is RemoteMessage.Request -> {
                    // 获取目标 Actor
                    val pid = PID(actorSystem.address, message.target)
                    
                    // 发送请求并等待响应
                    val response = actorSystem.root.requestAwait<Any>(pid, message.envelope.message)
                    
                    // 发送响应
                    val responseMessage = RemoteMessage.Response(message.requestId, response)
                    val serializedResponse = serializeMessage(responseMessage)
                    
                    // 打开到请求节点的流
                    val sender = message.envelope.sender ?: throw IllegalArgumentException("Sender is null")
                    val senderPeerId = getPeerIdFromAddress(sender)
                    val responseStream = host.newStream(senderPeerId, listOf(PROTOCOL_ID)).get()
                    
                    // 发送响应
                    responseStream.writeAndFlush(serializedResponse)
                    responseStream.close()
                }
                is RemoteMessage.Response -> {
                    // 处理响应
                    handleResponse(message.requestId, message.message)
                }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error handling incoming message" }
        }
    }
}

/**
 * 远程消息
 */
sealed class RemoteMessage {
    /**
     * 发送消息
     */
    data class Send(val target: String, val envelope: MessageEnvelope) : RemoteMessage()
    
    /**
     * 请求消息
     */
    data class Request(val target: String, val envelope: MessageEnvelope, val requestId: String) : RemoteMessage()
    
    /**
     * 响应消息
     */
    data class Response(val requestId: String, val message: Any) : RemoteMessage()
}
