package actor.proto.cluster.libp2p

import actor.proto.ActorSystem
import actor.proto.MessageEnvelope
import actor.proto.PID
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterIdentity
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
import kotlin.random.Random

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
    private val streamCache = ConcurrentHashMap<String, Stream>()
    private val streamCacheTTL = ConcurrentHashMap<String, Long>()
    
    /**
     * 启动远程通信服务
     */
    fun start() {
        // 注册协议处理器
        host.addProtocolHandler(RemoteProtocol())
        
        // 启动定期清理缓存的任务
        startCacheCleanupTask()
        
        logger.info { "P2P remote started" }
    }
    
    /**
     * 停止远程通信服务
     */
    fun stop() {
        // 关闭所有缓存的流
        streamCache.forEach { (_, stream) ->
            try {
                stream.close()
            } catch (e: Exception) {
                logger.error(e) { "Error closing stream" }
            }
        }
        streamCache.clear()
        streamCacheTTL.clear()
        
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
     * 启动定期清理缓存的任务
     */
    private fun startCacheCleanupTask() {
        scope.launch {
            while (true) {
                try {
                    // 清理过期的流缓存
                    cleanupStreamCache()
                    
                    // 等待一段时间
                    kotlinx.coroutines.delay(60000) // 每分钟清理一次
                } catch (e: Exception) {
                    logger.error(e) { "Error cleaning up stream cache" }
                }
            }
        }
    }
    
    /**
     * 清理过期的流缓存
     */
    private fun cleanupStreamCache() {
        val now = System.currentTimeMillis()
        val expiredKeys = mutableListOf<String>()
        
        // 找出过期的键
        streamCacheTTL.forEach { (key, timestamp) ->
            if (now - timestamp > 300000) { // 5分钟过期
                expiredKeys.add(key)
            }
        }
        
        // 关闭并移除过期的流
        expiredKeys.forEach { key ->
            val stream = streamCache.remove(key)
            streamCacheTTL.remove(key)
            
            if (stream != null) {
                try {
                    stream.close()
                } catch (e: Exception) {
                    logger.error(e) { "Error closing expired stream" }
                }
            }
        }
        
        if (expiredKeys.isNotEmpty()) {
            logger.debug { "Cleaned up ${expiredKeys.size} expired streams" }
        }
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
            
            // 获取或创建到目标节点的流
            val stream = getOrCreateStream(peerId)
            
            // 发送消息
            stream.writeAndFlush(serializedMessage)
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
            
            // 获取或创建到目标节点的流
            val stream = getOrCreateStream(peerId)
            
            // 发送消息
            stream.writeAndFlush(serializedMessage)
            
            // 等待响应
            return deferred.await()
        } catch (e: Exception) {
            logger.error(e) { "Error sending request to ${pid.address}/${pid.id}" }
            throw e
        }
    }
    
    /**
     * 获取或创建到目标节点的流
     */
    private fun getOrCreateStream(peerId: PeerId): Stream {
        val key = peerId.toBase58()
        
        // 检查缓存中是否有可用的流
        val cachedStream = streamCache[key]
        if (cachedStream != null && cachedStream.isActive()) {
            // 更新最后使用时间
            streamCacheTTL[key] = System.currentTimeMillis()
            return cachedStream
        }
        
        // 创建新的流
        val stream = host.newStream(peerId, listOf(PROTOCOL_ID)).get()
        
        // 缓存流
        streamCache[key] = stream
        streamCacheTTL[key] = System.currentTimeMillis()
        
        return stream
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
        // 使用更高效的序列化方式
        val buffer = ByteBuffer.allocate(1024) // 初始分配 1KB
        
        when (message) {
            is RemoteMessage.Send -> {
                // 消息类型: 1 字节
                buffer.put(1)
                
                // 目标长度: 2 字节
                val targetBytes = message.target.toByteArray(StandardCharsets.UTF_8)
                buffer.putShort(targetBytes.size.toShort())
                
                // 目标
                buffer.put(targetBytes)
                
                // 序列化消息内容
                val messageBytes = serializeMessageContent(message.envelope.message)
                
                // 消息长度: 4 字节
                buffer.putInt(messageBytes.size)
                
                // 消息内容
                buffer.put(messageBytes)
            }
            is RemoteMessage.Request -> {
                // 消息类型: 1 字节
                buffer.put(2)
                
                // 目标长度: 2 字节
                val targetBytes = message.target.toByteArray(StandardCharsets.UTF_8)
                buffer.putShort(targetBytes.size.toShort())
                
                // 目标
                buffer.put(targetBytes)
                
                // 请求 ID 长度: 2 字节
                val requestIdBytes = message.requestId.toByteArray(StandardCharsets.UTF_8)
                buffer.putShort(requestIdBytes.size.toShort())
                
                // 请求 ID
                buffer.put(requestIdBytes)
                
                // 发送者长度: 2 字节
                val senderBytes = (message.envelope.sender ?: "").toByteArray(StandardCharsets.UTF_8)
                buffer.putShort(senderBytes.size.toShort())
                
                // 发送者
                buffer.put(senderBytes)
                
                // 序列化消息内容
                val messageBytes = serializeMessageContent(message.envelope.message)
                
                // 消息长度: 4 字节
                buffer.putInt(messageBytes.size)
                
                // 消息内容
                buffer.put(messageBytes)
            }
            is RemoteMessage.Response -> {
                // 消息类型: 1 字节
                buffer.put(3)
                
                // 请求 ID 长度: 2 字节
                val requestIdBytes = message.requestId.toByteArray(StandardCharsets.UTF_8)
                buffer.putShort(requestIdBytes.size.toShort())
                
                // 请求 ID
                buffer.put(requestIdBytes)
                
                // 序列化消息内容
                val messageBytes = serializeMessageContent(message.message)
                
                // 消息长度: 4 字节
                buffer.putInt(messageBytes.size)
                
                // 消息内容
                buffer.put(messageBytes)
            }
        }
        
        // 设置位置并返回
        buffer.flip()
        return buffer
    }
    
    /**
     * 序列化消息内容
     */
    private fun serializeMessageContent(message: Any): ByteArray {
        // 如果是字符串，直接转换
        if (message is String) {
            return message.toByteArray(StandardCharsets.UTF_8)
        }
        
        // 如果是原始类型，转换为字符串
        if (message is Number || message is Boolean) {
            return message.toString().toByteArray(StandardCharsets.UTF_8)
        }
        
        // 其他类型使用 JSON 序列化
        // 在实际实现中，应该使用更高效的序列化方式，如 Protocol Buffers
        val json = message.toString()
        return json.toByteArray(StandardCharsets.UTF_8)
    }
    
    /**
     * 反序列化消息
     */
    private fun deserializeMessage(bytes: ByteArray): RemoteMessage {
        val buffer = ByteBuffer.wrap(bytes)
        
        // 读取消息类型
        val type = buffer.get()
        
        return when (type.toInt()) {
            1 -> { // Send
                // 读取目标
                val targetLength = buffer.getShort().toInt()
                val targetBytes = ByteArray(targetLength)
                buffer.get(targetBytes)
                val target = String(targetBytes, StandardCharsets.UTF_8)
                
                // 读取消息
                val messageLength = buffer.getInt()
                val messageBytes = ByteArray(messageLength)
                buffer.get(messageBytes)
                val message = deserializeMessageContent(messageBytes)
                
                RemoteMessage.Send(target, MessageEnvelope(message, null, null))
            }
            2 -> { // Request
                // 读取目标
                val targetLength = buffer.getShort().toInt()
                val targetBytes = ByteArray(targetLength)
                buffer.get(targetBytes)
                val target = String(targetBytes, StandardCharsets.UTF_8)
                
                // 读取请求 ID
                val requestIdLength = buffer.getShort().toInt()
                val requestIdBytes = ByteArray(requestIdLength)
                buffer.get(requestIdBytes)
                val requestId = String(requestIdBytes, StandardCharsets.UTF_8)
                
                // 读取发送者
                val senderLength = buffer.getShort().toInt()
                val senderBytes = ByteArray(senderLength)
                buffer.get(senderBytes)
                val sender = if (senderLength > 0) String(senderBytes, StandardCharsets.UTF_8) else null
                
                // 读取消息
                val messageLength = buffer.getInt()
                val messageBytes = ByteArray(messageLength)
                buffer.get(messageBytes)
                val message = deserializeMessageContent(messageBytes)
                
                RemoteMessage.Request(target, MessageEnvelope(message, sender, requestId), requestId)
            }
            3 -> { // Response
                // 读取请求 ID
                val requestIdLength = buffer.getShort().toInt()
                val requestIdBytes = ByteArray(requestIdLength)
                buffer.get(requestIdBytes)
                val requestId = String(requestIdBytes, StandardCharsets.UTF_8)
                
                // 读取消息
                val messageLength = buffer.getInt()
                val messageBytes = ByteArray(messageLength)
                buffer.get(messageBytes)
                val message = deserializeMessageContent(messageBytes)
                
                RemoteMessage.Response(requestId, message)
            }
            else -> throw IllegalArgumentException("Unknown message type: $type")
        }
    }
    
    /**
     * 反序列化消息内容
     */
    private fun deserializeMessageContent(bytes: ByteArray): Any {
        // 尝试将字节数组转换为字符串
        val str = String(bytes, StandardCharsets.UTF_8)
        
        // 尝试解析为数字
        str.toIntOrNull()?.let { return it }
        str.toLongOrNull()?.let { return it }
        str.toDoubleOrNull()?.let { return it }
        
        // 尝试解析为布尔值
        if (str == "true") return true
        if (str == "false") return false
        
        // 其他情况下返回字符串
        return str
    }
    
    /**
     * 处理接收到的消息
     */
    private suspend fun handleIncomingMessage(bytes: ByteArray, stream: Stream) {
        try {
            val message = deserializeMessage(bytes)
            
            when (message) {
                is RemoteMessage.Send -> {
                    // 如果是激活请求
                    if (message.target == "activator" && message.envelope.message is String) {
                        val msgStr = message.envelope.message as String
                        if (msgStr.startsWith("activate:")) {
                            handleActivationRequest(msgStr, stream)
                            return
                        } else if (msgStr.startsWith("activated:")) {
                            handleActivationResponse(msgStr)
                            return
                        }
                    }
                    
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
                    val responseStream = getOrCreateStream(senderPeerId)
                    
                    // 发送响应
                    responseStream.writeAndFlush(serializedResponse)
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
    
    /**
     * 处理激活请求
     */
    private suspend fun handleActivationRequest(message: String, stream: Stream) {
        try {
            // 解析消息
            // 格式: activate:kind/identity:requestId
            val parts = message.split(":", limit = 3)
            if (parts.size != 3) {
                logger.error { "Invalid activation request format: $message" }
                return
            }
            
            val clusterIdentityStr = parts[1]
            val requestId = parts[2]
            
            // 解析集群身份
            val identityParts = clusterIdentityStr.split("/", limit = 2)
            if (identityParts.size != 2) {
                logger.error { "Invalid cluster identity format: $clusterIdentityStr" }
                return
            }
            
            val kind = identityParts[0]
            val identity = identityParts[1]
            
            // 创建 ClusterIdentity 对象
            val clusterIdentity = ClusterIdentity(kind, identity)
            
            // 获取发送者地址
            val sender = actorSystem.address
            
            // 如果集群提供者是 P2PClusterProvider
            if (cluster is P2PClusterProvider) {
                // 获取 IdentityLookup
                val identityLookup = cluster.getIdentityLookup()
                
                // 处理激活请求
                identityLookup.handleActivationRequest(requestId, clusterIdentity, sender)
            }
        } catch (e: Exception) {
            logger.error(e) { "Error handling activation request" }
        }
    }
    
    /**
     * 处理激活响应
     */
    private fun handleActivationResponse(message: String) {
        try {
            // 解析消息
            // 格式: activated:requestId:address/id
            val parts = message.split(":", limit = 3)
            if (parts.size != 3) {
                logger.error { "Invalid activation response format: $message" }
                return
            }
            
            val requestId = parts[1]
            val pidStr = parts[2]
            
            // 如果集群提供者是 P2PClusterProvider
            if (cluster is P2PClusterProvider) {
                // 获取 IdentityLookup
                val identityLookup = cluster.getIdentityLookup()
                
                // 处理激活响应
                identityLookup.handleActivationResponse(requestId, pidStr)
            }
        } catch (e: Exception) {
            logger.error(e) { "Error handling activation response" }
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
