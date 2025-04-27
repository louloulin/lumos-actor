package actor.proto.cluster.libp2p.interop

import actor.proto.MessageEnvelope
import actor.proto.PID
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterIdentity
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * CrossLanguageAdapter 是一个跨语言通信适配器，用于与其他语言实现的 libp2p 节点通信
 */
class CrossLanguageAdapter(
    private val cluster: Cluster
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val pendingRequests = ConcurrentHashMap<String, CompletableDeferred<Any>>()
    
    /**
     * 处理接收到的标准消息
     */
    fun handleIncomingMessage(message: StandardMessage) {
        scope.launch {
            try {
                when (message.type) {
                    MessageType.ACTOR_MESSAGE -> handleActorMessage(message)
                    MessageType.REQUEST -> handleRequest(message)
                    MessageType.RESPONSE -> handleResponse(message)
                    MessageType.HEARTBEAT -> handleHeartbeat(message)
                    MessageType.MEMBER_UP -> handleMemberUp(message)
                    MessageType.MEMBER_DOWN -> handleMemberDown(message)
                    MessageType.MEMBER_LEAVING -> handleMemberLeaving(message)
                    MessageType.PING -> handlePing(message)
                    MessageType.PONG -> handlePong(message)
                    MessageType.ACTOR_ACTIVATION -> handleActorActivation(message)
                    MessageType.DHT_PUT -> handleDhtPut(message)
                    MessageType.DHT_GET -> handleDhtGet(message)
                    MessageType.DHT_REMOVE -> handleDhtRemove(message)
                }
            } catch (e: Exception) {
                logger.error(e) { "Error handling incoming message: ${message.type}" }
            }
        }
    }
    
    /**
     * 处理 Actor 消息
     */
    private suspend fun handleActorMessage(message: StandardMessage) {
        val targetId = message.target ?: throw IllegalArgumentException("Target is required for ACTOR_MESSAGE")
        
        // 创建目标 PID
        val pid = PID(cluster.actorSystem.address, targetId)
        
        // 反序列化消息内容
        val messageContent = deserializeMessageContent(message.payload)
        
        // 发送消息到 Actor
        cluster.actorSystem.root.send(pid, messageContent)
    }
    
    /**
     * 处理请求
     */
    private suspend fun handleRequest(message: StandardMessage) {
        val targetId = message.target ?: throw IllegalArgumentException("Target is required for REQUEST")
        val requestId = message.requestId ?: throw IllegalArgumentException("RequestId is required for REQUEST")
        
        // 创建目标 PID
        val pid = PID(cluster.actorSystem.address, targetId)
        
        // 反序列化消息内容
        val messageContent = deserializeMessageContent(message.payload)
        
        // 发送请求到 Actor
        val response = cluster.actorSystem.root.request(pid, messageContent)
        
        // 发送响应
        sendResponse(message.sender, requestId, response)
    }
    
    /**
     * 处理响应
     */
    private fun handleResponse(message: StandardMessage) {
        val requestId = message.requestId ?: throw IllegalArgumentException("RequestId is required for RESPONSE")
        
        // 获取挂起的请求
        val deferred = pendingRequests.remove(requestId)
        
        if (deferred != null) {
            // 反序列化响应内容
            val responseContent = deserializeMessageContent(message.payload)
            
            // 完成请求
            deferred.complete(responseContent)
        } else {
            logger.warn { "Received response for unknown request: $requestId" }
        }
    }
    
    /**
     * 处理心跳
     */
    private fun handleHeartbeat(message: StandardMessage) {
        // 更新成员状态
        val memberId = message.sender
        cluster.memberList.updateMemberStatus(memberId, actor.proto.cluster.MemberStatus.ALIVE)
    }
    
    /**
     * 处理成员上线
     */
    private fun handleMemberUp(message: StandardMessage) {
        // 解析成员信息
        val memberJson = String(message.payload, StandardCharsets.UTF_8)
        val member = parseMember(memberJson)
        
        // 添加成员
        cluster.memberList.addMember(member)
    }
    
    /**
     * 处理成员下线
     */
    private fun handleMemberDown(message: StandardMessage) {
        // 更新成员状态
        val memberId = message.sender
        cluster.memberList.updateMemberStatus(memberId, actor.proto.cluster.MemberStatus.UNAVAILABLE)
    }
    
    /**
     * 处理成员离开
     */
    private fun handleMemberLeaving(message: StandardMessage) {
        // 更新成员状态
        val memberId = message.sender
        cluster.memberList.updateMemberStatus(memberId, actor.proto.cluster.MemberStatus.LEAVING)
    }
    
    /**
     * 处理 Ping
     */
    private fun handlePing(message: StandardMessage) {
        // 发送 Pong 响应
        sendPong(message.sender)
    }
    
    /**
     * 处理 Pong
     */
    private fun handlePong(message: StandardMessage) {
        // 更新成员状态
        val memberId = message.sender
        cluster.memberList.updateMemberStatus(memberId, actor.proto.cluster.MemberStatus.ALIVE)
    }
    
    /**
     * 处理 Actor 激活
     */
    private suspend fun handleActorActivation(message: StandardMessage) {
        // 解析 Actor 标识
        val identityJson = String(message.payload, StandardCharsets.UTF_8)
        val identity = parseClusterIdentity(identityJson)
        
        // 获取 Actor 类型
        val kind = cluster.getClusterKinds()[identity.kind]
            ?: throw IllegalArgumentException("Unknown actor kind: ${identity.kind}")
        
        // 激活 Actor
        val pid = kind.spawn(identity.identity, cluster)
        
        // 添加到缓存
        cluster.pidCache.add(identity, pid)
    }
    
    /**
     * 处理 DHT 存储
     */
    private fun handleDhtPut(message: StandardMessage) {
        // 解析键值对
        val key = message.metadata["key"] ?: throw IllegalArgumentException("Key is required for DHT_PUT")
        val value = message.payload
        
        // 存储到本地 DHT
        // 注意：这里需要实际的 DHT 实现
        logger.debug { "DHT PUT: $key" }
    }
    
    /**
     * 处理 DHT 获取
     */
    private fun handleDhtGet(message: StandardMessage) {
        val key = message.metadata["key"] ?: throw IllegalArgumentException("Key is required for DHT_GET")
        val requestId = message.requestId ?: throw IllegalArgumentException("RequestId is required for DHT_GET")
        
        // 从本地 DHT 获取
        // 注意：这里需要实际的 DHT 实现
        logger.debug { "DHT GET: $key" }
        
        // 模拟响应
        val value = ByteArray(0)
        
        // 发送响应
        sendDhtResponse(message.sender, requestId, key, value)
    }
    
    /**
     * 处理 DHT 删除
     */
    private fun handleDhtRemove(message: StandardMessage) {
        val key = message.metadata["key"] ?: throw IllegalArgumentException("Key is required for DHT_REMOVE")
        
        // 从本地 DHT 删除
        // 注意：这里需要实际的 DHT 实现
        logger.debug { "DHT REMOVE: $key" }
    }
    
    /**
     * 发送 Actor 消息
     */
    suspend fun sendActorMessage(target: String, targetId: String, message: Any): Boolean {
        try {
            // 序列化消息内容
            val payload = serializeMessageContent(message)
            
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.ACTOR_MESSAGE,
                sender = cluster.actorSystem.address,
                target = targetId,
                payload = payload
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending actor message to $target/$targetId" }
            return false
        }
    }
    
    /**
     * 发送请求
     */
    suspend fun sendRequest(target: String, targetId: String, message: Any): Any {
        try {
            // 创建请求 ID
            val requestId = UUID.randomUUID().toString()
            
            // 创建 CompletableDeferred 用于等待响应
            val deferred = CompletableDeferred<Any>()
            pendingRequests[requestId] = deferred
            
            // 序列化消息内容
            val payload = serializeMessageContent(message)
            
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.REQUEST,
                sender = cluster.actorSystem.address,
                target = targetId,
                requestId = requestId,
                payload = payload
            )
            
            // 发送消息
            if (!sendMessage(target, standardMessage)) {
                pendingRequests.remove(requestId)
                throw Exception("Failed to send request to $target/$targetId")
            }
            
            // 等待响应
            return deferred.await()
        } catch (e: Exception) {
            logger.error(e) { "Error sending request to $target/$targetId" }
            throw e
        }
    }
    
    /**
     * 发送响应
     */
    private fun sendResponse(target: String, requestId: String, response: Any): Boolean {
        try {
            // 序列化响应内容
            val payload = serializeMessageContent(response)
            
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.RESPONSE,
                sender = cluster.actorSystem.address,
                requestId = requestId,
                payload = payload
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending response to $target for request $requestId" }
            return false
        }
    }
    
    /**
     * 发送心跳
     */
    fun sendHeartbeat(target: String): Boolean {
        try {
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.HEARTBEAT,
                sender = cluster.actorSystem.address,
                payload = ByteArray(0)
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending heartbeat to $target" }
            return false
        }
    }
    
    /**
     * 发送成员上线通知
     */
    fun sendMemberUp(target: String, member: actor.proto.cluster.Member): Boolean {
        try {
            // 序列化成员信息
            val memberJson = serializeMember(member)
            val payload = memberJson.toByteArray(StandardCharsets.UTF_8)
            
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.MEMBER_UP,
                sender = cluster.actorSystem.address,
                payload = payload
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending member up notification to $target" }
            return false
        }
    }
    
    /**
     * 发送成员下线通知
     */
    fun sendMemberDown(target: String): Boolean {
        try {
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.MEMBER_DOWN,
                sender = cluster.actorSystem.address,
                payload = ByteArray(0)
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending member down notification to $target" }
            return false
        }
    }
    
    /**
     * 发送成员离开通知
     */
    fun sendMemberLeaving(target: String): Boolean {
        try {
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.MEMBER_LEAVING,
                sender = cluster.actorSystem.address,
                payload = ByteArray(0)
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending member leaving notification to $target" }
            return false
        }
    }
    
    /**
     * 发送 Ping
     */
    fun sendPing(target: String): Boolean {
        try {
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.PING,
                sender = cluster.actorSystem.address,
                payload = ByteArray(0)
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending ping to $target" }
            return false
        }
    }
    
    /**
     * 发送 Pong
     */
    fun sendPong(target: String): Boolean {
        try {
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.PONG,
                sender = cluster.actorSystem.address,
                payload = ByteArray(0)
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending pong to $target" }
            return false
        }
    }
    
    /**
     * 发送 Actor 激活请求
     */
    fun sendActorActivation(target: String, identity: ClusterIdentity): Boolean {
        try {
            // 序列化 Actor 标识
            val identityJson = serializeClusterIdentity(identity)
            val payload = identityJson.toByteArray(StandardCharsets.UTF_8)
            
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.ACTOR_ACTIVATION,
                sender = cluster.actorSystem.address,
                payload = payload
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending actor activation request to $target" }
            return false
        }
    }
    
    /**
     * 发送 DHT 存储请求
     */
    fun sendDhtPut(target: String, key: String, value: ByteArray): Boolean {
        try {
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.DHT_PUT,
                sender = cluster.actorSystem.address,
                payload = value,
                metadata = mapOf("key" to key)
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending DHT put request to $target" }
            return false
        }
    }
    
    /**
     * 发送 DHT 获取请求
     */
    suspend fun sendDhtGet(target: String, key: String): ByteArray? {
        try {
            // 创建请求 ID
            val requestId = UUID.randomUUID().toString()
            
            // 创建 CompletableDeferred 用于等待响应
            val deferred = CompletableDeferred<Any>()
            pendingRequests[requestId] = deferred
            
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.DHT_GET,
                sender = cluster.actorSystem.address,
                requestId = requestId,
                payload = ByteArray(0),
                metadata = mapOf("key" to key)
            )
            
            // 发送消息
            if (!sendMessage(target, standardMessage)) {
                pendingRequests.remove(requestId)
                throw Exception("Failed to send DHT get request to $target")
            }
            
            // 等待响应
            val response = deferred.await()
            
            // 处理响应
            return when (response) {
                is ByteArray -> response
                else -> null
            }
        } catch (e: Exception) {
            logger.error(e) { "Error sending DHT get request to $target" }
            return null
        }
    }
    
    /**
     * 发送 DHT 删除请求
     */
    fun sendDhtRemove(target: String, key: String): Boolean {
        try {
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.DHT_REMOVE,
                sender = cluster.actorSystem.address,
                payload = ByteArray(0),
                metadata = mapOf("key" to key)
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending DHT remove request to $target" }
            return false
        }
    }
    
    /**
     * 发送 DHT 响应
     */
    private fun sendDhtResponse(target: String, requestId: String, key: String, value: ByteArray): Boolean {
        try {
            // 创建标准消息
            val standardMessage = StandardMessage(
                type = MessageType.RESPONSE,
                sender = cluster.actorSystem.address,
                requestId = requestId,
                payload = value,
                metadata = mapOf("key" to key)
            )
            
            // 发送消息
            return sendMessage(target, standardMessage)
        } catch (e: Exception) {
            logger.error(e) { "Error sending DHT response to $target" }
            return false
        }
    }
    
    /**
     * 发送消息
     */
    private fun sendMessage(target: String, message: StandardMessage): Boolean {
        try {
            // 序列化消息
            val messageBytes = message.toBytes()
            
            // 在实际实现中，这里会使用 libp2p 发送消息
            // 这里使用模拟实现
            logger.debug { "Sending ${message.type} message to $target" }
            
            return true
        } catch (e: Exception) {
            logger.error(e) { "Error sending message to $target" }
            return false
        }
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
        
        // 如果是字节数组，直接返回
        if (message is ByteArray) {
            return message
        }
        
        // 其他类型使用 JSON 序列化
        // 在实际实现中，应该使用更高效的序列化方式，如 Protocol Buffers
        val json = message.toString()
        return json.toByteArray(StandardCharsets.UTF_8)
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
     * 序列化成员信息
     */
    private fun serializeMember(member: actor.proto.cluster.Member): String {
        // 在实际实现中，应该使用更高效的序列化方式，如 Protocol Buffers
        return """
            {
                "id": "${member.id}",
                "host": "${member.host}",
                "port": ${member.port},
                "status": "${member.status}",
                "labels": ${serializeLabels(member.labels)}
            }
        """.trimIndent()
    }
    
    /**
     * 序列化标签
     */
    private fun serializeLabels(labels: Map<String, String>): String {
        if (labels.isEmpty()) return "{}"
        
        val entries = labels.entries.joinToString(",") { (key, value) ->
            "\"$key\": \"$value\""
        }
        
        return "{$entries}"
    }
    
    /**
     * 解析成员信息
     */
    private fun parseMember(json: String): actor.proto.cluster.Member {
        // 在实际实现中，应该使用更高效的反序列化方式，如 Protocol Buffers
        // 这里使用简单的解析
        val id = json.substringAfter("\"id\": \"").substringBefore("\"")
        val host = json.substringAfter("\"host\": \"").substringBefore("\"")
        val port = json.substringAfter("\"port\": ").substringBefore(",").toInt()
        val status = actor.proto.cluster.MemberStatus.valueOf(
            json.substringAfter("\"status\": \"").substringBefore("\"")
        )
        
        // 解析标签
        val labels = mutableMapOf<String, String>()
        val labelsJson = json.substringAfter("\"labels\": {").substringBefore("}")
        if (labelsJson.isNotEmpty()) {
            labelsJson.split(",").forEach { entry ->
                val parts = entry.split(":")
                if (parts.size == 2) {
                    val key = parts[0].trim().removeSurrounding("\"")
                    val value = parts[1].trim().removeSurrounding("\"")
                    labels[key] = value
                }
            }
        }
        
        return actor.proto.cluster.Member(id, host, port, labels, status)
    }
    
    /**
     * 序列化集群标识
     */
    private fun serializeClusterIdentity(identity: ClusterIdentity): String {
        return """
            {
                "kind": "${identity.kind}",
                "identity": "${identity.identity}"
            }
        """.trimIndent()
    }
    
    /**
     * 解析集群标识
     */
    private fun parseClusterIdentity(json: String): ClusterIdentity {
        val kind = json.substringAfter("\"kind\": \"").substringBefore("\"")
        val identity = json.substringAfter("\"identity\": \"").substringBefore("\"")
        
        return ClusterIdentity(kind, identity)
    }
}
