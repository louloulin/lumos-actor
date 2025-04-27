package actor.proto.cluster.libp2p

import actor.proto.cluster.Cluster
import actor.proto.cluster.Member
import actor.proto.cluster.MemberStatus
import com.google.protobuf.ByteString
import io.libp2p.core.Host
import io.libp2p.core.PeerId
import io.libp2p.core.pubsub.Topic
import io.libp2p.pubsub.gossip.Gossip
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

/**
 * 消息类型枚举
 */
enum class GossipMessageType {
    MEMBER_UP,
    MEMBER_DOWN,
    MEMBER_JOINED,
    MEMBER_LEFT,
    HEARTBEAT,
    GRACEFUL_LEAVE,
    ACTOR_ACTIVATION,
    ACTOR_DEACTIVATION
}

/**
 * Gossip 消息
 */
data class GossipMessage(
    val type: GossipMessageType,
    val senderId: String,
    val timestamp: Long,
    val payload: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as GossipMessage

        if (type != other.type) return false
        if (senderId != other.senderId) return false
        if (timestamp != other.timestamp) return false
        if (!payload.contentEquals(other.payload)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + senderId.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + payload.contentHashCode()
        return result
    }
}

/**
 * P2PGossiper 负责集群状态同步
 */
class P2PGossiper(
    private val cluster: Cluster,
    private val host: Host,
    private val config: P2PClusterConfig
) {
    private lateinit var gossip: Gossip
    private lateinit var clusterTopic: Topic
    private val messageCounter = AtomicLong(0)
    private val processedMessages = ConcurrentHashMap<String, Long>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    
    /**
     * 启动 gossip 服务
     */
    fun start() {
        // 初始化 gossipsub
        gossip = Gossip(host)
        
        // 订阅集群主题
        val topicName = "protoactor-cluster-${config.clusterName}"
        clusterTopic = Topic(topicName)
        
        // 启动 gossip
        gossip.start()
        
        // 订阅主题
        gossip.subscribe(clusterTopic) { message ->
            handleGossipMessage(message.data, message.from)
        }
        
        logger.info { "Subscribed to gossip topic: $topicName" }
    }
    
    /**
     * 停止 gossip 服务
     */
    fun stop() {
        gossip.unsubscribe(clusterTopic)
        gossip.stop()
    }
    
    /**
     * 发布成员上线消息
     */
    fun publishMemberUp(memberInfo: MemberInfo) {
        val message = createGossipMessage(
            GossipMessageType.MEMBER_UP,
            memberInfoToBytes(memberInfo)
        )
        publishMessage(message)
    }
    
    /**
     * 发布心跳消息
     */
    fun publishHeartbeat(memberInfo: MemberInfo) {
        val message = createGossipMessage(
            GossipMessageType.HEARTBEAT,
            memberInfoToBytes(memberInfo)
        )
        publishMessage(message)
    }
    
    /**
     * 发布优雅离开消息
     */
    fun publishGracefulLeave() {
        val message = createGossipMessage(
            GossipMessageType.GRACEFUL_LEAVE,
            ByteArray(0)
        )
        publishMessage(message)
    }
    
    /**
     * 发布 Actor 激活消息
     */
    fun publishActorActivation(clusterIdentity: String, pid: String) {
        val payload = "$clusterIdentity:$pid".toByteArray(StandardCharsets.UTF_8)
        val message = createGossipMessage(
            GossipMessageType.ACTOR_ACTIVATION,
            payload
        )
        publishMessage(message)
    }
    
    /**
     * 发布 Actor 停用消息
     */
    fun publishActorDeactivation(clusterIdentity: String) {
        val payload = clusterIdentity.toByteArray(StandardCharsets.UTF_8)
        val message = createGossipMessage(
            GossipMessageType.ACTOR_DEACTIVATION,
            payload
        )
        publishMessage(message)
    }
    
    /**
     * 创建 Gossip 消息
     */
    private fun createGossipMessage(type: GossipMessageType, payload: ByteArray): GossipMessage {
        return GossipMessage(
            type = type,
            senderId = cluster.actorSystem.address,
            timestamp = System.currentTimeMillis(),
            payload = payload
        )
    }
    
    /**
     * 发布消息
     */
    private fun publishMessage(message: GossipMessage) {
        try {
            val messageBytes = gossipMessageToBytes(message)
            gossip.publish(clusterTopic, messageBytes)
        } catch (e: Exception) {
            logger.error(e) { "Error publishing gossip message" }
        }
    }
    
    /**
     * 处理接收到的 gossip 消息
     */
    fun handleGossipMessage(data: ByteArray, from: PeerId) {
        scope.launch {
            try {
                val message = bytesToGossipMessage(data)
                
                // 检查是否已处理过该消息
                val messageId = "${message.senderId}:${message.timestamp}"
                if (processedMessages.containsKey(messageId)) {
                    return@launch
                }
                
                // 标记为已处理
                processedMessages[messageId] = System.currentTimeMillis()
                
                // 清理过期的已处理消息
                cleanupProcessedMessages()
                
                // 处理消息
                when (message.type) {
                    GossipMessageType.MEMBER_UP -> handleMemberUp(message)
                    GossipMessageType.HEARTBEAT -> handleHeartbeat(message)
                    GossipMessageType.GRACEFUL_LEAVE -> handleGracefulLeave(message)
                    GossipMessageType.ACTOR_ACTIVATION -> handleActorActivation(message)
                    GossipMessageType.ACTOR_DEACTIVATION -> handleActorDeactivation(message)
                    else -> logger.warn { "Unhandled gossip message type: ${message.type}" }
                }
            } catch (e: Exception) {
                logger.error(e) { "Error handling gossip message" }
            }
        }
    }
    
    /**
     * 处理原始消息
     */
    fun handleRawMessage(data: ByteArray, from: PeerId) {
        handleGossipMessage(data, from)
    }
    
    /**
     * 处理成员上线消息
     */
    private fun handleMemberUp(message: GossipMessage) {
        val memberInfo = bytesToMemberInfo(message.payload)
        
        // 创建集群成员
        val member = Member(
            id = memberInfo.id,
            host = memberInfo.host,
            port = memberInfo.port,
            kinds = memberInfo.kinds
        )
        
        // 更新成员列表
        cluster.memberList.updateClusterTopology(listOf(member))
        
        logger.info { "Member up: ${member.id}" }
    }
    
    /**
     * 处理心跳消息
     */
    private fun handleHeartbeat(message: GossipMessage) {
        val memberInfo = bytesToMemberInfo(message.payload)
        
        // 创建集群成员
        val member = Member(
            id = memberInfo.id,
            host = memberInfo.host,
            port = memberInfo.port,
            kinds = memberInfo.kinds
        )
        
        // 更新成员列表
        cluster.memberList.updateClusterTopology(listOf(member))
    }
    
    /**
     * 处理优雅离开消息
     */
    private fun handleGracefulLeave(message: GossipMessage) {
        // 将成员标记为已离开
        val memberId = message.senderId
        cluster.memberList.updateMemberStatus(memberId, MemberStatus.LEAVING)
        
        logger.info { "Member gracefully leaving: $memberId" }
    }
    
    /**
     * 处理 Actor 激活消息
     */
    private fun handleActorActivation(message: GossipMessage) {
        val payload = String(message.payload, StandardCharsets.UTF_8)
        val parts = payload.split(":", limit = 2)
        if (parts.size != 2) {
            logger.error { "Invalid actor activation payload: $payload" }
            return
        }
        
        val clusterIdentity = parts[0]
        val pid = parts[1]
        
        // 更新 PID 缓存
        // 注意：这里需要解析 clusterIdentity 为 kind 和 identity
        val identityParts = clusterIdentity.split("/", limit = 2)
        if (identityParts.size != 2) {
            logger.error { "Invalid cluster identity: $clusterIdentity" }
            return
        }
        
        val kind = identityParts[0]
        val identity = identityParts[1]
        
        // 创建 ClusterIdentity 对象
        val clusterIdentityObj = actor.proto.cluster.ClusterIdentity(kind, identity)
        
        // 创建 PID 对象
        val pidObj = actor.proto.PID(message.senderId, pid)
        
        // 更新缓存
        cluster.pidCache.add(clusterIdentityObj, pidObj)
        
        logger.debug { "Actor activated: $clusterIdentity -> $pid" }
    }
    
    /**
     * 处理 Actor 停用消息
     */
    private fun handleActorDeactivation(message: GossipMessage) {
        val clusterIdentity = String(message.payload, StandardCharsets.UTF_8)
        
        // 解析 clusterIdentity 为 kind 和 identity
        val parts = clusterIdentity.split("/", limit = 2)
        if (parts.size != 2) {
            logger.error { "Invalid cluster identity: $clusterIdentity" }
            return
        }
        
        val kind = parts[0]
        val identity = parts[1]
        
        // 创建 ClusterIdentity 对象
        val clusterIdentityObj = actor.proto.cluster.ClusterIdentity(kind, identity)
        
        // 从缓存中移除
        cluster.pidCache.remove(clusterIdentityObj)
        
        logger.debug { "Actor deactivated: $clusterIdentity" }
    }
    
    /**
     * 清理过期的已处理消息
     */
    private fun cleanupProcessedMessages() {
        val now = System.currentTimeMillis()
        val expireTime = now - Duration.ofMinutes(10).toMillis()
        
        processedMessages.entries.removeIf { (_, timestamp) ->
            timestamp < expireTime
        }
    }
    
    /**
     * 将 MemberInfo 转换为字节数组
     */
    private fun memberInfoToBytes(memberInfo: MemberInfo): ByteArray {
        // 简单的序列化实现
        val sb = StringBuilder()
        sb.append(memberInfo.id).append("|")
        sb.append(memberInfo.host).append("|")
        sb.append(memberInfo.port).append("|")
        sb.append(memberInfo.kinds.joinToString(","))
        
        return sb.toString().toByteArray(StandardCharsets.UTF_8)
    }
    
    /**
     * 将字节数组转换为 MemberInfo
     */
    private fun bytesToMemberInfo(bytes: ByteArray): MemberInfo {
        val str = String(bytes, StandardCharsets.UTF_8)
        val parts = str.split("|", limit = 4)
        
        if (parts.size != 4) {
            throw IllegalArgumentException("Invalid MemberInfo bytes")
        }
        
        val id = parts[0]
        val host = parts[1]
        val port = parts[2].toInt()
        val kinds = if (parts[3].isEmpty()) emptyList() else parts[3].split(",")
        
        return MemberInfo(id, host, port, kinds)
    }
    
    /**
     * 将 GossipMessage 转换为字节数组
     */
    private fun gossipMessageToBytes(message: GossipMessage): ByteArray {
        // 简单的序列化实现
        val sb = StringBuilder()
        sb.append(message.type.ordinal).append("|")
        sb.append(message.senderId).append("|")
        sb.append(message.timestamp).append("|")
        
        // 将 payload 编码为 Base64
        val payloadBase64 = java.util.Base64.getEncoder().encodeToString(message.payload)
        sb.append(payloadBase64)
        
        return sb.toString().toByteArray(StandardCharsets.UTF_8)
    }
    
    /**
     * 将字节数组转换为 GossipMessage
     */
    private fun bytesToGossipMessage(bytes: ByteArray): GossipMessage {
        val str = String(bytes, StandardCharsets.UTF_8)
        val parts = str.split("|", limit = 4)
        
        if (parts.size != 4) {
            throw IllegalArgumentException("Invalid GossipMessage bytes")
        }
        
        val type = GossipMessageType.values()[parts[0].toInt()]
        val senderId = parts[1]
        val timestamp = parts[2].toLong()
        
        // 将 Base64 解码为 payload
        val payloadBase64 = parts[3]
        val payload = java.util.Base64.getDecoder().decode(payloadBase64)
        
        return GossipMessage(type, senderId, timestamp, payload)
    }
}
