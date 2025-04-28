package com.dataflare.cluster

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import mu.KotlinLogging
import actor.proto.ActorSystem
import java.time.Instant

private val logger = KotlinLogging.logger {}

/**
 * 集群节点信息
 */
data class NodeInfo(
    val id: String,
    val address: String,
    val roles: Set<String>,
    val metadata: Map<String, String> = emptyMap(),
    val joinedAt: Long = Instant.now().toEpochMilli(),
    val status: NodeStatus = NodeStatus.ALIVE
)

/**
 * 节点状态
 */
enum class NodeStatus {
    ALIVE,
    SUSPECT,
    DEAD,
    LEFT
}

/**
 * 集群事件
 */
sealed class ClusterEvent {
    data class NodeJoined(val node: NodeInfo) : ClusterEvent()
    data class NodeLeft(val nodeId: String) : ClusterEvent()
    data class NodeDead(val nodeId: String) : ClusterEvent()
    data class LeaderElected(val nodeId: String) : ClusterEvent()
    data class MembershipChanged(val nodes: List<NodeInfo>) : ClusterEvent()
}

/**
 * 集群提供者接口
 */
interface ClusterProvider {
    /**
     * 启动集群成员
     */
    suspend fun startMember(cluster: ActorSystem): Boolean

    /**
     * 启动集群客户端
     */
    suspend fun startClient(cluster: ActorSystem): Boolean

    /**
     * 关闭集群
     */
    suspend fun shutdown(graceful: Boolean = true): Boolean

    /**
     * 获取集群事件流
     */
    fun events(): Flow<ClusterEvent>

    /**
     * 获取集群成员
     */
    fun members(): List<NodeInfo>

    /**
     * 获取集群领导者
     */
    fun leader(): NodeInfo?

    /**
     * 当前节点是否是领导者
     */
    fun isLeader(): Boolean
}

/**
 * 集群配置基类
 */
abstract class ClusterConfig {
    abstract val clusterName: String
    abstract val nodeName: String
    abstract val nodeRoles: Set<String>
    abstract val nodeMetadata: Map<String, String>
}

/**
 * P2P集群配置
 */
data class P2PClusterConfig(
    override val clusterName: String,
    override val nodeName: String,
    override val nodeRoles: Set<String> = setOf("member"),
    override val nodeMetadata: Map<String, String> = emptyMap(),
    val seedNodes: List<String> = emptyList(),
    val discoveryMethod: DiscoveryMethod = DiscoveryMethod.MDNS,
    val gossipInterval: Long = 1000,
    val probeInterval: Long = 1000,
    val probeTimeout: Long = 500,
    val suspicionMultiplier: Int = 5
) : ClusterConfig()

/**
 * 发现方法
 */
enum class DiscoveryMethod {
    MDNS,
    DHT,
    STATIC,
    KUBERNETES
}

/**
 * 中心化集群配置
 */
data class CentralizedClusterConfig(
    override val clusterName: String,
    override val nodeName: String,
    override val nodeRoles: Set<String> = setOf("member"),
    override val nodeMetadata: Map<String, String> = emptyMap(),
    val coordinatorNodes: List<String>,
    val electionStrategy: ElectionStrategy = ElectionStrategy.RAFT,
    val heartbeatInterval: Long = 1000,
    val heartbeatTimeout: Long = 3000
) : ClusterConfig()

/**
 * 选举策略
 */
enum class ElectionStrategy {
    RAFT,
    BULLY,
    STATIC
}

/**
 * 混合集群配置
 */
data class HybridClusterConfig(
    override val clusterName: String,
    override val nodeName: String,
    override val nodeRoles: Set<String> = setOf("member"),
    override val nodeMetadata: Map<String, String> = emptyMap(),
    val p2pConfig: P2PClusterConfig,
    val centralizedConfig: CentralizedClusterConfig,
    val regionName: String,
    val interRegionCommunication: Boolean = true
) : ClusterConfig()

/**
 * 抽象集群提供者
 */
abstract class AbstractClusterProvider : ClusterProvider {
    protected val _events = kotlinx.coroutines.flow.MutableSharedFlow<ClusterEvent>(
        replay = 10,
        extraBufferCapacity = 10
    )
    protected val _members = mutableListOf<NodeInfo>()
    protected var _leader: NodeInfo? = null
    protected var _isLeader = false

    override fun events(): Flow<ClusterEvent> = _events

    override fun members(): List<NodeInfo> = _members.toList()

    override fun leader(): NodeInfo? = _leader

    override fun isLeader(): Boolean = _isLeader

    protected suspend fun updateMembers(members: List<NodeInfo>) {
        val membersCopy: List<NodeInfo>
        synchronized(_members) {
            _members.clear()
            _members.addAll(members)
            membersCopy = members.toList()
        }
        _events.emit(ClusterEvent.MembershipChanged(membersCopy))
    }

    protected suspend fun updateLeader(leader: NodeInfo) {
        _leader = leader
        _events.emit(ClusterEvent.LeaderElected(leader.id))
    }
}
