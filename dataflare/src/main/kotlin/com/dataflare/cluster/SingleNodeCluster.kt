package com.dataflare.cluster

import actor.proto.ActorSystem
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.time.Instant
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * 单机模式集群配置
 */
data class SingleNodeConfig(
    override val clusterName: String,
    override val nodeName: String,
    override val nodeRoles: Set<String> = setOf("member"),
    override val nodeMetadata: Map<String, String> = emptyMap()
) : ClusterConfig()

/**
 * 单机模式集群提供者实现
 * 这是一个轻量级实现，适用于单机部署场景，不需要网络通信
 */
class SingleNodeCluster(val config: SingleNodeConfig) : AbstractClusterProvider() {
    private val nodeId = UUID.randomUUID().toString()
    private var isStarted = false
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    override suspend fun startMember(cluster: ActorSystem): Boolean {
        if (isStarted) {
            logger.warn { "Single node cluster already started" }
            return false
        }

        logger.info { "Starting single node cluster: ${config.nodeName}" }

        // 创建本地节点信息
        val localNode = NodeInfo(
            id = nodeId,
            address = "localhost:0",
            roles = config.nodeRoles,
            metadata = config.nodeMetadata
        )

        // 设置为领导者
        _leader = localNode
        _isLeader = true

        // 添加到成员列表
        _members.add(localNode)

        // 发送成员变更事件和领导者选举事件
        scope.launch {
            _events.emit(ClusterEvent.MembershipChanged(_members.toList()))
            _events.emit(ClusterEvent.LeaderElected(nodeId))
        }

        isStarted = true
        logger.info { "Single node cluster started successfully" }

        return true
    }

    override suspend fun startClient(cluster: ActorSystem): Boolean {
        // 单机模式下，客户端和成员是相同的
        return startMember(cluster)
    }

    override suspend fun shutdown(graceful: Boolean): Boolean {
        if (!isStarted) {
            logger.warn { "Single node cluster not started" }
            return false
        }

        logger.info { "Shutting down single node cluster: ${config.nodeName}" }

        // 清理资源
        _members.clear()
        _leader = null
        _isLeader = false

        isStarted = false
        logger.info { "Single node cluster shut down successfully" }

        return true
    }
}
