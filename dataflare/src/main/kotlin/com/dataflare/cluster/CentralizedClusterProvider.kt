package com.dataflare.cluster

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import actor.proto.ActorSystem
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * 中心化集群提供者实现
 */
class CentralizedClusterProvider(val config: CentralizedClusterConfig) : AbstractClusterProvider() {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val running = AtomicBoolean(false)
    private val nodeId = UUID.randomUUID().toString()
    private val membershipTable = ConcurrentHashMap<String, NodeInfo>()
    private val heartbeatTable = ConcurrentHashMap<String, Long>()
    private val leaderElection = LeaderElectionStrategy(config.electionStrategy)

    override suspend fun startMember(cluster: ActorSystem): Boolean {
        if (running.getAndSet(true)) {
            logger.warn { "Centralized cluster provider already running" }
            return false
        }

        logger.info { "Starting centralized cluster member: ${config.nodeName}" }

        // 创建本地节点信息
        val localNode = NodeInfo(
            id = nodeId,
            address = "localhost:0", // 在实际实现中，这里会是真实的网络地址
            roles = config.nodeRoles,
            metadata = config.nodeMetadata
        )

        // 添加到成员表
        membershipTable[nodeId] = localNode

        // 连接到协调节点
        connectToCoordinators()

        // 启动心跳
        startHeartbeat()

        // 启动领导者选举
        startLeaderElection()

        // 更新成员列表
        updateMembers(membershipTable.values.toList())

        return true
    }

    override suspend fun startClient(cluster: ActorSystem): Boolean {
        if (running.getAndSet(true)) {
            logger.warn { "Centralized cluster provider already running" }
            return false
        }

        logger.info { "Starting centralized cluster client: ${config.nodeName}" }

        // 创建本地节点信息
        val localNode = NodeInfo(
            id = nodeId,
            address = "localhost:0", // 在实际实现中，这里会是真实的网络地址
            roles = setOf("client"),
            metadata = config.nodeMetadata
        )

        // 添加到成员表
        membershipTable[nodeId] = localNode

        // 连接到协调节点
        connectToCoordinators()

        // 启动心跳
        startHeartbeat()

        // 更新成员列表
        updateMembers(membershipTable.values.toList())

        return true
    }

    override suspend fun shutdown(graceful: Boolean): Boolean {
        if (!running.getAndSet(false)) {
            logger.warn { "Centralized cluster provider not running" }
            return false
        }

        logger.info { "Shutting down centralized cluster provider: ${config.nodeName}" }

        if (graceful) {
            // 通知协调节点我们正在离开
            val localNode = membershipTable[nodeId]
            if (localNode != null) {
                val updatedNode = localNode.copy(status = NodeStatus.LEFT)
                membershipTable[nodeId] = updatedNode

                // 在实际实现中，这里会发送离开消息
                logger.info { "Sending leave message to coordinators for node: $nodeId" }

                // 给一些时间让消息传播
                delay(1000)
            }
        }

        // 取消所有协程
        scope.cancel()

        // 清理资源
        membershipTable.clear()
        heartbeatTable.clear()

        return true
    }

    private fun connectToCoordinators() {
        scope.launch {
            logger.info { "Connecting to coordinator nodes: ${config.coordinatorNodes}" }

            // 在实际实现中，这里会连接到协调节点
            // 这里简单模拟连接过程

            // 模拟协调节点
            config.coordinatorNodes.forEachIndexed { index, address ->
                val node = NodeInfo(
                    id = "coordinator-$index",
                    address = address,
                    roles = setOf("coordinator"),
                    metadata = mapOf("coordinator" to "true")
                )
                membershipTable[node.id] = node
                heartbeatTable[node.id] = System.currentTimeMillis()
            }

            // 模拟从协调节点获取成员信息
            val discoveredNodes = (1..5).map { i ->
                NodeInfo(
                    id = "member-$i",
                    address = "192.168.1.$i:5000",
                    roles = setOf("member"),
                    metadata = mapOf("region" to "us-west")
                )
            }

            // 添加到成员表
            discoveredNodes.forEach { node ->
                membershipTable[node.id] = node
                heartbeatTable[node.id] = System.currentTimeMillis()
            }

            // 更新成员列表
            updateMembers(membershipTable.values.toList())
        }
    }

    private fun startHeartbeat() {
        scope.launch {
            logger.info { "Starting heartbeat with interval: ${config.heartbeatInterval}ms" }

            while (running.get()) {
                try {
                    // 在实际实现中，这里会向协调节点发送心跳
                    // 这里简单模拟心跳过程

                    // 更新本地节点的心跳时间
                    heartbeatTable[nodeId] = System.currentTimeMillis()

                    // 检查其他节点的心跳
                    val now = System.currentTimeMillis()
                    val deadNodes = mutableListOf<String>()

                    heartbeatTable.forEach { (id, lastHeartbeat) ->
                        if (id != nodeId && now - lastHeartbeat > config.heartbeatTimeout) {
                            // 节点被认为已死亡
                            val node = membershipTable[id]
                            if (node != null && node.status == NodeStatus.ALIVE) {
                                val deadNode = node.copy(status = NodeStatus.DEAD)
                                membershipTable[id] = deadNode
                                deadNodes.add(id)
                                logger.info { "Node marked as dead due to heartbeat timeout: $id" }
                            }
                        }
                    }

                    // 发送节点死亡事件
                    deadNodes.forEach { nodeId ->
                        scope.launch {
                            _events.emit(ClusterEvent.NodeDead(nodeId))
                        }
                    }

                    // 如果有节点死亡，更新成员列表
                    if (deadNodes.isNotEmpty()) {
                        updateMembers(membershipTable.values.toList())
                    }

                    // 模拟接收到新节点的心跳
                    if (Math.random() < 0.1) {
                        val newNodeId = "heartbeat-node-${UUID.randomUUID().toString().substring(0, 8)}"
                        val newNode = NodeInfo(
                            id = newNodeId,
                            address = "192.168.2.${(Math.random() * 254).toInt() + 1}:5000",
                            roles = setOf("member"),
                            metadata = mapOf("discovered" to "heartbeat")
                        )

                        membershipTable[newNodeId] = newNode
                        heartbeatTable[newNodeId] = now
                        logger.debug { "Discovered new node via heartbeat: $newNodeId" }

                        // 更新成员列表
                        updateMembers(membershipTable.values.toList())
                        scope.launch {
                            _events.emit(ClusterEvent.NodeJoined(newNode))
                        }
                    }

                } catch (e: Exception) {
                    logger.error(e) { "Error in heartbeat" }
                }

                delay(config.heartbeatInterval)
            }
        }
    }

    private fun startLeaderElection() {
        scope.launch {
            logger.info { "Starting leader election with strategy: ${config.electionStrategy}" }

            // 在实际实现中，这里会启动领导者选举
            // 这里简单模拟选举过程

            delay(1000) // 给一些时间让成员表填充

            // 执行选举
            val leader = leaderElection.electLeader(membershipTable.values.toList())

            if (leader != null) {
                logger.info { "Leader elected: ${leader.id}" }
                _leader = leader
                _isLeader = leader.id == nodeId
                scope.launch {
                    _events.emit(ClusterEvent.LeaderElected(leader.id))
                }
            } else {
                logger.warn { "No leader elected" }
            }

            // 定期重新选举
            while (running.get()) {
                try {
                    delay(10000) // 每10秒重新选举一次

                    // 只有在当前没有领导者或者领导者已经死亡的情况下才重新选举
                    val currentLeader = _leader
                    if (currentLeader == null ||
                        membershipTable[currentLeader.id]?.status != NodeStatus.ALIVE) {

                        val newLeader = leaderElection.electLeader(membershipTable.values.toList())

                        if (newLeader != null && (currentLeader == null || newLeader.id != currentLeader.id)) {
                            logger.info { "New leader elected: ${newLeader.id}" }
                            _leader = newLeader
                            _isLeader = newLeader.id == nodeId
                            scope.launch {
                                _events.emit(ClusterEvent.LeaderElected(newLeader.id))
                            }
                        }
                    }

                } catch (e: Exception) {
                    logger.error(e) { "Error in leader election" }
                }
            }
        }
    }
}

/**
 * 领导者选举策略
 */
class LeaderElectionStrategy(private val strategy: ElectionStrategy) {
    private val logger = KotlinLogging.logger {}

    fun electLeader(nodes: List<NodeInfo>): NodeInfo? {
        // 过滤出活着的非客户端节点
        val eligibleNodes = nodes.filter {
            it.status == NodeStatus.ALIVE && !it.roles.contains("client")
        }

        if (eligibleNodes.isEmpty()) {
            logger.warn { "No eligible nodes for leader election" }
            return null
        }

        return when (strategy) {
            ElectionStrategy.RAFT -> electLeaderRaft(eligibleNodes)
            ElectionStrategy.BULLY -> electLeaderBully(eligibleNodes)
            ElectionStrategy.STATIC -> electLeaderStatic(eligibleNodes)
        }
    }

    private fun electLeaderRaft(nodes: List<NodeInfo>): NodeInfo? {
        // 在实际实现中，这里会实现Raft选举算法
        // 这里简单模拟Raft选举
        logger.info { "Electing leader using Raft algorithm" }

        // 简单地选择第一个协调节点作为领导者
        val coordinators = nodes.filter { it.roles.contains("coordinator") }
        return coordinators.firstOrNull() ?: nodes.firstOrNull()
    }

    private fun electLeaderBully(nodes: List<NodeInfo>): NodeInfo? {
        // 在实际实现中，这里会实现Bully选举算法
        // 这里简单模拟Bully选举
        logger.info { "Electing leader using Bully algorithm" }

        // 简单地选择ID最大的节点作为领导者
        return nodes.maxByOrNull { it.id }
    }

    private fun electLeaderStatic(nodes: List<NodeInfo>): NodeInfo? {
        // 在实际实现中，这里会使用静态配置的领导者
        // 这里简单模拟静态选举
        logger.info { "Electing leader using Static configuration" }

        // 简单地选择第一个标记为leader的节点作为领导者
        val staticLeaders = nodes.filter { it.metadata["leader"] == "true" }
        return staticLeaders.firstOrNull() ?: nodes.firstOrNull()
    }
}
