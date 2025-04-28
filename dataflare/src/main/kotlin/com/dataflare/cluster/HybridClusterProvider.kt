package com.dataflare.cluster

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.launch
import mu.KotlinLogging
import actor.proto.ActorSystem
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * 混合集群提供者实现
 */
class HybridClusterProvider(
    val config: HybridClusterConfig,
    val p2pProvider: P2PClusterProvider,
    val centralizedProvider: CentralizedClusterProvider
) : AbstractClusterProvider() {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val running = AtomicBoolean(false)
    private val nodeId = UUID.randomUUID().toString()
    private val regionMembers = ConcurrentHashMap<String, MutableList<NodeInfo>>()
    private val regionLeaders = ConcurrentHashMap<String, NodeInfo>()

    override suspend fun startMember(cluster: ActorSystem): Boolean {
        if (running.getAndSet(true)) {
            logger.warn { "Hybrid cluster provider already running" }
            return false
        }

        logger.info { "Starting hybrid cluster member: ${config.nodeName} in region: ${config.regionName}" }

        // 启动区域内的P2P集群
        val p2pStarted = p2pProvider.startMember(cluster)
        if (!p2pStarted) {
            logger.error { "Failed to start P2P cluster provider" }
            running.set(false)
            return false
        }

        // 如果启用了区域间通信，启动中心化集群
        if (config.interRegionCommunication) {
            val centralizedStarted = centralizedProvider.startClient(cluster)
            if (!centralizedStarted) {
                logger.error { "Failed to start centralized cluster provider" }
                p2pProvider.shutdown(false)
                running.set(false)
                return false
            }
        }

        // 初始化区域成员
        regionMembers[config.regionName] = mutableListOf()

        // 监听P2P集群事件
        listenToP2PEvents()

        // 如果启用了区域间通信，监听中心化集群事件
        if (config.interRegionCommunication) {
            listenToCentralizedEvents()
        }

        // 启动区域间路由
        if (config.interRegionCommunication) {
            startInterRegionRouting()
        }

        return true
    }

    override suspend fun startClient(cluster: ActorSystem): Boolean {
        if (running.getAndSet(true)) {
            logger.warn { "Hybrid cluster provider already running" }
            return false
        }

        logger.info { "Starting hybrid cluster client: ${config.nodeName} in region: ${config.regionName}" }

        // 启动区域内的P2P集群客户端
        val p2pStarted = p2pProvider.startClient(cluster)
        if (!p2pStarted) {
            logger.error { "Failed to start P2P cluster client" }
            running.set(false)
            return false
        }

        // 如果启用了区域间通信，启动中心化集群客户端
        if (config.interRegionCommunication) {
            val centralizedStarted = centralizedProvider.startClient(cluster)
            if (!centralizedStarted) {
                logger.error { "Failed to start centralized cluster client" }
                p2pProvider.shutdown(false)
                running.set(false)
                return false
            }
        }

        // 初始化区域成员
        regionMembers[config.regionName] = mutableListOf()

        // 监听P2P集群事件
        listenToP2PEvents()

        // 如果启用了区域间通信，监听中心化集群事件
        if (config.interRegionCommunication) {
            listenToCentralizedEvents()
        }

        return true
    }

    override suspend fun shutdown(graceful: Boolean): Boolean {
        if (!running.getAndSet(false)) {
            logger.warn { "Hybrid cluster provider not running" }
            return false
        }

        logger.info { "Shutting down hybrid cluster provider: ${config.nodeName}" }

        // 关闭P2P集群
        val p2pShutdown = p2pProvider.shutdown(graceful)

        // 如果启用了区域间通信，关闭中心化集群
        var centralizedShutdown = true
        if (config.interRegionCommunication) {
            centralizedShutdown = centralizedProvider.shutdown(graceful)
        }

        // 取消所有协程
        scope.cancel()

        // 清理资源
        regionMembers.clear()
        regionLeaders.clear()

        return p2pShutdown && centralizedShutdown
    }

    override fun events(): Flow<ClusterEvent> = _events

    override fun members(): List<NodeInfo> {
        // 合并P2P和中心化集群的成员
        val allMembers = mutableListOf<NodeInfo>()

        // 添加P2P集群成员
        allMembers.addAll(p2pProvider.members())

        // 如果启用了区域间通信，添加中心化集群成员
        if (config.interRegionCommunication) {
            // 过滤掉已经在P2P集群中的成员
            val p2pMemberIds = p2pProvider.members().map { it.id }.toSet()
            val centralizedMembers = centralizedProvider.members().filter { !p2pMemberIds.contains(it.id) }
            allMembers.addAll(centralizedMembers)
        }

        return allMembers
    }

    override fun leader(): NodeInfo? {
        // 返回当前区域的领导者
        return regionLeaders[config.regionName] ?: p2pProvider.leader()
    }

    override fun isLeader(): Boolean {
        // 检查当前节点是否是区域领导者
        val regionLeader = regionLeaders[config.regionName]
        return regionLeader?.id == nodeId || p2pProvider.isLeader()
    }

    private fun listenToP2PEvents() {
        scope.launch {
            p2pProvider.events().collect { event ->
                when (event) {
                    is ClusterEvent.NodeJoined -> {
                        // 添加到区域成员
                        val region = event.node.metadata["region"] ?: config.regionName
                        regionMembers.getOrPut(region) { mutableListOf() }.add(event.node)

                        // 转发事件
                        scope.launch {
                            _events.emit(event)
                        }
                    }

                    is ClusterEvent.NodeLeft, is ClusterEvent.NodeDead -> {
                        val nodeId = when (event) {
                            is ClusterEvent.NodeLeft -> event.nodeId
                            is ClusterEvent.NodeDead -> event.nodeId
                            else -> null
                        }

                        if (nodeId != null) {
                            // 从区域成员中移除
                            regionMembers.forEach { (_, members) ->
                                members.removeIf { it.id == nodeId }
                            }

                            // 如果是区域领导者，需要重新选举
                            regionLeaders.forEach { (region, leader) ->
                                if (leader.id == nodeId) {
                                    regionLeaders.remove(region)

                                    // 在实际实现中，这里会触发区域领导者重新选举
                                    logger.info { "Region leader left or died, need to re-elect: $region" }
                                }
                            }
                        }

                        // 转发事件
                        scope.launch {
                            _events.emit(event)
                        }
                    }

                    is ClusterEvent.LeaderElected -> {
                        // 更新区域领导者
                        regionLeaders[config.regionName] = p2pProvider.leader() ?: return@collect

                        // 转发事件
                        scope.launch {
                            _events.emit(event)
                        }
                    }

                    is ClusterEvent.MembershipChanged -> {
                        // 更新区域成员
                        val regionMembersMap = event.nodes.groupBy { it.metadata["region"] ?: config.regionName }
                        regionMembersMap.forEach { (region, members) ->
                            regionMembers[region] = members.toMutableList()
                        }

                        // 更新成员列表
                        updateMembers(members())

                        // 转发事件
                        scope.launch {
                            _events.emit(event)
                        }
                    }
                }
            }
        }
    }

    private fun listenToCentralizedEvents() {
        scope.launch {
            centralizedProvider.events().collect { event ->
                when (event) {
                    is ClusterEvent.NodeJoined -> {
                        // 检查节点是否已经在P2P集群中
                        val p2pMemberIds = p2pProvider.members().map { it.id }.toSet()
                        if (!p2pMemberIds.contains(event.node.id)) {
                            // 添加到区域成员
                            val region = event.node.metadata["region"] ?: "unknown"
                            if (region != config.regionName) {
                                regionMembers.getOrPut(region) { mutableListOf() }.add(event.node)

                                // 转发事件
                                scope.launch {
                                    _events.emit(event)
                                }
                            }
                        }
                    }

                    is ClusterEvent.NodeLeft, is ClusterEvent.NodeDead -> {
                        val nodeId = when (event) {
                            is ClusterEvent.NodeLeft -> event.nodeId
                            is ClusterEvent.NodeDead -> event.nodeId
                            else -> null
                        }

                        if (nodeId != null) {
                            // 检查节点是否在P2P集群中
                            val p2pMemberIds = p2pProvider.members().map { it.id }.toSet()
                            if (!p2pMemberIds.contains(nodeId)) {
                                // 从区域成员中移除
                                regionMembers.forEach { (_, members) ->
                                    members.removeIf { it.id == nodeId }
                                }

                                // 如果是区域领导者，需要重新选举
                                regionLeaders.forEach { (region, leader) ->
                                    if (leader.id == nodeId) {
                                        regionLeaders.remove(region)

                                        // 在实际实现中，这里会触发区域领导者重新选举
                                        logger.info { "Region leader left or died, need to re-elect: $region" }
                                    }
                                }

                                // 转发事件
                                scope.launch {
                                    _events.emit(event)
                                }
                            }
                        }
                    }

                    is ClusterEvent.LeaderElected -> {
                        // 更新全局领导者
                        _leader = centralizedProvider.leader()
                        _isLeader = centralizedProvider.isLeader()

                        // 转发事件
                        scope.launch {
                            _events.emit(event)
                        }
                    }

                    is ClusterEvent.MembershipChanged -> {
                        // 更新成员列表
                        updateMembers(members())

                        // 转发事件
                        scope.launch {
                            _events.emit(event)
                        }
                    }
                }
            }
        }
    }

    private fun startInterRegionRouting() {
        scope.launch {
            logger.info { "Starting inter-region routing" }

            // 在实际实现中，这里会实现区域间的消息路由
            // 这里简单模拟区域间路由

            // 定期更新区域领导者
            while (running.get()) {
                try {
                    // 获取所有区域的领导者
                    val allRegions = regionMembers.keys.toSet()

                    // 对于每个区域，如果没有领导者，选择一个
                    allRegions.forEach { region ->
                        if (!regionLeaders.containsKey(region)) {
                            val regionNodes = regionMembers[region] ?: return@forEach
                            if (regionNodes.isNotEmpty()) {
                                // 简单地选择第一个节点作为区域领导者
                                val leader = regionNodes.first()
                                regionLeaders[region] = leader
                                logger.info { "Selected region leader for $region: ${leader.id}" }

                                // 发送区域领导者选举事件
                                scope.launch {
                                    _events.emit(ClusterEvent.LeaderElected(leader.id))
                                }
                            }
                        }
                    }

                    // 如果当前节点是区域领导者，负责与其他区域通信
                    if (isLeader()) {
                        // 在实际实现中，这里会与其他区域的领导者通信
                        logger.debug { "Current node is region leader, communicating with other regions" }
                    }

                    kotlinx.coroutines.delay(5000) // 每5秒检查一次

                } catch (e: Exception) {
                    logger.error(e) { "Error in inter-region routing" }
                    kotlinx.coroutines.delay(1000) // 出错后等待1秒再重试
                }
            }
        }
    }
}
