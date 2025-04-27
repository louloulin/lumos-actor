package actor.proto.cluster.libp2p.loadbalancing

import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterIdentity
import actor.proto.cluster.Member
import actor.proto.cluster.MemberStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import kotlin.math.min

private val logger = KotlinLogging.logger {}

/**
 * LoadBalancer 负责优化集群中的工作分配
 */
class LoadBalancer(
    private val cluster: Cluster,
    private val strategy: LoadBalancingStrategy = LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN,
    private val updateInterval: Long = 10000 // 10秒
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var isRunning = false
    
    // 节点负载信息
    private val nodeLoads = ConcurrentHashMap<String, NodeLoad>()
    
    // 节点权重
    private val nodeWeights = ConcurrentHashMap<String, Double>()
    
    // 轮询计数器
    private val roundRobinCounter = ConcurrentHashMap<String, AtomicInteger>()
    
    // 统计信息
    private val placementDecisions = AtomicInteger(0)
    private val rebalanceOperations = AtomicInteger(0)
    private val totalPlacementTime = AtomicLong(0)
    
    /**
     * 启动负载均衡器
     */
    fun start() {
        if (isRunning) return
        
        logger.info { "Starting load balancer with strategy: $strategy" }
        
        isRunning = true
        
        // 初始化节点负载
        initializeNodeLoads()
        
        // 启动负载更新任务
        startLoadUpdateTask()
    }
    
    /**
     * 停止负载均衡器
     */
    fun stop() {
        if (!isRunning) return
        
        logger.info { "Stopping load balancer" }
        
        isRunning = false
        
        // 清理状态
        nodeLoads.clear()
        nodeWeights.clear()
        roundRobinCounter.clear()
    }
    
    /**
     * 初始化节点负载
     */
    private fun initializeNodeLoads() {
        // 获取集群成员
        val members = cluster.memberList.getMembers()
        
        // 初始化每个成员的负载信息
        members.forEach { member ->
            if (member.status == MemberStatus.ALIVE) {
                nodeLoads[member.id] = NodeLoad(
                    nodeId = member.id,
                    cpuLoad = 0.0,
                    memoryLoad = 0.0,
                    actorCount = 0,
                    messageRate = 0.0
                )
                
                nodeWeights[member.id] = 1.0
                roundRobinCounter[member.id] = AtomicInteger(0)
            }
        }
    }
    
    /**
     * 启动负载更新任务
     */
    private fun startLoadUpdateTask() {
        scope.launch {
            while (isRunning) {
                try {
                    // 更新节点负载
                    updateNodeLoads()
                    
                    // 更新节点权重
                    updateNodeWeights()
                    
                    // 等待下一个更新间隔
                    delay(updateInterval)
                } catch (e: Exception) {
                    logger.error(e) { "Error updating node loads" }
                    delay(1000) // 出错后等待一段时间再重试
                }
            }
        }
    }
    
    /**
     * 更新节点负载
     */
    private fun updateNodeLoads() {
        // 获取集群成员
        val members = cluster.memberList.getMembers()
        
        // 更新成员列表
        val currentMembers = members
            .filter { it.status == MemberStatus.ALIVE }
            .map { it.id }
            .toSet()
        
        // 移除不再活跃的成员
        val removedMembers = nodeLoads.keys.filter { it !in currentMembers }
        removedMembers.forEach { nodeId ->
            nodeLoads.remove(nodeId)
            nodeWeights.remove(nodeId)
            roundRobinCounter.remove(nodeId)
        }
        
        // 添加新成员
        val newMembers = currentMembers.filter { it !in nodeLoads.keys }
        newMembers.forEach { nodeId ->
            nodeLoads[nodeId] = NodeLoad(
                nodeId = nodeId,
                cpuLoad = 0.0,
                memoryLoad = 0.0,
                actorCount = 0,
                messageRate = 0.0
            )
            
            nodeWeights[nodeId] = 1.0
            roundRobinCounter[nodeId] = AtomicInteger(0)
        }
        
        // 在实际实现中，这里会从各个节点收集负载信息
        // 这里使用模拟实现
        nodeLoads.forEach { (nodeId, load) ->
            // 模拟负载变化
            val cpuLoad = min(1.0, max(0.1, load.cpuLoad + (Math.random() - 0.5) * 0.1))
            val memoryLoad = min(1.0, max(0.1, load.memoryLoad + (Math.random() - 0.5) * 0.1))
            val actorCount = max(0, load.actorCount + (Math.random() * 10 - 5).toInt())
            val messageRate = max(0.0, load.messageRate + (Math.random() * 100 - 50))
            
            nodeLoads[nodeId] = NodeLoad(
                nodeId = nodeId,
                cpuLoad = cpuLoad,
                memoryLoad = memoryLoad,
                actorCount = actorCount,
                messageRate = messageRate
            )
        }
    }
    
    /**
     * 更新节点权重
     */
    private fun updateNodeWeights() {
        when (strategy) {
            LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN -> {
                // 基于负载计算权重
                nodeLoads.forEach { (nodeId, load) ->
                    // 计算综合负载
                    val compositeLoad = 0.4 * load.cpuLoad + 0.3 * load.memoryLoad +
                            0.2 * (load.actorCount / 100.0) + 0.1 * (load.messageRate / 1000.0)
                    
                    // 计算权重 (负载越低，权重越高)
                    val weight = 1.0 - min(0.9, compositeLoad)
                    
                    nodeWeights[nodeId] = weight
                }
            }
            LoadBalancingStrategy.LEAST_CONNECTIONS -> {
                // 基于 Actor 数量计算权重
                nodeLoads.forEach { (nodeId, load) ->
                    // 计算权重 (Actor 数量越少，权重越高)
                    val weight = 1.0 / (1.0 + load.actorCount)
                    
                    nodeWeights[nodeId] = weight
                }
            }
            LoadBalancingStrategy.CONSISTENT_HASHING -> {
                // 一致性哈希不需要权重
                nodeWeights.keys.forEach { nodeId ->
                    nodeWeights[nodeId] = 1.0
                }
            }
            LoadBalancingStrategy.ROUND_ROBIN -> {
                // 轮询不需要权重
                nodeWeights.keys.forEach { nodeId ->
                    nodeWeights[nodeId] = 1.0
                }
            }
        }
    }
    
    /**
     * 选择节点
     */
    fun selectNode(identity: ClusterIdentity): String? {
        val startTime = System.currentTimeMillis()
        
        try {
            // 获取可用节点
            val availableNodes = nodeLoads.keys.toList()
            if (availableNodes.isEmpty()) {
                return null
            }
            
            // 根据策略选择节点
            val selectedNode = when (strategy) {
                LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN -> selectNodeWeightedRoundRobin(identity)
                LoadBalancingStrategy.LEAST_CONNECTIONS -> selectNodeLeastConnections(identity)
                LoadBalancingStrategy.CONSISTENT_HASHING -> selectNodeConsistentHashing(identity)
                LoadBalancingStrategy.ROUND_ROBIN -> selectNodeRoundRobin(identity)
            }
            
            // 更新统计信息
            placementDecisions.incrementAndGet()
            
            return selectedNode
        } finally {
            // 更新统计信息
            val placementTime = System.currentTimeMillis() - startTime
            totalPlacementTime.addAndGet(placementTime)
        }
    }
    
    /**
     * 使用加权轮询选择节点
     */
    private fun selectNodeWeightedRoundRobin(identity: ClusterIdentity): String? {
        // 获取可用节点和权重
        val nodes = nodeWeights.entries.toList()
        if (nodes.isEmpty()) {
            return null
        }
        
        // 计算总权重
        val totalWeight = nodes.sumOf { it.value }
        
        // 选择节点
        var remainingWeight = Math.random() * totalWeight
        
        for ((nodeId, weight) in nodes) {
            remainingWeight -= weight
            if (remainingWeight <= 0) {
                return nodeId
            }
        }
        
        // 如果没有选中节点，返回第一个
        return nodes.first().key
    }
    
    /**
     * 使用最少连接选择节点
     */
    private fun selectNodeLeastConnections(identity: ClusterIdentity): String? {
        // 获取可用节点
        val nodes = nodeLoads.entries.toList()
        if (nodes.isEmpty()) {
            return null
        }
        
        // 选择 Actor 数量最少的节点
        return nodes.minByOrNull { it.value.actorCount }?.key
    }
    
    /**
     * 使用一致性哈希选择节点
     */
    private fun selectNodeConsistentHashing(identity: ClusterIdentity): String? {
        // 获取可用节点
        val nodes = nodeLoads.keys.toList()
        if (nodes.isEmpty()) {
            return null
        }
        
        // 计算哈希
        val hash = identity.identity.hashCode()
        
        // 选择节点
        return nodes[Math.abs(hash) % nodes.size]
    }
    
    /**
     * 使用轮询选择节点
     */
    private fun selectNodeRoundRobin(identity: ClusterIdentity): String? {
        // 获取可用节点
        val nodes = roundRobinCounter.keys.toList()
        if (nodes.isEmpty()) {
            return null
        }
        
        // 获取 kind 的计数器
        val kind = identity.kind
        val counter = roundRobinCounter.computeIfAbsent(kind) { AtomicInteger(0) }
        
        // 增加计数器
        val index = counter.getAndIncrement() % nodes.size
        
        // 选择节点
        return nodes[index]
    }
    
    /**
     * 更新节点负载
     */
    fun updateNodeLoad(nodeId: String, load: NodeLoad) {
        nodeLoads[nodeId] = load
    }
    
    /**
     * 获取节点负载
     */
    fun getNodeLoad(nodeId: String): NodeLoad? {
        return nodeLoads[nodeId]
    }
    
    /**
     * 获取所有节点负载
     */
    fun getAllNodeLoads(): Map<String, NodeLoad> {
        return nodeLoads.toMap()
    }
    
    /**
     * 获取统计信息
     */
    fun getStats(): LoadBalancerStats {
        val avgPlacementTime = if (placementDecisions.get() > 0) {
            totalPlacementTime.get() / placementDecisions.get()
        } else {
            0
        }
        
        return LoadBalancerStats(
            strategy = strategy,
            nodeCount = nodeLoads.size,
            placementDecisions = placementDecisions.get(),
            rebalanceOperations = rebalanceOperations.get(),
            avgPlacementTimeMs = avgPlacementTime
        )
    }
    
    /**
     * 重新平衡集群
     */
    fun rebalance(): Int {
        // 在实际实现中，这里会重新分配 Actor
        // 这里使用模拟实现
        val rebalanceCount = (Math.random() * 10).toInt()
        
        // 更新统计信息
        rebalanceOperations.incrementAndGet()
        
        return rebalanceCount
    }
}

/**
 * 节点负载
 */
data class NodeLoad(
    val nodeId: String,
    val cpuLoad: Double, // 0.0 - 1.0
    val memoryLoad: Double, // 0.0 - 1.0
    val actorCount: Int,
    val messageRate: Double // 消息/秒
)

/**
 * 负载均衡策略
 */
enum class LoadBalancingStrategy {
    ROUND_ROBIN,
    WEIGHTED_ROUND_ROBIN,
    LEAST_CONNECTIONS,
    CONSISTENT_HASHING
}

/**
 * 负载均衡器统计信息
 */
data class LoadBalancerStats(
    val strategy: LoadBalancingStrategy,
    val nodeCount: Int,
    val placementDecisions: Int,
    val rebalanceOperations: Int,
    val avgPlacementTimeMs: Long
)
