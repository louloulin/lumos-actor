package com.dataflare

import com.dataflare.cluster.CentralizedClusterConfig
import com.dataflare.cluster.CentralizedClusterProvider
import com.dataflare.cluster.ClusterEvent
import com.dataflare.cluster.DiscoveryMethod
import com.dataflare.cluster.ElectionStrategy
import com.dataflare.cluster.HybridClusterConfig
import com.dataflare.cluster.HybridClusterProvider
import com.dataflare.cluster.P2PClusterConfig
import com.dataflare.cluster.P2PClusterProvider
import com.dataflare.core.DataProcessingSystem
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import actor.proto.ActorSystem

private val logger = KotlinLogging.logger {}

/**
 * 演示如何使用 DataFlare 系统
 */
fun main() = runBlocking {
    logger.info { "Starting DataFlare demo" }

    // 创建一个简单的 P2P 集群演示
    demoP2PCluster()

    // 创建一个简单的中心化集群演示
    demoCentralizedCluster()

    // 创建一个简单的混合集群演示
    demoHybridCluster()

    // 创建一个简单的数据处理系统演示
    demoDataProcessingSystem()

    logger.info { "DataFlare demo completed" }
}

/**
 * 演示 P2P 集群
 */
private suspend fun demoP2PCluster() {
    logger.info { "=== P2P Cluster Demo ===" }

    // 创建配置
    val config = P2PClusterConfig(
        clusterName = "demo-cluster",
        nodeName = "p2p-node-1",
        discoveryMethod = DiscoveryMethod.STATIC,
        seedNodes = listOf("localhost:5000")
    )

    // 创建集群提供者
    val provider = P2PClusterProvider(config)

    // 创建一个模拟的集群
    val cluster = ActorSystem("demo-system")

    // 监听集群事件
    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val job = scope.launch {
        provider.events().collect { event ->
            when (event) {
                is ClusterEvent.NodeJoined -> logger.info { "Node joined: ${event.node.id}" }
                is ClusterEvent.NodeLeft -> logger.info { "Node left: ${event.nodeId}" }
                is ClusterEvent.NodeDead -> logger.info { "Node dead: ${event.nodeId}" }
                is ClusterEvent.LeaderElected -> logger.info { "Leader elected: ${event.nodeId}" }
                is ClusterEvent.MembershipChanged -> logger.info { "Membership changed: ${event.nodes.size} nodes" }
            }
        }
    }

    // 启动集群成员
    logger.info { "Starting P2P cluster member" }
    provider.startMember(cluster)

    // 等待一段时间让集群稳定
    delay(2000)

    // 显示集群成员
    logger.info { "Cluster members: ${provider.members().size}" }
    provider.members().forEach { node ->
        logger.info { "  - ${node.id} (${node.address}): ${node.roles}" }
    }

    // 显示集群领导者
    val leader = provider.leader()
    if (leader != null) {
        logger.info { "Cluster leader: ${leader.id}" }
    } else {
        logger.info { "No cluster leader" }
    }

    // 关闭集群
    logger.info { "Shutting down P2P cluster" }
    provider.shutdown(true)
    job.cancel()

    logger.info { "P2P cluster demo completed" }
}

/**
 * 演示中心化集群
 */
private suspend fun demoCentralizedCluster() {
    logger.info { "=== Centralized Cluster Demo ===" }

    // 创建配置
    val config = CentralizedClusterConfig(
        clusterName = "demo-cluster",
        nodeName = "centralized-node-1",
        coordinatorNodes = listOf("localhost:5000"),
        electionStrategy = ElectionStrategy.STATIC
    )

    // 创建集群提供者
    val provider = CentralizedClusterProvider(config)

    // 创建一个模拟的集群
    val cluster = ActorSystem("demo-system")

    // 监听集群事件
    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val job = scope.launch {
        provider.events().collect { event ->
            when (event) {
                is ClusterEvent.NodeJoined -> logger.info { "Node joined: ${event.node.id}" }
                is ClusterEvent.NodeLeft -> logger.info { "Node left: ${event.nodeId}" }
                is ClusterEvent.NodeDead -> logger.info { "Node dead: ${event.nodeId}" }
                is ClusterEvent.LeaderElected -> logger.info { "Leader elected: ${event.nodeId}" }
                is ClusterEvent.MembershipChanged -> logger.info { "Membership changed: ${event.nodes.size} nodes" }
            }
        }
    }

    // 启动集群成员
    logger.info { "Starting centralized cluster member" }
    provider.startMember(cluster)

    // 等待一段时间让集群稳定
    delay(2000)

    // 显示集群成员
    logger.info { "Cluster members: ${provider.members().size}" }
    provider.members().forEach { node ->
        logger.info { "  - ${node.id} (${node.address}): ${node.roles}" }
    }

    // 显示集群领导者
    val leader = provider.leader()
    if (leader != null) {
        logger.info { "Cluster leader: ${leader.id}" }
    } else {
        logger.info { "No cluster leader" }
    }

    // 关闭集群
    logger.info { "Shutting down centralized cluster" }
    provider.shutdown(true)
    job.cancel()

    logger.info { "Centralized cluster demo completed" }
}

/**
 * 演示混合集群
 */
private suspend fun demoHybridCluster() {
    logger.info { "=== Hybrid Cluster Demo ===" }

    // 创建P2P配置
    val p2pConfig = P2PClusterConfig(
        clusterName = "demo-cluster",
        nodeName = "hybrid-node-1",
        discoveryMethod = DiscoveryMethod.STATIC,
        seedNodes = listOf("localhost:5000")
    )

    // 创建中心化配置
    val centralizedConfig = CentralizedClusterConfig(
        clusterName = "demo-cluster",
        nodeName = "hybrid-node-1",
        coordinatorNodes = listOf("localhost:5000"),
        electionStrategy = ElectionStrategy.STATIC
    )

    // 创建混合配置
    val hybridConfig = HybridClusterConfig(
        clusterName = "demo-cluster",
        nodeName = "hybrid-node-1",
        regionName = "region-1",
        p2pConfig = p2pConfig,
        centralizedConfig = centralizedConfig,
        interRegionCommunication = true
    )

    // 创建集群提供者
    val p2pProvider = P2PClusterProvider(p2pConfig)
    val centralizedProvider = CentralizedClusterProvider(centralizedConfig)
    val hybridProvider = HybridClusterProvider(hybridConfig, p2pProvider, centralizedProvider)

    // 创建一个模拟的集群
    val cluster = ActorSystem("demo-system")

    // 监听集群事件
    val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val job = scope.launch {
        hybridProvider.events().collect { event ->
            when (event) {
                is ClusterEvent.NodeJoined -> logger.info { "Node joined: ${event.node.id}" }
                is ClusterEvent.NodeLeft -> logger.info { "Node left: ${event.nodeId}" }
                is ClusterEvent.NodeDead -> logger.info { "Node dead: ${event.nodeId}" }
                is ClusterEvent.LeaderElected -> logger.info { "Leader elected: ${event.nodeId}" }
                is ClusterEvent.MembershipChanged -> logger.info { "Membership changed: ${event.nodes.size} nodes" }
            }
        }
    }

    // 启动集群成员
    logger.info { "Starting hybrid cluster member" }
    hybridProvider.startMember(cluster)

    // 等待一段时间让集群稳定
    delay(2000)

    // 显示集群成员
    logger.info { "Cluster members: ${hybridProvider.members().size}" }
    hybridProvider.members().forEach { node ->
        logger.info { "  - ${node.id} (${node.address}): ${node.roles}" }
    }

    // 显示集群领导者
    val leader = hybridProvider.leader()
    if (leader != null) {
        logger.info { "Cluster leader: ${leader.id}" }
    } else {
        logger.info { "No cluster leader" }
    }

    // 关闭集群
    logger.info { "Shutting down hybrid cluster" }
    hybridProvider.shutdown(true)
    job.cancel()

    logger.info { "Hybrid cluster demo completed" }
}

/**
 * 演示数据处理系统
 */
private suspend fun demoDataProcessingSystem() {
    logger.info { "=== Data Processing System Demo ===" }

    // 创建数据处理系统
    val system = DataProcessingSystem("demo-system")

    // 启动系统
    logger.info { "Starting data processing system" }
    system.start()

    // 创建一个简单的工作流
    logger.info { "Creating workflow" }
    val workflowConfig = WorkflowConfig(
        name = "demo-workflow",
        inputs = mapOf(
            "input1" to com.dataflare.workflow.InputConfig("file", mapOf("path" to "/tmp/input.txt"))
        ),
        processors = mapOf(
            "processor1" to com.dataflare.workflow.ProcessorConfig("mapping", listOf("input1"), mapOf("mapping" to ".processed = true"))
        ),
        outputs = mapOf(
            "output1" to com.dataflare.workflow.OutputConfig("file", listOf("processor1"), mapOf("path" to "/tmp/output.txt"))
        ),
        connections = listOf(
            com.dataflare.workflow.Connection("input1", "processor1"),
            com.dataflare.workflow.Connection("processor1", "output1")
        )
    )

    val workflowHandle = system.createWorkflow(workflowConfig)

    // 启动工作流
    logger.info { "Starting workflow: ${workflowHandle.name}" }
    system.startWorkflow(workflowHandle)

    // 等待一段时间让工作流运行
    delay(1000)

    // 暂停工作流
    logger.info { "Pausing workflow: ${workflowHandle.name}" }
    system.pauseWorkflow(workflowHandle)

    // 等待一段时间
    delay(1000)

    // 恢复工作流
    logger.info { "Resuming workflow: ${workflowHandle.name}" }
    system.resumeWorkflow(workflowHandle)

    // 等待一段时间
    delay(1000)

    // 停止工作流
    logger.info { "Stopping workflow: ${workflowHandle.name}" }
    system.stopWorkflow(workflowHandle)

    // 测试DSL
    logger.info { "Testing DSL" }
    val dslScript = """
        workflow("test-workflow") {
            val input = input("file") {
                path = "/tmp/input.txt"
            }

            val processor = processor("mapping") {
                mapping = ".processed = true"
            }

            val output = output("file") {
                path = "/tmp/output.txt"
            }

            connect(input to processor)
            connect(processor to output)
        }
    """.trimIndent()

    val validationResult = system.validateDsl(dslScript)
    logger.info { "DSL validation result: ${validationResult.isValid}" }

    if (validationResult.isValid) {
        val compiledWorkflow = system.compileDsl(dslScript)
        logger.info { "Compiled workflow: ${compiledWorkflow.config.name}" }
    }

    // 停止系统
    logger.info { "Stopping data processing system" }
    system.stop()

    logger.info { "Data processing system demo completed" }
}
