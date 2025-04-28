package com.dataflare

import com.dataflare.cluster.ClusterEvent
import com.dataflare.cluster.SingleNodeCluster
import com.dataflare.cluster.SingleNodeConfig
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
 * 演示单机模式的轻量化实现
 */
fun main() = runBlocking {
    logger.info { "Starting DataFlare Single Node Demo" }
    
    // 演示单机模式集群
    demoSingleNodeCluster()
    
    // 演示单机模式数据处理系统
    demoSingleNodeDataProcessingSystem()
    
    logger.info { "DataFlare Single Node Demo completed" }
}

/**
 * 演示单机模式集群
 */
private suspend fun demoSingleNodeCluster() {
    logger.info { "=== Single Node Cluster Demo ===" }
    
    // 创建配置
    val config = SingleNodeConfig(
        clusterName = "single-node-cluster",
        nodeName = "local-node"
    )
    
    // 创建集群提供者
    val provider = SingleNodeCluster(config)
    
    // 创建一个模拟的集群
    val system = ActorSystem("single-node-system")
    
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
    logger.info { "Starting single node cluster" }
    provider.startMember(system)
    
    // 等待一段时间让集群稳定
    delay(1000)
    
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
    logger.info { "Shutting down single node cluster" }
    provider.shutdown(true)
    job.cancel()
    
    logger.info { "Single node cluster demo completed" }
}

/**
 * 演示单机模式数据处理系统
 */
private suspend fun demoSingleNodeDataProcessingSystem() {
    logger.info { "=== Single Node Data Processing System Demo ===" }
    
    // 创建数据处理系统
    val system = DataProcessingSystem("single-node-system")
    
    // 启动系统
    logger.info { "Starting data processing system" }
    system.start()
    
    // 创建一个简单的工作流
    logger.info { "Creating workflow" }
    val workflowConfig = WorkflowConfig(
        name = "simple-workflow",
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
    
    // 停止工作流
    logger.info { "Stopping workflow: ${workflowHandle.name}" }
    system.stopWorkflow(workflowHandle)
    
    // 停止系统
    logger.info { "Stopping data processing system" }
    system.stop()
    
    logger.info { "Single node data processing system demo completed" }
}
