package actor.proto.cluster.libp2p.loadbalancing

import actor.proto.ActorSystem
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.ClusterIdentity
import actor.proto.cluster.Member
import actor.proto.cluster.MemberList
import actor.proto.cluster.MemberStatus
import actor.proto.cluster.libp2p.P2PClusterConfig
import actor.proto.cluster.libp2p.P2PClusterProvider
import actor.proto.cluster.libp2p.P2PIdentityLookup
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.mockito.Mockito
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class LoadBalancerTest {
    
    private lateinit var system: ActorSystem
    private lateinit var cluster: Cluster
    private lateinit var loadBalancer: LoadBalancer
    private lateinit var memberList: MemberList
    
    @BeforeEach
    fun setup() {
        // 创建 Actor 系统
        system = ActorSystem("test-system")
        
        // 创建 P2P 集群配置
        val p2pConfig = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4001,
            heartbeatInterval = Duration.ofSeconds(1),
            monitorInterval = Duration.ofSeconds(3)
        )
        
        // 创建集群提供者
        val clusterProvider = P2PClusterProvider(p2pConfig)
        
        // 创建身份查找服务
        val identityLookup = P2PIdentityLookup()
        
        // 创建集群配置
        val clusterConfig = ClusterConfig(
            name = "test-cluster",
            clusterProvider = clusterProvider,
            identityLookup = identityLookup
        )
        
        // 创建集群
        cluster = Cluster(system, clusterConfig)
        
        // 模拟成员列表
        memberList = Mockito.mock(MemberList::class.java)
        
        // 设置模拟成员
        val members = listOf(
            Member("node1", "localhost", 4001, mapOf(), MemberStatus.ALIVE),
            Member("node2", "localhost", 4002, mapOf(), MemberStatus.ALIVE),
            Member("node3", "localhost", 4003, mapOf(), MemberStatus.ALIVE)
        )
        
        Mockito.`when`(memberList.getMembers()).thenReturn(members)
        
        // 替换集群的成员列表
        val memberListField = Cluster::class.java.getDeclaredField("memberList")
        memberListField.isAccessible = true
        memberListField.set(cluster, memberList)
        
        // 创建负载均衡器
        loadBalancer = LoadBalancer(
            cluster = cluster,
            strategy = LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN,
            updateInterval = 1000
        )
        
        // 启动负载均衡器
        loadBalancer.start()
    }
    
    @AfterEach
    fun tearDown() = runBlocking {
        // 停止负载均衡器
        loadBalancer.stop()
        
        // 关闭集群和 Actor 系统
        if (::cluster.isInitialized) {
            cluster.shutdown(true)
        }
        
        system.shutdown()
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should select node using weighted round robin`() = runBlocking {
        // 创建测试 Actor 标识
        val identity = ClusterIdentity("test", "test-actor")
        
        // 设置节点负载
        loadBalancer.updateNodeLoad("node1", NodeLoad("node1", 0.2, 0.3, 10, 100.0))
        loadBalancer.updateNodeLoad("node2", NodeLoad("node2", 0.5, 0.6, 20, 200.0))
        loadBalancer.updateNodeLoad("node3", NodeLoad("node3", 0.8, 0.9, 30, 300.0))
        
        // 等待负载更新
        delay(1500)
        
        // 选择节点多次
        val selections = (1..100).map { loadBalancer.selectNode(identity) }
        
        // 验证结果
        assertNotNull(selections.first(), "Selected node should not be null")
        
        // 计算每个节点的选择次数
        val counts = selections.groupingBy { it }.eachCount()
        
        // 验证节点 1 (低负载) 被选择的次数多于节点 3 (高负载)
        assertTrue(counts["node1"]!! > counts["node3"]!!, "Node1 should be selected more often than Node3")
        
        // 验证统计信息
        val stats = loadBalancer.getStats()
        assertEquals(LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN, stats.strategy, "Strategy should be WEIGHTED_ROUND_ROBIN")
        assertEquals(3, stats.nodeCount, "Node count should be 3")
        assertEquals(100, stats.placementDecisions, "Placement decisions should be 100")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should select node using least connections`() = runBlocking {
        // 创建负载均衡器
        val leastConnectionsBalancer = LoadBalancer(
            cluster = cluster,
            strategy = LoadBalancingStrategy.LEAST_CONNECTIONS,
            updateInterval = 1000
        )
        
        // 启动负载均衡器
        leastConnectionsBalancer.start()
        
        try {
            // 创建测试 Actor 标识
            val identity = ClusterIdentity("test", "test-actor")
            
            // 设置节点负载
            leastConnectionsBalancer.updateNodeLoad("node1", NodeLoad("node1", 0.2, 0.3, 10, 100.0))
            leastConnectionsBalancer.updateNodeLoad("node2", NodeLoad("node2", 0.5, 0.6, 5, 200.0))
            leastConnectionsBalancer.updateNodeLoad("node3", NodeLoad("node3", 0.8, 0.9, 20, 300.0))
            
            // 等待负载更新
            delay(1500)
            
            // 选择节点多次
            val selections = (1..100).map { leastConnectionsBalancer.selectNode(identity) }
            
            // 验证结果
            assertNotNull(selections.first(), "Selected node should not be null")
            
            // 计算每个节点的选择次数
            val counts = selections.groupingBy { it }.eachCount()
            
            // 验证节点 2 (最少连接) 被选择的次数最多
            assertTrue(counts["node2"]!! > counts["node1"]!!, "Node2 should be selected more often than Node1")
            assertTrue(counts["node2"]!! > counts["node3"]!!, "Node2 should be selected more often than Node3")
            
            // 验证统计信息
            val stats = leastConnectionsBalancer.getStats()
            assertEquals(LoadBalancingStrategy.LEAST_CONNECTIONS, stats.strategy, "Strategy should be LEAST_CONNECTIONS")
        } finally {
            // 停止负载均衡器
            leastConnectionsBalancer.stop()
        }
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should select node using consistent hashing`() = runBlocking {
        // 创建负载均衡器
        val consistentHashingBalancer = LoadBalancer(
            cluster = cluster,
            strategy = LoadBalancingStrategy.CONSISTENT_HASHING,
            updateInterval = 1000
        )
        
        // 启动负载均衡器
        consistentHashingBalancer.start()
        
        try {
            // 创建测试 Actor 标识
            val identities = (1..10).map { ClusterIdentity("test", "test-actor-$it") }
            
            // 选择节点
            val selections = identities.map { consistentHashingBalancer.selectNode(it) }
            
            // 验证结果
            selections.forEach { assertNotNull(it, "Selected node should not be null") }
            
            // 再次选择相同的 Actor 标识
            val repeatSelections = identities.map { consistentHashingBalancer.selectNode(it) }
            
            // 验证相同的 Actor 标识总是映射到相同的节点
            identities.forEachIndexed { index, _ ->
                assertEquals(selections[index], repeatSelections[index], "Same identity should map to same node")
            }
            
            // 验证统计信息
            val stats = consistentHashingBalancer.getStats()
            assertEquals(LoadBalancingStrategy.CONSISTENT_HASHING, stats.strategy, "Strategy should be CONSISTENT_HASHING")
        } finally {
            // 停止负载均衡器
            consistentHashingBalancer.stop()
        }
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should select node using round robin`() = runBlocking {
        // 创建负载均衡器
        val roundRobinBalancer = LoadBalancer(
            cluster = cluster,
            strategy = LoadBalancingStrategy.ROUND_ROBIN,
            updateInterval = 1000
        )
        
        // 启动负载均衡器
        roundRobinBalancer.start()
        
        try {
            // 创建测试 Actor 标识
            val identity = ClusterIdentity("test", "test-actor")
            
            // 选择节点多次
            val selections = (1..30).map { roundRobinBalancer.selectNode(identity) }
            
            // 验证结果
            assertNotNull(selections.first(), "Selected node should not be null")
            
            // 计算每个节点的选择次数
            val counts = selections.groupingBy { it }.eachCount()
            
            // 验证每个节点被选择的次数大致相等
            val expectedCount = selections.size / 3
            counts.forEach { (node, count) ->
                assertTrue(Math.abs(count - expectedCount) <= 1, "Node $node should be selected approximately $expectedCount times")
            }
            
            // 验证统计信息
            val stats = roundRobinBalancer.getStats()
            assertEquals(LoadBalancingStrategy.ROUND_ROBIN, stats.strategy, "Strategy should be ROUND_ROBIN")
        } finally {
            // 停止负载均衡器
            roundRobinBalancer.stop()
        }
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should rebalance cluster`() = runBlocking {
        // 执行重新平衡
        val rebalanceCount = loadBalancer.rebalance()
        
        // 验证结果
        assertTrue(rebalanceCount >= 0, "Rebalance count should be non-negative")
        
        // 验证统计信息
        val stats = loadBalancer.getStats()
        assertEquals(1, stats.rebalanceOperations, "Rebalance operations should be 1")
    }
}
