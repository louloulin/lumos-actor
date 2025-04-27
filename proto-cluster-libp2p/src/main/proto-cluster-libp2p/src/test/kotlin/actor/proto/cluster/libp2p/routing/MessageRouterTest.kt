package actor.proto.cluster.libp2p.routing

import actor.proto.ActorSystem
import actor.proto.PID
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.ClusterIdentity
import actor.proto.cluster.IdentityLookup
import actor.proto.cluster.libp2p.P2PClusterConfig
import actor.proto.cluster.libp2p.P2PClusterProvider
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

class MessageRouterTest {
    
    private lateinit var system: ActorSystem
    private lateinit var cluster: Cluster
    private lateinit var router: MessageRouter
    private lateinit var identityLookup: IdentityLookup
    
    @BeforeEach
    fun setup() {
        // 创建 Actor 系统
        system = ActorSystem("test-system")
        
        // 创建模拟的身份查找服务
        identityLookup = Mockito.mock(IdentityLookup::class.java)
        
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
        
        // 创建集群配置
        val clusterConfig = ClusterConfig(
            name = "test-cluster",
            clusterProvider = clusterProvider,
            identityLookup = identityLookup
        )
        
        // 创建集群
        cluster = Cluster(system, clusterConfig)
        
        // 创建消息路由器
        router = MessageRouter(cluster)
        
        // 启动路由器
        router.start()
    }
    
    @AfterEach
    fun tearDown() = runBlocking {
        // 停止路由器
        router.stop()
        
        // 关闭集群和 Actor 系统
        if (::cluster.isInitialized) {
            cluster.shutdown(true)
        }
        
        system.shutdown()
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should route message and cache route`() = runBlocking {
        // 创建测试 Actor 标识
        val identity = ClusterIdentity("test", "test-actor")
        
        // 创建测试 PID
        val pid = PID("test-node", "test-actor")
        
        // 设置模拟行为
        Mockito.`when`(identityLookup.lookup(identity)).thenReturn(pid)
        
        // 路由消息
        val routedPid = router.routeMessage(identity)
        
        // 验证结果
        assertEquals(pid, routedPid, "Routed PID should match expected PID")
        
        // 验证缓存命中
        val stats = router.getStats()
        assertEquals(1, stats.cacheMisses, "Cache misses should be 1")
        assertEquals(0, stats.cacheHits, "Cache hits should be 0")
        
        // 再次路由相同的消息
        val cachedPid = router.routeMessage(identity)
        
        // 验证缓存命中
        assertEquals(pid, cachedPid, "Cached PID should match expected PID")
        
        // 验证统计信息
        val updatedStats = router.getStats()
        assertEquals(1, updatedStats.cacheMisses, "Cache misses should still be 1")
        assertEquals(1, updatedStats.cacheHits, "Cache hits should be 1")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should update route`() = runBlocking {
        // 创建测试 Actor 标识
        val identity = ClusterIdentity("test", "test-actor")
        
        // 创建测试 PID
        val pid1 = PID("test-node-1", "test-actor")
        val pid2 = PID("test-node-2", "test-actor")
        
        // 设置模拟行为
        Mockito.`when`(identityLookup.lookup(identity)).thenReturn(pid1)
        
        // 路由消息
        val routedPid = router.routeMessage(identity)
        
        // 验证结果
        assertEquals(pid1, routedPid, "Routed PID should match expected PID")
        
        // 更新路由
        router.updateRoute(identity, pid2)
        
        // 再次路由相同的消息
        val updatedPid = router.routeMessage(identity)
        
        // 验证结果
        assertEquals(pid2, updatedPid, "Updated PID should match expected PID")
        
        // 验证统计信息
        val stats = router.getStats()
        assertEquals(1, stats.cacheMisses, "Cache misses should be 1")
        assertEquals(1, stats.cacheHits, "Cache hits should be 1")
        assertEquals(1, stats.routeUpdates, "Route updates should be 1")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should remove route`() = runBlocking {
        // 创建测试 Actor 标识
        val identity = ClusterIdentity("test", "test-actor")
        
        // 创建测试 PID
        val pid = PID("test-node", "test-actor")
        
        // 设置模拟行为
        Mockito.`when`(identityLookup.lookup(identity)).thenReturn(pid)
        
        // 路由消息
        val routedPid = router.routeMessage(identity)
        
        // 验证结果
        assertEquals(pid, routedPid, "Routed PID should match expected PID")
        
        // 移除路由
        router.removeRoute(identity)
        
        // 再次路由相同的消息
        val newPid = router.routeMessage(identity)
        
        // 验证结果
        assertEquals(pid, newPid, "New PID should match expected PID")
        
        // 验证统计信息
        val stats = router.getStats()
        assertEquals(2, stats.cacheMisses, "Cache misses should be 2")
        assertEquals(0, stats.cacheHits, "Cache hits should be 0")
        assertEquals(1, stats.routeEvictions, "Route evictions should be 1")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should handle multiple routes`() = runBlocking {
        // 创建多个测试 Actor 标识
        val identities = (1..100).map { ClusterIdentity("test", "test-actor-$it") }
        
        // 创建多个测试 PID
        val pids = (1..100).map { PID("test-node-${it % 10}", "test-actor-$it") }
        
        // 设置模拟行为
        identities.forEachIndexed { index, identity ->
            Mockito.`when`(identityLookup.lookup(identity)).thenReturn(pids[index])
        }
        
        // 路由所有消息
        identities.forEachIndexed { index, identity ->
            val routedPid = router.routeMessage(identity)
            assertEquals(pids[index], routedPid, "Routed PID should match expected PID")
        }
        
        // 验证缓存大小
        val stats = router.getStats()
        assertEquals(100, stats.cacheSize, "Cache size should be 100")
        assertEquals(100, stats.cacheMisses, "Cache misses should be 100")
        
        // 再次路由所有消息
        identities.forEachIndexed { index, identity ->
            val cachedPid = router.routeMessage(identity)
            assertEquals(pids[index], cachedPid, "Cached PID should match expected PID")
        }
        
        // 验证缓存命中
        val updatedStats = router.getStats()
        assertEquals(100, updatedStats.cacheSize, "Cache size should still be 100")
        assertEquals(100, updatedStats.cacheMisses, "Cache misses should still be 100")
        assertEquals(100, updatedStats.cacheHits, "Cache hits should be 100")
    }
}
