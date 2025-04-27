package actor.proto.cluster.libp2p

import actor.proto.ActorSystem
import actor.proto.PID
import actor.proto.Props
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.Kind
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class P2PClusterTest {
    
    private lateinit var system1: ActorSystem
    private lateinit var system2: ActorSystem
    private lateinit var cluster1: Cluster
    private lateinit var cluster2: Cluster
    
    @BeforeEach
    fun setup() {
        // 创建两个 Actor 系统
        system1 = ActorSystem("system1")
        system2 = ActorSystem("system2")
    }
    
    @AfterEach
    fun tearDown() = runBlocking {
        // 关闭集群和 Actor 系统
        if (::cluster1.isInitialized) {
            cluster1.shutdown(true)
        }
        if (::cluster2.isInitialized) {
            cluster2.shutdown(true)
        }
        
        system1.shutdown()
        system2.shutdown()
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should form a cluster with two nodes`() = runBlocking {
        // 创建 P2P 集群配置
        val p2pConfig1 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4001,
            heartbeatInterval = Duration.ofSeconds(1),
            monitorInterval = Duration.ofSeconds(3)
        )
        
        val p2pConfig2 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4002,
            heartbeatInterval = Duration.ofSeconds(1),
            monitorInterval = Duration.ofSeconds(3)
        )
        
        // 创建集群提供者
        val clusterProvider1 = P2PClusterProvider(p2pConfig1)
        val clusterProvider2 = P2PClusterProvider(p2pConfig2)
        
        // 创建身份查找服务
        val identityLookup1 = P2PIdentityLookup()
        val identityLookup2 = P2PIdentityLookup()
        
        // 创建集群配置
        val clusterConfig1 = ClusterConfig(
            name = "test-cluster",
            clusterProvider = clusterProvider1,
            identityLookup = identityLookup1
        )
        
        val clusterConfig2 = ClusterConfig(
            name = "test-cluster",
            clusterProvider = clusterProvider2,
            identityLookup = identityLookup2
        )
        
        // 创建并启动集群
        cluster1 = Cluster(system1, clusterConfig1)
        cluster2 = Cluster(system2, clusterConfig2)
        
        // 注册 Actor 类型
        val latch = CountDownLatch(1)
        var receivedMessage: String? = null
        
        // 创建测试 Actor
        val testActorProps = Props.fromProducer {
            object : actor.proto.Actor {
                override suspend fun receive(context: actor.proto.Context) {
                    val msg = context.message
                    if (msg is String) {
                        receivedMessage = msg
                        latch.countDown()
                    }
                }
            }
        }
        
        // 创建 Kind
        val testKind = Kind("test", testActorProps)
        
        // 注册集群 Kind
        cluster1.registerKind(testKind)
        cluster2.registerKind(testKind)
        
        // 启动集群
        cluster1.startMember()
        cluster2.startMember()
        
        // 等待集群形成
        delay(5000)
        
        // 验证集群成员
        val members1 = cluster1.memberList.getMembers()
        val members2 = cluster2.memberList.getMembers()
        
        assertEquals(2, members1.size, "Cluster 1 should have 2 members")
        assertEquals(2, members2.size, "Cluster 2 should have 2 members")
        
        // 获取虚拟 Actor
        val pid = cluster1.get("test-actor", "test")
        assertNotNull(pid, "Virtual actor PID should not be null")
        
        // 发送消息
        system1.root.send(pid, "hello")
        
        // 等待消息处理
        val received = latch.await(10, TimeUnit.SECONDS)
        assertTrue(received, "Should receive message")
        assertEquals("hello", receivedMessage, "Should receive correct message")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should handle node failure and recovery`() = runBlocking {
        // 创建 P2P 集群配置
        val p2pConfig1 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4001,
            heartbeatInterval = Duration.ofSeconds(1),
            monitorInterval = Duration.ofSeconds(3)
        )
        
        val p2pConfig2 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4002,
            heartbeatInterval = Duration.ofSeconds(1),
            monitorInterval = Duration.ofSeconds(3)
        )
        
        // 创建集群提供者
        val clusterProvider1 = P2PClusterProvider(p2pConfig1)
        val clusterProvider2 = P2PClusterProvider(p2pConfig2)
        
        // 创建身份查找服务
        val identityLookup1 = P2PIdentityLookup()
        val identityLookup2 = P2PIdentityLookup()
        
        // 创建集群配置
        val clusterConfig1 = ClusterConfig(
            name = "test-cluster",
            clusterProvider = clusterProvider1,
            identityLookup = identityLookup1
        )
        
        val clusterConfig2 = ClusterConfig(
            name = "test-cluster",
            clusterProvider = clusterProvider2,
            identityLookup = identityLookup2
        )
        
        // 创建并启动集群
        cluster1 = Cluster(system1, clusterConfig1)
        cluster2 = Cluster(system2, clusterConfig2)
        
        // 创建 Kind
        val testKind = Kind("test", Props.empty())
        
        // 注册集群 Kind
        cluster1.registerKind(testKind)
        cluster2.registerKind(testKind)
        
        // 启动集群
        cluster1.startMember()
        cluster2.startMember()
        
        // 等待集群形成
        delay(5000)
        
        // 验证集群成员
        val members1 = cluster1.memberList.getMembers()
        assertEquals(2, members1.size, "Cluster 1 should have 2 members")
        
        // 模拟节点 2 故障
        cluster2.shutdown(false)
        
        // 等待故障检测
        delay(10000)
        
        // 验证节点 2 被标记为不可用
        val membersAfterFailure = cluster1.memberList.getMembers()
        assertEquals(1, membersAfterFailure.size, "Cluster 1 should have 1 member after failure")
        
        // 重新启动节点 2
        cluster2 = Cluster(system2, clusterConfig2)
        cluster2.registerKind(testKind)
        cluster2.startMember()
        
        // 等待节点恢复
        delay(5000)
        
        // 验证节点 2 被重新添加到集群
        val membersAfterRecovery = cluster1.memberList.getMembers()
        assertEquals(2, membersAfterRecovery.size, "Cluster 1 should have 2 members after recovery")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should handle message batching and compression`() = runBlocking {
        // 创建 P2P 集群配置
        val p2pConfig1 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4001,
            heartbeatInterval = Duration.ofSeconds(1),
            monitorInterval = Duration.ofSeconds(3)
        )
        
        val p2pConfig2 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4002,
            heartbeatInterval = Duration.ofSeconds(1),
            monitorInterval = Duration.ofSeconds(3)
        )
        
        // 创建集群提供者
        val clusterProvider1 = P2PClusterProvider(p2pConfig1)
        val clusterProvider2 = P2PClusterProvider(p2pConfig2)
        
        // 创建身份查找服务
        val identityLookup1 = P2PIdentityLookup()
        val identityLookup2 = P2PIdentityLookup()
        
        // 创建集群配置
        val clusterConfig1 = ClusterConfig(
            name = "test-cluster",
            clusterProvider = clusterProvider1,
            identityLookup = identityLookup1
        )
        
        val clusterConfig2 = ClusterConfig(
            name = "test-cluster",
            clusterProvider = clusterProvider2,
            identityLookup = identityLookup2
        )
        
        // 创建并启动集群
        cluster1 = Cluster(system1, clusterConfig1)
        cluster2 = Cluster(system2, clusterConfig2)
        
        // 注册 Actor 类型
        val latch = CountDownLatch(100)
        var messageCount = 0
        
        // 创建测试 Actor
        val testActorProps = Props.fromProducer {
            object : actor.proto.Actor {
                override suspend fun receive(context: actor.proto.Context) {
                    val msg = context.message
                    if (msg is String) {
                        messageCount++
                        latch.countDown()
                    }
                }
            }
        }
        
        // 创建 Kind
        val testKind = Kind("test", testActorProps)
        
        // 注册集群 Kind
        cluster1.registerKind(testKind)
        cluster2.registerKind(testKind)
        
        // 启动集群
        cluster1.startMember()
        cluster2.startMember()
        
        // 等待集群形成
        delay(5000)
        
        // 获取虚拟 Actor
        val pid = cluster1.get("test-actor", "test")
        assertNotNull(pid, "Virtual actor PID should not be null")
        
        // 发送多条消息
        for (i in 1..100) {
            system1.root.send(pid, "message-$i")
        }
        
        // 等待消息处理
        val received = latch.await(10, TimeUnit.SECONDS)
        assertTrue(received, "Should receive all messages")
        assertEquals(100, messageCount, "Should receive 100 messages")
    }
}
