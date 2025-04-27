package actor.proto.cluster.libp2p

import actor.proto.ActorSystem
import actor.proto.PID
import actor.proto.Props
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.ClusterKind
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
            listenPort = 4001
        )
        
        val p2pConfig2 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            listenPort = 4002
        )
        
        // 创建集群提供者
        val clusterProvider1 = P2PClusterProvider(p2pConfig1)
        val clusterProvider2 = P2PClusterProvider(p2pConfig2)
        
        // 创建集群配置
        val clusterConfig1 = ClusterConfig(
            clusterName = "test-cluster",
            clusterProvider = clusterProvider1
        )
        
        val clusterConfig2 = ClusterConfig(
            clusterName = "test-cluster",
            clusterProvider = clusterProvider2
        )
        
        // 创建并启动集群
        cluster1 = Cluster(system1, clusterConfig1)
        cluster2 = Cluster(system2, clusterConfig2)
        
        // 启动集群
        cluster1.startMember()
        cluster2.startMember()
        
        // 等待集群形成
        delay(5000)
        
        // 验证集群成员
        val members1 = cluster1.memberList.getMembers()
        val members2 = cluster2.memberList.getMembers()
        
        println("Cluster 1 members: $members1")
        println("Cluster 2 members: $members2")
        
        // 验证每个集群都有两个成员
        assertEquals(2, members1.size, "Cluster 1 should have 2 members")
        assertEquals(2, members2.size, "Cluster 2 should have 2 members")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should activate virtual actor across nodes`() = runBlocking {
        // 创建 P2P 集群配置
        val p2pConfig1 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            listenPort = 4001
        )
        
        val p2pConfig2 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            listenPort = 4002
        )
        
        // 创建集群提供者
        val clusterProvider1 = P2PClusterProvider(p2pConfig1)
        val clusterProvider2 = P2PClusterProvider(p2pConfig2)
        
        // 创建身份查找
        val identityLookup1 = P2PIdentityLookup(cluster1, clusterProvider1.getHost())
        val identityLookup2 = P2PIdentityLookup(cluster2, clusterProvider2.getHost())
        
        // 创建集群配置
        val clusterConfig1 = ClusterConfig(
            clusterName = "test-cluster",
            clusterProvider = clusterProvider1,
            identityLookup = identityLookup1
        )
        
        val clusterConfig2 = ClusterConfig(
            clusterName = "test-cluster",
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
        
        // 注册集群 Kind
        cluster1.registerKind("test", ClusterKind.fromProps("test", testActorProps))
        cluster2.registerKind("test", ClusterKind.fromProps("test", testActorProps))
        
        // 启动集群
        cluster1.startMember()
        cluster2.startMember()
        
        // 等待集群形成
        delay(5000)
        
        // 获取虚拟 Actor
        val pid: PID = cluster1.get("test-actor", "test")
        assertNotNull(pid, "Virtual actor PID should not be null")
        
        // 发送消息
        system1.root.send(pid, "hello")
        
        // 等待消息处理
        val received = latch.await(10, TimeUnit.SECONDS)
        assertEquals(true, received, "Should receive message")
        assertEquals("hello", receivedMessage, "Should receive correct message")
    }
}
