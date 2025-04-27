package actor.proto.cluster.libp2p

import actor.proto.ActorSystem
import actor.proto.PID
import actor.proto.Props
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.ClusterIdentity
import actor.proto.cluster.ClusterKind
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class P2PDHTTest {
    
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
    fun `should register and lookup actor in DHT`() = runBlocking {
        // 创建 P2P 集群配置
        val p2pConfig1 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4001
        )
        
        val p2pConfig2 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
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
        
        // 在第一个节点上激活 Actor
        val clusterIdentity = ClusterIdentity("test", "test-actor")
        val dht1 = clusterProvider1.getDHT()
        assertNotNull(dht1, "DHT should not be null")
        
        // 在本地激活 Actor
        val pid1 = cluster1.get("test-actor", "test")
        assertNotNull(pid1, "PID should not be null")
        
        // 等待 DHT 同步
        delay(2000)
        
        // 从第二个节点查找 Actor
        val dht2 = clusterProvider2.getDHT()
        assertNotNull(dht2, "DHT should not be null")
        
        val pid2 = dht2.lookup(clusterIdentity)
        assertNotNull(pid2, "PID should be found in DHT")
        
        // 验证 PID 是否相同
        assertEquals(pid1.address, pid2?.address, "PID address should match")
        assertEquals(pid1.id, pid2?.id, "PID id should match")
        
        // 发送消息
        system2.root.send(pid2!!, "hello")
        
        // 等待消息处理
        val received = latch.await(10, TimeUnit.SECONDS)
        assertEquals(true, received, "Should receive message")
        assertEquals("hello", receivedMessage, "Should receive correct message")
    }
}
