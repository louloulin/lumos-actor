package actor.proto.cluster.libp2p

import actor.proto.ActorSystem
import actor.proto.PID
import actor.proto.Props
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.ClusterKind
import actor.proto.cluster.MemberStatus
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
import kotlin.test.assertTrue

class P2PFailureDetectorTest {
    
    private lateinit var system1: ActorSystem
    private lateinit var system2: ActorSystem
    private lateinit var cluster1: Cluster
    private lateinit var cluster2: Cluster
    private lateinit var provider1: P2PClusterProvider
    private lateinit var provider2: P2PClusterProvider
    
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
    @Timeout(60) // 60 秒超时
    fun `should detect node failure and recovery`() = runBlocking {
        // 创建 P2P 集群配置，使用较短的心跳和监控间隔
        val p2pConfig1 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4001,
            heartbeatInterval = java.time.Duration.ofSeconds(1),
            monitorInterval = java.time.Duration.ofSeconds(3)
        )
        
        val p2pConfig2 = P2PClusterConfig(
            clusterName = "test-cluster",
            enableMDns = true,
            enableDHT = true,
            listenPort = 4002,
            heartbeatInterval = java.time.Duration.ofSeconds(1),
            monitorInterval = java.time.Duration.ofSeconds(3)
        )
        
        // 创建集群提供者
        provider1 = P2PClusterProvider(p2pConfig1)
        provider2 = P2PClusterProvider(p2pConfig2)
        
        // 创建集群配置
        val clusterConfig1 = ClusterConfig(
            clusterName = "test-cluster",
            clusterProvider = provider1
        )
        
        val clusterConfig2 = ClusterConfig(
            clusterName = "test-cluster",
            clusterProvider = provider2
        )
        
        // 创建并启动集群
        cluster1 = Cluster(system1, clusterConfig1)
        cluster2 = Cluster(system2, clusterConfig2)
        
        // 注册 Actor 类型
        val testActorProps = Props.fromProducer {
            object : actor.proto.Actor {
                override suspend fun receive(context: actor.proto.Context) {
                    val msg = context.message
                    if (msg is String) {
                        context.respond("Response: $msg")
                    }
                }
            }
        }
        
        cluster1.registerKind("test", ClusterKind.fromProps("test", testActorProps))
        cluster2.registerKind("test", ClusterKind.fromProps("test", testActorProps))
        
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
        
        // 验证所有成员都是 ALIVE 状态
        assertTrue(members1.all { it.status == MemberStatus.ALIVE }, "All members in cluster 1 should be ALIVE")
        assertTrue(members2.all { it.status == MemberStatus.ALIVE }, "All members in cluster 2 should be ALIVE")
        
        // 模拟节点故障：停止节点 2 的心跳
        val failureDetector2 = provider2.getFailureDetector()
        val originalRecordHeartbeat = failureDetector2::recordHeartbeat
        
        // 替换 recordHeartbeat 方法，使其不做任何事
        val field = P2PFailureDetector::class.java.getDeclaredField("lastHeartbeats")
        field.isAccessible = true
        val lastHeartbeats = field.get(failureDetector2) as ConcurrentHashMap<String, Long>
        lastHeartbeats.remove(system2.address)
        
        // 等待故障检测
        println("Waiting for failure detection...")
        delay(15000) // 等待足够长的时间让故障检测器检测到故障
        
        // 验证节点 2 被标记为 UNAVAILABLE 或 DEAD
        val updatedMembers1 = cluster1.memberList.getMembers()
        val member2InCluster1 = updatedMembers1.find { it.id == system2.address }
        
        println("Updated cluster 1 members: $updatedMembers1")
        assertNotNull(member2InCluster1, "Member 2 should still be in the member list")
        assertTrue(
            member2InCluster1.status == MemberStatus.UNAVAILABLE || member2InCluster1.status == MemberStatus.DEAD,
            "Member 2 should be marked as UNAVAILABLE or DEAD"
        )
        
        // 模拟节点恢复：恢复节点 2 的心跳
        println("Simulating node recovery...")
        provider2.getFailureDetector().recordHeartbeat(system2.address)
        
        // 等待恢复检测
        delay(5000)
        
        // 验证节点 2 被标记为 ALIVE
        val recoveredMembers1 = cluster1.memberList.getMembers()
        val recoveredMember2InCluster1 = recoveredMembers1.find { it.id == system2.address }
        
        println("Recovered cluster 1 members: $recoveredMembers1")
        assertNotNull(recoveredMember2InCluster1, "Member 2 should still be in the member list")
        assertEquals(MemberStatus.ALIVE, recoveredMember2InCluster1.status, "Member 2 should be marked as ALIVE")
    }
}
