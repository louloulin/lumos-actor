package actor.proto.cluster.libp2p.interop

import actor.proto.ActorSystem
import actor.proto.PID
import actor.proto.Props
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.ClusterIdentity
import actor.proto.cluster.Kind
import actor.proto.cluster.libp2p.P2PClusterConfig
import actor.proto.cluster.libp2p.P2PClusterProvider
import actor.proto.cluster.libp2p.P2PIdentityLookup
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class CrossLanguageAdapterTest {
    
    private lateinit var system: ActorSystem
    private lateinit var cluster: Cluster
    private lateinit var adapter: CrossLanguageAdapter
    
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
        
        // 创建跨语言适配器
        adapter = CrossLanguageAdapter(cluster)
    }
    
    @AfterEach
    fun tearDown() = runBlocking {
        // 关闭集群和 Actor 系统
        if (::cluster.isInitialized) {
            cluster.shutdown(true)
        }
        
        system.shutdown()
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should handle actor message`() = runBlocking {
        // 创建测试 Actor
        val latch = CountDownLatch(1)
        var receivedMessage: String? = null
        
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
        
        // 创建 Actor
        val pid = system.root.spawnNamed(testActorProps, "test-actor")
        
        // 创建标准消息
        val message = StandardMessage(
            type = MessageType.ACTOR_MESSAGE,
            sender = "test-sender",
            target = "test-actor",
            payload = "Hello, Actor!".toByteArray(StandardCharsets.UTF_8)
        )
        
        // 处理消息
        adapter.handleIncomingMessage(message)
        
        // 等待消息处理
        val received = latch.await(5, TimeUnit.SECONDS)
        assertTrue(received, "Should receive message")
        assertEquals("Hello, Actor!", receivedMessage, "Should receive correct message")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should handle request and response`() = runBlocking {
        // 创建测试 Actor
        val testActorProps = Props.fromProducer {
            object : actor.proto.Actor {
                override suspend fun receive(context: actor.proto.Context) {
                    val msg = context.message
                    if (msg is String && msg == "Hello, Actor!") {
                        context.respond("Hello, Sender!")
                    }
                }
            }
        }
        
        // 创建 Actor
        val pid = system.root.spawnNamed(testActorProps, "test-actor")
        
        // 创建标准消息
        val message = StandardMessage(
            type = MessageType.REQUEST,
            sender = "test-sender",
            target = "test-actor",
            requestId = "test-request-id",
            payload = "Hello, Actor!".toByteArray(StandardCharsets.UTF_8)
        )
        
        // 创建响应接收器
        val latch = CountDownLatch(1)
        var receivedResponse: String? = null
        
        // 模拟发送响应
        val originalSendMessage = adapter::sendMessage
        val sendMessageMock: (String, StandardMessage) -> Boolean = { target, msg ->
            if (msg.type == MessageType.RESPONSE && msg.requestId == "test-request-id") {
                receivedResponse = String(msg.payload, StandardCharsets.UTF_8)
                latch.countDown()
            }
            true
        }
        
        // 替换发送方法
        val sendMessageField = CrossLanguageAdapter::class.java.getDeclaredField("sendMessage")
        sendMessageField.isAccessible = true
        sendMessageField.set(adapter, sendMessageMock)
        
        // 处理请求
        adapter.handleIncomingMessage(message)
        
        // 等待响应
        val received = latch.await(5, TimeUnit.SECONDS)
        assertTrue(received, "Should receive response")
        assertEquals("Hello, Sender!", receivedResponse, "Should receive correct response")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should handle member up and down`() = runBlocking {
        // 启动集群
        cluster.startMember()
        
        // 等待集群启动
        delay(1000)
        
        // 创建成员信息
        val memberJson = """
            {
                "id": "test-member",
                "host": "localhost",
                "port": 4002,
                "status": "ALIVE",
                "labels": {"kind": "test"}
            }
        """.trimIndent()
        
        // 创建标准消息
        val memberUpMessage = StandardMessage(
            type = MessageType.MEMBER_UP,
            sender = "test-member",
            payload = memberJson.toByteArray(StandardCharsets.UTF_8)
        )
        
        // 处理成员上线消息
        adapter.handleIncomingMessage(memberUpMessage)
        
        // 等待处理
        delay(1000)
        
        // 验证成员是否已添加
        val members = cluster.memberList.getMembers()
        assertTrue(members.any { it.id == "test-member" }, "Member should be added")
        
        // 创建成员下线消息
        val memberDownMessage = StandardMessage(
            type = MessageType.MEMBER_DOWN,
            sender = "test-member",
            payload = ByteArray(0)
        )
        
        // 处理成员下线消息
        adapter.handleIncomingMessage(memberDownMessage)
        
        // 等待处理
        delay(1000)
        
        // 验证成员是否已标记为不可用
        val updatedMembers = cluster.memberList.getMembers()
        val member = updatedMembers.find { it.id == "test-member" }
        assertNotNull(member, "Member should still exist")
        assertEquals(actor.proto.cluster.MemberStatus.UNAVAILABLE, member.status, "Member should be marked as unavailable")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should handle actor activation`() = runBlocking {
        // 创建测试 Actor 类型
        val testActorProps = Props.fromProducer {
            object : actor.proto.Actor {
                override suspend fun receive(context: actor.proto.Context) {
                    // 空实现
                }
            }
        }
        
        // 创建 Kind
        val testKind = Kind("test", testActorProps)
        
        // 注册集群 Kind
        cluster.registerKind(testKind)
        
        // 启动集群
        cluster.startMember()
        
        // 等待集群启动
        delay(1000)
        
        // 创建 Actor 标识
        val identityJson = """
            {
                "kind": "test",
                "identity": "test-actor"
            }
        """.trimIndent()
        
        // 创建标准消息
        val activationMessage = StandardMessage(
            type = MessageType.ACTOR_ACTIVATION,
            sender = "test-sender",
            payload = identityJson.toByteArray(StandardCharsets.UTF_8)
        )
        
        // 处理 Actor 激活消息
        adapter.handleIncomingMessage(activationMessage)
        
        // 等待处理
        delay(1000)
        
        // 验证 Actor 是否已激活
        val identity = ClusterIdentity("test", "test-actor")
        val pid = cluster.pidCache.get(identity)
        assertNotNull(pid, "Actor should be activated")
    }
}
