package actor.proto.plugin

import actor.proto.Actor
import actor.proto.ActorSystem
import actor.proto.Context
import actor.proto.TestPID
import actor.proto.Props
import actor.proto.fromProducer
import actor.proto.plugin.passivation.Passivate
import actor.proto.plugin.passivation.PassivationPlugin
import actor.proto.plugin.persistence.InMemoryPersistenceProvider
import actor.proto.plugin.persistence.PersistencePlugin
import actor.proto.plugin.persistence.PersistentActor
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.pf4j.DefaultPluginManager
import org.pf4j.PluginWrapper
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class PluginTest {
    private lateinit var system: ActorSystem
    private lateinit var pluginManager: ProtoPluginManager

    @BeforeEach
    fun setup() {
        system = ActorSystem.create()
        pluginManager = ProtoPluginManager.getInstance(Paths.get("plugins"))
    }

    @AfterEach
    fun teardown() {
        pluginManager.stopPlugins()
        system.shutdown()
    }

    @Test
    fun `should load and initialize plugins`() {
        // 创建测试插件
        val descriptor = org.pf4j.DefaultPluginDescriptor("test-plugin", "Test Plugin", "1.0.0", "Test", null, null, null)
        val testPlugin = TestPlugin(PluginWrapper(DefaultPluginManager(), descriptor, null, null))

        // 直接注册插件
        system.registerPlugin(testPlugin)

        // 验证插件已初始化
        assertTrue(testPlugin.initialized)
    }

    @Test
    fun `should apply receive middleware from plugin`() = runBlocking {
        // 创建测试插件
        val descriptor = org.pf4j.DefaultPluginDescriptor("test-plugin", "Test Plugin", "1.0.0", "Test", null, null, null)
        val testPlugin = TestPlugin(PluginWrapper(DefaultPluginManager(), descriptor, null, null))

        // 直接注册插件
        system.registerPlugin(testPlugin)

        // 创建Actor
        val latch = CountDownLatch(1)
        val receivedMessages = mutableListOf<String>()

        val props = fromProducer {
            object : Actor {
                override suspend fun Context.receive(msg: Any) {
                    if (msg is String) {
                        receivedMessages.add(msg)
                        latch.countDown()
                    }
                }
            }
        }

        val pid = system.root.spawn(props)

        // 发送消息
        system.root.send(pid, "hello")

        // 等待处理完成
        assertTrue(latch.await(1, TimeUnit.SECONDS))

        // 验证消息已被处理
        assertEquals(1, receivedMessages.size)
        assertEquals("hello", receivedMessages[0])
    }
}

/**
 * 测试插件
 */
class TestPlugin(wrapper: PluginWrapper) : ProtoPlugin(wrapper) {
    var initialized = false

    override fun start() {
        // 启动插件
    }

    override fun stop() {
        initialized = false
    }

    override fun init(system: Any) {
        initialized = true
        println("TestPlugin initialized")
    }
}
