package actor.proto.cluster.libp2p

import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterProvider
import actor.proto.cluster.Member
import actor.proto.cluster.MemberStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * P2PClusterProvider 是基于 libp2p 的集群提供者实现
 * 注意: 当前实现是简化版本，仅用于演示
 */
class P2PClusterProvider(
    val config: P2PClusterConfig
) : ClusterProvider {
    private lateinit var cluster: Cluster
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var isRunning = false

    override suspend fun startMember(cluster: Cluster): Boolean {
        this.cluster = cluster

        logger.info { "Starting P2P cluster member at ${cluster.actorSystem.address}" }

        // 注册集群成员
        registerMember()

        isRunning = true

        // 启动心跳
        startHeartbeat()

        return true
    }

    override suspend fun startClient(cluster: Cluster): Boolean {
        this.cluster = cluster

        logger.info { "Starting P2P cluster client at ${cluster.actorSystem.address}" }

        isRunning = true

        return true
    }

    override suspend fun shutdown(graceful: Boolean): Boolean {
        if (!isRunning) return true

        logger.info { "Shutting down P2P cluster provider" }

        isRunning = false

        return true
    }

    /**
     * 注册集群成员
     */
    private fun registerMember() {
        // 将自己注册为集群成员
        val member = createMemberInfo()
        cluster.memberList.updateClusterTopology(listOf(member))
    }

    /**
     * 创建成员信息
     */
    private fun createMemberInfo(): Member {
        return Member(
            id = cluster.actorSystem.address,
            host = cluster.actorSystem.address,
            port = config.listenPort,
            kinds = cluster.getClusterKinds().keys.toList(),
            status = MemberStatus.ALIVE
        )
    }

    /**
     * 启动心跳
     */
    private fun startHeartbeat() {
        scope.launch {
            while (isRunning) {
                try {
                    // 发送心跳
                    val member = createMemberInfo()
                    cluster.memberList.updateClusterTopology(listOf(member))

                    // 等待下一个心跳间隔
                    delay(config.heartbeatInterval.toMillis())
                } catch (e: Exception) {
                    logger.error(e) { "Error in heartbeat" }
                }
            }
        }
    }
}
