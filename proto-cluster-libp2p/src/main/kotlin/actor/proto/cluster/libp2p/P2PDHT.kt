package actor.proto.cluster.libp2p

import actor.proto.PID
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterIdentity
import actor.proto.cluster.MemberStatus
import io.libp2p.core.Host
import io.libp2p.core.PeerId
import io.libp2p.core.dht.Kademlia
import io.libp2p.core.dht.KademliaConfig
import io.libp2p.core.dht.PeerInfo
import io.libp2p.core.dht.PeerTable
import io.libp2p.core.dht.Record
import io.libp2p.core.dht.RecordStore
import io.libp2p.core.dht.SimpleRecordStore
import io.libp2p.core.multiformats.Multiaddr
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.random.Random

private val logger = KotlinLogging.logger {}

/**
 * P2PDHT 负责基于 Kademlia DHT 的 Actor 定位
 */
class P2PDHT(
    private val cluster: Cluster,
    private val host: Host,
    private val config: P2PClusterConfig
) {
    private lateinit var dht: Kademlia
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val pendingActivations = ConcurrentHashMap<String, CompletableDeferred<PID>>()

    /**
     * 启动 DHT
     */
    fun start() {
        // 创建 DHT
        val recordStore = SimpleRecordStore()
        val peerTable = PeerTable(host.peerId)

        // 收集引导节点
        val bootstrapPeers = collectBootstrapPeers()

        val kademliaConfig = KademliaConfig(
            peerTable = peerTable,
            recordStore = recordStore,
            bootstrapPeers = bootstrapPeers
        )

        dht = Kademlia(host, kademliaConfig)
        dht.start()

        logger.info { "P2P DHT started with ${bootstrapPeers.size} bootstrap peers" }

        // 启动定期刷新
        startRefreshLoop()

        // 启动定期清理
        startCleanupLoop()
    }

    /**
     * 收集引导节点
     */
    private fun collectBootstrapPeers(): List<PeerInfo> {
        val bootstrapPeers = mutableListOf<PeerInfo>()

        // 从种子节点收集
        config.seedNodes.forEach { seedNode ->
            try {
                // 解析种子节点地址
                // 格式: PeerId@/ip4/address/tcp/port
                val parts = seedNode.split("@", limit = 2)
                if (parts.size != 2) {
                    logger.error { "Invalid seed node format: $seedNode" }
                    return@forEach
                }

                val peerId = PeerId.fromBase58(parts[0])
                val multiaddr = Multiaddr(parts[1])

                // 创建 PeerInfo
                val peerInfo = PeerInfo(peerId, listOf(multiaddr))
                bootstrapPeers.add(peerInfo)
            } catch (e: Exception) {
                logger.error(e) { "Error parsing seed node: $seedNode" }
            }
        }

        // 从集群成员收集
        cluster.memberList.getMembers().forEach { member ->
            if (member.status == MemberStatus.ALIVE && member.id != cluster.actorSystem.address) {
                try {
                    val peerId = PeerId.fromBase58(member.host)
                    bootstrapPeers.add(PeerInfo(peerId))
                } catch (e: Exception) {
                    logger.error(e) { "Error parsing member host: ${member.host}" }
                }
            }
        }

        return bootstrapPeers
    }

    /**
     * 停止 DHT
     */
    fun stop() {
        dht.stop()

        // 取消所有未完成的激活请求
        pendingActivations.forEach { (_, deferred) ->
            if (!deferred.isCompleted) {
                deferred.completeExceptionally(Exception("DHT shutting down"))
            }
        }
        pendingActivations.clear()

        logger.info { "P2P DHT stopped" }
    }

    /**
     * 启动定期刷新循环
     */
    private fun startRefreshLoop() {
        scope.launch {
            while (true) {
                try {
                    // 刷新 DHT
                    dht.refresh()

                    // 随机化刷新间隔，避免所有节点同时刷新
                    val jitter = Random.nextLong(0, config.dhtRefreshInterval.toMillis() / 5)
                    val interval = config.dhtRefreshInterval.toMillis() + jitter

                    // 等待下一个刷新间隔
                    delay(interval)
                } catch (e: Exception) {
                    logger.error(e) { "Error refreshing DHT" }
                    delay(1000) // 出错后等待一段时间再重试
                }
            }
        }
    }

    /**
     * 启动定期清理循环
     */
    private fun startCleanupLoop() {
        scope.launch {
            while (true) {
                try {
                    // 清理过期的记录
                    cleanupExpiredRecords()

                    // 随机化清理间隔，避免所有节点同时清理
                    val jitter = Random.nextLong(0, config.dhtRefreshInterval.toMillis() / 5)
                    val interval = config.dhtRefreshInterval.toMillis() * 2 + jitter

                    // 等待下一个清理间隔
                    delay(interval)
                } catch (e: Exception) {
                    logger.error(e) { "Error cleaning up DHT" }
                    delay(1000) // 出错后等待一段时间再重试
                }
            }
        }
    }

    /**
     * 清理过期的记录
     */
    private suspend fun cleanupExpiredRecords() {
        // 获取所有记录
        val records = dht.recordStore.allRecords()

        // 当前时间
        val now = System.currentTimeMillis()

        // 过期时间，默认为 24 小时
        val expireTime = now - Duration.ofHours(24).toMillis()

        // 清理过期的记录
        var cleanedCount = 0
        records.forEach { record ->
            val key = String(record.key, StandardCharsets.UTF_8)

            // 检查是否是 Actor 记录
            if (key.startsWith("/protoactor/actor/")) {
                // 检查记录是否过期
                if (record.timeReceived < expireTime) {
                    // 删除过期记录
                    dht.removeRecord(key).get(config.dhtPutTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    cleanedCount++
                }
            }
        }

        if (cleanedCount > 0) {
            logger.info { "Cleaned up $cleanedCount expired DHT records" }
        }
    }

    /**
     * 查找 Actor
     */
    suspend fun lookup(clusterIdentity: ClusterIdentity): PID? {
        try {
            // 构造 DHT 键
            val key = "/protoactor/actor/${clusterIdentity.kind}/${clusterIdentity.identity}"

            // 从 DHT 查找
            val record = dht.getRecord(key).get(config.dhtLookupTimeout.toMillis(), TimeUnit.MILLISECONDS)

            if (record != null) {
                // 解析 PID
                val pid = parsePID(record.value)

                // 验证 PID 所在节点是否存活
                val member = cluster.memberList.getMember(pid.address)
                if (member != null && member.status == MemberStatus.ALIVE) {
                    return pid
                } else {
                    // 节点不存在或已离开，删除记录
                    logger.info { "Removing stale DHT record for $clusterIdentity, node ${pid.address} is not alive" }
                    dht.removeRecord(key).get(config.dhtPutTimeout.toMillis(), TimeUnit.MILLISECONDS)
                }
            }

            return null
        } catch (e: Exception) {
            logger.error(e) { "Error looking up actor in DHT: $clusterIdentity" }
            return null
        }
    }

    /**
     * 注册 Actor
     */
    suspend fun register(clusterIdentity: ClusterIdentity, pid: PID): Boolean {
        try {
            // 构造 DHT 键
            val key = "/protoactor/actor/${clusterIdentity.kind}/${clusterIdentity.identity}"

            // 序列化 PID
            val value = serializePID(pid)

            // 创建记录
            val record = Record(key.toByteArray(StandardCharsets.UTF_8), value)

            // 设置记录过期时间，默认 24 小时
            record.timeReceived = System.currentTimeMillis()
            record.ttl = Duration.ofHours(24).toMillis()

            // 存储到 DHT
            dht.putRecord(record).get(config.dhtPutTimeout.toMillis(), TimeUnit.MILLISECONDS)

            logger.debug { "Registered actor in DHT: $clusterIdentity -> $pid" }

            // 定期刷新记录，避免过期
            scheduleRecordRefresh(key, value)

            return true
        } catch (e: Exception) {
            logger.error(e) { "Error registering actor in DHT: $clusterIdentity" }
            return false
        }
    }

    /**
     * 定期刷新记录
     */
    private fun scheduleRecordRefresh(key: String, value: ByteArray) {
        scope.launch {
            try {
                // 等待一半过期时间
                delay(Duration.ofHours(12).toMillis())

                // 创建新记录
                val record = Record(key.toByteArray(StandardCharsets.UTF_8), value)
                record.timeReceived = System.currentTimeMillis()
                record.ttl = Duration.ofHours(24).toMillis()

                // 刷新记录
                dht.putRecord(record).get(config.dhtPutTimeout.toMillis(), TimeUnit.MILLISECONDS)

                logger.debug { "Refreshed DHT record: $key" }

                // 再次调度刷新
                scheduleRecordRefresh(key, value)
            } catch (e: Exception) {
                logger.error(e) { "Error refreshing DHT record: $key" }
            }
        }
    }

    /**
     * 注销 Actor
     */
    suspend fun unregister(clusterIdentity: ClusterIdentity): Boolean {
        try {
            // 构造 DHT 键
            val key = "/protoactor/actor/${clusterIdentity.kind}/${clusterIdentity.identity}"

            // 从 DHT 中删除
            dht.removeRecord(key).get(config.dhtPutTimeout.toMillis(), TimeUnit.MILLISECONDS)

            logger.debug { "Unregistered actor from DHT: $clusterIdentity" }

            return true
        } catch (e: Exception) {
            logger.error(e) { "Error unregistering actor from DHT: $clusterIdentity" }
            return false
        }
    }

    /**
     * 请求激活 Actor
     */
    suspend fun requestActivation(memberId: String, clusterIdentity: ClusterIdentity): PID {
        try {
            // 创建请求 ID
            val requestId = "${clusterIdentity.kind}/${clusterIdentity.identity}/${System.currentTimeMillis()}"

            // 创建 CompletableDeferred 用于等待响应
            val deferred = CompletableDeferred<PID>()
            pendingActivations[requestId] = deferred

            // 发送激活请求
            // 这里我们使用 DHT 来发布激活请求
            val key = "/protoactor/activation-request/$requestId"
            val value = "${clusterIdentity.kind}/${clusterIdentity.identity}/${cluster.actorSystem.address}".toByteArray(StandardCharsets.UTF_8)
            val record = Record(key.toByteArray(StandardCharsets.UTF_8), value)
            dht.putRecord(record).get(config.dhtPutTimeout.toMillis(), TimeUnit.MILLISECONDS)

            // 等待响应，设置超时
            return withTimeout(config.activationTimeout.toMillis()) {
                deferred.await()
            }
        } catch (e: Exception) {
            logger.error(e) { "Error requesting activation: $clusterIdentity" }
            throw e
        }
    }

    /**
     * 处理激活响应
     */
    fun handleActivationResponse(requestId: String, pid: PID) {
        val deferred = pendingActivations.remove(requestId)
        deferred?.complete(pid)
    }

    /**
     * 序列化 PID
     */
    private fun serializePID(pid: PID): ByteArray {
        // 简单的序列化实现
        val str = "${pid.address}/${pid.id}"
        return str.toByteArray(StandardCharsets.UTF_8)
    }

    /**
     * 解析 PID
     */
    private fun parsePID(bytes: ByteArray): PID {
        // 简单的反序列化实现
        val str = String(bytes, StandardCharsets.UTF_8)
        val parts = str.split("/", limit = 2)

        if (parts.size != 2) {
            throw IllegalArgumentException("Invalid PID format: $str")
        }

        val address = parts[0]
        val id = parts[1]

        return PID(address, id)
    }
}
