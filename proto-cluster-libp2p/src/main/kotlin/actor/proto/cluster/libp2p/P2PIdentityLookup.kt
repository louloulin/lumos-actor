package actor.proto.cluster.libp2p

import actor.proto.PID
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterIdentity
import actor.proto.cluster.IdentityLookup
import io.libp2p.core.Host
import io.libp2p.core.PeerId
import io.libp2p.core.multiformats.Multiaddr
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * P2PIdentityLookup 负责虚拟 Actor 定位
 */
class P2PIdentityLookup(
    private val cluster: Cluster,
    private val host: Host
) : IdentityLookup {
    private var isClient = false
    private val activationRequests = ConcurrentHashMap<String, CompletableDeferred<PID>>()
    
    override suspend fun setup(cluster: Cluster, kinds: List<String>, isClient: Boolean) {
        this.isClient = isClient
        logger.info { "Setting up P2PIdentityLookup as ${if (isClient) "client" else "member"}" }
    }
    
    override suspend fun lookup(clusterIdentity: ClusterIdentity): PID {
        // 检查是否在缓存中
        val cachedPid = cluster.pidCache.get(clusterIdentity)
        if (cachedPid != null) {
            return cachedPid
        }
        
        // 确定应该托管 Actor 的节点
        val memberId = cluster.memberList.getPartitionMember(clusterIdentity)
            ?: throw Exception("No member available for kind ${clusterIdentity.kind}")
            
        // 如果是本地节点，激活 Actor
        if (memberId == cluster.actorSystem.address && !isClient) {
            return activateLocally(clusterIdentity)
        }
        
        // 否则请求远程激活
        return requestActivation(memberId, clusterIdentity)
    }
    
    override suspend fun shutdown() {
        // 取消所有未完成的激活请求
        activationRequests.forEach { (_, deferred) ->
            if (!deferred.isCompleted) {
                deferred.completeExceptionally(Exception("Identity lookup shutting down"))
            }
        }
        activationRequests.clear()
    }
    
    /**
     * 在本地激活 Actor
     */
    private suspend fun activateLocally(clusterIdentity: ClusterIdentity): PID {
        logger.debug { "Activating actor locally: $clusterIdentity" }
        
        // 获取 kind 的激活器
        val kind = cluster.getClusterKind(clusterIdentity.kind)
            ?: throw Exception("No cluster kind found for ${clusterIdentity.kind}")
            
        // 激活 Actor
        val pid = kind.spawn(clusterIdentity.identity, cluster)
        
        // 添加到缓存
        cluster.pidCache.add(clusterIdentity, pid)
        
        // 通知其他节点
        val clusterIdentityStr = "${clusterIdentity.kind}/${clusterIdentity.identity}"
        cluster.gossip.SetState("actor-activation:$clusterIdentityStr", pid.id)
        
        // 如果有 gossiper，发布激活消息
        if (cluster is P2PClusterProvider) {
            val gossiper = cluster.getGossiper()
            gossiper.publishActorActivation(clusterIdentityStr, pid.id)
        }
        
        return pid
    }
    
    /**
     * 请求远程激活 Actor
     */
    private suspend fun requestActivation(memberId: String, clusterIdentity: ClusterIdentity): PID {
        logger.debug { "Requesting actor activation from $memberId: $clusterIdentity" }
        
        // 创建请求 ID
        val requestId = "${clusterIdentity.kind}/${clusterIdentity.identity}/${System.currentTimeMillis()}"
        
        // 创建 CompletableDeferred 用于等待响应
        val deferred = CompletableDeferred<PID>()
        activationRequests[requestId] = deferred
        
        try {
            // 发送激活请求
            // 这里我们使用 gossip 消息来请求激活
            // 在实际实现中，可能需要使用更直接的通信方式
            
            // 等待响应，设置超时
            return withTimeout(Duration.ofSeconds(5).toMillis()) {
                deferred.await()
            }
        } catch (e: TimeoutCancellationException) {
            activationRequests.remove(requestId)
            throw Exception("Activation request timed out for $clusterIdentity", e)
        } catch (e: Exception) {
            activationRequests.remove(requestId)
            throw Exception("Error requesting activation for $clusterIdentity", e)
        }
    }
    
    /**
     * 处理激活响应
     */
    fun handleActivationResponse(requestId: String, pid: PID) {
        val deferred = activationRequests.remove(requestId)
        deferred?.complete(pid)
    }
}
