package actor.proto.cluster.libp2p

import actor.proto.cluster.Cluster
import actor.proto.cluster.Member
import actor.proto.cluster.MemberStatus
import io.libp2p.core.Host
import io.libp2p.core.PeerId
import io.libp2p.protocol.Ping
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.random.Random

private val logger = KotlinLogging.logger {}

/**
 * P2PFailureDetector 负责检测节点故障并触发恢复
 */
class P2PFailureDetector(
    private val cluster: Cluster,
    private val host: Host,
    private val config: P2PClusterConfig
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val lastHeartbeats = ConcurrentHashMap<String, Long>()
    private val suspectedMembers = ConcurrentHashMap<String, Long>()
    private val ping = Ping()
    private var isRunning = false
    
    /**
     * 启动故障检测器
     */
    fun start() {
        if (isRunning) return
        
        isRunning = true
        
        // 添加 Ping 协议
        host.addProtocolHandler(ping)
        
        // 启动监控任务
        startMonitoringTask()
        
        logger.info { "P2P failure detector started" }
    }
    
    /**
     * 停止故障检测器
     */
    fun stop() {
        if (!isRunning) return
        
        isRunning = false
        
        // 移除 Ping 协议
        host.removeProtocolHandler(ping)
        
        // 清理状态
        lastHeartbeats.clear()
        suspectedMembers.clear()
        
        logger.info { "P2P failure detector stopped" }
    }
    
    /**
     * 记录心跳
     */
    fun recordHeartbeat(memberId: String) {
        lastHeartbeats[memberId] = System.currentTimeMillis()
        
        // 如果节点之前被怀疑故障，现在恢复了
        if (suspectedMembers.containsKey(memberId)) {
            suspectedMembers.remove(memberId)
            logger.info { "Member recovered: $memberId" }
            
            // 更新成员状态为 ALIVE
            cluster.memberList.updateMemberStatus(memberId, MemberStatus.ALIVE)
        }
    }
    
    /**
     * 启动监控任务
     */
    private fun startMonitoringTask() {
        scope.launch {
            while (isRunning) {
                try {
                    // 检查所有成员
                    checkMembers()
                    
                    // 随机化监控间隔，避免所有节点同时检查
                    val jitter = Random.nextLong(0, config.monitorInterval.toMillis() / 5)
                    val interval = config.monitorInterval.toMillis() + jitter
                    
                    // 等待下一个监控间隔
                    delay(interval)
                } catch (e: Exception) {
                    logger.error(e) { "Error in monitoring task" }
                    delay(1000) // 出错后等待一段时间再重试
                }
            }
        }
    }
    
    /**
     * 检查所有成员
     */
    private suspend fun checkMembers() {
        val now = System.currentTimeMillis()
        val heartbeatTimeout = config.heartbeatInterval.toMillis() * 3 // 3个心跳周期
        
        // 获取所有活跃成员
        val members = cluster.memberList.getMembers()
            .filter { it.status == MemberStatus.ALIVE && it.id != cluster.actorSystem.address }
        
        for (member in members) {
            val memberId = member.id
            
            // 检查最后一次心跳时间
            val lastHeartbeat = lastHeartbeats[memberId] ?: 0
            
            if (now - lastHeartbeat > heartbeatTimeout) {
                // 如果超过心跳超时时间，尝试 ping
                if (pingMember(member)) {
                    // Ping 成功，更新心跳时间
                    recordHeartbeat(memberId)
                } else {
                    // Ping 失败，标记为可疑
                    markAsSuspected(memberId, now)
                }
            }
        }
        
        // 检查可疑成员
        checkSuspectedMembers(now)
    }
    
    /**
     * Ping 成员
     */
    private suspend fun pingMember(member: Member): Boolean {
        try {
            // 获取 PeerId
            val peerId = PeerId.fromBase58(member.host)
            
            // 尝试 ping
            val result = ping.ping(host, peerId).get(5, TimeUnit.SECONDS)
            
            // 如果 ping 成功，返回 true
            return result != null
        } catch (e: Exception) {
            logger.debug { "Failed to ping member ${member.id}: ${e.message}" }
            return false
        }
    }
    
    /**
     * 标记成员为可疑
     */
    private fun markAsSuspected(memberId: String, now: Long) {
        // 如果成员已经被标记为可疑，不做任何事
        if (suspectedMembers.containsKey(memberId)) {
            return
        }
        
        // 标记为可疑
        suspectedMembers[memberId] = now
        logger.info { "Member suspected: $memberId" }
        
        // 更新成员状态为 UNAVAILABLE
        cluster.memberList.updateMemberStatus(memberId, MemberStatus.UNAVAILABLE)
    }
    
    /**
     * 检查可疑成员
     */
    private fun checkSuspectedMembers(now: Long) {
        val deadTimeout = config.monitorInterval.toMillis() * 3 // 3个监控周期
        
        // 找出长时间可疑的成员
        val deadMembers = suspectedMembers.entries
            .filter { now - it.value > deadTimeout }
            .map { it.key }
        
        // 标记为死亡
        for (memberId in deadMembers) {
            suspectedMembers.remove(memberId)
            lastHeartbeats.remove(memberId)
            
            logger.info { "Member dead: $memberId" }
            
            // 更新成员状态为 DEAD
            cluster.memberList.updateMemberStatus(memberId, MemberStatus.DEAD)
        }
    }
}
