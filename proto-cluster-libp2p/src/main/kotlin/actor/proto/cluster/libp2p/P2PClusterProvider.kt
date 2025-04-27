package actor.proto.cluster.libp2p

import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterProvider
import io.libp2p.core.Host
import io.libp2p.core.dsl.host
import io.libp2p.core.multiformats.Multiaddr
import io.libp2p.core.multistream.ProtocolBinding
import io.libp2p.crypto.keys.generateEd25519KeyPair
import io.libp2p.protocol.Ping
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * P2PClusterProvider 是基于 libp2p 的集群提供者实现
 */
class P2PClusterProvider(
    private val config: P2PClusterConfig
) : ClusterProvider {
    private lateinit var cluster: Cluster
    private lateinit var libp2pHost: Host
    private lateinit var discovery: P2PDiscovery
    private lateinit var gossiper: P2PGossiper
    private lateinit var identityLookup: P2PIdentityLookup
    
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var isRunning = false
    
    override suspend fun startMember(cluster: Cluster): Boolean {
        this.cluster = cluster
        
        logger.info { "Starting P2P cluster member at ${cluster.actorSystem.address}" }
        
        // 初始化 libp2p 主机
        libp2pHost = createLibp2pHost()
        libp2pHost.start().get()
        
        logger.info { "P2P node started with ID: ${libp2pHost.peerId.toBase58()}" }
        logger.info { "Listening on: ${libp2pHost.listenAddresses.joinToString()}" }
        
        // 初始化发现服务
        discovery = P2PDiscovery(libp2pHost, config, cluster)
        discovery.start()
        
        // 初始化 gossip 服务
        gossiper = P2PGossiper(cluster, libp2pHost, config)
        gossiper.start()
        
        // 初始化身份查找服务
        identityLookup = P2PIdentityLookup(cluster, libp2pHost)
        
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
        
        // 初始化 libp2p 主机
        libp2pHost = createLibp2pHost()
        libp2pHost.start().get()
        
        logger.info { "P2P node started with ID: ${libp2pHost.peerId.toBase58()}" }
        logger.info { "Listening on: ${libp2pHost.listenAddresses.joinToString()}" }
        
        // 初始化发现服务
        discovery = P2PDiscovery(libp2pHost, config, cluster)
        discovery.start()
        
        // 初始化 gossip 服务
        gossiper = P2PGossiper(cluster, libp2pHost, config)
        gossiper.start()
        
        // 初始化身份查找服务
        identityLookup = P2PIdentityLookup(cluster, libp2pHost)
        
        isRunning = true
        
        return true
    }
    
    override suspend fun shutdown(graceful: Boolean): Boolean {
        if (!isRunning) return true
        
        logger.info { "Shutting down P2P cluster provider" }
        
        if (graceful) {
            // 通知其他节点自己将要离开
            gossiper.publishGracefulLeave()
            delay(1000) // 给一些时间让消息传播
        }
        
        // 关闭服务
        discovery.stop()
        gossiper.stop()
        
        // 关闭 libp2p 主机
        val shutdownComplete = CompletableDeferred<Boolean>()
        libp2pHost.stop()
            .thenAccept { shutdownComplete.complete(true) }
            .exceptionally {
                logger.error(it) { "Error shutting down libp2p host" }
                shutdownComplete.complete(false)
                null
            }
        
        isRunning = false
        
        return shutdownComplete.await()
    }
    
    /**
     * 创建 libp2p 主机
     */
    private fun createLibp2pHost(): Host {
        return host {
            identity {
                // 生成随机的 Ed25519 密钥对
                val keyPair = generateEd25519KeyPair()
                privateKey = keyPair.first
                publicKey = keyPair.second
            }
            
            network {
                // 配置监听地址
                listen("/ip4/${config.listenAddress}/tcp/${config.listenPort}")
            }
            
            protocols {
                // 添加 ping 协议用于测试连接
                add(Ping())
                
                // 添加自定义协议
                add(P2PClusterProtocol(this@P2PClusterProvider) as ProtocolBinding<*>)
            }
            
            // 如果启用了 mDNS 发现
            if (config.enableMDns) {
                mdns { }
            }
        }
    }
    
    /**
     * 注册集群成员
     */
    private fun registerMember() {
        // 将自己注册为集群成员
        val member = createMemberInfo()
        gossiper.publishMemberUp(member)
    }
    
    /**
     * 创建成员信息
     */
    private fun createMemberInfo(): MemberInfo {
        return MemberInfo(
            id = cluster.actorSystem.address,
            host = libp2pHost.peerId.toBase58(),
            port = 0, // 不使用端口，使用 libp2p 连接
            kinds = cluster.getClusterKinds()
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
                    gossiper.publishHeartbeat(member)
                    
                    // 等待下一个心跳间隔
                    delay(config.heartbeatInterval.toMillis())
                } catch (e: Exception) {
                    logger.error(e) { "Error in heartbeat" }
                }
            }
        }
    }
    
    /**
     * 获取 libp2p 主机
     */
    fun getHost(): Host = libp2pHost
    
    /**
     * 获取 gossiper
     */
    fun getGossiper(): P2PGossiper = gossiper
    
    /**
     * 获取 discovery
     */
    fun getDiscovery(): P2PDiscovery = discovery
    
    /**
     * 获取 identityLookup
     */
    fun getIdentityLookup(): P2PIdentityLookup = identityLookup
}
