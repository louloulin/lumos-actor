package actor.proto.cluster.libp2p.security

import actor.proto.cluster.Cluster
import actor.proto.cluster.libp2p.P2PClusterConfig
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.MessageDigest
import java.security.PrivateKey
import java.security.PublicKey
import java.security.SecureRandom
import java.security.Signature
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import javax.crypto.Cipher

private val logger = KotlinLogging.logger {}

/**
 * ClusterSecurity 负责集群安全，包括节点身份验证和授权
 */
class ClusterSecurity(
    private val cluster: Cluster,
    private val config: P2PClusterConfig
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var isRunning = false
    
    // 密钥对
    private lateinit var keyPair: KeyPair
    
    // 已验证的节点
    private val verifiedNodes = ConcurrentHashMap<String, NodeCredential>()
    
    // 授权令牌
    private val authTokens = ConcurrentHashMap<String, AuthToken>()
    
    // 统计信息
    private val authRequests = AtomicInteger(0)
    private val authSuccess = AtomicInteger(0)
    private val authFailure = AtomicInteger(0)
    private val tokenRevocations = AtomicInteger(0)
    
    /**
     * 启动安全服务
     */
    fun start() {
        if (isRunning) return
        
        logger.info { "Starting cluster security service" }
        
        // 生成密钥对
        keyPair = generateKeyPair()
        
        isRunning = true
        
        // 启动令牌清理任务
        startTokenCleanupTask()
    }
    
    /**
     * 停止安全服务
     */
    fun stop() {
        if (!isRunning) return
        
        logger.info { "Stopping cluster security service" }
        
        isRunning = false
        
        // 清理状态
        verifiedNodes.clear()
        authTokens.clear()
    }
    
    /**
     * 启动令牌清理任务
     */
    private fun startTokenCleanupTask() {
        scope.launch {
            while (isRunning) {
                try {
                    // 清理过期的令牌
                    cleanupExpiredTokens()
                    
                    // 等待一段时间
                    kotlinx.coroutines.delay(60000) // 每分钟清理一次
                } catch (e: Exception) {
                    logger.error(e) { "Error cleaning up expired tokens" }
                    kotlinx.coroutines.delay(1000) // 出错后等待一段时间再重试
                }
            }
        }
    }
    
    /**
     * 清理过期的令牌
     */
    private fun cleanupExpiredTokens() {
        val now = Instant.now()
        val expiredTokens = mutableListOf<String>()
        
        // 找出过期的令牌
        authTokens.forEach { (tokenId, token) ->
            if (token.expiresAt.isBefore(now)) {
                expiredTokens.add(tokenId)
            }
        }
        
        // 移除过期的令牌
        expiredTokens.forEach { tokenId ->
            authTokens.remove(tokenId)
            tokenRevocations.incrementAndGet()
        }
        
        if (expiredTokens.isNotEmpty()) {
            logger.debug { "Cleaned up ${expiredTokens.size} expired tokens" }
        }
    }
    
    /**
     * 生成密钥对
     */
    private fun generateKeyPair(): KeyPair {
        val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
        keyPairGenerator.initialize(2048, SecureRandom())
        return keyPairGenerator.generateKeyPair()
    }
    
    /**
     * 获取公钥
     */
    fun getPublicKey(): PublicKey {
        return keyPair.public
    }
    
    /**
     * 获取公钥字符串
     */
    fun getPublicKeyString(): String {
        val publicKeyBytes = keyPair.public.encoded
        return Base64.getEncoder().encodeToString(publicKeyBytes)
    }
    
    /**
     * 验证节点
     */
    suspend fun verifyNode(nodeId: String, publicKeyString: String, challenge: String, signature: String): Boolean {
        try {
            authRequests.incrementAndGet()
            
            // 解析公钥
            val publicKeyBytes = Base64.getDecoder().decode(publicKeyString)
            val publicKey = java.security.KeyFactory.getInstance("RSA")
                .generatePublic(java.security.spec.X509EncodedKeySpec(publicKeyBytes))
            
            // 验证签名
            val signatureBytes = Base64.getDecoder().decode(signature)
            val verifier = Signature.getInstance("SHA256withRSA")
            verifier.initVerify(publicKey)
            verifier.update(challenge.toByteArray())
            
            val isValid = verifier.verify(signatureBytes)
            
            if (isValid) {
                // 创建节点凭证
                val credential = NodeCredential(
                    nodeId = nodeId,
                    publicKey = publicKey,
                    verifiedAt = Instant.now()
                )
                
                // 存储节点凭证
                verifiedNodes[nodeId] = credential
                
                authSuccess.incrementAndGet()
                logger.info { "Node verified: $nodeId" }
            } else {
                authFailure.incrementAndGet()
                logger.warn { "Node verification failed: $nodeId" }
            }
            
            return isValid
        } catch (e: Exception) {
            authFailure.incrementAndGet()
            logger.error(e) { "Error verifying node: $nodeId" }
            return false
        }
    }
    
    /**
     * 创建身份验证挑战
     */
    fun createAuthChallenge(): AuthChallenge {
        val challenge = UUID.randomUUID().toString()
        val timestamp = Instant.now()
        
        // 签名挑战
        val signature = signData(challenge)
        
        return AuthChallenge(
            challenge = challenge,
            signature = signature,
            timestamp = timestamp,
            publicKey = getPublicKeyString()
        )
    }
    
    /**
     * 签名数据
     */
    private fun signData(data: String): String {
        val signer = Signature.getInstance("SHA256withRSA")
        signer.initSign(keyPair.private)
        signer.update(data.toByteArray())
        val signatureBytes = signer.sign()
        return Base64.getEncoder().encodeToString(signatureBytes)
    }
    
    /**
     * 创建授权令牌
     */
    fun createAuthToken(nodeId: String): AuthToken? {
        // 检查节点是否已验证
        val credential = verifiedNodes[nodeId]
        if (credential == null) {
            logger.warn { "Cannot create auth token for unverified node: $nodeId" }
            return null
        }
        
        // 创建令牌
        val tokenId = UUID.randomUUID().toString()
        val issuedAt = Instant.now()
        val expiresAt = issuedAt.plus(Duration.ofHours(24))
        
        val token = AuthToken(
            tokenId = tokenId,
            nodeId = nodeId,
            issuedAt = issuedAt,
            expiresAt = expiresAt
        )
        
        // 存储令牌
        authTokens[tokenId] = token
        
        logger.info { "Created auth token for node: $nodeId" }
        
        return token
    }
    
    /**
     * 验证授权令牌
     */
    fun verifyAuthToken(tokenId: String, nodeId: String): Boolean {
        // 获取令牌
        val token = authTokens[tokenId]
        
        // 检查令牌是否存在
        if (token == null) {
            logger.warn { "Auth token not found: $tokenId" }
            return false
        }
        
        // 检查令牌是否属于该节点
        if (token.nodeId != nodeId) {
            logger.warn { "Auth token belongs to different node: $tokenId" }
            return false
        }
        
        // 检查令牌是否过期
        if (token.expiresAt.isBefore(Instant.now())) {
            logger.warn { "Auth token expired: $tokenId" }
            authTokens.remove(tokenId)
            return false
        }
        
        return true
    }
    
    /**
     * 吊销授权令牌
     */
    fun revokeAuthToken(tokenId: String): Boolean {
        val removed = authTokens.remove(tokenId) != null
        
        if (removed) {
            tokenRevocations.incrementAndGet()
            logger.info { "Revoked auth token: $tokenId" }
        }
        
        return removed
    }
    
    /**
     * 加密数据
     */
    fun encryptData(data: ByteArray, publicKey: PublicKey): ByteArray {
        val cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding")
        cipher.init(Cipher.ENCRYPT_MODE, publicKey)
        return cipher.doFinal(data)
    }
    
    /**
     * 解密数据
     */
    fun decryptData(encryptedData: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding")
        cipher.init(Cipher.DECRYPT_MODE, keyPair.private)
        return cipher.doFinal(encryptedData)
    }
    
    /**
     * 计算数据哈希
     */
    fun hashData(data: ByteArray): String {
        val digest = MessageDigest.getInstance("SHA-256")
        val hashBytes = digest.digest(data)
        return Base64.getEncoder().encodeToString(hashBytes)
    }
    
    /**
     * 获取统计信息
     */
    fun getStats(): SecurityStats {
        return SecurityStats(
            verifiedNodesCount = verifiedNodes.size,
            authTokensCount = authTokens.size,
            authRequests = authRequests.get(),
            authSuccess = authSuccess.get(),
            authFailure = authFailure.get(),
            tokenRevocations = tokenRevocations.get()
        )
    }
}

/**
 * 节点凭证
 */
data class NodeCredential(
    val nodeId: String,
    val publicKey: PublicKey,
    val verifiedAt: Instant
)

/**
 * 授权令牌
 */
data class AuthToken(
    val tokenId: String,
    val nodeId: String,
    val issuedAt: Instant,
    val expiresAt: Instant
)

/**
 * 身份验证挑战
 */
data class AuthChallenge(
    val challenge: String,
    val signature: String,
    val timestamp: Instant,
    val publicKey: String
)

/**
 * 安全统计信息
 */
data class SecurityStats(
    val verifiedNodesCount: Int,
    val authTokensCount: Int,
    val authRequests: Int,
    val authSuccess: Int,
    val authFailure: Int,
    val tokenRevocations: Int
)
