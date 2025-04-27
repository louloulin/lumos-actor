package actor.proto.cluster.libp2p.security

import actor.proto.ActorSystem
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.libp2p.P2PClusterConfig
import actor.proto.cluster.libp2p.P2PClusterProvider
import actor.proto.cluster.libp2p.P2PIdentityLookup
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.Signature
import java.security.spec.X509EncodedKeySpec
import java.time.Duration
import java.util.Base64
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ClusterSecurityTest {
    
    private lateinit var system: ActorSystem
    private lateinit var cluster: Cluster
    private lateinit var security: ClusterSecurity
    private lateinit var clientKeyPair: KeyPair
    
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
        
        // 创建安全服务
        security = ClusterSecurity(cluster, p2pConfig)
        
        // 启动安全服务
        security.start()
        
        // 创建客户端密钥对
        val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
        keyPairGenerator.initialize(2048)
        clientKeyPair = keyPairGenerator.generateKeyPair()
    }
    
    @AfterEach
    fun tearDown() = runBlocking {
        // 停止安全服务
        security.stop()
        
        // 关闭集群和 Actor 系统
        if (::cluster.isInitialized) {
            cluster.shutdown(true)
        }
        
        system.shutdown()
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should create and verify auth challenge`() {
        // 创建身份验证挑战
        val challenge = security.createAuthChallenge()
        
        // 验证挑战
        assertNotNull(challenge.challenge, "Challenge should not be null")
        assertNotNull(challenge.signature, "Signature should not be null")
        assertNotNull(challenge.timestamp, "Timestamp should not be null")
        assertNotNull(challenge.publicKey, "Public key should not be null")
        
        // 解析公钥
        val publicKeyBytes = Base64.getDecoder().decode(challenge.publicKey)
        val publicKey = java.security.KeyFactory.getInstance("RSA")
            .generatePublic(X509EncodedKeySpec(publicKeyBytes))
        
        // 验证签名
        val signatureBytes = Base64.getDecoder().decode(challenge.signature)
        val verifier = Signature.getInstance("SHA256withRSA")
        verifier.initVerify(publicKey)
        verifier.update(challenge.challenge.toByteArray())
        
        assertTrue(verifier.verify(signatureBytes), "Signature should be valid")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should verify node`() = runBlocking {
        // 创建身份验证挑战
        val challenge = security.createAuthChallenge()
        
        // 签名挑战
        val signer = Signature.getInstance("SHA256withRSA")
        signer.initSign(clientKeyPair.private)
        signer.update(challenge.challenge.toByteArray())
        val signatureBytes = signer.sign()
        val signature = Base64.getEncoder().encodeToString(signatureBytes)
        
        // 获取公钥字符串
        val publicKeyBytes = clientKeyPair.public.encoded
        val publicKeyString = Base64.getEncoder().encodeToString(publicKeyBytes)
        
        // 验证节点
        val result = security.verifyNode("test-node", publicKeyString, challenge.challenge, signature)
        
        assertTrue(result, "Node verification should succeed")
        
        // 验证统计信息
        val stats = security.getStats()
        assertEquals(1, stats.authRequests, "Auth requests count should be 1")
        assertEquals(1, stats.authSuccess, "Auth success count should be 1")
        assertEquals(0, stats.authFailure, "Auth failure count should be 0")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should create and verify auth token`() = runBlocking {
        // 验证节点
        val challenge = security.createAuthChallenge()
        val signer = Signature.getInstance("SHA256withRSA")
        signer.initSign(clientKeyPair.private)
        signer.update(challenge.challenge.toByteArray())
        val signatureBytes = signer.sign()
        val signature = Base64.getEncoder().encodeToString(signatureBytes)
        val publicKeyString = Base64.getEncoder().encodeToString(clientKeyPair.public.encoded)
        
        security.verifyNode("test-node", publicKeyString, challenge.challenge, signature)
        
        // 创建授权令牌
        val token = security.createAuthToken("test-node")
        
        assertNotNull(token, "Token should not be null")
        assertEquals("test-node", token.nodeId, "Token should be for the correct node")
        
        // 验证授权令牌
        val isValid = security.verifyAuthToken(token.tokenId, "test-node")
        
        assertTrue(isValid, "Token should be valid")
        
        // 吊销授权令牌
        val isRevoked = security.revokeAuthToken(token.tokenId)
        
        assertTrue(isRevoked, "Token should be revoked")
        
        // 验证已吊销的令牌
        val isStillValid = security.verifyAuthToken(token.tokenId, "test-node")
        
        assertFalse(isStillValid, "Revoked token should not be valid")
        
        // 验证统计信息
        val stats = security.getStats()
        assertEquals(1, stats.tokenRevocations, "Token revocations count should be 1")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should encrypt and decrypt data`() {
        // 准备测试数据
        val testData = "Hello, World!".toByteArray()
        
        // 加密数据
        val encryptedData = security.encryptData(testData, security.getPublicKey())
        
        // 解密数据
        val decryptedData = security.decryptData(encryptedData)
        
        // 验证结果
        assertEquals(String(testData), String(decryptedData), "Decrypted data should match original")
    }
    
    @Test
    @Timeout(30) // 30 秒超时
    fun `should hash data`() {
        // 准备测试数据
        val testData = "Hello, World!".toByteArray()
        
        // 计算哈希
        val hash = security.hashData(testData)
        
        // 验证哈希不为空
        assertNotNull(hash, "Hash should not be null")
        assertTrue(hash.isNotEmpty(), "Hash should not be empty")
    }
}
