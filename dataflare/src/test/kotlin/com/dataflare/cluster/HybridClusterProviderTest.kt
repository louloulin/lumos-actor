package com.dataflare.cluster

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import actor.proto.ActorSystem

class HybridClusterProviderTest : FunSpec({

    test("Hybrid cluster provider should start and shutdown properly") {
        // 创建P2P配置
        val p2pConfig = P2PClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            discoveryMethod = DiscoveryMethod.STATIC,
            seedNodes = listOf("localhost:5000")
        )

        // 创建中心化配置
        val centralizedConfig = CentralizedClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            coordinatorNodes = listOf("localhost:5000"),
            electionStrategy = ElectionStrategy.STATIC
        )

        // 创建混合配置
        val hybridConfig = HybridClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            regionName = "test-region",
            p2pConfig = p2pConfig,
            centralizedConfig = centralizedConfig,
            interRegionCommunication = true
        )

        // 创建集群提供者
        val p2pProvider = P2PClusterProvider(p2pConfig)
        val centralizedProvider = CentralizedClusterProvider(centralizedConfig)
        val hybridProvider = HybridClusterProvider(hybridConfig, p2pProvider, centralizedProvider)

        // 创建一个模拟的集群
        val cluster = ActorSystem("test-system")

        // 启动集群成员
        val startResult = hybridProvider.startMember(cluster)
        startResult shouldBe true

        // 等待一段时间让集群稳定
        delay(1000)

        // 验证成员列表不为空
        hybridProvider.members().isNotEmpty() shouldBe true

        // 关闭集群
        val shutdownResult = hybridProvider.shutdown(true)
        shutdownResult shouldBe true
    }

    test("Hybrid cluster provider should emit cluster events") {
        // 创建P2P配置
        val p2pConfig = P2PClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            discoveryMethod = DiscoveryMethod.STATIC,
            seedNodes = listOf("localhost:5000")
        )

        // 创建中心化配置
        val centralizedConfig = CentralizedClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            coordinatorNodes = listOf("localhost:5000"),
            electionStrategy = ElectionStrategy.STATIC
        )

        // 创建混合配置
        val hybridConfig = HybridClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            regionName = "test-region",
            p2pConfig = p2pConfig,
            centralizedConfig = centralizedConfig,
            interRegionCommunication = true
        )

        // 创建集群提供者
        val p2pProvider = P2PClusterProvider(p2pConfig)
        val centralizedProvider = CentralizedClusterProvider(centralizedConfig)
        val hybridProvider = HybridClusterProvider(hybridConfig, p2pProvider, centralizedProvider)

        // 创建一个模拟的集群
        val cluster = ActorSystem("test-system")

        // 收集事件
        val events = mutableListOf<ClusterEvent>()
        val job = launch {
            hybridProvider.events().take(3).toList(events)
        }

        // 启动集群成员
        hybridProvider.startMember(cluster)

        // 等待事件收集完成
        withTimeout(5000) {
            while (events.size < 3) {
                delay(100)
            }
        }

        // 验证事件
        events.isNotEmpty() shouldBe true

        // 关闭集群
        hybridProvider.shutdown(true)
        job.cancel()
    }

    test("Hybrid cluster provider should handle inter-region communication") {
        // 创建P2P配置
        val p2pConfig = P2PClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            discoveryMethod = DiscoveryMethod.STATIC,
            seedNodes = listOf("localhost:5000")
        )

        // 创建中心化配置
        val centralizedConfig = CentralizedClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            coordinatorNodes = listOf("localhost:5000"),
            electionStrategy = ElectionStrategy.STATIC
        )

        // 创建混合配置
        val hybridConfig = HybridClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            regionName = "region-1",
            p2pConfig = p2pConfig,
            centralizedConfig = centralizedConfig,
            interRegionCommunication = true
        )

        // 创建集群提供者
        val p2pProvider = P2PClusterProvider(p2pConfig)
        val centralizedProvider = CentralizedClusterProvider(centralizedConfig)
        val hybridProvider = HybridClusterProvider(hybridConfig, p2pProvider, centralizedProvider)

        // 创建一个模拟的集群
        val cluster = ActorSystem("test-system")

        // 启动集群成员
        hybridProvider.startMember(cluster)

        // 等待一段时间让集群稳定
        delay(2000)

        // 验证区域领导者
        hybridProvider.leader() shouldNotBe null

        // 关闭集群
        hybridProvider.shutdown(true)
    }
})
