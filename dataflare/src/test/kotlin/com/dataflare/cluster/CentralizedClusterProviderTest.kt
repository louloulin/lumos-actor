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

class CentralizedClusterProviderTest : FunSpec({

    test("Centralized cluster provider should start and shutdown properly") {
        // 创建配置
        val config = CentralizedClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            coordinatorNodes = listOf("localhost:5000"),
            electionStrategy = ElectionStrategy.STATIC
        )

        // 创建集群提供者
        val provider = CentralizedClusterProvider(config)

        // 创建一个模拟的集群
        val cluster = ActorSystem("test-system")

        // 启动集群成员
        val startResult = provider.startMember(cluster)
        startResult shouldBe true

        // 等待一段时间让集群稳定
        delay(1000)

        // 验证成员列表不为空
        provider.members().isNotEmpty() shouldBe true

        // 验证领导者选举
        delay(2000) // 给一些时间让领导者选举完成
        provider.leader() shouldNotBe null

        // 关闭集群
        val shutdownResult = provider.shutdown(true)
        shutdownResult shouldBe true
    }

    test("Centralized cluster provider should emit cluster events") {
        // 创建配置
        val config = CentralizedClusterConfig(
            clusterName = "test-cluster",
            nodeName = "test-node",
            coordinatorNodes = listOf("localhost:5000"),
            electionStrategy = ElectionStrategy.STATIC
        )

        // 创建集群提供者
        val provider = CentralizedClusterProvider(config)

        // 创建一个模拟的集群
        val cluster = ActorSystem("test-system")

        // 收集事件
        val events = mutableListOf<ClusterEvent>()
        val job = launch {
            provider.events().take(3).toList(events)
        }

        // 启动集群成员
        provider.startMember(cluster)

        // 等待事件收集完成
        withTimeout(5000) {
            while (events.size < 3) {
                delay(100)
            }
        }

        // 验证事件
        events.isNotEmpty() shouldBe true
        events.any { it is ClusterEvent.MembershipChanged } shouldBe true
        events.any { it is ClusterEvent.LeaderElected } shouldBe true

        // 关闭集群
        provider.shutdown(true)
        job.cancel()
    }
})
