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

class SingleNodeClusterTest : FunSpec({

    test("Single node cluster should start and shutdown properly") {
        // 创建配置
        val config = SingleNodeConfig(
            clusterName = "test-cluster",
            nodeName = "test-node"
        )

        // 创建集群提供者
        val provider = SingleNodeCluster(config)

        // 创建一个模拟的集群
        val system = ActorSystem("test-system")

        // 启动集群成员
        val startResult = provider.startMember(system)
        startResult shouldBe true

        // 等待一段时间让集群稳定
        delay(500)

        // 验证成员列表不为空
        provider.members().isNotEmpty() shouldBe true

        // 验证领导者
        provider.leader() shouldNotBe null
        provider.isLeader() shouldBe true

        // 关闭集群
        val shutdownResult = provider.shutdown(true)
        shutdownResult shouldBe true
    }

    test("Single node cluster should emit cluster events") {
        // 创建配置
        val config = SingleNodeConfig(
            clusterName = "test-cluster",
            nodeName = "test-node"
        )

        // 创建集群提供者
        val provider = SingleNodeCluster(config)

        // 创建一个模拟的集群
        val system = ActorSystem("test-system")

        // 收集事件
        val events = mutableListOf<ClusterEvent>()
        val job = launch {
            provider.events().take(2).toList(events)
        }

        // 启动集群成员
        provider.startMember(system)

        // 等待一段时间让事件发送完成
        delay(500)

        // 验证事件
        events.isNotEmpty() shouldBe true
        events.any { it is ClusterEvent.MembershipChanged } shouldBe true
        events.any { it is ClusterEvent.LeaderElected } shouldBe true

        // 关闭集群
        provider.shutdown(true)
        job.cancel()
    }
})
