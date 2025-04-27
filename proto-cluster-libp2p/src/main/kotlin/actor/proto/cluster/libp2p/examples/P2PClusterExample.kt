package actor.proto.cluster.libp2p.examples

import actor.proto.ActorSystem
import actor.proto.PID
import actor.proto.Props
import actor.proto.cluster.Cluster
import actor.proto.cluster.ClusterConfig
import actor.proto.cluster.ClusterKind
import actor.proto.cluster.libp2p.P2PClusterConfig
import actor.proto.cluster.libp2p.P2PClusterProvider
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.util.Scanner
import kotlin.system.exitProcess

/**
 * 示例 Actor
 */
class GreetingActor : actor.proto.Actor {
    override suspend fun receive(context: actor.proto.Context) {
        val msg = context.message
        if (msg is String) {
            println("GreetingActor received: $msg")
            context.respond("Hello from GreetingActor: $msg")
        }
    }
}

/**
 * P2P 集群示例应用程序
 */
object P2PClusterExample {
    
    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        // 解析命令行参数
        if (args.size < 2) {
            println("Usage: P2PClusterExample <node-id> <port> [seed-node]")
            exitProcess(1)
        }
        
        val nodeId = args[0]
        val port = args[1].toInt()
        val seedNode = if (args.size > 2) args[2] else null
        
        println("Starting P2P cluster node: $nodeId on port $port")
        if (seedNode != null) {
            println("Using seed node: $seedNode")
        }
        
        // 创建 Actor 系统
        val system = ActorSystem(nodeId)
        
        // 创建 P2P 集群配置
        val p2pConfig = P2PClusterConfig(
            clusterName = "example-cluster",
            enableMDns = true,
            listenPort = port,
            seedNodes = if (seedNode != null) listOf(seedNode) else emptyList()
        )
        
        // 创建集群提供者
        val clusterProvider = P2PClusterProvider(p2pConfig)
        
        // 创建集群配置
        val clusterConfig = ClusterConfig(
            clusterName = "example-cluster",
            clusterProvider = clusterProvider
        )
        
        // 创建集群
        val cluster = Cluster(system, clusterConfig)
        
        // 注册 Actor 类型
        val greetingProps = Props.fromProducer { GreetingActor() }
        cluster.registerKind("greeting", ClusterKind.fromProps("greeting", greetingProps))
        
        // 启动集群
        cluster.startMember()
        
        // 等待集群形成
        delay(5000)
        
        // 显示集群成员
        val members = cluster.memberList.getMembers()
        println("Cluster members: $members")
        
        // 命令行交互
        val scanner = Scanner(System.`in`)
        println("\nCommands:")
        println("  get <id> - Get a virtual actor")
        println("  send <id> <message> - Send a message to a virtual actor")
        println("  members - List cluster members")
        println("  exit - Exit the application")
        
        var running = true
        while (running) {
            print("> ")
            val line = scanner.nextLine()
            val parts = line.split(" ")
            
            when (parts[0]) {
                "get" -> {
                    if (parts.size < 2) {
                        println("Usage: get <id>")
                        continue
                    }
                    
                    val id = parts[1]
                    val pid = cluster.get(id, "greeting")
                    println("Got PID: $pid")
                }
                "send" -> {
                    if (parts.size < 3) {
                        println("Usage: send <id> <message>")
                        continue
                    }
                    
                    val id = parts[1]
                    val message = parts.subList(2, parts.size).joinToString(" ")
                    
                    val pid = cluster.get(id, "greeting")
                    val response = system.root.requestAwait<String>(pid, message, Duration.ofSeconds(5))
                    println("Response: $response")
                }
                "members" -> {
                    val members = cluster.memberList.getMembers()
                    println("Cluster members: $members")
                }
                "exit" -> {
                    running = false
                }
                else -> {
                    println("Unknown command: ${parts[0]}")
                }
            }
        }
        
        // 关闭集群和 Actor 系统
        cluster.shutdown(true)
        system.shutdown()
    }
}
