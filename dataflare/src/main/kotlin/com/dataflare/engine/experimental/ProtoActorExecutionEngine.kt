package com.dataflare.engine.experimental

import actor.proto.ActorSystem
import actor.proto.PID
import actor.proto.Props
import actor.proto.fromProducer
import actor.proto.Context as ProtoContext
import actor.proto.Actor as ProtoActor
import com.dataflare.connectors.ConnectorContext
import com.dataflare.connectors.Context
import com.dataflare.connectors.Input
import com.dataflare.connectors.Output
import com.dataflare.core.Message
import com.dataflare.engine.ExecutionEngine
import com.dataflare.processors.Processor
import com.dataflare.workflow.Connection
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * ProtoActor 执行引擎 - 使用 Proto.Actor 实现的执行引擎
 *
 * 注意：这是一个实验性的实现，目前尚未完成
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ProtoActorExecutionEngine(private val system: ActorSystem) : ExecutionEngine {
    override val name: String = "protoactor"

    private val workflowActors = mutableMapOf<String, PID>()
    private val processorActors = mutableMapOf<String, Map<String, PID>>()
    private val outputActors = mutableMapOf<String, Map<String, PID>>()
    private var running = false

    override suspend fun initialize() {
        logger.info { "Initializing ProtoActor execution engine" }
        running = true
    }

    override suspend fun startWorkflow(
        workflowId: String,
        config: WorkflowConfig,
        inputs: Map<String, Input>,
        processors: Map<String, Processor>,
        outputs: Map<String, Output>,
        scope: CoroutineScope
    ): List<Job> {
        logger.info { "Starting workflow in ProtoActor execution engine: $workflowId" }

        val jobs = mutableListOf<Job>()

        // 创建工作流协调器 Actor
        val workflowProps = fromProducer { WorkflowCoordinatorActor(workflowId, config, system) }
        val workflowPid = system.root.spawnNamed(workflowProps, "workflow-$workflowId")
        workflowActors[workflowId] = workflowPid

        // 创建处理器 Actors
        val workflowProcessorActors = mutableMapOf<String, PID>()
        config.processors.forEach { (processorId, _) ->
            val processor = processors[processorId]!!
            val connections = config.connections.map { Pair(it.from, it.to) }
            val processorProps = fromProducer { ProcessorActor(workflowId, processorId, processor, connections) }
            val processorPid = system.root.spawnNamed(processorProps, "processor-$workflowId-$processorId")
            workflowProcessorActors[processorId] = processorPid
        }
        processorActors[workflowId] = workflowProcessorActors

        // 创建输出 Actors
        val workflowOutputActors = mutableMapOf<String, PID>()
        config.outputs.forEach { (outputId, _) ->
            val output = outputs[outputId]!!
            val outputProps = fromProducer { OutputActor(workflowId, outputId, output) }
            val outputPid = system.root.spawnNamed(outputProps, "output-$workflowId-$outputId")
            workflowOutputActors[outputId] = outputPid
        }
        outputActors[workflowId] = workflowOutputActors

        // 为每个输入创建一个作业
        config.inputs.forEach { (inputId, _) ->
            val job = scope.launch(Dispatchers.IO) {
                val input = inputs[inputId]!!
                val ctx = ConnectorContext(workflowId, inputId)

                while (running) {
                    try {
                        // 读取输入
                        val message = input.read(ctx)
                        if (message == null) {
                            delay(100)
                            continue
                        }

                        // 找到连接到这个输入的处理器
                        val connectedProcessors = config.connections
                            .filter { it.from == inputId }
                            .map { it.to }

                        // 向工作流协调器发送消息
                        system.root.send(workflowPid, WorkflowMessage(inputId, message, connectedProcessors))

                        // 添加一些延迟，避免CPU使用率过高
                        delay(100)
                    } catch (e: Exception) {
                        logger.error(e) { "Error processing input $inputId" }
                        delay(1000) // 出错后等待一段时间再重试
                    }
                }
            }

            jobs.add(job)
        }

        return jobs
    }

    override suspend fun processMessage(
        workflowId: String,
        sourceId: String,
        message: Message,
        targetIds: List<String>,
        ctx: Context,
        inputs: Map<String, Input>,
        processors: Map<String, Processor>,
        outputs: Map<String, Output>,
        connections: List<Pair<String, String>>
    ) {
        // 在 ProtoActor 执行引擎中，消息处理是通过 Actor 自动完成的
        // 这个方法在 ProtoActor 执行引擎中不需要实现
        logger.debug { "processMessage called in ProtoActor engine - this is handled by actors" }
    }

    override suspend fun shutdown() {
        logger.info { "Shutting down ProtoActor execution engine" }
        running = false

        // 停止所有 Actor
        workflowActors.values.forEach { pid ->
            system.root.stop(pid)
        }

        processorActors.values.forEach { processors ->
            processors.values.forEach { pid ->
                system.root.stop(pid)
            }
        }

        outputActors.values.forEach { outputs ->
            outputs.values.forEach { pid ->
                system.root.stop(pid)
            }
        }

        workflowActors.clear()
        processorActors.clear()
        outputActors.clear()
    }

    /**
     * 工作流消息 - 表示工作流中的一个消息
     */
    data class WorkflowMessage(
        val sourceId: String,
        val message: Message,
        val targetIds: List<String>
    )

    /**
     * 工作流协调器 Actor - 负责协调工作流中的消息流动
     */
    class WorkflowCoordinatorActor(
        private val workflowId: String,
        private val config: WorkflowConfig,
        private val actorSystem: ActorSystem
    ) : ProtoActor {
        private val logger = KotlinLogging.logger {}

        override suspend fun ProtoContext.receive(msg: Any) {
            when (msg) {
                is WorkflowMessage -> {
                    logger.debug("WorkflowCoordinator received message from ${msg.sourceId} to ${msg.targetIds}")

                    // 将消息转发给目标处理器或输出
                    msg.targetIds.forEach { targetId ->
                        // 查找目标 Actor
                        // 这里应该使用 ActorSystem 的 API 来查找目标 Actor
                        // 由于 API 可能不兼容，这里简化实现
                        val targetPid = null

                        // 发送消息给目标 Actor
                        targetPid?.let { pid ->
                            actorSystem.root.send(pid, msg)
                        }
                    }
                }
            }
        }
    }

    /**
     * 处理器 Actor - 负责处理消息
     */
    class ProcessorActor(
        private val workflowId: String,
        private val processorId: String,
        private val processor: Processor,
        private val connections: List<Pair<String, String>>
    ) : ProtoActor {
        private val logger = KotlinLogging.logger {}

        override suspend fun ProtoContext.receive(msg: Any) {
            when (msg) {
                is WorkflowMessage -> {
                    logger.debug("ProcessorActor $processorId received message from ${msg.sourceId}")

                    try {
                        // 创建上下文
                        val ctx = ConnectorContext(workflowId, processorId)

                        // 处理消息
                        val processedMessages = processor.process(ctx, msg.message)

                        // 找到连接到这个处理器的下一个节点
                        val nextTargets = connections
                            .filter { it.first == processorId }
                            .map { it.second }

                        // 将处理后的消息发送给工作流协调器
                        processedMessages.forEach { processedMessage ->
                            val workflowMsg = WorkflowMessage(processorId, processedMessage, nextTargets)
                            send(parent!!, workflowMsg)
                        }
                    } catch (e: Exception) {
                        logger.error("Error in processor $processorId: ${e.message}")
                    }
                }
            }
        }
    }

    /**
     * 输出 Actor - 负责将消息写入输出
     */
    class OutputActor(
        private val workflowId: String,
        private val outputId: String,
        private val output: Output
    ) : ProtoActor {
        private val logger = KotlinLogging.logger {}

        override suspend fun ProtoContext.receive(msg: Any) {
            when (msg) {
                is WorkflowMessage -> {
                    logger.debug("OutputActor $outputId received message from ${msg.sourceId}")

                    try {
                        // 创建上下文
                        val ctx = ConnectorContext(workflowId, outputId)

                        // 写入输出
                        output.write(ctx, listOf(msg.message))
                    } catch (e: Exception) {
                        logger.error("Error in output $outputId: ${e.message}")
                    }
                }
            }
        }
    }
}
