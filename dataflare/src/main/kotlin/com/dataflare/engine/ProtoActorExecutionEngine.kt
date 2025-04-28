package com.dataflare.engine

import actor.proto.Actor
import actor.proto.ActorSystem
import actor.proto.Context as ProtoContext
import actor.proto.PID
import actor.proto.Props
import actor.proto.fromProducer
import com.dataflare.connectors.ConnectorContext
import com.dataflare.connectors.Context
import com.dataflare.connectors.Input
import com.dataflare.connectors.Output
import com.dataflare.core.Message
import com.dataflare.processors.Processor
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * ProtoActor 执行引擎 - 使用 Proto.Actor 实现的执行引擎
 */
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
        val workflowProps = fromProducer { WorkflowCoordinatorActor(workflowId, config) }
        val workflowPid = system.root.spawnNamed(workflowProps, "workflow-$workflowId")
        workflowActors[workflowId] = workflowPid

        // 创建处理器 Actors
        val workflowProcessorActors = mutableMapOf<String, PID>()
        config.processors.forEach { (processorId, _) ->
            val processor = processors[processorId]!!
            val processorProps = fromProducer {
                ProcessorActor(
                    workflowId = workflowId,
                    processorId = processorId,
                    processor = processor,
                    config = config,
                    system = system
                )
            }
            val processorPid = system.root.spawnNamed(processorProps, "processor-$workflowId-$processorId")
            workflowProcessorActors[processorId] = processorPid

            // 设置批处理定时器
            system.send(processorPid, ProcessBatchTimerMessage)
        }
        processorActors[workflowId] = workflowProcessorActors

        // 创建输出 Actors
        val workflowOutputActors = mutableMapOf<String, PID>()
        config.outputs.forEach { (outputId, _) ->
            val output = outputs[outputId]!!

            // 确保输出连接器已连接
            val ctx = ConnectorContext(workflowId, outputId)
            if (!output.connect(ctx)) {
                logger.error("Failed to connect to output $outputId")
            } else {
                logger.info("Successfully connected to output $outputId")
            }

            val outputProps = fromProducer { OutputActor(workflowId, outputId, output, system) }
            val outputPid = system.root.spawnNamed(outputProps, "output-$workflowId-$outputId")
            workflowOutputActors[outputId] = outputPid

            // 设置批处理定时器
            system.send(outputPid, ProcessBatchTimerMessage)
        }
        outputActors[workflowId] = workflowOutputActors

        // 为每个输入创建一个作业
        config.inputs.forEach { (inputId, _) ->
            val job = scope.launch(Dispatchers.IO) {
                val input = inputs[inputId]!!
                val ctx = ConnectorContext(workflowId, inputId)

                var messageCount = 0
                val maxMessages = 100 // 设置一个上限，避免无限循环

                while (running && messageCount < maxMessages) {
                    try {
                        // 读取输入
                        val message = input.read(ctx)
                        if (message == null) {
                            logger.debug("No message from input $inputId, waiting...")
                            delay(100)
                            continue
                        }

                        messageCount++
                        logger.info("Read message from input $inputId: $message")

                        // 找到连接到这个输入的处理器
                        val connectedProcessors = config.connections
                            .filter { it.from == inputId }
                            .map { it.to }

                        logger.info("Connected processors for $inputId: $connectedProcessors")

                        // 向每个连接的处理器发送消息
                        connectedProcessors.forEach { targetId ->
                            val targetPid = when {
                                processors.containsKey(targetId) -> {
                                    val pid = processorActors[workflowId]?.get(targetId)
                                    logger.info("Found processor PID for $targetId: $pid")
                                    pid
                                }
                                outputs.containsKey(targetId) -> {
                                    val pid = outputActors[workflowId]?.get(targetId)
                                    logger.info("Found output PID for $targetId: $pid")
                                    pid
                                }
                                else -> {
                                    logger.warn("No PID found for target $targetId")
                                    null
                                }
                            }

                            targetPid?.let { pid ->
                                logger.info("Sending message from $inputId to $targetId")
                                system.send(pid, WorkflowMessage(inputId, message))
                            }
                        }

                        // 添加一些延迟，避免CPU使用率过高
                        delay(100)
                    } catch (e: Exception) {
                        logger.error("Error processing input $inputId: ${e.message}", e)
                        delay(1000) // 出错后等待一段时间再重试
                    }
                }

                logger.info("Input $inputId processed $messageCount messages")
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
            system.stop(pid)
        }

        processorActors.values.forEach { processors ->
            processors.values.forEach { pid ->
                system.stop(pid)
            }
        }

        outputActors.values.forEach { outputs ->
            outputs.values.forEach { pid ->
                system.stop(pid)
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
        val targetIds: List<String> = emptyList()
    )

    /**
     * 工作流状态消息 - 用于报告工作流组件的状态
     */
    sealed class WorkflowStatusMessage {
        /**
         * 处理器状态消息
         */
        data class ProcessorStatus(
            val processorId: String,
            val status: Status,
            val error: Throwable? = null,
            val messageId: String? = null
        ) : WorkflowStatusMessage()

        /**
         * 输出状态消息
         */
        data class OutputStatus(
            val outputId: String,
            val status: Status,
            val error: Throwable? = null,
            val messageId: String? = null
        ) : WorkflowStatusMessage()

        /**
         * 状态枚举
         */
        enum class Status {
            STARTED, PROCESSING, COMPLETED, FAILED, RETRYING
        }
    }

    /**
     * 工作流统计消息 - 用于收集工作流执行的统计信息
     */
    data class WorkflowStatsMessage(
        val componentId: String,
        val messageCount: Int = 1,
        val processingTimeMs: Long = 0,
        val timestamp: Long = System.currentTimeMillis()
    )

    /**
     * 工作流协调器 Actor - 负责协调工作流中的消息流动
     */
    class WorkflowCoordinatorActor(
        private val workflowId: String,
        private val config: WorkflowConfig
    ) : Actor {
        private val logger = KotlinLogging.logger {}

        // 组件状态跟踪
        private val componentStatus = mutableMapOf<String, WorkflowStatusMessage.Status>()

        // 消息统计
        private val messageStats = mutableMapOf<String, Int>()
        private val processingTimeStats = mutableMapOf<String, Long>()

        // 失败消息跟踪 (组件ID -> 消息ID -> 重试次数)
        private val failedMessages = mutableMapOf<String, MutableMap<String, Int>>()

        // 最大重试次数
        private val maxRetries = 3

        override suspend fun ProtoContext.receive(msg: Any) {
            when (msg) {
                is WorkflowMessage -> {
                    logger.debug("WorkflowCoordinator received message from ${msg.sourceId}")

                    // 更新消息统计
                    messageStats[msg.sourceId] = (messageStats[msg.sourceId] ?: 0) + 1

                    // 如果消息没有指定目标，则根据工作流配置路由消息
                    if (msg.targetIds.isEmpty()) {
                        val targetIds = config.connections
                            .filter { it.from == msg.sourceId }
                            .map { it.to }

                        // 将消息路由到目标组件
                        routeMessage(msg.copy(targetIds = targetIds))
                    } else {
                        // 使用消息中指定的目标
                        routeMessage(msg)
                    }
                }

                is WorkflowStatusMessage -> {
                    when (msg) {
                        is WorkflowStatusMessage.ProcessorStatus -> {
                            handleProcessorStatus(msg)
                        }
                        is WorkflowStatusMessage.OutputStatus -> {
                            handleOutputStatus(msg)
                        }
                    }
                }

                is WorkflowStatsMessage -> {
                    // 更新组件统计信息
                    messageStats[msg.componentId] = (messageStats[msg.componentId] ?: 0) + msg.messageCount
                    processingTimeStats[msg.componentId] = (processingTimeStats[msg.componentId] ?: 0) + msg.processingTimeMs

                    // 记录统计信息
                    logger.info("Workflow stats - Component: ${msg.componentId}, Messages: ${messageStats[msg.componentId]}, " +
                            "Avg Processing Time: ${processingTimeStats[msg.componentId]?.div(messageStats[msg.componentId] ?: 1)} ms")
                }
            }
        }

        /**
         * 路由消息到目标组件
         */
        private suspend fun ProtoContext.routeMessage(msg: WorkflowMessage) {
            msg.targetIds.forEach { targetId ->
                // 查找目标 Actor 的 PID
                val targetActorName = if (config.processors.containsKey(targetId)) {
                    "processor-$workflowId-$targetId"
                } else if (config.outputs.containsKey(targetId)) {
                    "output-$workflowId-$targetId"
                } else {
                    null
                }

                targetActorName?.let { name ->
                    try {
                        // 创建目标 PID
                        val targetPid = PID(self.address, name)
                        send(targetPid, msg)

                        // 更新组件状态
                        componentStatus[targetId] = WorkflowStatusMessage.Status.PROCESSING
                    } catch (e: Exception) {
                        logger.error("Failed to route message to target: $name", e)
                    }
                }
            }
        }

        /**
         * 处理处理器状态消息
         */
        private fun handleProcessorStatus(status: WorkflowStatusMessage.ProcessorStatus) {
            // 更新处理器状态
            componentStatus[status.processorId] = status.status

            when (status.status) {
                WorkflowStatusMessage.Status.FAILED -> {
                    logger.error("Processor ${status.processorId} failed to process message ${status.messageId}: ${status.error?.message}")

                    // 处理失败消息
                    status.messageId?.let { messageId ->
                        val retryCount = failedMessages
                            .getOrPut(status.processorId) { mutableMapOf() }
                            .getOrPut(messageId) { 0 } + 1

                        failedMessages[status.processorId]!![messageId] = retryCount

                        if (retryCount <= maxRetries) {
                            logger.info("Retrying message $messageId for processor ${status.processorId}, attempt $retryCount")
                            // 这里可以实现重试逻辑
                        } else {
                            logger.error("Message $messageId for processor ${status.processorId} failed after $maxRetries retries")
                        }
                    }
                }
                WorkflowStatusMessage.Status.COMPLETED -> {
                    logger.debug("Processor ${status.processorId} completed processing message ${status.messageId}")
                }
                else -> {
                    logger.debug("Processor ${status.processorId} status changed to ${status.status}")
                }
            }
        }

        /**
         * 处理输出状态消息
         */
        private fun handleOutputStatus(status: WorkflowStatusMessage.OutputStatus) {
            // 更新输出状态
            componentStatus[status.outputId] = status.status

            when (status.status) {
                WorkflowStatusMessage.Status.FAILED -> {
                    logger.error("Output ${status.outputId} failed to process message ${status.messageId}: ${status.error?.message}")

                    // 处理失败消息
                    status.messageId?.let { messageId ->
                        val retryCount = failedMessages
                            .getOrPut(status.outputId) { mutableMapOf() }
                            .getOrPut(messageId) { 0 } + 1

                        failedMessages[status.outputId]!![messageId] = retryCount

                        if (retryCount <= maxRetries) {
                            logger.info("Retrying message $messageId for output ${status.outputId}, attempt $retryCount")
                            // 这里可以实现重试逻辑
                        } else {
                            logger.error("Message $messageId for output ${status.outputId} failed after $maxRetries retries")
                        }
                    }
                }
                WorkflowStatusMessage.Status.COMPLETED -> {
                    logger.debug("Output ${status.outputId} completed processing message ${status.messageId}")
                }
                else -> {
                    logger.debug("Output ${status.outputId} status changed to ${status.status}")
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
        private val config: WorkflowConfig,
        private val system: ActorSystem
    ) : Actor {
        private val logger = KotlinLogging.logger {}

        // 批处理大小
        private val batchSize = 10

        // 批处理缓冲区
        private val messageBuffer = mutableListOf<WorkflowMessage>()

        // 统计信息
        private var processedMessageCount = 0
        private var totalProcessingTimeMs = 0L

        override suspend fun ProtoContext.receive(msg: Any) {
            when (msg) {
                is WorkflowMessage -> {
                    logger.debug("ProcessorActor $processorId received message from ${msg.sourceId}")

                    // 向工作流协调器发送状态消息
                    sendStatusToCoordinator(WorkflowStatusMessage.Status.PROCESSING, messageId = msg.message.id)

                    // 直接处理消息，不使用批处理
                    processMessage(msg)
                }

                // 处理批处理定时器消息
                is ProcessBatchTimerMessage -> {
                    // 不再需要处理批处理
                }
            }
        }

        /**
         * 处理单个消息
         */
        private suspend fun ProtoContext.processMessage(msg: WorkflowMessage) {
            val startTime = System.currentTimeMillis()

            try {
                // 创建上下文
                val ctx = ConnectorContext(workflowId, processorId)

                // 处理消息
                logger.info("Processing message in processor $processorId: ${msg.message}")
                val processedMessages = processor.process(ctx, msg.message)
                logger.info("Processor $processorId produced ${processedMessages.size} messages")

                // 找到连接到这个处理器的下一个节点
                val nextTargetIds = config.connections
                    .filter { it.from == processorId }
                    .map { it.to }

                logger.info("Processor $processorId processed message from ${msg.sourceId}, sending to $nextTargetIds")

                // 直接将处理后的消息发送给下一个节点
                processedMessages.forEach { processedMessage ->
                    logger.info("Sending processed message: $processedMessage")
                    nextTargetIds.forEach { targetId ->
                        // 查找目标 Actor 的 PID
                        val targetActorName = if (config.processors.containsKey(targetId)) {
                            "processor-$workflowId-$targetId"
                        } else if (config.outputs.containsKey(targetId)) {
                            "output-$workflowId-$targetId"
                        } else {
                            null
                        }

                        logger.info("Target actor name for $targetId: $targetActorName")

                        targetActorName?.let { name ->
                            try {
                                // 创建目标 PID
                                val targetPid = PID(self.address, name)
                                logger.info("Sending message from $processorId to $name")
                                send(targetPid, WorkflowMessage(processorId, processedMessage))
                                logger.info("Successfully sent message to $name")
                            } catch (e: Exception) {
                                logger.error("Failed to send to target: $name", e)
                            }
                        }
                    }
                }

                // 向工作流协调器发送状态消息
                sendStatusToCoordinator(WorkflowStatusMessage.Status.COMPLETED, messageId = msg.message.id)

                // 更新统计信息
                processedMessageCount++

                // 计算处理时间
                val processingTime = System.currentTimeMillis() - startTime
                totalProcessingTimeMs += processingTime

                // 发送统计信息给工作流协调器
                val coordinatorPid = PID(self.address, "workflow-$workflowId")
                send(coordinatorPid, WorkflowStatsMessage(
                    componentId = processorId,
                    messageCount = 1,
                    processingTimeMs = processingTime
                ))
            } catch (e: Exception) {
                logger.error("Error processing message ${msg.message.id} in processor $processorId: ${e.message}", e)

                // 向工作流协调器发送状态消息
                sendStatusToCoordinator(WorkflowStatusMessage.Status.FAILED, error = e, messageId = msg.message.id)
            }
        }

        /**
         * 向工作流协调器发送状态消息
         */
        private suspend fun ProtoContext.sendStatusToCoordinator(
            status: WorkflowStatusMessage.Status,
            error: Throwable? = null,
            messageId: String? = null
        ) {
            try {
                val coordinatorPid = PID(self.address, "workflow-$workflowId")
                send(coordinatorPid, WorkflowStatusMessage.ProcessorStatus(
                    processorId = processorId,
                    status = status,
                    error = error,
                    messageId = messageId
                ))
            } catch (e: Exception) {
                logger.error("Failed to send status to coordinator", e)
            }
        }
    }

    /**
     * 批处理定时器消息
     */
    object ProcessBatchTimerMessage

    /**
     * 输出 Actor - 负责将消息写入输出
     */
    class OutputActor(
        private val workflowId: String,
        private val outputId: String,
        private val output: Output,
        private val system: ActorSystem = ActorSystem.default()
    ) : Actor {
        private val logger = KotlinLogging.logger {}

        // 批处理大小
        private val batchSize = 10

        // 批处理缓冲区
        private val messageBuffer = mutableListOf<WorkflowMessage>()

        // 统计信息
        private var processedMessageCount = 0
        private var totalProcessingTimeMs = 0L

        override suspend fun ProtoContext.receive(msg: Any) {
            when (msg) {
                is WorkflowMessage -> {
                    logger.debug("OutputActor $outputId received message from ${msg.sourceId}")

                    // 向工作流协调器发送状态消息
                    sendStatusToCoordinator(WorkflowStatusMessage.Status.PROCESSING, messageId = msg.message.id)

                    // 直接处理消息
                    try {
                        // 创建上下文
                        val ctx = ConnectorContext(workflowId, outputId)

                        // 确保输出连接器已连接
                        if (!output.connect(ctx)) {
                            logger.error("Failed to connect to output $outputId")
                            return
                        }

                        // 写入输出
                        logger.info("Writing message to output $outputId: ${msg.message}")
                        val result = output.write(ctx, listOf(msg.message))

                        if (result.success) {
                            logger.info("Successfully wrote message to output $outputId: ${result.recordsWritten} records written")

                            // 向工作流协调器发送状态消息
                            sendStatusToCoordinator(WorkflowStatusMessage.Status.COMPLETED, messageId = msg.message.id)
                        } else {
                            logger.error("Failed to write message to output $outputId: ${result.errors}")

                            // 向工作流协调器发送状态消息
                            sendStatusToCoordinator(
                                WorkflowStatusMessage.Status.FAILED,
                                error = Exception(result.errors.joinToString("; ")),
                                messageId = msg.message.id
                            )
                        }
                    } catch (e: Exception) {
                        logger.error("Error in output $outputId: ${e.message}", e)

                        // 向工作流协调器发送状态消息
                        sendStatusToCoordinator(WorkflowStatusMessage.Status.FAILED, error = e, messageId = msg.message.id)
                    }
                }

                // 处理批处理定时器消息
                is ProcessBatchTimerMessage -> {
                    // 不再需要处理批处理
                }
            }
        }

        // 不再需要批处理方法

        /**
         * 向工作流协调器发送状态消息
         */
        private suspend fun ProtoContext.sendStatusToCoordinator(
            status: WorkflowStatusMessage.Status,
            error: Throwable? = null,
            messageId: String? = null
        ) {
            try {
                val coordinatorPid = PID(self.address, "workflow-$workflowId")
                send(coordinatorPid, WorkflowStatusMessage.OutputStatus(
                    outputId = outputId,
                    status = status,
                    error = error,
                    messageId = messageId
                ))
            } catch (e: Exception) {
                logger.error("Failed to send status to coordinator", e)
            }
        }
    }
}
