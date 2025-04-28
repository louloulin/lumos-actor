package com.dataflare.engine

import com.dataflare.connectors.ConnectorContext
import com.dataflare.connectors.Context
import com.dataflare.connectors.Input
import com.dataflare.connectors.Output
import com.dataflare.core.Message
import com.dataflare.processors.Processor
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * Flow 执行引擎 - 使用 Kotlin Flow 实现的执行引擎
 */
@OptIn(ExperimentalCoroutinesApi::class)
class FlowExecutionEngine : ExecutionEngine {
    override val name: String = "flow"

    private val workflowFlows = mutableMapOf<String, MutableSharedFlow<WorkflowEvent>>()
    private var running = false

    override suspend fun initialize() {
        logger.info { "Initializing Flow execution engine" }
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
        logger.info { "Starting workflow in Flow execution engine: $workflowId" }

        // 为工作流创建一个共享流
        val workflowFlow = MutableSharedFlow<WorkflowEvent>(replay = 0, extraBufferCapacity = 100)
        workflowFlows[workflowId] = workflowFlow

        val jobs = mutableListOf<Job>()

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

                        // 发送消息到工作流流
                        workflowFlow.emit(WorkflowEvent(inputId, message))

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

        // 为每个处理器创建一个作业
        config.processors.forEach { (processorId, processorConfig) ->
            val job = scope.launch(Dispatchers.Default) {
                val processor = processors[processorId]!!
                val ctx = ConnectorContext(workflowId, processorId)

                // 订阅工作流流，处理相关消息
                workflowFlow
                    .filter { event ->
                        // 只处理连接到这个处理器的消息
                        config.connections.any { it.from == event.sourceId && it.to == processorId }
                    }
                    .collect { event ->
                        try {
                            // 处理消息
                            val processedMessages = processor.process(ctx, event.message)

                            // 将处理后的消息发送到工作流流
                            processedMessages.forEach { processedMessage ->
                                workflowFlow.emit(WorkflowEvent(processorId, processedMessage))
                            }
                        } catch (e: Exception) {
                            logger.error(e) { "Error in processor $processorId" }
                        }
                    }
            }

            jobs.add(job)
        }

        // 为每个输出创建一个作业
        config.outputs.forEach { (outputId, outputConfig) ->
            val job = scope.launch(Dispatchers.IO) {
                val output = outputs[outputId]!!
                val ctx = ConnectorContext(workflowId, outputId)

                // 订阅工作流流，处理相关消息
                workflowFlow
                    .filter { event ->
                        // 只处理连接到这个输出的消息
                        config.connections.any { it.from == event.sourceId && it.to == outputId }
                    }
                    .collect { event ->
                        try {
                            // 写入输出
                            output.write(ctx, listOf(event.message))
                        } catch (e: Exception) {
                            logger.error(e) { "Error in output $outputId" }
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
        // 在 Flow 执行引擎中，消息处理是通过流自动完成的
        // 这个方法在 Flow 执行引擎中不需要实现
        logger.debug { "processMessage called in Flow engine - this is handled automatically by flows" }
    }

    override suspend fun shutdown() {
        logger.info { "Shutting down Flow execution engine" }
        running = false
        workflowFlows.clear()
    }

    /**
     * 工作流事件 - 表示工作流中的一个消息事件
     */
    data class WorkflowEvent(
        val sourceId: String,
        val message: Message
    )
}
