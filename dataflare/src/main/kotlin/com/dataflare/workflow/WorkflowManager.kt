package com.dataflare.workflow

import actor.proto.ActorSystem
import com.dataflare.connectors.ConnectorContext
import com.dataflare.connectors.ConnectorRegistry
import com.dataflare.connectors.Context
import com.dataflare.connectors.Input
import com.dataflare.connectors.Output
import com.dataflare.core.Message
import com.dataflare.engine.ExecutionEngine
import com.dataflare.engine.ExecutionEngineFactory
import com.dataflare.processors.Processor
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * 工作流管理器 - 负责创建、启动、停止和管理工作流
 */
class WorkflowManager(
    private val system: ActorSystem,
    private val connectorRegistry: ConnectorRegistry,
    private val defaultEngineName: String = "flow" // 默认使用 Flow 执行引擎
) {
    private val workflows = ConcurrentHashMap<String, WorkflowInstance>()
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val engines = mutableMapOf<String, ExecutionEngine>()

    /**
     * 初始化工作流管理器
     */
    suspend fun initialize() {
        logger.info { "Initializing WorkflowManager" }

        // 初始化所有执行引擎
        ExecutionEngineFactory.getAllEngines().forEach { (name, engine) ->
            engines[name] = engine
            engine.initialize()
        }
    }

    /**
     * 创建工作流
     */
    fun createWorkflow(config: WorkflowConfig, engineName: String = defaultEngineName): WorkflowHandle {
        logger.info { "Creating workflow: ${config.name} with engine: $engineName" }

        // 检查执行引擎是否存在
        val engine = engines[engineName] ?: throw IllegalArgumentException("Execution engine not found: $engineName")

        val workflowId = UUID.randomUUID().toString()
        val handle = WorkflowHandle(workflowId, config.name, engineName)

        // 创建工作流实例
        val instance = WorkflowInstance(
            id = workflowId,
            name = config.name,
            config = config,
            connectorRegistry = connectorRegistry,
            engine = engine,
            scope = scope
        )

        workflows[workflowId] = instance
        logger.info { "Workflow created: $workflowId (${config.name}) with engine: $engineName" }

        return handle
    }

    /**
     * 启动工作流
     */
    fun startWorkflow(handle: WorkflowHandle) {
        logger.info { "Starting workflow: ${handle.id}" }

        val instance = workflows[handle.id] ?: throw IllegalArgumentException("Workflow not found: ${handle.id}")
        instance.start()
    }

    /**
     * 停止工作流
     */
    fun stopWorkflow(handle: WorkflowHandle) {
        logger.info { "Stopping workflow: ${handle.id}" }

        val instance = workflows[handle.id] ?: throw IllegalArgumentException("Workflow not found: ${handle.id}")
        instance.stop()
    }

    /**
     * 暂停工作流
     */
    fun pauseWorkflow(handle: WorkflowHandle) {
        logger.info { "Pausing workflow: ${handle.id}" }

        val instance = workflows[handle.id] ?: throw IllegalArgumentException("Workflow not found: ${handle.id}")
        instance.pause()
    }

    /**
     * 恢复工作流
     */
    fun resumeWorkflow(handle: WorkflowHandle) {
        logger.info { "Resuming workflow: ${handle.id}" }

        val instance = workflows[handle.id] ?: throw IllegalArgumentException("Workflow not found: ${handle.id}")
        instance.resume()
    }

    /**
     * 关闭工作流管理器
     */
    suspend fun shutdown() {
        logger.info { "Shutting down WorkflowManager" }

        // 停止所有工作流
        workflows.values.forEach { it.stop() }
        workflows.clear()

        // 关闭所有执行引擎
        engines.values.forEach { it.shutdown() }
        engines.clear()

        // 取消协程作用域
        scope.cancel()
    }
}

/**
 * 工作流实例 - 表示一个正在运行的工作流
 */
class WorkflowInstance(
    val id: String,
    val name: String,
    val config: WorkflowConfig,
    private val connectorRegistry: ConnectorRegistry,
    private val engine: ExecutionEngine,
    private val scope: CoroutineScope
) {
    private var running = false
    private var paused = false
    private val jobs = mutableListOf<Job>()
    private val inputs = mutableMapOf<String, Input>()
    private val processors = mutableMapOf<String, Processor>()
    private val outputs = mutableMapOf<String, Output>()

    /**
     * 启动工作流
     */
    fun start() {
        if (running) {
            logger.warn { "Workflow $id is already running" }
            return
        }

        logger.info { "Starting workflow: $id ($name)" }

        try {
            // 创建输入连接器
            config.inputs.forEach { (id, inputConfig) ->
                val input = connectorRegistry.createInput(inputConfig.type, inputConfig.config)
                inputs[id] = input
                logger.info { "Created input connector: $id (${inputConfig.type})" }
            }

            // 创建处理器
            config.processors.forEach { (id, processorConfig) ->
                val processor = connectorRegistry.createProcessor(processorConfig.type, processorConfig.config)
                processors[id] = processor
                logger.info { "Created processor: $id (${processorConfig.type})" }
            }

            // 创建输出连接器
            config.outputs.forEach { (id, outputConfig) ->
                val output = connectorRegistry.createOutput(outputConfig.type, outputConfig.config)
                outputs[id] = output
                logger.info { "Created output connector: $id (${outputConfig.type})" }
            }

            // 连接所有组件

            // 连接输入
            inputs.forEach { (id, input) ->
                val ctx = ConnectorContext(this.id, id)
                runBlocking { input.connect(ctx) }
            }

            // 连接输出
            outputs.forEach { (id, output) ->
                val ctx = ConnectorContext(this.id, id)
                runBlocking { output.connect(ctx) }
            }

            // 使用执行引擎启动工作流
            val engineJobs = runBlocking {
                engine.startWorkflow(
                    workflowId = id,
                    config = config,
                    inputs = inputs,
                    processors = processors,
                    outputs = outputs,
                    scope = scope
                )
            }

            jobs.addAll(engineJobs)

            running = true
            logger.info { "Workflow $id started successfully" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to start workflow $id" }
            // 清理资源
            cleanup()
            throw e
        }
    }

    /**
     * 处理消息 - 使用执行引擎处理消息
     */
    private suspend fun processMessage(
        sourceId: String,
        message: Message,
        targetIds: List<String>,
        ctx: Context
    ) {
        // 将消息处理委托给执行引擎
        val connections = config.connections.map { Pair(it.from, it.to) }
        engine.processMessage(
            workflowId = id,
            sourceId = sourceId,
            message = message,
            targetIds = targetIds,
            ctx = ctx,
            inputs = inputs,
            processors = processors,
            outputs = outputs,
            connections = connections
        )
    }

    /**
     * 停止工作流
     */
    fun stop() {
        if (!running) {
            logger.warn { "Workflow $id is not running" }
            return
        }

        logger.info { "Stopping workflow: $id" }

        running = false
        paused = false

        // 取消所有作业
        jobs.forEach { it.cancel() }
        jobs.clear()

        // 清理资源
        cleanup()

        logger.info { "Workflow $id stopped successfully" }
    }

    /**
     * 暂停工作流
     */
    fun pause() {
        if (!running) {
            logger.warn { "Workflow $id is not running" }
            return
        }

        if (paused) {
            logger.warn { "Workflow $id is already paused" }
            return
        }

        logger.info { "Pausing workflow: $id" }
        paused = true
    }

    /**
     * 恢复工作流
     */
    fun resume() {
        if (!running) {
            logger.warn { "Workflow $id is not running" }
            return
        }

        if (!paused) {
            logger.warn { "Workflow $id is not paused" }
            return
        }

        logger.info { "Resuming workflow: $id" }
        paused = false
    }

    /**
     * 清理资源
     */
    private fun cleanup() {
        logger.info { "Cleaning up workflow resources: $id" }

        // 关闭输入
        inputs.forEach { (connectorId, input) ->
            try {
                val ctx = ConnectorContext(id, connectorId)
                runBlocking { input.close(ctx) }
            } catch (e: Exception) {
                logger.error(e) { "Error closing input $connectorId" }
            }
        }
        inputs.clear()

        // 关闭处理器
        processors.forEach { (connectorId, processor) ->
            try {
                val ctx = ConnectorContext(id, connectorId)
                runBlocking { processor.close(ctx) }
            } catch (e: Exception) {
                logger.error(e) { "Error closing processor $connectorId" }
            }
        }
        processors.clear()

        // 关闭输出
        outputs.forEach { (connectorId, output) ->
            try {
                val ctx = ConnectorContext(id, connectorId)
                runBlocking { output.close(ctx) }
            } catch (e: Exception) {
                logger.error(e) { "Error closing output $connectorId" }
            }
        }
        outputs.clear()
    }
}


