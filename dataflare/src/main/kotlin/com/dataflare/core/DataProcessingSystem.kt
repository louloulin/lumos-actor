package com.dataflare.core

import com.dataflare.connectors.ConnectorRegistry
import com.dataflare.dsl.DslEngine
import com.dataflare.dsl.ValidationResult
import com.dataflare.workflow.CompiledWorkflow
import com.dataflare.workflow.WorkflowConfig
import com.dataflare.workflow.WorkflowHandle
import com.dataflare.workflow.WorkflowManager
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import actor.proto.Actor
import actor.proto.ActorSystem
import actor.proto.Props
import actor.proto.spawn

private val logger = KotlinLogging.logger {}

/**
 * 数据处理系统的核心类，管理整个系统的生命周期和组件
 */
class DataProcessingSystem(val name: String) {
    private val system = ActorSystem(name)
    private val connectorRegistry = ConnectorRegistry(system)
    private val workflowManager = WorkflowManager(system, connectorRegistry)
    private val dslEngine = DslEngine()

    private var isRunning = false

    /**
     * 启动数据处理系统
     */
    fun start() {
        if (isRunning) {
            logger.warn { "System $name is already running" }
            return
        }

        logger.info { "Starting DataProcessingSystem: $name" }

        // 初始化系统组件
        connectorRegistry.initialize()
        workflowManager.initialize()

        isRunning = true
        logger.info { "DataProcessingSystem $name started successfully" }
    }

    /**
     * 停止数据处理系统
     */
    fun stop() {
        if (!isRunning) {
            logger.warn { "System $name is not running" }
            return
        }

        logger.info { "Stopping DataProcessingSystem: $name" }

        // 优雅关闭组件
        runBlocking {
            workflowManager.shutdown()
            connectorRegistry.shutdown()
            system.shutdown()
        }

        isRunning = false
        logger.info { "DataProcessingSystem $name stopped successfully" }
    }

    /**
     * 创建工作流
     */
    fun createWorkflow(config: WorkflowConfig): WorkflowHandle {
        logger.info { "Creating workflow: ${config.name}" }
        return workflowManager.createWorkflow(config)
    }

    /**
     * 启动工作流
     */
    fun startWorkflow(handle: WorkflowHandle) {
        logger.info { "Starting workflow: ${handle.id}" }
        workflowManager.startWorkflow(handle)
    }

    /**
     * 停止工作流
     */
    fun stopWorkflow(handle: WorkflowHandle) {
        logger.info { "Stopping workflow: ${handle.id}" }
        workflowManager.stopWorkflow(handle)
    }

    /**
     * 暂停工作流
     */
    fun pauseWorkflow(handle: WorkflowHandle) {
        logger.info { "Pausing workflow: ${handle.id}" }
        workflowManager.pauseWorkflow(handle)
    }

    /**
     * 恢复工作流
     */
    fun resumeWorkflow(handle: WorkflowHandle) {
        logger.info { "Resuming workflow: ${handle.id}" }
        workflowManager.resumeWorkflow(handle)
    }

    /**
     * 编译DSL脚本
     */
    fun compileDsl(dslScript: String): CompiledWorkflow {
        logger.info { "Compiling DSL script" }
        return dslEngine.compile(dslScript)
    }

    /**
     * 验证DSL脚本
     */
    fun validateDsl(dslScript: String): ValidationResult {
        logger.info { "Validating DSL script" }
        return dslEngine.validate(dslScript)
    }
}
