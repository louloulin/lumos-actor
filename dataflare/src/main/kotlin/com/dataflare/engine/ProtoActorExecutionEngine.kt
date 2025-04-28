package com.dataflare.engine

import actor.proto.ActorSystem
import com.dataflare.connectors.Context
import com.dataflare.connectors.Input
import com.dataflare.connectors.Output
import com.dataflare.core.Message
import com.dataflare.processors.Processor
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * ProtoActor 执行引擎 - 使用 Proto.Actor 实现的执行引擎
 * 
 * 注意：这是一个简化的实现，实际上使用 Flow 执行引擎来处理工作流
 */
class ProtoActorExecutionEngine(private val system: ActorSystem) : ExecutionEngine {
    override val name: String = "protoactor"
    
    private val flowEngine = FlowExecutionEngine()
    
    override suspend fun initialize() {
        logger.info { "Initializing ProtoActor execution engine (using Flow engine internally)" }
        flowEngine.initialize()
    }
    
    override suspend fun startWorkflow(
        workflowId: String,
        config: WorkflowConfig,
        inputs: Map<String, Input>,
        processors: Map<String, Processor>,
        outputs: Map<String, Output>,
        scope: CoroutineScope
    ): List<Job> {
        logger.info { "Starting workflow in ProtoActor execution engine (using Flow engine internally): $workflowId" }
        return flowEngine.startWorkflow(workflowId, config, inputs, processors, outputs, scope)
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
        flowEngine.processMessage(workflowId, sourceId, message, targetIds, ctx, inputs, processors, outputs, connections)
    }
    
    override suspend fun shutdown() {
        logger.info { "Shutting down ProtoActor execution engine (using Flow engine internally)" }
        flowEngine.shutdown()
    }
}
