package com.dataflare.engine

import com.dataflare.connectors.Context
import com.dataflare.connectors.Input
import com.dataflare.connectors.Output
import com.dataflare.core.Message
import com.dataflare.processors.Processor
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job

/**
 * 执行引擎接口 - 定义了执行引擎的基本操作
 */
interface ExecutionEngine {
    /**
     * 引擎名称
     */
    val name: String
    
    /**
     * 初始化引擎
     */
    suspend fun initialize()
    
    /**
     * 启动工作流
     */
    suspend fun startWorkflow(
        workflowId: String,
        config: WorkflowConfig,
        inputs: Map<String, Input>,
        processors: Map<String, Processor>,
        outputs: Map<String, Output>,
        scope: CoroutineScope
    ): List<Job>
    
    /**
     * 处理消息
     */
    suspend fun processMessage(
        workflowId: String,
        sourceId: String,
        message: Message,
        targetIds: List<String>,
        ctx: Context,
        inputs: Map<String, Input>,
        processors: Map<String, Processor>,
        outputs: Map<String, Output>,
        connections: List<Pair<String, String>>
    )
    
    /**
     * 关闭引擎
     */
    suspend fun shutdown()
}

/**
 * 执行引擎工厂 - 用于创建执行引擎实例
 */
object ExecutionEngineFactory {
    private val engines = mutableMapOf<String, ExecutionEngine>()
    
    /**
     * 注册执行引擎
     */
    fun registerEngine(engine: ExecutionEngine) {
        engines[engine.name] = engine
    }
    
    /**
     * 获取执行引擎
     */
    fun getEngine(name: String): ExecutionEngine {
        return engines[name] ?: throw IllegalArgumentException("Execution engine not found: $name")
    }
    
    /**
     * 获取所有执行引擎
     */
    fun getAllEngines(): Map<String, ExecutionEngine> {
        return engines.toMap()
    }
}
