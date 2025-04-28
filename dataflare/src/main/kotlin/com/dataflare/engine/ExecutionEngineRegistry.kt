package com.dataflare.engine

import actor.proto.ActorSystem
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * 执行引擎注册表 - 用于注册和管理执行引擎
 */
class ExecutionEngineRegistry(private val system: ActorSystem) {

    /**
     * 注册执行引擎
     */
    fun registerEngine(engine: ExecutionEngine) {
        ExecutionEngineFactory.registerEngine(engine)
        logger.info { "Registered ${engine.name} execution engine" }
    }

    /**
     * 初始化执行引擎注册表
     */
    fun initialize() {
        logger.info { "Initializing ExecutionEngineRegistry" }

        // 如果没有注册任何引擎，则注册默认引擎
        if (ExecutionEngineFactory.getAllEngines().isEmpty()) {
            // 注册 Flow 执行引擎
            val flowEngine = FlowExecutionEngine()
            registerEngine(flowEngine)

            // 注册 ProtoActor 执行引擎
            val protoActorEngine = ProtoActorExecutionEngine(system)
            registerEngine(protoActorEngine)
        }
    }

    /**
     * 获取执行引擎
     */
    fun getEngine(name: String): ExecutionEngine {
        return ExecutionEngineFactory.getEngine(name)
    }

    /**
     * 获取所有执行引擎
     */
    fun getAllEngines(): Map<String, ExecutionEngine> {
        return ExecutionEngineFactory.getAllEngines()
    }

    /**
     * 关闭执行引擎注册表
     */
    suspend fun shutdown() {
        logger.info { "Shutting down ExecutionEngineRegistry" }

        // 关闭所有执行引擎
        ExecutionEngineFactory.getAllEngines().values.forEach { engine ->
            try {
                engine.shutdown()
            } catch (e: Exception) {
                logger.error(e) { "Error shutting down engine: ${engine.name}" }
            }
        }
    }
}
