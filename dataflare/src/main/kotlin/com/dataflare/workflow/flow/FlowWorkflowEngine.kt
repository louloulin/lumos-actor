package com.dataflare.workflow.flow

import com.dataflare.connectors.ConnectorContext
import com.dataflare.connectors.ConnectorRegistry
import com.dataflare.connectors.Input
import com.dataflare.connectors.Output
import com.dataflare.core.Message
import com.dataflare.processors.Processor
import com.dataflare.workflow.WorkflowConfig
import com.dataflare.workflow.WorkflowEngine
import com.dataflare.workflow.WorkflowResult
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Flow 工作流引擎 - 使用 Kotlin Flow 实现的简单工作流引擎
 */
class FlowWorkflowEngine : WorkflowEngine {
    private val connectorRegistry = ConnectorRegistry()
    
    override suspend fun execute(config: WorkflowConfig): WorkflowResult = withContext(Dispatchers.Default) {
        logger.info { "Executing workflow: ${config.name}" }
        
        val workflowId = UUID.randomUUID().toString()
        val errors = mutableListOf<String>()
        val outputMessages = mutableListOf<Message>()
        
        try {
            // 创建连接器和处理器
            val inputs = createInputs(config)
            val processors = createProcessors(config)
            val outputs = createOutputs(config)
            
            // 创建工作流事件流
            val workflowFlow = MutableSharedFlow<WorkflowEvent>(replay = 0, extraBufferCapacity = 100)
            
            // 连接所有组件
            connectComponents(workflowId, inputs, outputs)
            
            // 处理输入
            for ((inputId, input) in inputs) {
                val ctx = ConnectorContext(workflowId, inputId)
                
                // 读取所有输入消息
                while (true) {
                    val message = input.read(ctx) ?: break
                    logger.debug { "Read message from input $inputId" }
                    
                    // 发送消息到工作流流
                    workflowFlow.emit(WorkflowEvent(inputId, message))
                    
                    // 处理消息
                    val targetIds = config.connections
                        .filter { it.from == inputId }
                        .map { it.to }
                    
                    for (targetId in targetIds) {
                        if (processors.containsKey(targetId)) {
                            // 目标是处理器
                            val processor = processors[targetId]!!
                            val processorCtx = ConnectorContext(workflowId, targetId)
                            
                            try {
                                val processedMessages = processor.process(processorCtx, message)
                                
                                // 发送处理后的消息到工作流流
                                for (processedMessage in processedMessages) {
                                    workflowFlow.emit(WorkflowEvent(targetId, processedMessage))
                                    
                                    // 处理后续连接
                                    val nextTargetIds = config.connections
                                        .filter { it.from == targetId }
                                        .map { it.to }
                                    
                                    for (nextTargetId in nextTargetIds) {
                                        if (outputs.containsKey(nextTargetId)) {
                                            // 目标是输出
                                            val output = outputs[nextTargetId]!!
                                            val outputCtx = ConnectorContext(workflowId, nextTargetId)
                                            
                                            try {
                                                val result = output.write(outputCtx, listOf(processedMessage))
                                                if (!result.success) {
                                                    errors.addAll(result.errors)
                                                }
                                                outputMessages.add(processedMessage)
                                            } catch (e: Exception) {
                                                logger.error(e) { "Error writing to output $nextTargetId" }
                                                errors.add("Error writing to output $nextTargetId: ${e.message}")
                                            }
                                        }
                                    }
                                }
                            } catch (e: Exception) {
                                logger.error(e) { "Error processing message in processor $targetId" }
                                errors.add("Error processing message in processor $targetId: ${e.message}")
                            }
                        } else if (outputs.containsKey(targetId)) {
                            // 目标是输出
                            val output = outputs[targetId]!!
                            val outputCtx = ConnectorContext(workflowId, targetId)
                            
                            try {
                                val result = output.write(outputCtx, listOf(message))
                                if (!result.success) {
                                    errors.addAll(result.errors)
                                }
                                outputMessages.add(message)
                            } catch (e: Exception) {
                                logger.error(e) { "Error writing to output $targetId" }
                                errors.add("Error writing to output $targetId: ${e.message}")
                            }
                        }
                    }
                }
            }
            
            // 关闭所有组件
            closeComponents(workflowId, inputs, processors, outputs)
            
            if (errors.isEmpty()) {
                WorkflowResult("success", outputMessages)
            } else {
                WorkflowResult("error", outputMessages, errors)
            }
        } catch (e: Exception) {
            logger.error(e) { "Error executing workflow: ${config.name}" }
            WorkflowResult("error", errors = listOf("Error executing workflow: ${e.message}"))
        }
    }
    
    /**
     * 创建输入连接器
     */
    private fun createInputs(config: WorkflowConfig): Map<String, Input> {
        val inputs = mutableMapOf<String, Input>()
        
        config.inputs.forEach { (id, inputConfig) ->
            try {
                val input = connectorRegistry.createInput(inputConfig.type, inputConfig.config)
                inputs[id] = input
                logger.info { "Created input connector: $id (${inputConfig.type})" }
            } catch (e: Exception) {
                logger.error(e) { "Failed to create input connector: $id (${inputConfig.type})" }
                throw e
            }
        }
        
        return inputs
    }
    
    /**
     * 创建处理器
     */
    private fun createProcessors(config: WorkflowConfig): Map<String, Processor> {
        val processors = mutableMapOf<String, Processor>()
        
        config.processors.forEach { (id, processorConfig) ->
            try {
                val processor = connectorRegistry.createProcessor(processorConfig.type, processorConfig.config)
                processors[id] = processor
                logger.info { "Created processor: $id (${processorConfig.type})" }
            } catch (e: Exception) {
                logger.error(e) { "Failed to create processor: $id (${processorConfig.type})" }
                throw e
            }
        }
        
        return processors
    }
    
    /**
     * 创建输出连接器
     */
    private fun createOutputs(config: WorkflowConfig): Map<String, Output> {
        val outputs = mutableMapOf<String, Output>()
        
        config.outputs.forEach { (id, outputConfig) ->
            try {
                val output = connectorRegistry.createOutput(outputConfig.type, outputConfig.config)
                outputs[id] = output
                logger.info { "Created output connector: $id (${outputConfig.type})" }
            } catch (e: Exception) {
                logger.error(e) { "Failed to create output connector: $id (${outputConfig.type})" }
                throw e
            }
        }
        
        return outputs
    }
    
    /**
     * 连接所有组件
     */
    private suspend fun connectComponents(
        workflowId: String,
        inputs: Map<String, Input>,
        outputs: Map<String, Output>
    ) {
        // 连接输入
        inputs.forEach { (id, input) ->
            val ctx = ConnectorContext(workflowId, id)
            input.connect(ctx)
            logger.info { "Connected input: $id" }
        }
        
        // 连接输出
        outputs.forEach { (id, output) ->
            val ctx = ConnectorContext(workflowId, id)
            output.connect(ctx)
            logger.info { "Connected output: $id" }
        }
    }
    
    /**
     * 关闭所有组件
     */
    private suspend fun closeComponents(
        workflowId: String,
        inputs: Map<String, Input>,
        processors: Map<String, Processor>,
        outputs: Map<String, Output>
    ) {
        // 关闭输入
        inputs.forEach { (id, input) ->
            val ctx = ConnectorContext(workflowId, id)
            input.close(ctx)
            logger.info { "Closed input: $id" }
        }
        
        // 关闭处理器
        processors.forEach { (id, processor) ->
            val ctx = ConnectorContext(workflowId, id)
            processor.close(ctx)
            logger.info { "Closed processor: $id" }
        }
        
        // 关闭输出
        outputs.forEach { (id, output) ->
            val ctx = ConnectorContext(workflowId, id)
            output.close(ctx)
            logger.info { "Closed output: $id" }
        }
    }
    
    /**
     * 工作流事件 - 表示工作流中的一个消息事件
     */
    data class WorkflowEvent(
        val sourceId: String,
        val message: Message
    )
}
