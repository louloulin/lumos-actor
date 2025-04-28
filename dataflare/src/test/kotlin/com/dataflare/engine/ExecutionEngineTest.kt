package com.dataflare.engine

import actor.proto.ActorSystem
import com.dataflare.connectors.ConnectorContext
import com.dataflare.connectors.ConnectorRegistry
import com.dataflare.connectors.Input
import com.dataflare.connectors.Output
import com.dataflare.core.Message
import com.dataflare.processors.Processor
import com.dataflare.workflow.Connection
import com.dataflare.workflow.InputConfig
import com.dataflare.workflow.OutputConfig
import com.dataflare.workflow.ProcessorConfig
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class ExecutionEngineTest {

    private lateinit var system: ActorSystem
    private lateinit var connectorRegistry: ConnectorRegistry
    private lateinit var engineRegistry: ExecutionEngineRegistry
    private lateinit var scope: CoroutineScope

    @TempDir
    lateinit var tempDir: Path

    @BeforeEach
    fun setup() {
        logger.info("Setting up test environment")

        // 创建新的 ActorSystem
        system = ActorSystem("test-system")

        // 创建连接器注册表和执行引擎注册表
        connectorRegistry = ConnectorRegistry(system)
        engineRegistry = ExecutionEngineRegistry(system)

        // 创建协程作用域
        scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

        // 初始化连接器注册表
        connectorRegistry.initialize()

        // 初始化执行引擎注册表（不注册任何引擎，由测试方法自己注册）
        engineRegistry.initialize()

        logger.info("Test environment setup completed")
    }

    @AfterEach
    fun tearDown() = runBlocking {
        logger.info("Tearing down test environment")

        try {
            // 关闭执行引擎注册表
            engineRegistry.shutdown()
            logger.info("Engine registry shutdown completed")

            // 关闭连接器注册表
            connectorRegistry.shutdown()
            logger.info("Connector registry shutdown completed")

            // 关闭 ActorSystem
            system.shutdown()
            logger.info("Actor system shutdown completed")
        } catch (e: Exception) {
            logger.error(e) { "Error during test teardown" }
        }

        logger.info("Test environment teardown completed")
    }

    @Test
    fun testFlowExecutionEngine() = runBlocking {
        logger.info("Starting Flow execution engine test")

        // 注册 Flow 执行引擎
        engineRegistry.registerEngine(FlowExecutionEngine())

        // 获取 Flow 执行引擎
        val engine = engineRegistry.getEngine("flow")

        // 验证引擎名称
        assertEquals("flow", engine.name, "Engine name should be 'flow'")

        // 初始化引擎
        engine.initialize()
        logger.info("Flow engine initialized")

        // 关闭引擎
        engine.shutdown()
        logger.info("Flow engine shutdown completed")

        logger.info("Flow execution engine test completed successfully")
    }

    @Test
    fun testProtoActorExecutionEngine() = runBlocking {
        logger.info("Starting ProtoActor execution engine test")

        // 注册 ProtoActor 执行引擎
        engineRegistry.registerEngine(ProtoActorExecutionEngine(system))

        // 获取 ProtoActor 执行引擎
        val engine = engineRegistry.getEngine("protoactor")

        // 验证引擎名称
        assertEquals("protoactor", engine.name, "Engine name should be 'protoactor'")

        // 初始化引擎
        engine.initialize()
        logger.info("ProtoActor engine initialized")

        // 关闭引擎
        engine.shutdown()
        logger.info("ProtoActor engine shutdown completed")

        logger.info("ProtoActor execution engine test completed successfully")
    }

    private fun createTestWorkflowConfig(inputPath: String, outputPath: String): WorkflowConfig {
        return WorkflowConfig(
            name = "test-workflow",
            inputs = mapOf(
                "input" to InputConfig(
                    "file",
                    mapOf(
                        "path" to inputPath,
                        "format" to "json"
                    )
                )
            ),
            processors = mapOf(
                "processor" to ProcessorConfig(
                    "mapping",
                    listOf("input"),
                    mapOf("mapping" to "json.parse(content)")
                )
            ),
            outputs = mapOf(
                "output" to OutputConfig(
                    "file",
                    listOf("processor"),
                    mapOf(
                        "path" to outputPath,
                        "format" to "json"
                    )
                )
            ),
            connections = listOf(
                Connection("input", "processor"),
                Connection("processor", "output")
            )
        )
    }

    private fun createInputs(config: WorkflowConfig): Map<String, Input> {
        val inputs = mutableMapOf<String, Input>()

        config.inputs.forEach { (id, inputConfig) ->
            val input = connectorRegistry.createInput(inputConfig.type, inputConfig.config)
            inputs[id] = input

            // 连接输入
            runBlocking {
                val ctx = ConnectorContext("test-workflow", id)
                input.connect(ctx)
            }
        }

        return inputs
    }

    private fun createProcessors(config: WorkflowConfig): Map<String, Processor> {
        val processors = mutableMapOf<String, Processor>()

        config.processors.forEach { (id, processorConfig) ->
            val processor = connectorRegistry.createProcessor(processorConfig.type, processorConfig.config)
            processors[id] = processor
        }

        return processors
    }

    private fun createOutputs(config: WorkflowConfig): Map<String, Output> {
        val outputs = mutableMapOf<String, Output>()

        config.outputs.forEach { (id, outputConfig) ->
            val output = connectorRegistry.createOutput(outputConfig.type, outputConfig.config)
            outputs[id] = output

            // 连接输出
            runBlocking {
                val ctx = ConnectorContext("test-workflow", id)
                output.connect(ctx)
            }
        }

        return outputs
    }
}
