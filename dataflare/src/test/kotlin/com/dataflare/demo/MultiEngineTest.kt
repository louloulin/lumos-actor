package com.dataflare.demo

import com.dataflare.core.DataProcessingSystem
import com.dataflare.engine.ExecutionEngineFactory
import com.dataflare.engine.FlowExecutionEngine
import com.dataflare.engine.ProtoActorExecutionEngine
import com.dataflare.workflow.Connection
import com.dataflare.workflow.InputConfig
import com.dataflare.workflow.OutputConfig
import com.dataflare.workflow.ProcessorConfig
import com.dataflare.workflow.WorkflowConfig
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import kotlin.test.assertTrue

class MultiEngineTest {
    
    private lateinit var dataProcessingSystem: DataProcessingSystem
    
    @TempDir
    lateinit var tempDir: Path
    
    @BeforeEach
    fun setup() {
        // 注册执行引擎
        ExecutionEngineFactory.registerEngine(FlowExecutionEngine())
        ExecutionEngineFactory.registerEngine(ProtoActorExecutionEngine(actor.proto.ActorSystem("test-system")))
        
        // 创建数据处理系统
        dataProcessingSystem = DataProcessingSystem("test-system")
        dataProcessingSystem.start()
    }
    
    @AfterEach
    fun tearDown() {
        dataProcessingSystem.stop()
    }
    
    @Test
    fun testFlowEngine() = runBlocking {
        // 创建测试数据
        val testDataFile = File(tempDir.toFile(), "test-data-flow.json")
        testDataFile.writeText("""[{"id": 1, "name": "FlowTest", "value": 100}]""")
        
        // 创建输出文件
        val outputFile = File(tempDir.toFile(), "output-flow.json")
        
        // 创建工作流配置
        val config = createTestWorkflowConfig(
            "flow-workflow",
            testDataFile.absolutePath,
            outputFile.absolutePath,
            "flow"
        )
        
        // 部署工作流
        val handle = dataProcessingSystem.createWorkflow(config, "flow")
        
        // 启动工作流
        dataProcessingSystem.startWorkflow(handle)
        
        // 等待工作流执行完成
        delay(2000)
        
        // 停止工作流
        dataProcessingSystem.stopWorkflow(handle)
        
        // 验证输出文件是否存在
        assertTrue(outputFile.exists(), "Output file should exist")
        
        // 验证输出文件内容
        val outputContent = outputFile.readText()
        assertTrue(outputContent.contains("FlowTest"), "Output should contain test data")
    }
    
    @Test
    fun testProtoActorEngine() = runBlocking {
        // 创建测试数据
        val testDataFile = File(tempDir.toFile(), "test-data-protoactor.json")
        testDataFile.writeText("""[{"id": 2, "name": "ProtoActorTest", "value": 200}]""")
        
        // 创建输出文件
        val outputFile = File(tempDir.toFile(), "output-protoactor.json")
        
        // 创建工作流配置
        val config = createTestWorkflowConfig(
            "protoactor-workflow",
            testDataFile.absolutePath,
            outputFile.absolutePath,
            "protoactor"
        )
        
        // 部署工作流
        val handle = dataProcessingSystem.createWorkflow(config, "protoactor")
        
        // 启动工作流
        dataProcessingSystem.startWorkflow(handle)
        
        // 等待工作流执行完成
        delay(2000)
        
        // 停止工作流
        dataProcessingSystem.stopWorkflow(handle)
        
        // 验证输出文件是否存在
        assertTrue(outputFile.exists(), "Output file should exist")
        
        // 验证输出文件内容
        val outputContent = outputFile.readText()
        assertTrue(outputContent.contains("ProtoActorTest"), "Output should contain test data")
    }
    
    private fun createTestWorkflowConfig(
        name: String,
        inputPath: String,
        outputPath: String,
        engineName: String
    ): WorkflowConfig {
        return WorkflowConfig(
            name = name,
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
            ),
            engineName = engineName
        )
    }
}
