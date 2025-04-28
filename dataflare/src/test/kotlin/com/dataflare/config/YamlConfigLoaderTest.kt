package com.dataflare.config

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class YamlConfigLoaderTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `test loading workflow config from YAML file`() {
        // 创建测试 YAML 文件
        val yamlContent = """
            workflow:
              name: "test-workflow"
              version: "1.0"
              engineName: "flow"

              inputs:
                test_input:
                  type: "file"
                  config:
                    path: "test-input.json"
                    format: "json"

              processors:
                test_processor:
                  type: "mapping"
                  inputs: ["test_input"]
                  config:
                    mapping: "json.parse(content)"

              outputs:
                test_output:
                  type: "file"
                  inputs: ["test_processor"]
                  config:
                    path: "test-output.json"
                    format: "json"

              connections:
                - from: "test_input"
                  to: "test_processor"
                - from: "test_processor"
                  to: "test_output"
        """.trimIndent()

        val yamlFile = tempDir.resolve("test-workflow.yaml").toFile()
        yamlFile.writeText(yamlContent)

        // 加载配置
        val configLoader = YamlConfigLoader()
        val workflowConfig = configLoader.loadWorkflowConfig(yamlFile.absolutePath)

        // 验证配置
        assertEquals("test-workflow", workflowConfig.name)
        assertEquals("flow", workflowConfig.engineName)

        // 验证输入
        assertTrue(workflowConfig.inputs.containsKey("test_input"))
        assertEquals("file", workflowConfig.inputs["test_input"]?.type)
        assertEquals("test-input.json", workflowConfig.inputs["test_input"]?.config?.get("path").toString())
        assertEquals("json", workflowConfig.inputs["test_input"]?.config?.get("format").toString())

        // 验证处理器
        assertTrue(workflowConfig.processors.containsKey("test_processor"))
        assertEquals("mapping", workflowConfig.processors["test_processor"]?.type)
        assertEquals(listOf("test_input"), workflowConfig.processors["test_processor"]?.inputs)
        assertEquals("json.parse(content)", workflowConfig.processors["test_processor"]?.config?.get("mapping").toString())

        // 验证输出
        assertTrue(workflowConfig.outputs.containsKey("test_output"))
        assertEquals("file", workflowConfig.outputs["test_output"]?.type)
        assertEquals(listOf("test_processor"), workflowConfig.outputs["test_output"]?.inputs)
        assertEquals("test-output.json", workflowConfig.outputs["test_output"]?.config?.get("path").toString())
        assertEquals("json", workflowConfig.outputs["test_output"]?.config?.get("format").toString())

        // 验证连接
        assertEquals(2, workflowConfig.connections.size)
        assertEquals("test_input", workflowConfig.connections[0].from)
        assertEquals("test_processor", workflowConfig.connections[0].to)
        assertEquals("test_processor", workflowConfig.connections[1].from)
        assertEquals("test_output", workflowConfig.connections[1].to)
    }

    @Test
    fun `test loading complex workflow config from YAML file`() {
        // 创建测试 YAML 文件
        val yamlContent = """
            workflow:
              name: "complex-workflow"
              version: "1.0"
              engineName: "protoactor"

              inputs:
                input1:
                  type: "file"
                  config:
                    path: "input1.json"
                    format: "json"
                input2:
                  type: "redis"
                  config:
                    host: "localhost"
                    port: 6379
                    key: "test-key"

              processors:
                processor1:
                  type: "mapping"
                  inputs: ["input1"]
                  config:
                    mapping: "json.parse(content)"
                processor2:
                  type: "filter"
                  inputs: ["processor1"]
                  config:
                    condition: "items.value > 100"
                processor3:
                  type: "javascript"
                  inputs: ["processor2"]
                  config:
                    script: "function process(message) { return message; }"
                    engineName: "nashorn"

              outputs:
                output1:
                  type: "file"
                  inputs: ["processor3"]
                  config:
                    path: "output1.json"
                    format: "json"
                output2:
                  type: "postgres"
                  inputs: ["processor3"]
                  config:
                    connectionString: "jdbc:postgresql://localhost:5432/test"
                    table: "test_table"

              connections:
                - from: "input1"
                  to: "processor1"
                - from: "processor1"
                  to: "processor2"
                - from: "processor2"
                  to: "processor3"
                - from: "processor3"
                  to: "output1"
                - from: "processor3"
                  to: "output2"
        """.trimIndent()

        val yamlFile = tempDir.resolve("complex-workflow.yaml").toFile()
        yamlFile.writeText(yamlContent)

        // 加载配置
        val configLoader = YamlConfigLoader()
        val workflowConfig = configLoader.loadWorkflowConfig(yamlFile.absolutePath)

        // 验证配置
        assertEquals("complex-workflow", workflowConfig.name)
        assertEquals("protoactor", workflowConfig.engineName)

        // 验证输入
        assertEquals(2, workflowConfig.inputs.size)
        assertTrue(workflowConfig.inputs.containsKey("input1"))
        assertTrue(workflowConfig.inputs.containsKey("input2"))

        // 验证处理器
        assertEquals(3, workflowConfig.processors.size)
        assertTrue(workflowConfig.processors.containsKey("processor1"))
        assertTrue(workflowConfig.processors.containsKey("processor2"))
        assertTrue(workflowConfig.processors.containsKey("processor3"))

        // 验证 JavaScript 处理器
        assertEquals("javascript", workflowConfig.processors["processor3"]?.type)
        assertEquals("function process(message) { return message; }", workflowConfig.processors["processor3"]?.config?.get("script"))
        assertEquals("nashorn", workflowConfig.processors["processor3"]?.config?.get("engineName"))

        // 验证输出
        assertEquals(2, workflowConfig.outputs.size)
        assertTrue(workflowConfig.outputs.containsKey("output1"))
        assertTrue(workflowConfig.outputs.containsKey("output2"))

        // 验证 PostgreSQL 输出
        assertEquals("postgres", workflowConfig.outputs["output2"]?.type)
        assertEquals("jdbc:postgresql://localhost:5432/test", workflowConfig.outputs["output2"]?.config?.get("connectionString"))
        assertEquals("test_table", workflowConfig.outputs["output2"]?.config?.get("table"))

        // 验证连接
        assertEquals(5, workflowConfig.connections.size)
    }
}
