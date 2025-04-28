package com.dataflare.config

import com.dataflare.workflow.WorkflowConfig
import com.sksamuel.hoplite.ConfigLoader
import com.sksamuel.hoplite.yaml.YamlParser
import mu.KotlinLogging
import java.io.File

private val logger = KotlinLogging.logger {}

/**
 * YAML 配置加载器
 *
 * 用于从 YAML 文件加载工作流配置
 */
class YamlConfigLoader {

    /**
     * 从 YAML 文件加载工作流配置
     *
     * @param filePath YAML 文件路径
     * @return 工作流配置
     */
    fun loadWorkflowConfig(filePath: String): WorkflowConfig {
        logger.info { "Loading workflow config from YAML file: $filePath" }

        try {
            val file = File(filePath)
            if (!file.exists()) {
                throw IllegalArgumentException("YAML file not found: $filePath")
            }

            val config = ConfigLoader.builder()
                .addFileExtensionMapping("yml", YamlParser())
                .addFileExtensionMapping("yaml", YamlParser())
                .build()
                .loadConfigOrThrow<YamlWorkflowConfig>(filePath)

            logger.info { "Successfully loaded workflow config: ${config.workflow.name}" }

            return config.toWorkflowConfig()
        } catch (e: Exception) {
            logger.error(e) { "Failed to load workflow config from YAML file: $filePath" }
            throw e
        }
    }
}

/**
 * YAML 工作流配置
 *
 * 用于从 YAML 文件解析工作流配置
 */
data class YamlWorkflowConfig(
    val workflow: YamlWorkflow
) {
    fun toWorkflowConfig(): WorkflowConfig {
        return WorkflowConfig(
            name = workflow.name,
            inputs = workflow.inputs.mapValues { (_, input) ->
                com.dataflare.workflow.InputConfig(
                    type = input.type,
                    config = input.config.mapValues { (_, value) -> value as Any }
                )
            },
            processors = workflow.processors.mapValues { (_, processor) ->
                com.dataflare.workflow.ProcessorConfig(
                    type = processor.type,
                    inputs = processor.inputs,
                    config = processor.config.mapValues { (_, value) -> value as Any }
                )
            },
            outputs = workflow.outputs.mapValues { (_, output) ->
                com.dataflare.workflow.OutputConfig(
                    type = output.type,
                    inputs = output.inputs,
                    config = output.config.mapValues { (_, value) -> value as Any }
                )
            },
            connections = workflow.connections.map { connection ->
                com.dataflare.workflow.Connection(
                    from = connection.from,
                    to = connection.to
                )
            },
            engineName = workflow.engineName ?: "flow"
        )
    }
}

/**
 * YAML 工作流
 */
data class YamlWorkflow(
    val name: String,
    val version: String? = "1.0",
    val inputs: Map<String, YamlInput>,
    val processors: Map<String, YamlProcessor>,
    val outputs: Map<String, YamlOutput>,
    val connections: List<YamlConnection>,
    val engineName: String? = null
)

/**
 * YAML 输入
 */
data class YamlInput(
    val type: String,
    val config: Map<String, String>
)

/**
 * YAML 处理器
 */
data class YamlProcessor(
    val type: String,
    val inputs: List<String>,
    val config: Map<String, String>
)

/**
 * YAML 输出
 */
data class YamlOutput(
    val type: String,
    val inputs: List<String>,
    val config: Map<String, String>
)



/**
 * YAML 连接
 */
data class YamlConnection(
    val from: String,
    val to: String
)
