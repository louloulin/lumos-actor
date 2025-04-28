package com.dataflare.native

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.io.File

/**
 * 简单的 Dataflare Native 应用程序
 *
 * 这是一个最小化的示例，展示如何使用 GraalVM Native Image 构建 Dataflare 应用程序
 */
object SimpleNativeApp {
    private val logger: Logger = LogManager.getLogger(SimpleNativeApp::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        try {
            logger.info("Dataflare Native Application")
            logger.info("Version: 0.1.0")
            logger.info("Build: GraalVM Native Image 24.0.1")

            // 确保输出目录存在
            File("output").mkdirs()

            if (args.isNotEmpty()) {
                logger.info("Arguments: ${args.joinToString(", ")}")

                val configFile = args[0]
                logger.info("Config file: $configFile")

                // 在这里，我们不实际加载和执行工作流
                // 这只是一个示例，展示 Native Image 构建成功
            } else {
                logger.info("No arguments provided")
                logger.info("Usage: dataflare <workflow-config-file>")
                logger.info("Example: dataflare workflows/simple-workflow.yaml")
            }

            logger.info("Native Image build successful!")
        } catch (e: Exception) {
            logger.error("Error: ${e.message}", e)
        }
    }
}
