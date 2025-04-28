package com.dataflare.native

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.io.File

/**
 * Dataflare Native 应用程序
 *
 * 这是一个简单的示例，展示如何使用 GraalVM Native Image 构建 Dataflare 应用程序
 */
object NativeApp {
    private val logger: Logger = LogManager.getLogger(NativeApp::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        logger.info("Starting Dataflare Native Application")

        // 检查命令行参数
        if (args.isEmpty()) {
            logger.info("Usage: dataflare <workflow-config-file>")
            logger.info("Example: dataflare workflows/simple-workflow.yaml")
            return
        }

        val configFile = args[0]
        logger.info("Loading workflow config from: $configFile")

        try {
            // 确保输出目录存在
            File("output").mkdirs()

            // 简单的配置信息
            logger.info("Dataflare Native Application")
            logger.info("Version: 0.1.0")
            logger.info("Build: GraalVM Native Image")
            logger.info("Config file: $configFile")

            // 在这里，我们不实际加载和执行工作流
            // 这只是一个示例，展示 Native Image 构建成功

            logger.info("Native Image build successful!")
            logger.info("In a real application, we would load and execute the workflow here.")
        } catch (e: Exception) {
            logger.error("Error: ${e.message}", e)
        }

        logger.info("Dataflare Native Application completed")
    }
}
