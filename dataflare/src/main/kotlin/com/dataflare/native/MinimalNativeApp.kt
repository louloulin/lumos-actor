package com.dataflare.native

import java.io.File

/**
 * 最小化的 Dataflare Native 应用程序
 * 
 * 这是一个极简的示例，不使用任何日志或JMX功能，展示如何使用 GraalVM Native Image 构建 Dataflare 应用程序
 */
object MinimalNativeApp {
    @JvmStatic
    fun main(args: Array<String>) {
        println("Dataflare Minimal Native Application")
        println("Version: 0.1.0")
        println("Build: GraalVM Native Image")
        
        // 确保输出目录存在
        File("output").mkdirs()
        
        if (args.isNotEmpty()) {
            println("Arguments: ${args.joinToString(", ")}")
            
            val configFile = args[0]
            println("Config file: $configFile")
            
            // 在这里，我们不实际加载和执行工作流
            // 这只是一个示例，展示 Native Image 构建成功
        } else {
            println("No arguments provided")
            println("Usage: dataflare <workflow-config-file>")
            println("Example: dataflare workflows/simple-workflow.yaml")
        }
        
        println("Native Image build successful!")
    }
}
