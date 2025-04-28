package com.dataflare.native

/**
 * 最小化的 Native 应用程序
 * 
 * 这是一个极简的示例，不使用任何项目相关的类，只是一个简单的 Hello World 程序
 */
object NativeMinimalApp {
    @JvmStatic
    fun main(args: Array<String>) {
        println("Dataflare Native Minimal Application")
        println("Version: 0.1.0")
        println("Build: GraalVM Native Image")
        
        if (args.isNotEmpty()) {
            println("Arguments: ${args.joinToString(", ")}")
        } else {
            println("No arguments provided")
        }
        
        println("Native Image build successful!")
    }
}
