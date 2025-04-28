package com.dataflare.native

import com.dataflare.native.bridge.DataflareBridge
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * 增强版的 Dataflare Native 应用程序
 *
 * 这个版本使用真实的 Dataflare 核心功能
 */
object NativeMinimalApp {
    private const val VERSION = "0.1.0"
    private val startTime = LocalDateTime.now()
    private var systemInitialized = false

    @JvmStatic
    fun main(args: Array<String>) {
        printBanner()

        // 处理命令行参数
        if (args.isEmpty()) {
            printUsage()
            return
        }

        when (args[0]) {
            "version" -> printVersion()
            "info" -> printSystemInfo()
            "run" -> runWorkflow(args.drop(1).toTypedArray())
            "validate" -> validateWorkflow(args.drop(1).toTypedArray())
            "dsl" -> runDsl(args.drop(1).toTypedArray())
            "help" -> printUsage()
            else -> {
                println("未知命令: ${args[0]}")
                printUsage()
            }
        }

        // 关闭系统
        if (systemInitialized) {
            DataflareBridge.shutdown()
        }
    }

    private fun printBanner() {
        println("""
            ╔═══════════════════════════════════════════════╗
            ║                 DATAFLARE                     ║
            ║        Native Data Processing Engine          ║
            ╚═══════════════════════════════════════════════╝
        """.trimIndent())
    }

    private fun printVersion() {
        println("Dataflare 版本: $VERSION")
        println("构建类型: GraalVM Native Image")
        println("构建时间: ${LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}")
    }

    private fun printSystemInfo() {
        println("系统信息:")
        println("操作系统: ${System.getProperty("os.name")} ${System.getProperty("os.version")}")
        println("架构: ${System.getProperty("os.arch")}")
        println("Java 版本: ${System.getProperty("java.version")}")
        println("可用处理器: ${Runtime.getRuntime().availableProcessors()}")
        println("最大内存: ${Runtime.getRuntime().maxMemory() / 1024 / 1024} MB")
        println("已分配内存: ${Runtime.getRuntime().totalMemory() / 1024 / 1024} MB")
        println("空闲内存: ${Runtime.getRuntime().freeMemory() / 1024 / 1024} MB")
        println("启动时间: ${startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}")
        println("当前时间: ${LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}")
        println("运行时长: ${LocalDateTime.now().second - startTime.second} 秒")
    }

    private fun initializeSystem(): Boolean {
        if (!systemInitialized) {
            systemInitialized = DataflareBridge.initialize("dataflare-native")
        }
        return systemInitialized
    }

    private fun runWorkflow(args: Array<String>) {
        if (args.isEmpty()) {
            println("错误: 未指定工作流配置文件")
            println("用法: dataflare run <workflow-config-file>")
            return
        }

        val configFile = args[0]
        if (!Files.exists(Paths.get(configFile))) {
            println("错误: 配置文件不存在: $configFile")
            return
        }

        println("正在运行工作流: $configFile")

        // 确保输出目录存在
        File("output").mkdirs()

        try {
            // 初始化系统
            if (!initializeSystem()) {
                println("错误: 无法初始化 Dataflare 系统")
                return
            }

            // 执行工作流
            val success = DataflareBridge.executeWorkflow(File(configFile))
            if (success) {
                println("工作流执行成功!")
            } else {
                println("工作流执行失败!")
            }
        } catch (e: Exception) {
            println("工作流执行失败: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun validateWorkflow(args: Array<String>) {
        if (args.isEmpty()) {
            println("错误: 未指定工作流配置文件")
            println("用法: dataflare validate <workflow-config-file>")
            return
        }

        val configFile = args[0]
        if (!Files.exists(Paths.get(configFile))) {
            println("错误: 配置文件不存在: $configFile")
            return
        }

        println("正在验证工作流: $configFile")

        try {
            // 初始化系统
            if (!initializeSystem()) {
                println("错误: 无法初始化 Dataflare 系统")
                return
            }

            // 加载工作流配置
            val config = DataflareBridge.loadWorkflow(File(configFile))
            if (config != null) {
                println("工作流配置验证成功:")
                println("- 工作流名称: ${config.name}")
                println("- 输入数量: ${config.inputs.size}")
                println("- 处理器数量: ${config.processors.size}")
                println("- 输出数量: ${config.outputs.size}")
                println("- 连接数量: ${config.connections.size}")
                println("- 执行引擎: ${config.engineName}")
                println("工作流配置验证成功!")
            } else {
                println("工作流配置验证失败!")
            }
        } catch (e: Exception) {
            println("工作流配置验证失败: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun runDsl(args: Array<String>) {
        if (args.isEmpty()) {
            println("错误: 未指定 DSL 脚本文件")
            println("用法: dataflare dsl <dsl-script-file>")
            return
        }

        val scriptFile = args[0]
        if (!Files.exists(Paths.get(scriptFile))) {
            println("错误: DSL 脚本文件不存在: $scriptFile")
            return
        }

        println("正在运行 DSL 脚本: $scriptFile")

        try {
            // 初始化系统
            if (!initializeSystem()) {
                println("错误: 无法初始化 Dataflare 系统")
                return
            }

            // 读取 DSL 脚本
            val dslScript = File(scriptFile).readText()

            // 验证 DSL 脚本
            val isValid = DataflareBridge.validateDsl(dslScript)
            if (isValid) {
                println("DSL 脚本验证成功!")

                // 编译 DSL 脚本
                val workflowName = DataflareBridge.compileDsl(dslScript)
                if (workflowName != null) {
                    println("DSL 脚本编译成功: $workflowName")
                } else {
                    println("DSL 脚本编译失败!")
                }
            } else {
                println("DSL 脚本验证失败!")
            }
        } catch (e: Exception) {
            println("DSL 脚本执行失败: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun printUsage() {
        println("用法: dataflare <命令> [参数]")
        println("可用命令:")
        println("  version    显示版本信息")
        println("  info       显示系统信息")
        println("  run        运行工作流")
        println("  validate   验证工作流配置")
        println("  dsl        运行 DSL 脚本")
        println("  help       显示帮助信息")
        println()
        println("示例:")
        println("  dataflare version")
        println("  dataflare info")
        println("  dataflare run workflows/simple-workflow.yaml")
        println("  dataflare validate workflows/simple-workflow.yaml")
        println("  dataflare dsl workflows/simple-workflow.dsl")
    }
}
