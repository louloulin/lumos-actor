package com.dataflare.native

import com.dataflare.native.engine.WorkflowExecutor
import com.dataflare.native.util.YamlParser
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * 增强版的 Dataflare Native 应用程序
 *
 * 这个版本包含更多功能，包括 YAML 解析和工作流执行
 */
object NativeMinimalApp {
    private const val VERSION = "0.1.0"
    private val startTime = LocalDateTime.now()

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
            "help" -> printUsage()
            else -> {
                println("未知命令: ${args[0]}")
                printUsage()
            }
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
            // 解析工作流配置
            val workflowConfig = YamlParser.parseWorkflowConfig(File(configFile))

            // 打印工作流信息
            println("工作流名称: ${workflowConfig.name}")
            println("工作流描述: ${workflowConfig.description ?: "无"}")

            // 执行工作流
            val executor = WorkflowExecutor()
            val result = executor.execute(workflowConfig)

            println("工作流执行状态: ${result.status}")
            println("处理记录数: ${result.recordsProcessed}")
            println("执行时间: ${result.duration.toMillis()} 毫秒")
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
            // 解析工作流配置
            val workflowConfig = YamlParser.parseWorkflowConfig(File(configFile))

            // 验证工作流配置
            println("工作流配置验证成功:")
            println("- 工作流名称: ${workflowConfig.name}")
            println("- 工作流描述: ${workflowConfig.description ?: "无"}")
            println("- 数据源类型: ${workflowConfig.source.type}")
            println("- 处理器数量: ${workflowConfig.processors.size}")
            println("- 数据汇类型: ${workflowConfig.sink.type}")

            // 验证数据源配置
            println("数据源配置:")
            workflowConfig.source.config.forEach { (key, value) ->
                println("  - $key: $value")
            }

            // 验证处理器配置
            println("处理器配置:")
            workflowConfig.processors.forEachIndexed { index, processor ->
                println("  ${index + 1}. ${processor.name} (${processor.type}):")
                processor.config.forEach { (key, value) ->
                    println("    - $key: $value")
                }
            }

            // 验证数据汇配置
            println("数据汇配置:")
            workflowConfig.sink.config.forEach { (key, value) ->
                println("  - $key: $value")
            }

            println("工作流配置验证成功!")
        } catch (e: Exception) {
            println("工作流配置验证失败: ${e.message}")
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
        println("  help       显示帮助信息")
        println()
        println("示例:")
        println("  dataflare version")
        println("  dataflare info")
        println("  dataflare run workflows/simple-workflow.yaml")
        println("  dataflare validate workflows/simple-workflow.yaml")
    }
}
