plugins {
    kotlin("jvm")
    application
}

application {
    mainClass.set("actor.proto.simple.SimpleHelloKt")
}

dependencies {
    // 不依赖任何外部库
}

// 创建 Native 编译任务
tasks.register<JavaExec>("runWithAgent") {
    group = "Native"
    description = "使用 GraalVM Agent 运行应用程序以生成配置"

    doFirst {
        File(layout.buildDirectory.get().asFile, "native").mkdirs()
        File(layout.buildDirectory.get().asFile, "native/config").mkdirs()
    }

    mainClass.set("actor.proto.simple.SimpleNativeKt")
    classpath = sourceSets["main"].runtimeClasspath

    jvmArgs = listOf(
        "-agentlib:native-image-agent=config-output-dir=${layout.buildDirectory.get().asFile}/native/config",
        "-Dorg.graalvm.nativeimage.imagecode=agent"
    )
}

tasks.register<Exec>("compileNative") {
    group = "Native"
    description = "编译 Native Image"

    dependsOn("jar")

    doFirst {
        val javaHome = System.getProperty("java.home")
        val nativeImageExecutable = File(javaHome, "bin/native-image")

        if (!nativeImageExecutable.exists()) {
            throw IllegalStateException("无法找到 native-image 工具。请确保安装了 GraalVM 并运行 'gu install native-image'。")
        }

        val jarTask = tasks.getByName("jar")
        val jarFile = jarTask.outputs.files.singleFile

        val outputDir = File(layout.buildDirectory.get().asFile, "native")
        val outputFile = File(outputDir, "proto-actor-simple")

        val configDir = File(layout.buildDirectory.get().asFile, "native/config")

        // 构建命令
        commandLine = listOfNotNull(
            nativeImageExecutable.absolutePath,
            "-cp", jarFile.absolutePath,
            "-H:ConfigurationFileDirectories=${configDir.absolutePath}",
            "--no-fallback",
            "--report-unsupported-elements-at-runtime",
            "-H:+ReportExceptionStackTraces",
            "--initialize-at-build-time=org.slf4j",
            "-o", outputFile.absolutePath,
            "actor.proto.simple.SimpleNativeKt"
        )

        // 设置工作目录
        workingDir = projectDir
    }
}

tasks.register<Exec>("runNative") {
    group = "Native"
    description = "运行 Native Image"

    dependsOn("compileNative")

    doFirst {
        val outputDir = File(layout.buildDirectory.get().asFile, "native")
        val outputFile = File(outputDir, "proto-actor-simple")

        if (!outputFile.exists()) {
            throw IllegalStateException("Native Image 不存在。请先运行 compileNative 任务。")
        }

        // 确保文件可执行
        outputFile.setExecutable(true)

        commandLine = listOf(outputFile.absolutePath)
    }
}

tasks.register("buildNative") {
    group = "Native"
    description = "一键构建 Native Image（生成配置并编译）"

    dependsOn("runWithAgent", "compileNative")
}
