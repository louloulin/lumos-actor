plugins {
    application
    id("org.graalvm.buildtools.native") version "0.10.1"
}

description = "DataFlare - Data Processing Platform"

artifact {
    name = "DataFlare"
}

group = "com.dataflare"
version = "0.1.0"

dependencies {
    // Proto.Actor dependencies
    implementation(project(":proto-actor"))
    implementation(project(":proto-remote"))
    implementation(project(":proto-mailbox"))
    implementation(project(":proto-cluster"))

    // Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")

    // Serialization
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")
    implementation("org.json:json:20240303")

    // Logging
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("org.apache.logging.log4j:log4j-api:2.22.1")
    implementation("org.apache.logging.log4j:log4j-core:2.22.1")
    implementation("org.apache.logging.log4j:log4j-slf4j2-impl:2.22.1")

    // Config
    implementation("com.sksamuel.hoplite:hoplite-core:2.7.5")
    implementation("com.sksamuel.hoplite:hoplite-yaml:2.7.5")

    // Database
    implementation("org.postgresql:postgresql:42.6.0")
    implementation("mysql:mysql-connector-java:8.0.33")

    // Message Queue
    implementation("redis.clients:jedis:5.1.0")

    // JavaScript Engine
    implementation("org.graalvm.js:js:22.3.1")
    implementation("org.graalvm.js:js-scriptengine:22.3.1")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-runner-junit5:5.6.2")
    testImplementation("io.kotest:kotest-assertions-core:5.6.2")
    testImplementation("io.mockk:mockk:1.13.7")
    testImplementation("org.testcontainers:testcontainers:1.19.1")
    testImplementation("org.testcontainers:junit-jupiter:1.19.1")
    testImplementation("org.testcontainers:postgresql:1.19.1")
    testImplementation("org.testcontainers:mysql:1.19.1")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("com.dataflare.MainKt")
}

// 添加 Native 应用程序任务
tasks.register<JavaExec>("runNativeApp") {
    group = "application"
    description = "Runs the Native application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.dataflare.native.NativeApp")

    // 传递命令行参数
    if (project.hasProperty("args")) {
        args = project.property("args").toString().split(",")
    } else {
        args = listOf("src/main/resources/workflows/simple-workflow.yaml")
    }
}

// GraalVM Native Image 配置
graalvmNative {
    binaries {
        named("main") {
            imageName.set("dataflare-ultra-minimal")
            mainClass.set("com.dataflare.native.UltraMinimalNativeApp")
            debug.set(true) // 开发阶段启用调试信息
            buildArgs.add("--verbose")
            buildArgs.add("--no-fallback")
            buildArgs.add("-H:+ReportExceptionStackTraces")
            buildArgs.add("-H:+AllowDeprecatedBuilderClassesOnImageClasspath")
            buildArgs.add("-H:MaxDuplicationFactor=100.0")
            buildArgs.add("-H:+RemoveSaturatedTypeFlows")
            buildArgs.add("-H:-AddAllCharsets")
            buildArgs.add("-H:+IncludeAllTimeZones")
            // 不使用任何JMX或日志功能
        }
    }
    metadataRepository {
        enabled.set(true)
    }
}

// 添加一个运行任务，允许通过命令行参数指定主类
tasks.register<JavaExec>("runClass") {
    group = "application"
    description = "Runs a specific class with main() method"
    classpath = sourceSets["main"].runtimeClasspath

    // 默认使用 MainKt 类
    mainClass.set(project.findProperty("mainClass")?.toString() ?: "com.dataflare.MainKt")

    // 传递命令行参数
    if (project.hasProperty("args")) {
        args = project.property("args").toString().split(",")
    }
}
