plugins {
    kotlin("jvm")
    id("org.graalvm.buildtools.native") version "0.10.1"
}

group = "com.dataflare"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.yaml:snakeyaml:2.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.16.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.16.1")
}

// 配置资源文件
tasks.withType<ProcessResources> {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

// GraalVM Native Image 配置
graalvmNative {
    binaries {
        named("main") {
            imageName.set("dataflare")
            mainClass.set("com.dataflare.native.NativeMinimalApp")
            debug.set(true) // 开发阶段启用调试信息
            buildArgs.add("--verbose")
            buildArgs.add("--no-fallback")
            buildArgs.add("-H:+ReportExceptionStackTraces")
            buildArgs.add("-H:IncludeResources=workflows/.*\\.yaml|input/.*\\.csv")
            buildArgs.add("-H:+PrintClassInitialization")
            buildArgs.add("-H:+PrintAnalysisCallTree")
            buildArgs.add("-H:Log=registerResource")
            buildArgs.add("-H:+IncludeAllTimeZones")
            buildArgs.add("-H:ResourceConfigurationFiles=${project.projectDir}/src/main/resources/META-INF/native-image/resource-config.json")
            buildArgs.add("-H:ReflectionConfigurationFiles=${project.projectDir}/src/main/resources/META-INF/native-image/reflect-config.json")
        }
    }
}
