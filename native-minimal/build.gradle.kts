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
}

// GraalVM Native Image 配置
graalvmNative {
    binaries {
        named("main") {
            imageName.set("native-minimal")
            mainClass.set("com.dataflare.native.NativeMinimalApp")
            debug.set(true) // 开发阶段启用调试信息
            buildArgs.add("--verbose")
            buildArgs.add("--no-fallback")
            buildArgs.add("-H:+ReportExceptionStackTraces")
        }
    }
}
