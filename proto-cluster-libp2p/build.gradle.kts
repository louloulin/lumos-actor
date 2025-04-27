plugins {
    kotlin("jvm")
}

dependencies {
    api(project(":proto-actor"))
    api(project(":proto-remote"))
    api(project(":proto-cluster"))

    // 使用本地 jvm-libp2p 依赖
    implementation(files("../libs/jvm-libp2p/jvm-libp2p-minimal.jar"))
    implementation(files("../libs/jvm-libp2p/jvm-libp2p-core.jar"))
    implementation(files("../libs/jvm-libp2p/jvm-libp2p-crypto.jar"))
    implementation(files("../libs/jvm-libp2p/jvm-libp2p-discovery.jar"))
    implementation(files("../libs/jvm-libp2p/jvm-libp2p-protocol.jar"))
    implementation(files("../libs/jvm-libp2p/jvm-libp2p-pubsub.jar"))

    // 如果您有本地 jvm-libp2p 源码，也可以使用项目依赖
    // implementation(project(":jvm-libp2p"))

    // 其他依赖
    implementation("io.github.microutils:kotlin-logging:2.0.11")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.8.0")

    // 测试依赖
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.0")
    testImplementation("org.mockito:mockito-core:4.11.0")
    testImplementation("org.mockito.kotlin:mockito-kotlin:4.1.0")
}

tasks.test {
    useJUnitPlatform()
}
