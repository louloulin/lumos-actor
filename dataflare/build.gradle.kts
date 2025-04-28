plugins {
    application
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
    implementation("ch.qos.logback:logback-classic:1.4.11")

    // Config
    implementation("com.sksamuel.hoplite:hoplite-core:2.7.5")
    implementation("com.sksamuel.hoplite:hoplite-yaml:2.7.5")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-runner-junit5:5.6.2")
    testImplementation("io.kotest:kotest-assertions-core:5.6.2")
    testImplementation("io.mockk:mockk:1.13.7")
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
