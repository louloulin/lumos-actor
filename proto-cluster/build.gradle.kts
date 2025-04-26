plugins {
    kotlin("jvm")
    id("com.google.protobuf")
}

dependencies {
    api(project(":proto-actor"))
    api(project(":proto-remote"))

    implementation("io.github.microutils:kotlin-logging:2.0.11")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.0")
}

plugins.withId("com.google.protobuf") {
    configure<com.google.protobuf.gradle.ProtobufExtension> {
        protoc {
            artifact = "com.google.protobuf:protoc:3.17.3"
        }
    }
}

tasks.test {
    useJUnitPlatform()
}
