plugins {
    kotlin("jvm")
}

group = "actor.proto.plugin.examples"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":proto-actor"))
    implementation(project(":proto-plugin"))
    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")

    // PF4J
    implementation("org.pf4j:pf4j:3.9.0")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.0")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.9.0")
}

// 创建插件描述文件
tasks.register<Copy>("createPluginMetadata") {
    from("src/main/resources/plugin.properties")
    into("$buildDir/resources/main")
}

tasks.named("processResources") {
    dependsOn("createPluginMetadata")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = "17"
        freeCompilerArgs = listOf("-Xjsr305=strict")
    }
}

tasks.withType<JavaCompile> {
    targetCompatibility = JavaVersion.VERSION_17.toString()
    sourceCompatibility = JavaVersion.VERSION_17.toString()
}
