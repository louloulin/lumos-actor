plugins {
    kotlin("jvm")
    id("me.champeau.jmh") version "0.7.0"
}

dependencies {
    implementation(project(":proto-actor"))
    implementation(project(":proto-remote"))
    implementation(project(":proto-cluster"))
    implementation(project(":proto-persistence"))
    
    implementation("io.github.microutils:kotlin-logging:2.0.11")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2")
    
    // JMH dependencies
    jmh("org.openjdk.jmh:jmh-core:1.35")
    jmh("org.openjdk.jmh:jmh-generator-annprocess:1.35")
    
    // Metrics
    implementation("io.dropwizard.metrics:metrics-core:4.2.13")
    implementation("io.dropwizard.metrics:metrics-jmx:4.2.13")
    
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.0")
}

jmh {
    warmupIterations.set(3)
    iterations.set(5)
    fork.set(2)
    benchmarkMode.set(listOf("thrpt"))
    timeUnit.set("ms")
}

tasks.test {
    useJUnitPlatform()
}
