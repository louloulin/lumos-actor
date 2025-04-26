plugins {
    kotlin("jvm")
    id("me.champeau.jmh") version "0.7.1"
}

dependencies {
    implementation(project(":proto-actor"))
    implementation(project(":proto-remote"))
    implementation(project(":proto-cluster"))
    implementation(project(":proto-persistence"))

    implementation("io.github.microutils:kotlin-logging:3.0.5")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")

    // JMH dependencies
    implementation("org.openjdk.jmh:jmh-core:1.37")
    implementation("org.openjdk.jmh:jmh-generator-annprocess:1.37")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.37")
    jmh("org.openjdk.jmh:jmh-core:1.37")
    jmh("org.openjdk.jmh:jmh-generator-annprocess:1.37")

    // Jackson for JSON processing
    implementation("com.fasterxml.jackson.core:jackson-core:2.16.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")

    // Metrics
    implementation("io.dropwizard.metrics:metrics-core:4.2.25")
    implementation("io.dropwizard.metrics:metrics-jmx:4.2.25")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
}

jmh {
    warmupIterations.set(3)
    iterations.set(5)
    fork.set(2)
    benchmarkMode.set(listOf("thrpt"))
    timeUnit.set("ms")
    jmhVersion.set("1.37")
}

tasks.test {
    useJUnitPlatform()
}
