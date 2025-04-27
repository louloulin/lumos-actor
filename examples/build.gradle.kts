plugins {
    application
    java
    id("org.graalvm.buildtools.native")
}

application {
    mainClass.set("actor.proto.examples.inprocessbenchmark.InProcessBenchmarkKt")
}

dependencies {
    add("implementation", "org.jctools:jctools-core:${project.extra["jctoolsVersion"]}")
    add("implementation", "com.google.protobuf:protobuf-java:${project.extra["protobufVersion"]}")
    add("implementation", "org.slf4j:slf4j-simple:${project.extra["slf4jVersion"]}")
    add("implementation", "org.jetbrains.kotlinx:kotlinx-coroutines-core:${project.extra["coroutinesVersion"]}")

    add("implementation", project(":proto-actor"))
    add("implementation", project(":proto-router"))
    add("implementation", project(":proto-remote"))
    add("implementation", project(":proto-mailbox"))
    add("implementation", project(":proto-persistence"))
    add("implementation", project(":proto-cluster"))
}

// Task to run ProtobufPersistenceExample
tasks.register<JavaExec>("runProtobufPersistenceExample") {
    group = "application"
    description = "Run the ProtobufPersistenceExample"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("actor.proto.examples.persistence.ProtobufPersistenceExampleKt")
}

// Task to run PubSubExtensionsExample
tasks.register<JavaExec>("runPubSubExtensionsExample") {
    group = "application"
    description = "Run the PubSubExtensionsExample"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("actor.proto.examples.pubsub.PubSubExtensionsExampleKt")
    jvmArgs = listOf("-Xms512m", "-Xmx1024m")
    // Print the classpath for debugging
    doFirst {
        println("Classpath: ${classpath.asPath}")
    }
}

// Task to run ConsensusExample
tasks.register<JavaExec>("runConsensusExample") {
    group = "application"
    description = "Run the ConsensusExample"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("actor.proto.examples.consensus.ConsensusExampleKt")
    jvmArgs = listOf("-Xms512m", "-Xmx1024m")
}

// Task to run MessageBatch example
tasks.register<JavaExec>("runMessageBatchExample") {
    group = "application"
    description = "Run the MessageBatch example"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("actor.proto.examples.messagebatch.KotlinBatchDemoKt")
    jvmArgs = listOf("-Xms512m", "-Xmx1024m")
}

graalvmNative {
    binaries {
        named("main") {
            imageName.set("protoactor-example")
            mainClass.set("actor.proto.examples.helloworld.HelloWorldKt")
            buildArgs.add("--no-fallback")
            buildArgs.add("--report-unsupported-elements-at-runtime")
            buildArgs.add("-H:+ReportExceptionStackTraces")
            buildArgs.add("-H:+PrintClassInitialization")
        }
    }
    metadataRepository {
        enabled.set(true)
    }
}
