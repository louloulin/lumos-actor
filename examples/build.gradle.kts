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
