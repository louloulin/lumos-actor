plugins {
    application
    java
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
}
