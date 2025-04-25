apply(plugin = "java-library")

// Set the artifact name
extensions.configure<ArtifactExtension> {
    name = "Proto.Actor Remote"
}

tasks.jar {
    from("src/main/proto") {
        include("**/*.proto")
    }
}

dependencies {
    add("api", "io.grpc:grpc-netty:${project.extra["grpcVersion"]}")
    add("api", "io.grpc:grpc-protobuf:${project.extra["grpcVersion"]}")
    add("api", "io.grpc:grpc-stub:${project.extra["grpcVersion"]}")
    add("api", "org.jctools:jctools-core:${project.extra["jctoolsVersion"]}")
    add("implementation", "com.google.protobuf:protobuf-java-util:${project.extra["protobufVersion"]}")
    add("implementation", "io.github.microutils:kotlin-logging:${project.extra["kotlinLoggingVersion"]}")
    add("implementation", "javax.annotation:javax.annotation-api:${project.extra["javaxAnnotationsVersion"]}")

    add("api", project(":proto-actor"))
    add("api", project(":proto-mailbox"))

    add("testImplementation", "org.slf4j:slf4j-simple:${project.extra["slf4jVersion"]}")
}
