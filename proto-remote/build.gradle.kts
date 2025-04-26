apply(plugin = "java-library")

// Set the artifact name
extensions.configure<ArtifactExtension> {
    name = "Proto.Actor Remote"
}

plugins.withId("com.google.protobuf") {
    configure<com.google.protobuf.gradle.ProtobufExtension> {
        protoc {
            artifact = "com.google.protobuf:protoc:3.17.3"
        }

        plugins {
            create("grpc") {
                artifact = "io.grpc:protoc-gen-grpc-java:${project.extra["grpcVersion"]}"
            }
        }
    }
}

tasks.jar {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from("src/main/proto") {
        include("**/*.proto")
    }
}

sourceSets {
    main {
        java {
            srcDir("build/generated/source/proto/main/java")
        }
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
    add("implementation", "org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:${project.extra["coroutinesVersion"]}")

    add("api", project(":proto-actor"))
    add("api", project(":proto-mailbox"))

    add("testImplementation", "org.slf4j:slf4j-simple:${project.extra["slf4jVersion"]}")
}
