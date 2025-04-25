apply(plugin = "java-library")

// Set the artifact name
extensions.configure<ArtifactExtension> {
    name = "Proto.Actor Core"
}

tasks.jar {
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
    add("api", "com.google.protobuf:protobuf-java:${project.extra["protobufVersion"]}")
    add("api", project(":proto-mailbox"))
    add("implementation", "io.github.microutils:kotlin-logging:${project.extra["kotlinLoggingVersion"]}")
    add("implementation", "org.jetbrains.kotlinx:kotlinx-coroutines-core:${project.extra["coroutinesVersion"]}")
    add("implementation", "org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:${project.extra["coroutinesVersion"]}")

    add("testImplementation", "org.slf4j:slf4j-simple:${project.extra["slf4jVersion"]}")
    add("testImplementation", "org.awaitility:awaitility:${project.extra["awaitilityVersion"]}")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
