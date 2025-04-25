// Set the artifact name
extensions.configure<ArtifactExtension> {
    name = "Proto.Actor Router"
}

dependencies {
    add("api", project(":proto-actor"))
    add("api", project(":proto-mailbox"))
    add("api", "com.google.protobuf:protobuf-java:${project.extra["protobufVersion"]}")
    add("implementation", "org.jetbrains.kotlinx:kotlinx-coroutines-core:${project.extra["coroutinesVersion"]}")

    add("testImplementation", "org.slf4j:slf4j-simple:${project.extra["slf4jVersion"]}")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
