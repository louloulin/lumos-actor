// Set the artifact name
extensions.configure<ArtifactExtension> {
    name = "Proto.Actor Mailbox"
}

dependencies {
    add("api", "org.jctools:jctools-core:${project.extra["jctoolsVersion"]}")
    add("implementation", "org.slf4j:slf4j-api:${project.extra["slf4jVersion"]}")
    add("implementation", "org.jetbrains.kotlinx:kotlinx-coroutines-core:${project.extra["coroutinesVersion"]}")
    add("testImplementation", "org.slf4j:slf4j-simple:${project.extra["slf4jVersion"]}")
    add("testImplementation", "org.awaitility:awaitility:${project.extra["awaitilityVersion"]}")
    add("testImplementation", "org.mockito:mockito-core:4.11.0")
    add("testImplementation", "org.mockito.kotlin:mockito-kotlin:4.1.0")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
