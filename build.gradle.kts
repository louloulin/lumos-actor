buildscript {
    val kotlinVersion by extra("1.9.20")

    repositories {
        mavenCentral()
    }

    dependencies {
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:$kotlinVersion")
        classpath("com.netflix.nebula:nebula-release-plugin:18.0.6")
    }
}

plugins {
    id("com.github.ben-manes.versions") version "0.50.0"
}

allprojects {
    group = "actor.proto"

    apply(plugin = "idea")
    apply(plugin = "nebula.release")
    
    // Create the artifact extension for all projects
    extensions.create<ArtifactExtension>("artifact")
}

subprojects {
    apply(plugin = "kotlin")

    val kotlinVersion: String by rootProject.extra
    extra["coroutinesVersion"] = "1.7.3"
    extra["protobufVersion"] = "3.24.0"
    extra["grpcVersion"] = "1.58.0"
    extra["slf4jVersion"] = "2.0.9"
    extra["awaitilityVersion"] = "4.2.0"
    extra["junitPlatformVersion"] = "5.10.0"
    extra["kotlinLoggingVersion"] = "3.0.5"
    extra["jctoolsVersion"] = "4.0.5"
    extra["javaxAnnotationsVersion"] = "1.3.2"

    repositories {
        mavenCentral()
    }

    dependencies {
        add("implementation", "org.jetbrains.kotlin:kotlin-stdlib-jdk8:$kotlinVersion")

        add("testImplementation", "org.junit.jupiter:junit-jupiter-api:${project.extra["junitPlatformVersion"]}")
        add("testRuntimeOnly", "org.junit.jupiter:junit-jupiter-engine:${project.extra["junitPlatformVersion"]}")
    }

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        kotlinOptions {
            jvmTarget = "17"
            allWarningsAsErrors = false
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}

configure(subprojects.filter { it.name != "examples" }) {
    apply(plugin = "maven-publish")
    apply(plugin = "jacoco")

    tasks.register<Jar>("sourcesJar") {
        archiveClassifier.set("sources")
        from(project.the<SourceSetContainer>()["main"].allSource)
        dependsOn(tasks["classes"])
    }

    configure<PublishingExtension> {
        publications {
            create<MavenPublication>("maven") {
                from(components["java"])
                artifact(tasks["sourcesJar"])

                pom {
                    description.set("Proto.Actor is a Next generation Actor Model framework")
                    name.set(project.extensions.getByType<ArtifactExtension>().name)
                    url.set("http://proto.actor")
                    licenses {
                        license {
                            name.set("Apache License, Version 2.0")
                            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                            distribution.set("repo")
                        }
                    }
                    developers {
                        developer {
                            id.set("rogeralsing")
                            name.set("Roger Alsing")
                            email.set("rogeralsing@gmail.com")
                        }
                    }
                    scm {
                        url.set("https://github.com/AsynkronIT/protoactor-kotlin")
                    }
                }
            }
        }
    }

    tasks.named<JacocoReport>("jacocoTestReport") {
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    tasks.named("check") {
        dependsOn("jacocoTestReport")
    }
}

tasks.wrapper {
    gradleVersion = "8.5"
}
