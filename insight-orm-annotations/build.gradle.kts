plugins {
    kotlin("jvm")
    `maven-publish`
}

group = "org.darchest.insight.orm"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(8)
}

publishing {
    publications {
        create<MavenPublication>("release") {
            from(components["java"])
        }
    }
    repositories {
        mavenLocal()
        maven {
            name = "darchest"
            url = uri("https://mvn.darchest.org/repository/snapshots/")
            credentials {
                username = findProperty("mvn.darchest.user") as String? ?: ""
                password = findProperty("mvn.darchest.password") as String? ?: ""
            }
        }
    }
}
