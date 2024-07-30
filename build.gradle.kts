plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.beam:beam-runners-direct-java:2.25.0")
    implementation("org.apache.beam:beam-sdks-java-core:2.25.0") // Replace with the latest version
    implementation(platform("org.apache.beam:beam-sdks-java-google-cloud-platform-bom:2.34.0"))
    implementation("org.apache.beam:beam-sdks-java-io-google-cloud-platform")

    implementation("org.apache.beam:beam-sdks-java-io-jdbc:2.25.0")
    implementation("org.postgresql:postgresql:42.1.4")
    implementation("mysql:mysql-connector-java:8.0.33")

}

tasks.test {
    useJUnitPlatform()
}