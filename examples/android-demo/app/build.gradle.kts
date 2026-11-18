plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "dev.pulse.demo"
    compileSdk = 35
    defaultConfig {
        applicationId = "dev.pulse.demo"
        minSdk = 24
        targetSdk = 35
        // 10.0.2.2 is the host machine from the Android emulator.
        buildConfigField("String", "PULSE_ENDPOINT", "\"http://10.0.2.2:18190\"")
        buildConfigField("String", "PULSE_KEY", "\"${project.findProperty("pulseKey") ?: "pk_live_REPLACE_ME"}\"")
    }
    buildFeatures { buildConfig = true }
    // build-rust.sh puts libpulse_sdk_core.so here (jniLibs/<abi>/) and the UniFFI .kt under src/main/java.
    sourceSets["main"].jniLibs.srcDir("src/main/jniLibs")
    compileOptions { sourceCompatibility = JavaVersion.VERSION_17; targetCompatibility = JavaVersion.VERSION_17 }
    kotlinOptions { jvmTarget = "17" }
}

dependencies {
    implementation("net.java.dev.jna:jna:5.15.0@aar")   // UniFFI Kotlin bindings load the .so through JNA
}
