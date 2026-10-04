# User Instruction Memory

This file records user instructions, preferences, and teachings for reference in future interactions.

## Format

### User Instruction Entry
User instruction entries should follow this format:

[User Instruction Summary]
- Date: [YYYY-MM-DD]
- Context: [Mentioned scenario or time]
- Instructions:
  - [Content of user teaching or instruction, described line by line]

### Project Knowledge Entry
Entries discovered by the Agent during task execution should follow this format:

[Project Knowledge Summary]
- Date: [YYYY-MM-DD]
- Context: Discovered by Agent while performing [specific task description]
- Category: [Operations & Deployment|Build Methods|Testing Methods|Troubleshooting & Debugging|Workflow & Collaboration|Environment Configuration]
- Instructions:
  - [Specific knowledge points, described line by line]

## Deduplication Strategy
- Before adding a new entry, check for similar or identical instructions.
- If a duplicate is found, skip the new entry or merge it with the existing one.
- When merging, update the context or date information.
- This helps avoid redundant entries and keeps the memory file tidy.

## Entries

[HHHL 客户端构建环境搭建]
- Date: 2026-10-03
- Context: Discovered by Agent while verifying chat module changes
- Category: Build Methods|Environment Configuration
- Instructions:
  - 该仓库是 Kotlin Multiplatform（Compose Multiplatform）项目，构建 androidApp/shared 需要 JDK 17 和 Android SDK（compileSdk/targetSdk 35），环境中默认都未安装。
  - 安装 JDK 17：`DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y openjdk-17-jdk-headless`，安装后路径为 `/usr/lib/jvm/java-17-openjdk-amd64`。
  - 安装 Android SDK：下载 `https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip`，解压到 `/opt/android-sdk/cmdline-tools/latest`，然后 `yes | sdkmanager --licenses`，再 `sdkmanager --install "platform-tools" "platforms;android-35" "build-tools;35.0.0"`。
  - Gradle 构建前必须写入 `/workspace/local.properties`，内容为 `sdk.dir=/opt/android-sdk`（该文件已在 `.gitignore` 中）。
  - 仓库内 `gradlew` 没有可执行权限，新克隆后需要先 `chmod +x gradlew` 才能直接运行 `./gradlew`。
  - Gradle 发行版通过 `gradle/wrapper/gradle-wrapper.properties` 从 `mirrors.cloud.tencent.com` 下载，首次构建耗时较长（约 15 分钟，含依赖下载），后续构建会明显变快。
  - `plugins.gradle.org` 在本环境经常 TLS 握手失败（`SSL_ERROR_SYSCALL` / `Remote host terminated the handshake`），Maven Central 和 Google Maven 可访问。`settings.gradle.kts` 的 `pluginManagement.repositories` 应把 `mavenCentral()`、`google()` 放在 `gradlePluginPortal()` 前面；缓存热时可用 `./gradlew --offline`。
  - 构建/测试属于编译类命令，按环境规则必须通过 `background_terminal_create` 执行（设置 `JAVA_HOME`、`ANDROID_HOME`/`ANDROID_SDK_ROOT`；建议 `memory_percent` 60-65，`cpu_percent` 200，`timeout` 20-30 分钟），禁止直接用前台 bash 长时间运行 Gradle。
  - background_terminal 的命令由 `sh` 执行，不支持 bash 特有语法（如 `${PIPESTATUS[0]}` 会报 Bad substitution 导致误判失败）；判断构建结果应把输出重定向到文件后直接取 `$?`，不要经管道取退出码。
  - 本机约 8GB RAM、无 swap。不要跑 `:shared:build`（会并行 `compileDebugKotlinAndroid` + `compileReleaseKotlinAndroid`，峰值超过 4.4GiB 后变慢）。验证用 `./gradlew --no-daemon --offline --max-workers=1 :shared:compileDebugKotlinAndroid :shared:testDebugUnitTest`。
