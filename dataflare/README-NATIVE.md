# Dataflare Native Image 构建指南

本文档介绍如何使用 GraalVM Native Image 构建 Dataflare 应用程序。

## 前提条件

1. 安装 GraalVM 22.3.0 或更高版本
2. 安装 Native Image 工具
3. 配置 JAVA_HOME 环境变量指向 GraalVM 安装目录

## 安装 GraalVM

### 使用 SDKMAN 安装 GraalVM

```bash
# 安装 SDKMAN
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

# 安装 GraalVM
sdk install java 22.3.1.r17-grl
sdk use java 22.3.1.r17-grl
```

### 安装 Native Image 工具

```bash
gu install native-image
```

## 构建 Native Image

### 1. 运行 Native Image 构建任务

```bash
./gradlew :dataflare:nativeCompile
```

构建完成后，可执行文件将位于 `dataflare/build/native/nativeCompile/dataflare` 目录下。

### 2. 运行 Native 应用程序

```bash
cd dataflare
./build/native/nativeCompile/dataflare workflows/simple-workflow.yaml
```

## 自定义 Native Image 构建

### 修改 Native Image 配置

可以在 `dataflare/build.gradle.kts` 文件中修改 Native Image 配置：

```kotlin
graalvmNative {
    binaries {
        named("main") {
            imageName.set("dataflare")
            mainClass.set("com.dataflare.native.NativeApp")
            debug.set(true) // 开发阶段启用调试信息
            buildArgs.add("--verbose")
            buildArgs.add("--no-fallback")
            // 添加更多构建参数...
        }
    }
}
```

### 修改反射配置

可以在 `dataflare/src/main/resources/META-INF/native-image/reflect-config.json` 文件中修改反射配置。

### 修改资源配置

可以在 `dataflare/src/main/resources/META-INF/native-image/resource-config.json` 文件中修改资源配置。

## 故障排除

### 1. 反射相关错误

如果遇到反射相关错误，需要在 `reflect-config.json` 文件中添加相应的类。

### 2. 资源相关错误

如果遇到资源相关错误，需要在 `resource-config.json` 文件中添加相应的资源。

### 3. 代理相关错误

如果遇到代理相关错误，需要在 `proxy-config.json` 文件中添加相应的接口。

### 4. 序列化相关错误

如果遇到序列化相关错误，需要在 `serialization-config.json` 文件中添加相应的类。

### 5. JNI 相关错误

如果遇到 JNI 相关错误，需要在 `jni-config.json` 文件中添加相应的类和方法。

## 性能优化

### 1. 减小镜像大小

可以通过以下方式减小镜像大小：

```kotlin
buildArgs.add("-H:+RemoveSaturatedTypeFlows")
buildArgs.add("-H:+ReportExceptionStackTraces")
buildArgs.add("-H:-AddAllCharsets")
buildArgs.add("-H:+IncludeAllTimeZones")
```

### 2. 提高启动速度

可以通过以下方式提高启动速度：

```kotlin
buildArgs.add("--initialize-at-build-time=com.dataflare")
```

### 3. 减少内存使用

可以通过以下方式减少内存使用：

```kotlin
buildArgs.add("-R:MaxHeapSize=64m")
```

## 参考资料

- [GraalVM Native Image 文档](https://www.graalvm.org/reference-manual/native-image/)
- [GraalVM Native Image 构建工具](https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html)
