# protoactor-kotlin libp2p 集群

这个模块提供了基于本地 jvm-libp2p 的 protoactor-kotlin 分布式集群实现。

## 使用本地 jvm-libp2p

本项目使用本地构建的 jvm-libp2p 库，而不是从远程仓库获取依赖。这样做的好处是：

1. 可以使用最新的 jvm-libp2p 代码
2. 可以对 jvm-libp2p 进行定制和修改
3. 可以更好地控制依赖版本

## 构建步骤

### 自动构建

使用 Gradle 任务自动构建 jvm-libp2p 并复制 JAR 文件：

```bash
./gradlew buildLibp2p
```

这个任务会：
1. 克隆 jvm-libp2p 仓库（如果不存在）
2. 构建 jvm-libp2p
3. 复制 JAR 文件到 libs/jvm-libp2p 目录

### 手动构建

如果您想手动构建 jvm-libp2p，可以执行以下步骤：

1. 克隆 jvm-libp2p 仓库：
   ```bash
   git clone https://github.com/libp2p/jvm-libp2p.git
   ```

2. 构建 jvm-libp2p：
   ```bash
   cd jvm-libp2p
   ./gradlew build -x test
   ```

3. 复制 JAR 文件到 libs/jvm-libp2p 目录：
   ```bash
   mkdir -p ../libs/jvm-libp2p
   cp jvm-libp2p-minimal/build/libs/jvm-libp2p-minimal-*.jar ../libs/jvm-libp2p/jvm-libp2p-minimal.jar
   cp jvm-libp2p-core/build/libs/jvm-libp2p-core-*.jar ../libs/jvm-libp2p/jvm-libp2p-core.jar
   cp jvm-libp2p-crypto/build/libs/jvm-libp2p-crypto-*.jar ../libs/jvm-libp2p/jvm-libp2p-crypto.jar
   cp jvm-libp2p-discovery/build/libs/jvm-libp2p-discovery-*.jar ../libs/jvm-libp2p/jvm-libp2p-discovery.jar
   cp jvm-libp2p-protocol/build/libs/jvm-libp2p-protocol-*.jar ../libs/jvm-libp2p/jvm-libp2p-protocol.jar
   cp jvm-libp2p-pubsub/build/libs/jvm-libp2p-pubsub-*.jar ../libs/jvm-libp2p/jvm-libp2p-pubsub.jar
   ```

## 使用源代码依赖（可选）

如果您想直接使用 jvm-libp2p 的源代码，而不是 JAR 文件，可以：

1. 将 jvm-libp2p 添加为 Git 子模块：
   ```bash
   git submodule add https://github.com/libp2p/jvm-libp2p.git
   ```

2. 在 settings.gradle.kts 中取消注释相关代码：
   ```kotlin
   includeBuild("jvm-libp2p") {
       dependencySubstitution {
           substitute(module("io.libp2p:jvm-libp2p-minimal")).using(project(":jvm-libp2p-minimal"))
           substitute(module("io.libp2p:jvm-libp2p-core")).using(project(":jvm-libp2p-core"))
           // ...其他模块...
       }
   }
   ```

3. 在 proto-cluster-libp2p/build.gradle.kts 中使用项目依赖：
   ```kotlin
   implementation(project(":jvm-libp2p"))
   ```

## 特性

- 完全去中心化的架构，没有单点故障
- 自动节点发现（使用 mDNS 和种子节点）
- 基于 gossip 的集群状态同步
- 虚拟 Actor 定位和激活
- 安全的点对点通信
