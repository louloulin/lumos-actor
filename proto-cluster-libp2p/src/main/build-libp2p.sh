#!/bin/bash

# 克隆 jvm-libp2p 仓库（如果不存在）
if [ ! -d "jvm-libp2p" ]; then
  git clone https://github.com/libp2p/jvm-libp2p.git
  cd jvm-libp2p
else
  cd jvm-libp2p
  git pull
fi

# 构建 jvm-libp2p
./gradlew build -x test

# 创建 libs 目录（如果不存在）
cd ..
mkdir -p libs/jvm-libp2p

# 复制 JAR 文件到 libs 目录
cp jvm-libp2p/jvm-libp2p-minimal/build/libs/jvm-libp2p-minimal-*.jar libs/jvm-libp2p/jvm-libp2p-minimal.jar
cp jvm-libp2p/jvm-libp2p-core/build/libs/jvm-libp2p-core-*.jar libs/jvm-libp2p/jvm-libp2p-core.jar
cp jvm-libp2p/jvm-libp2p-crypto/build/libs/jvm-libp2p-crypto-*.jar libs/jvm-libp2p/jvm-libp2p-crypto.jar
cp jvm-libp2p/jvm-libp2p-discovery/build/libs/jvm-libp2p-discovery-*.jar libs/jvm-libp2p/jvm-libp2p-discovery.jar
cp jvm-libp2p/jvm-libp2p-protocol/build/libs/jvm-libp2p-protocol-*.jar libs/jvm-libp2p/jvm-libp2p-protocol.jar
cp jvm-libp2p/jvm-libp2p-pubsub/build/libs/jvm-libp2p-pubsub-*.jar libs/jvm-libp2p/jvm-libp2p-pubsub.jar

echo "jvm-libp2p JAR files have been built and copied to libs/jvm-libp2p/"
