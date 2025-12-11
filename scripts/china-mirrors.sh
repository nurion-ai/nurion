#!/bin/bash
# Configure China package mirrors for faster dependency installation
# Usage: source scripts/china-mirrors.sh

set -euo pipefail

echo "=== Configuring China Package Mirrors ==="

# 1. Python/pip - Aliyun mirror
echo "Configuring pip..."
export PIP_INDEX_URL="https://mirrors.aliyun.com/pypi/simple/"
export PIP_TRUSTED_HOST="mirrors.aliyun.com"

# Create pip config file
mkdir -p ~/.pip
cat > ~/.pip/pip.conf << 'EOF'
[global]
index-url = https://mirrors.aliyun.com/pypi/simple/
trusted-host = mirrors.aliyun.com

[install]
trusted-host = mirrors.aliyun.com
EOF

# 2. npm - Taobao/npmmirror
echo "Configuring npm..."
if command -v npm &> /dev/null; then
    npm config set registry https://registry.npmmirror.com
fi

# 3. Maven - Aliyun mirror
echo "Configuring Maven..."
mkdir -p ~/.m2
cat > ~/.m2/settings.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              https://maven.apache.org/xsd/settings-1.0.0.xsd">
  <mirrors>
    <mirror>
      <id>aliyun</id>
      <name>Aliyun Maven Mirror</name>
      <url>https://maven.aliyun.com/repository/public</url>
      <mirrorOf>central</mirrorOf>
    </mirror>
    <mirror>
      <id>aliyun-google</id>
      <name>Aliyun Google Mirror</name>
      <url>https://maven.aliyun.com/repository/google</url>
      <mirrorOf>google</mirrorOf>
    </mirror>
  </mirrors>
  
  <profiles>
    <profile>
      <id>aliyun</id>
      <repositories>
        <repository>
          <id>aliyun</id>
          <url>https://maven.aliyun.com/repository/public</url>
          <releases>
            <enabled>true</enabled>
          </releases>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </repository>
      </repositories>
      <pluginRepositories>
        <pluginRepository>
          <id>aliyun</id>
          <url>https://maven.aliyun.com/repository/public</url>
          <releases>
            <enabled>true</enabled>
          </releases>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </pluginRepository>
      </pluginRepositories>
    </profile>
  </profiles>
  
  <activeProfiles>
    <activeProfile>aliyun</activeProfile>
  </activeProfiles>
</settings>
EOF

# 4. Gradle (if used)
echo "Configuring Gradle..."
mkdir -p ~/.gradle
cat > ~/.gradle/init.gradle << 'EOF'
allprojects {
    repositories {
        maven { url 'https://maven.aliyun.com/repository/public/' }
        maven { url 'https://maven.aliyun.com/repository/google/' }
        maven { url 'https://maven.aliyun.com/repository/gradle-plugin/' }
        mavenCentral()
    }
}
EOF

# 5. Docker - Configure daemon for registry mirrors (requires root)
echo "Docker mirror configuration (for reference):"
echo "Add to /etc/docker/daemon.json:"
cat << 'EOF'
{
  "registry-mirrors": [
    "https://registry.docker-cn.com",
    "https://docker.mirrors.ustc.edu.cn",
    "https://hub-mirror.c.163.com"
  ]
}
EOF

# 6. apt - Tsinghua mirror (Ubuntu)
echo "Configuring apt (Ubuntu)..."
if [ -f /etc/apt/sources.list ]; then
    # Backup original
    sudo cp /etc/apt/sources.list /etc/apt/sources.list.bak 2>/dev/null || true
    
    # Only modify if running as root or with sudo
    if [ "$EUID" -eq 0 ]; then
        sed -i 's/archive.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list
        sed -i 's/security.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list
    else
        echo "  Skipping apt mirror (requires root)"
    fi
fi

# 7. Go modules proxy
echo "Configuring Go proxy..."
export GOPROXY="https://goproxy.cn,https://goproxy.io,direct"
export GOSUMDB="sum.golang.google.cn"

# 8. Rust/Cargo (if used)
echo "Configuring Cargo..."
mkdir -p ~/.cargo
cat > ~/.cargo/config.toml << 'EOF'
[source.crates-io]
replace-with = 'ustc'

[source.ustc]
registry = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"
EOF

# 9. Hugging Face - Use China mirror for model downloads
echo "Configuring Hugging Face..."
export HF_ENDPOINT="https://hf-mirror.com"

# 10. Container registry - from environment or default
echo "Configuring container registry..."
export CONTAINER_REGISTRY="${CR_URL:-}"

# Export for use in CI
echo ""
echo "=== Mirror Configuration Complete ==="
echo ""
echo "Environment variables set:"
echo "  PIP_INDEX_URL=$PIP_INDEX_URL"
echo "  GOPROXY=$GOPROXY"
echo "  HF_ENDPOINT=$HF_ENDPOINT"
echo "  CONTAINER_REGISTRY=$CONTAINER_REGISTRY"
echo ""
echo "Config files created:"
echo "  ~/.pip/pip.conf"
echo "  ~/.m2/settings.xml"
echo "  ~/.gradle/init.gradle"
echo "  ~/.cargo/config.toml"
