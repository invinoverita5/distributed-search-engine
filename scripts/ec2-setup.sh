#!/bin/bash

# Setup script for EC2 instances - installs Eclipse Temurin JDK 23

TEMURIN_VERSION="23.0.2"
TEMURIN_BUILD="7"

echo "=== EC2 Instance Setup ==="
echo ""

# Install curl if not present
if ! command -v curl &> /dev/null; then
    echo "Installing curl..."
    if command -v apt-get &> /dev/null; then
        sudo apt-get update -y && sudo apt-get install -y curl
    elif command -v yum &> /dev/null; then
        sudo yum install -y curl
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y curl
    fi
fi

# Detect architecture
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
    TEMURIN_ARCH="x64"
elif [ "$ARCH" = "aarch64" ]; then
    TEMURIN_ARCH="aarch64"
else
    echo "ERROR: Unsupported architecture: $ARCH"
    exit 1
fi

echo "Detected architecture: $ARCH ($TEMURIN_ARCH)"
echo "Installing Eclipse Temurin JDK ${TEMURIN_VERSION}+${TEMURIN_BUILD}..."
echo ""

# Download and install Eclipse Temurin
TEMURIN_URL="https://github.com/adoptium/temurin23-binaries/releases/download/jdk-${TEMURIN_VERSION}%2B${TEMURIN_BUILD}/OpenJDK23U-jdk_${TEMURIN_ARCH}_linux_hotspot_${TEMURIN_VERSION}_${TEMURIN_BUILD}.tar.gz"
INSTALL_DIR="/opt/temurin-${TEMURIN_VERSION}"

echo "Downloading from: $TEMURIN_URL"
curl -fLo /tmp/temurin.tar.gz "$TEMURIN_URL"

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to download Temurin JDK"
    exit 1
fi

echo "Extracting to ${INSTALL_DIR}..."
sudo mkdir -p "$INSTALL_DIR"
sudo tar -xzf /tmp/temurin.tar.gz -C "$INSTALL_DIR" --strip-components=1
rm /tmp/temurin.tar.gz

# Set up environment
echo "Configuring environment..."
echo "export JAVA_HOME=${INSTALL_DIR}" | sudo tee /etc/profile.d/temurin.sh
echo 'export PATH=$JAVA_HOME/bin:$PATH' | sudo tee -a /etc/profile.d/temurin.sh
sudo chmod +x /etc/profile.d/temurin.sh

# Apply to current session
export JAVA_HOME=${INSTALL_DIR}
export PATH=$JAVA_HOME/bin:$PATH

echo ""

# Verify installation
if [ -x "${INSTALL_DIR}/bin/javac" ]; then
    echo "Java JDK installed successfully!"
    echo "   javac version: $(${INSTALL_DIR}/bin/javac -version 2>&1)"
    echo "   java version:  $(${INSTALL_DIR}/bin/java -version 2>&1 | head -1)"
else
    echo "ERROR: javac not found after installation"
    exit 1
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "NOTE: Run 'source /etc/profile.d/temurin.sh' or log out/in to use java/javac"
echo ""
echo "You can now run:"
echo "  ./scripts/ec2-coordinator.sh    # On coordinator node"
echo "  ./scripts/ec2-worker.sh <coordinator-ip> <worker-id>    # On worker nodes"
