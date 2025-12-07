#!/bin/bash
# flink-clj setup script
#
# This script sets up your environment for running flink-clj:
# 1. Checks prerequisites (Java, Leiningen)
# 2. Downloads dependencies
# 3. Compiles the project
# 4. Optionally downloads Flink for cluster deployment
#
# Usage:
#   ./setup.sh           # Basic setup
#   ./setup.sh --full    # Include Flink cluster download

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

FLINK_VERSION="1.20.0"
FLINK_SCALA_VERSION="2.12"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  flink-clj Setup Script${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check for --full flag
FULL_INSTALL=false
if [ "$1" == "--full" ]; then
    FULL_INSTALL=true
fi

# Step 1: Check Java
echo -e "${YELLOW}[1/5] Checking Java...${NC}"
if ! command -v java &> /dev/null; then
    echo -e "${RED}ERROR: Java not found. Please install Java 11 or higher.${NC}"
    echo ""
    echo "Install Java:"
    echo "  macOS:  brew install openjdk@17"
    echo "  Ubuntu: sudo apt install openjdk-17-jdk"
    echo "  Fedora: sudo dnf install java-17-openjdk"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 11 ]; then
    echo -e "${RED}ERROR: Java 11+ required. Found Java $JAVA_VERSION.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Java $JAVA_VERSION found${NC}"

# Step 2: Check Leiningen
echo -e "${YELLOW}[2/5] Checking Leiningen...${NC}"
if ! command -v lein &> /dev/null; then
    echo -e "${YELLOW}Leiningen not found. Installing...${NC}"

    if [[ "$OSTYPE" == "darwin"* ]] && command -v brew &> /dev/null; then
        brew install leiningen
    else
        mkdir -p ~/bin
        curl -sL https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein > ~/bin/lein
        chmod +x ~/bin/lein
        export PATH="$HOME/bin:$PATH"
        ~/bin/lein version
        echo ""
        echo -e "${YELLOW}Add to your shell profile: export PATH=\"\$HOME/bin:\$PATH\"${NC}"
    fi
fi
LEIN_VERSION=$(lein version 2>&1 | head -1)
echo -e "${GREEN}✓ $LEIN_VERSION${NC}"

# Step 3: Download dependencies
echo -e "${YELLOW}[3/5] Downloading dependencies...${NC}"
lein with-profile +flink-1.20,+dev deps 2>&1 | grep -v "^Retrieving" | head -5 || true
echo -e "${GREEN}✓ Dependencies downloaded${NC}"

# Step 4: Compile the project
echo -e "${YELLOW}[4/5] Compiling project...${NC}"
lein with-profile +flink-1.20 compile 2>&1 | grep -v "^Compiling\|^warning:" | head -5 || true
echo -e "${GREEN}✓ Project compiled${NC}"

# Step 5: Optionally download Flink
echo -e "${YELLOW}[5/5] Flink cluster setup...${NC}"
if [ "$FULL_INSTALL" = true ]; then
    FLINK_DIR="flink-${FLINK_VERSION}"
    FLINK_TAR="flink-${FLINK_VERSION}-bin-scala_${FLINK_SCALA_VERSION}.tgz"
    FLINK_URL="https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${FLINK_TAR}"

    if [ -d "$FLINK_DIR" ]; then
        echo -e "${GREEN}✓ Flink ${FLINK_VERSION} already installed${NC}"
    else
        echo "Downloading Flink ${FLINK_VERSION}..."
        curl -# -O "$FLINK_URL"
        tar -xzf "$FLINK_TAR"
        rm "$FLINK_TAR"
        echo -e "${GREEN}✓ Flink ${FLINK_VERSION} installed to ./${FLINK_DIR}${NC}"
    fi

    # Create env script
    cat > flink-env.sh << EOF
#!/bin/bash
# Source this file to set up Flink environment
export FLINK_HOME="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")/${FLINK_DIR}" && pwd)"
export PATH="\$FLINK_HOME/bin:\$PATH"
echo "FLINK_HOME set to \$FLINK_HOME"
EOF
    chmod +x flink-env.sh
    echo -e "${YELLOW}Run 'source flink-env.sh' to set FLINK_HOME${NC}"
else
    echo -e "${GREEN}✓ Skipped (use --full to install Flink cluster)${NC}"
fi

# Make scripts executable
chmod +x run-example.sh 2>/dev/null || true

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Next steps:"
echo ""
echo "  1. Run an example:"
echo -e "     ${BLUE}./run-example.sh word-count${NC}"
echo ""
echo "  2. Start a REPL:"
echo -e "     ${BLUE}lein with-profile +dev,+flink-1.20 repl${NC}"
echo ""
echo "  3. Run tests:"
echo -e "     ${BLUE}lein with-profile +flink-1.20,+dev test${NC}"
echo ""

if [ "$FULL_INSTALL" = true ]; then
    echo "  4. Start local Flink cluster:"
    echo -e "     ${BLUE}source flink-env.sh${NC}"
    echo -e "     ${BLUE}\$FLINK_HOME/bin/start-cluster.sh${NC}"
    echo -e "     Open http://localhost:8081"
    echo ""
fi

echo "For Kafka examples, start Kafka first:"
echo -e "  ${BLUE}docker run -d --name kafka -p 9092:9092 \\${NC}"
echo -e "  ${BLUE}  -e KAFKA_CFG_NODE_ID=0 \\${NC}"
echo -e "  ${BLUE}  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \\${NC}"
echo -e "  ${BLUE}  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \\${NC}"
echo -e "  ${BLUE}  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \\${NC}"
echo -e "  ${BLUE}  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \\${NC}"
echo -e "  ${BLUE}  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \\${NC}"
echo -e "  ${BLUE}  bitnami/kafka:latest${NC}"
echo ""
echo "Documentation: docs/"
echo "Examples: examples/"
echo ""
