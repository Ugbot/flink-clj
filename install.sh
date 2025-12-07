#!/usr/bin/env bash
#
# flink-clj installer
#
# Usage:
#   curl -sL https://raw.githubusercontent.com/Ugbot/flink-clj/main/install.sh | bash
#
# Or with custom version:
#   FLINK_CLJ_VERSION=0.2.0 curl -sL .../install.sh | bash
#
# Configuration via environment variables:
#   FLINK_CLJ_VERSION  - Version to install (default: 0.1.0)
#   FLINK_CLJ_HOME     - Installation directory (default: ~/.flink-clj)
#   INSTALL_DIR        - Directory for wrapper script (default: /usr/local/bin)

set -e

# Configuration
FLINK_CLJ_VERSION="${FLINK_CLJ_VERSION:-0.1.0}"
FLINK_CLJ_HOME="${FLINK_CLJ_HOME:-$HOME/.flink-clj}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
GITHUB_REPO="Ugbot/flink-clj"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_banner() {
    echo
    echo -e "${BLUE}╔═══════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}     flink-clj Installer v${FLINK_CLJ_VERSION}            ${BLUE}║${NC}"
    echo -e "${BLUE}║${NC}     Clojure CLI for Apache Flink         ${BLUE}║${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════╝${NC}"
    echo
}

check_requirements() {
    echo "Checking requirements..."

    # Check Java
    if ! command -v java &> /dev/null; then
        echo -e "${RED}Error: Java is not installed${NC}"
        echo
        echo "Install Java 11 or later:"
        echo "  macOS:  brew install openjdk@11"
        echo "  Ubuntu: sudo apt install openjdk-11-jdk"
        echo "  CentOS: sudo yum install java-11-openjdk"
        exit 1
    fi

    local java_version=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    echo "  Java: version $java_version ✓"

    # Check curl or wget
    if command -v curl &> /dev/null; then
        DOWNLOAD_CMD="curl -fsSL"
        DOWNLOAD_OUT="-o"
        echo "  curl: available ✓"
    elif command -v wget &> /dev/null; then
        DOWNLOAD_CMD="wget -q"
        DOWNLOAD_OUT="-O"
        echo "  wget: available ✓"
    else
        echo -e "${RED}Error: curl or wget required${NC}"
        exit 1
    fi

    echo
}

create_directories() {
    echo "Creating directories..."
    mkdir -p "$FLINK_CLJ_HOME"
    echo "  $FLINK_CLJ_HOME ✓"

    # Check if we can write to INSTALL_DIR
    if [[ ! -w "$INSTALL_DIR" ]]; then
        echo -e "${YELLOW}Note: $INSTALL_DIR requires sudo access${NC}"
        NEED_SUDO=1
    fi
    echo
}

download_cli_jar() {
    echo "Downloading CLI JAR..."
    local jar_url="https://github.com/${GITHUB_REPO}/releases/download/v${FLINK_CLJ_VERSION}/flink-clj-cli.jar"

    if $DOWNLOAD_CMD "$jar_url" $DOWNLOAD_OUT "${FLINK_CLJ_HOME}/flink-clj-cli.jar" 2>/dev/null; then
        echo "  Downloaded flink-clj-cli.jar ✓"
    else
        echo -e "${YELLOW}Warning: Could not download from releases${NC}"
        echo "  Will use wrapper to build from source on first run"
    fi
    echo
}

download_wrapper() {
    echo "Installing wrapper script..."
    local wrapper_url="https://raw.githubusercontent.com/${GITHUB_REPO}/v${FLINK_CLJ_VERSION}/bin/flink-clj"

    # Download to temp location first
    local temp_wrapper=$(mktemp)
    if $DOWNLOAD_CMD "$wrapper_url" $DOWNLOAD_OUT "$temp_wrapper" 2>/dev/null; then
        chmod +x "$temp_wrapper"

        # Install to final location
        if [[ "${NEED_SUDO:-}" == "1" ]]; then
            sudo mv "$temp_wrapper" "${INSTALL_DIR}/flink-clj"
        else
            mv "$temp_wrapper" "${INSTALL_DIR}/flink-clj"
        fi
        echo "  Installed ${INSTALL_DIR}/flink-clj ✓"
    else
        # Fall back to embedded wrapper
        echo "  Creating wrapper script..."
        cat > "$temp_wrapper" << 'WRAPPER'
#!/usr/bin/env bash
FLINK_CLJ_HOME="${FLINK_CLJ_HOME:-$HOME/.flink-clj}"
FLINK_CLJ_JAR="${FLINK_CLJ_HOME}/flink-clj-cli.jar"
if [[ ! -f "$FLINK_CLJ_JAR" ]]; then
    echo "Error: flink-clj CLI JAR not found at $FLINK_CLJ_JAR"
    echo "Please download from: https://github.com/Ugbot/flink-clj/releases"
    exit 1
fi
exec java -jar "$FLINK_CLJ_JAR" "$@"
WRAPPER
        chmod +x "$temp_wrapper"
        if [[ "${NEED_SUDO:-}" == "1" ]]; then
            sudo mv "$temp_wrapper" "${INSTALL_DIR}/flink-clj"
        else
            mv "$temp_wrapper" "${INSTALL_DIR}/flink-clj"
        fi
        echo "  Created wrapper script ✓"
    fi
    echo
}

verify_installation() {
    echo "Verifying installation..."

    if command -v flink-clj &> /dev/null; then
        echo "  flink-clj command available ✓"
    else
        echo -e "${YELLOW}Note: You may need to add ${INSTALL_DIR} to your PATH${NC}"
        echo
        echo "Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
        echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
    fi
    echo
}

print_success() {
    echo -e "${GREEN}═══════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Installation complete!${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════${NC}"
    echo
    echo "Get started:"
    echo
    echo "  # Create a new project"
    echo "  flink-clj new my-pipeline --template etl"
    echo
    echo "  # Start REPL"
    echo "  cd my-pipeline"
    echo "  flink-clj repl"
    echo
    echo "  # Build and deploy"
    echo "  flink-clj build"
    echo "  flink-clj deploy target/my-pipeline.jar"
    echo
    echo "For help:"
    echo "  flink-clj help"
    echo "  flink-clj <command> --help"
    echo
}

main() {
    print_banner
    check_requirements
    create_directories
    download_cli_jar
    download_wrapper
    verify_installation
    print_success
}

main "$@"
