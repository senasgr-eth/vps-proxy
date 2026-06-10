#!/usr/bin/env bash

set -euo pipefail

# Output directory
BUILD_DIR="build"

# Find Go binary
GO_BIN="go"
if ! command -v go &> /dev/null; then
    # Check common absolute path from user environment
    if [ -x "/usr/local/go/bin/go" ]; then
        GO_BIN="/usr/local/go/bin/go"
    else
        echo "Go compiler not found. Autodetecting package manager to install Go..."
        if [ -x "$(command -v apt-get)" ]; then
            echo "Installing Go using apt-get..."
            sudo apt-get update && sudo apt-get install -y golang-go
        elif [ -x "$(command -v dnf)" ]; then
            echo "Installing Go using dnf..."
            sudo dnf install -y golang
        elif [ -x "$(command -v yum)" ]; then
            echo "Installing Go using yum..."
            sudo yum install -y golang
        elif [ -x "$(command -v pacman)" ]; then
            echo "Installing Go using pacman..."
            sudo pacman -Syu --noconfirm go
        else
            echo "Error: Package manager not supported. Please install Go manually: https://go.dev/doc/install"
            exit 1
        fi
        
        # Verify install
        if command -v go &> /dev/null; then
            GO_BIN="go"
            echo "Go successfully installed!"
        else
            echo "Error: Go installation succeeded but binary was not found in PATH."
            exit 1
        fi
    fi
fi


show_help() {
    echo "Go Stratum TCP Proxy Build Script"
    echo "Usage: ./build.sh [option]"
    echo ""
    echo "Options:"
    echo "  (no arguments)  Build for your current OS and architecture (placed in $BUILD_DIR/bin/)"
    echo "  all             Build for both Linux AMD64 and Linux ARM64 (placed in $BUILD_DIR/linux-amd64/ and $BUILD_DIR/linux-arm64/)"
    echo "  clean           Clean up the build directory"
    echo "  help            Show this help menu"
    echo ""
}

clean_build() {
    echo "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
    echo "Done."
}

build_current() {
    local os
    local arch
    os=$("$GO_BIN" env GOOS)
    arch=$("$GO_BIN" env GOARCH)
    echo "Building for current system ($os-$arch)..."
    mkdir -p "$BUILD_DIR/bin"
    
    echo "Building stratum-proxy..."
    "$GO_BIN" build -ldflags="-s -w" -o "$BUILD_DIR/bin/stratum-proxy" main.go
    
    echo "Building stratum-agent..."
    "$GO_BIN" build -ldflags="-s -w" -o "$BUILD_DIR/bin/stratum-agent" agent/main.go
    
    echo "Successfully built binaries in $BUILD_DIR/bin/"
    ls -lh "$BUILD_DIR/bin"
}

build_all() {
    echo "Building for Linux AMD64..."
    mkdir -p "$BUILD_DIR/linux-amd64"
    GOOS=linux GOARCH=amd64 "$GO_BIN" build -ldflags="-s -w" -o "$BUILD_DIR/linux-amd64/stratum-proxy" main.go
    GOOS=linux GOARCH=amd64 "$GO_BIN" build -ldflags="-s -w" -o "$BUILD_DIR/linux-amd64/stratum-agent" agent/main.go

    echo "Building for Linux ARM64..."
    mkdir -p "$BUILD_DIR/linux-arm64"
    GOOS=linux GOARCH=arm64 "$GO_BIN" build -ldflags="-s -w" -o "$BUILD_DIR/linux-arm64/stratum-proxy" main.go
    GOOS=linux GOARCH=arm64 "$GO_BIN" build -ldflags="-s -w" -o "$BUILD_DIR/linux-arm64/stratum-agent" agent/main.go

    echo "Successfully built cross-compiled binaries:"
    echo " - AMD64: $BUILD_DIR/linux-amd64/"
    echo " - ARM64: $BUILD_DIR/linux-arm64/"
    ls -lh "$BUILD_DIR/linux-amd64" "$BUILD_DIR/linux-arm64"
}

# Main execution logic
if [ $# -eq 0 ]; then
    build_current
elif [ "$1" = "all" ]; then
    build_all
elif [ "$1" = "clean" ]; then
    clean_build
elif [ "$1" = "help" ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
else
    echo "Unknown option: $1"
    show_help
    exit 1
fi
