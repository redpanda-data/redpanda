#!/bin/bash
set -e

echo "=== Redpanda Development Environment Bootstrap ==="

# Get the repo root (parent of scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Build root - all tools go here
BUILD_ROOT="${BUILD_ROOT:-$REPO_ROOT/vbuild}"
PROTO_HOME="$BUILD_ROOT/proto"
BAZELISK_VERSION="1.25.0"
BAZELISK_INSTALL_DIR="$BUILD_ROOT/bazelisk/$BAZELISK_VERSION"

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
  x86_64) ARCH_SUFFIX="amd64" ;;
  aarch64) ARCH_SUFFIX="arm64" ;;
  *) echo "Unsupported architecture: $ARCH" && exit 1 ;;
esac

# 1. Minimal system dependencies
echo "Installing minimal system packages..."
if [ -f /etc/os-release ]; then
  source /etc/os-release
  case $ID in
    ubuntu | debian)
      sudo apt update
      sudo apt install -y curl git ca-certificates wget

      # Install Docker CE if not present
      if ! command -v docker &>/dev/null; then
        sudo install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/$ID/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        sudo chmod a+r /etc/apt/keyrings/docker.gpg
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/$ID $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
        sudo apt update
        sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        sudo usermod -aG docker $USER
      fi
      ;;
    fedora)
      sudo dnf install -y curl git ca-certificates dnf-plugins-core wget
      if ! command -v docker &>/dev/null; then
        sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
        sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        sudo systemctl enable --now docker
        sudo usermod -aG docker $USER
      fi
      ;;
    *)
      echo "Unsupported OS: $ID. Install curl, git, wget, docker CE manually."
      ;;
  esac
fi

# 2. Install bazelisk (Bazel version manager)
echo "Installing bazelisk to $BAZELISK_INSTALL_DIR..."
mkdir -p "$BAZELISK_INSTALL_DIR/bin"
if [ ! -f "$BAZELISK_INSTALL_DIR/bin/bazel" ]; then
  wget -q -O "$BAZELISK_INSTALL_DIR/bin/bazel" "https://github.com/bazelbuild/bazelisk/releases/download/v$BAZELISK_VERSION/bazelisk-linux-$ARCH_SUFFIX"
  chmod +x "$BAZELISK_INSTALL_DIR/bin/bazel"
fi
export PATH="$BAZELISK_INSTALL_DIR/bin:$PATH"

# 3. Install proto (toolchain manager) to vbuild/proto
export PROTO_HOME="$PROTO_HOME"
if [ ! -f "$PROTO_HOME/bin/proto" ]; then
  echo "Installing proto to $PROTO_HOME..."
  curl -fsSL https://moonrepo.dev/install/proto.sh | bash -s -- --yes --no-profile
fi
export PATH="$PROTO_HOME/bin:$PATH"

# 4. Install moon via proto
echo "Installing moon..."
proto install moon

# 5. Install all toolchains via proto (reads .prototools)
echo "Installing toolchains via proto..."
cd "$REPO_ROOT"
proto use

# 6. Setup PATH
echo ""
echo "=== Add to your ~/.bashrc or ~/.zshrc ==="
echo "# Redpanda development environment"
echo 'export PROTO_HOME="$HOME/src/redpanda/vbuild/proto"  # Adjust path to your repo'
cat <<'SHELL_CONFIG'
export PATH="$PROTO_HOME/shims:$PROTO_HOME/bin:$HOME/src/redpanda/vbuild/bazelisk/1.25.0/bin:$PATH"
SHELL_CONFIG
echo ""
echo "Or source the environment script:"
echo "  source $REPO_ROOT/scripts/env.sh"

# 7. Create env.sh for easy sourcing
cat >"$REPO_ROOT/scripts/env.sh" <<EOF
# Redpanda development environment
# Source this file: source scripts/env.sh

SCRIPT_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="\$(cd "\$SCRIPT_DIR/.." && pwd)"
BUILD_ROOT="\${BUILD_ROOT:-\$REPO_ROOT/vbuild}"

export PROTO_HOME="\$BUILD_ROOT/proto"
export PATH="\$PROTO_HOME/shims:\$PROTO_HOME/bin:\$BUILD_ROOT/bazelisk/1.25.0/bin:\$PATH"

# Rust toolchain (installed by proto)
if [ -d "\$PROTO_HOME/tools/rust" ]; then
  export PATH="\$PROTO_HOME/tools/rust/*/bin:\$PATH"
fi
EOF

# 8. Verify
echo ""
echo "Verifying installation..."
proto status
moon --version
go version
uv --version
"$PROTO_HOME/tools/rust/1.75.0/bin/cargo" --version || cargo --version 2>/dev/null || echo "cargo: check PATH after sourcing env.sh"
bazel --version

echo ""
echo "=== Bootstrap complete! ==="
echo "NOTE: Log out and back in for docker group to take effect"
echo ""
echo "To use the development environment, run:"
echo "  source $REPO_ROOT/scripts/env.sh"
echo ""
echo "Then run: moon run :build"
