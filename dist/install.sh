#!/bin/bash
set -euo pipefail

VERSION="${1:-latest}"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
  x86_64) ARCH="x86_64" ;;
  aarch64|arm64) ARCH="aarch64" ;;
  *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

case "$OS" in
  linux) TARGET="${ARCH}-unknown-linux-gnu" ;;
  darwin) TARGET="${ARCH}-apple-darwin" ;;
  *) echo "Unsupported OS: $OS"; exit 1 ;;
esac

if [ "$VERSION" = "latest" ]; then
  URL="https://github.com/fab679/graphmind/releases/latest/download/graphmind-${TARGET}.tar.gz"
else
  URL="https://github.com/fab679/graphmind/releases/download/${VERSION}/graphmind-${VERSION}-${TARGET}.tar.gz"
fi

echo "Downloading Graphmind for ${TARGET}..."
TMPDIR=$(mktemp -d)
curl -sL "$URL" | tar xz -C "$TMPDIR"

echo "Installing to /usr/local/bin..."
sudo install -m 755 "$TMPDIR/graphmind" /usr/local/bin/graphmind
[ -f "$TMPDIR/graphmind-cli" ] && sudo install -m 755 "$TMPDIR/graphmind-cli" /usr/local/bin/graphmind-cli

# Create config directory
sudo mkdir -p /etc/graphmind
if [ ! -f /etc/graphmind/config.toml ]; then
  sudo cp "$TMPDIR/graphmind.conf.example" /etc/graphmind/config.toml 2>/dev/null || true
fi

# Create data directory
sudo mkdir -p /var/lib/graphmind
sudo useradd -r -s /bin/false graphmind 2>/dev/null || true
sudo chown graphmind:graphmind /var/lib/graphmind

rm -rf "$TMPDIR"

echo ""
echo "Graphmind installed successfully!"
echo ""
echo "  Start server:  graphmind --config /etc/graphmind/config.toml"
echo "  CLI:           graphmind-cli status"
echo "  Visualizer:    http://localhost:8080"
echo ""
