---
sidebar_position: 2
title: Binary
description: Install Graphmind from a pre-built binary or cargo install
---

# Binary Installation

## Download from GitHub Releases

Download the latest release for your platform from [GitHub Releases](https://github.com/fab679/graphmind/releases).

### Linux (x86_64)

```bash
curl -LO https://github.com/fab679/graphmind/releases/download/v0.8.0/graphmind-v0.8.0-x86_64-unknown-linux-gnu.tar.gz
tar xzf graphmind-v0.8.0-x86_64-unknown-linux-gnu.tar.gz
sudo install -m 755 graphmind /usr/local/bin/graphmind
```

### macOS (Apple Silicon)

```bash
curl -LO https://github.com/fab679/graphmind/releases/download/v0.8.0/graphmind-v0.8.0-aarch64-apple-darwin.tar.gz
tar xzf graphmind-v0.8.0-aarch64-apple-darwin.tar.gz
sudo install -m 755 graphmind /usr/local/bin/graphmind
```

### macOS (Intel)

```bash
curl -LO https://github.com/fab679/graphmind/releases/download/v0.8.0/graphmind-v0.8.0-x86_64-apple-darwin.tar.gz
tar xzf graphmind-v0.8.0-x86_64-apple-darwin.tar.gz
sudo install -m 755 graphmind /usr/local/bin/graphmind
```

### Windows (x86_64)

Download and extract the zip from [GitHub Releases](https://github.com/fab679/graphmind/releases/download/v0.8.0/graphmind-v0.8.0-x86_64-pc-windows-msvc.zip), then add the `graphmind.exe` to your PATH.

### What's in the archive

The tarball/zip contains:

| File | Description |
|------|-------------|
| `graphmind` | The database binary |
| `config.toml` | Example configuration file |
| `install.sh` | Automated install script (Linux/macOS) |
| `graphmind.service` | systemd service file for Linux |
| `LICENSE` | Apache 2.0 license |
| `README.md` | Quick start guide |

### Quick install script

You can also use the included install script:

```bash
# After extracting the archive
chmod +x install.sh
sudo ./install.sh
```

This installs the binary to `/usr/local/bin/`, creates config at `/etc/graphmind/`, and optionally sets up the systemd service.

## Install with Cargo

If you have Rust installed:

```bash
cargo install graphmind
```

This compiles from source and installs the `graphmind` binary into `~/.cargo/bin/`.

### Prerequisites for Cargo Install

Graphmind depends on RocksDB, which requires a C++ compiler and CMake:

```bash
# Ubuntu/Debian
sudo apt-get install -y clang libclang-dev cmake

# macOS
brew install cmake llvm

# Fedora
sudo dnf install clang clang-devel cmake
```

## Verify Installation

```bash
graphmind --version
# graphmind 0.7.0
```

## Run the Server

```bash
# Start with defaults (RESP on :6379, HTTP on :8080)
graphmind

# Custom ports
graphmind --port 16379 --http-port 18080

# With a config file
graphmind --config /etc/graphmind/graphmind.toml

# With demo data
graphmind --demo social
```

## CLI Flags

| Flag                  | Description                          | Default            |
| --------------------- | ------------------------------------ | ------------------ |
| `--config <path>`     | Path to TOML config file             | `graphmind.toml`   |
| `--host <addr>`       | Bind address                         | `127.0.0.1`        |
| `--port <port>`       | RESP server port                     | `6379`             |
| `--http-port <port>`  | HTTP server port                     | `8080`             |
| `--data-dir <path>`   | Data storage directory               | `./graphmind_data` |
| `--log-level <level>` | Log level                            | `info`             |
| `--demo <mode>`       | Load demo data (`social` or `large`) | (none)             |

See [Configuration](configuration) for the full config file reference.

## Connect

Once the server is running:

```bash
# Web Visualizer
open http://localhost:8080

# Redis CLI
redis-cli -p 6379
127.0.0.1:6379> GRAPH.QUERY default "MATCH (n) RETURN count(n)"

# HTTP API
curl http://localhost:8080/api/status
```
