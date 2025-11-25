#!/usr/bin/env bash
set -euo pipefail

# Simple build helper for the gRPC + Crow server.
# Run from repo root: ./build.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# Ensure Conan profiles exist (no-op if already set up)
conan profile detect --force >/dev/null

# Install dependencies and generate toolchain/presets
conan install . --output-folder=build --build=missing

# Configure and build via generated preset
cmake --preset conan-release
cmake --build --preset conan-release
