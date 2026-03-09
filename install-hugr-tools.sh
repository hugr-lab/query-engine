#!/usr/bin/env bash
set -euo pipefail

REPO="hugr-lab/query-engine"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
BINARY_NAME="hugr-tools"

# --- Helper functions ---

info() {
  printf '[info] %s\n' "$@"
}

error() {
  printf '[error] %s\n' "$@" >&2
  exit 1
}

# HTTP GET that works with curl or wget
http_get() {
  local url="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- "$url"
  else
    error "Neither curl nor wget found. Please install one of them."
  fi
}

# HTTP download to file
http_download() {
  local url="$1"
  local dest="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "$dest" "$url"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$dest" "$url"
  else
    error "Neither curl nor wget found. Please install one of them."
  fi
}

# --- Detect OS ---

detect_os() {
  local os
  os="$(uname -s)"
  case "$os" in
    Linux)   echo "linux" ;;
    Darwin)  echo "darwin" ;;
    MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
    *)       error "Unsupported operating system: $os" ;;
  esac
}

# --- Detect Architecture ---

detect_arch() {
  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64)  echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *)             error "Unsupported architecture: $arch" ;;
  esac
}

# --- Get latest hugr-tools release tag ---

get_latest_tag() {
  local tags_json
  tags_json="$(http_get "https://api.github.com/repos/${REPO}/releases")"

  # Find the latest release whose tag starts with hugr-tools-v
  local tag
  tag="$(printf '%s' "$tags_json" \
    | grep -o '"tag_name":\s*"hugr-tools-v[^"]*"' \
    | head -1 \
    | sed 's/"tag_name":\s*"//;s/"//')"

  if [ -z "$tag" ]; then
    error "Could not find any hugr-tools-v* release in ${REPO}."
  fi

  echo "$tag"
}

# --- Main ---

main() {
  local os arch tag version asset_name download_url tmp_dir

  os="$(detect_os)"
  arch="$(detect_arch)"

  info "Detected OS: ${os}, Arch: ${arch}"

  # Validate supported combinations
  if [ "$os" = "darwin" ] && [ "$arch" = "amd64" ]; then
    error "No darwin-amd64 build available. macOS builds are arm64 only."
  fi
  if [ "$os" = "windows" ] && [ "$arch" = "arm64" ]; then
    error "No windows-arm64 build available."
  fi

  info "Fetching latest hugr-tools release..."
  tag="$(get_latest_tag)"
  version="${tag#hugr-tools-}"
  info "Latest version: ${version}"

  # Determine asset name
  if [ "$os" = "windows" ]; then
    asset_name="${BINARY_NAME}-${os}-${arch}.exe"
  else
    asset_name="${BINARY_NAME}-${os}-${arch}"
  fi

  download_url="https://github.com/${REPO}/releases/download/${tag}/${asset_name}"
  info "Downloading ${download_url}..."

  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' EXIT

  http_download "$download_url" "${tmp_dir}/${asset_name}"

  # Install
  info "Installing to ${INSTALL_DIR}/${BINARY_NAME}..."
  mkdir -p "$INSTALL_DIR"

  if [ "$os" = "windows" ]; then
    cp "${tmp_dir}/${asset_name}" "${INSTALL_DIR}/${BINARY_NAME}.exe"
  else
    cp "${tmp_dir}/${asset_name}" "${INSTALL_DIR}/${BINARY_NAME}"
    chmod +x "${INSTALL_DIR}/${BINARY_NAME}"
  fi

  info "hugr-tools ${version} installed successfully to ${INSTALL_DIR}/${BINARY_NAME}"
  info "Run 'hugr-tools --help' to get started."
}

main "$@"
