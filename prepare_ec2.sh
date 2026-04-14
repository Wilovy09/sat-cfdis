#!/usr/bin/env bash
set -e

PROJECT_DIR="${1:-/arena/sat-cfdis}"

sudo apt update
sudo apt install -y php8.3-cli php8.3-curl php8.3-gd php8.3-mbstring php8.3-zip php8.3-xml php8.3-ffi openssl

echo "ffi.enable=true" | sudo tee /etc/php/8.3/cli/conf.d/10-ffi.ini

cd "$PROJECT_DIR/php-cli"
php -r "require 'vendor/autoload.php'; OnnxRuntime\Vendor::check();"

if ! command -v cargo &>/dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
fi

cd "$PROJECT_DIR"
cargo build --release
