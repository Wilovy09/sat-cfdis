set dotenv-path := ".env"

default:
    cargo run

dev:
    cargo watch -x run

c:
    cargo check

l:
    cargo clippy --all-targets --all-features -- -D warnings

f:
    cargo fmt --all

fc:
    cargo fmt --all --check

prepare:
    cargo install cargo-watch