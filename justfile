# L6C-01: explicit dotenv-path makes `just` abort before running ANY recipe if `.env` is
# missing (confirmed with just 1.21.0+: "error: Failed to load environment file") -- the repo
# ships .env.example but not .env, so a fresh checkout/container has nothing to load. Without
# dotenv-required := false, `just test` (which needs no env vars at all) would fail for a
# reason that has nothing to do with the tests themselves.
set dotenv-path := ".env"
set dotenv-required := false

default:
    cargo run

# static_invariants: pure source-text checks, no DB connection, safe on every push.
#
# consistency_invariants moved to test-db below (L6C-10): its two invariants used to be
# static-text/data-precondition checks (no DB needed), but L6C-10 redid them as live
# relational comparisons between real function calls (hallazgos::get, payroll::get,
# payroll::monthly_series) -- which need POSTGRES_*, same as everything else in test-db.
# The original L6C-01 split ("dos archivos no tocan la base") was written against the
# PREVIOUS version of that file; leaving it in `test` here would silently need a DB or
# panic in a container with none, which is exactly the failure L6C-01 exists to prevent.
test:
    cargo test --test static_invariants

# migration_guard + number_contract + perf_budget + consistency_invariants: need
# POSTGRES_* and hit the shared database (perf_budget seeds and deletes 60 rows;
# number_contract and consistency_invariants read real data) -- scheduled, not on every
# push, so it isn't writing to the shared DB several times a day.
test-db:
    cargo test --test migration_guard --test number_contract --test perf_budget --test consistency_invariants

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