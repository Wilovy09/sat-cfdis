//! L18-25 point 3: the control-company RFCs these test suites exercise used to be written
//! as literal string constants (10 occurrences across consistency_invariants.rs,
//! perf_budget.rs, and number_contract.rs) -- a rule-7 violation (no client data in code).
//! They're read from environment variables instead now, one per role, set in the local
//! `.env` that already carries every other test-environment secret this suite needs
//! (POSTGRES_*). A missing variable panics immediately with the variable's own name, rather
//! than silently skipping the RFC or passing on an empty case -- "si falta una variable, la
//! prueba que la usa aborta, no pasa" is this item's own explicit requirement.

/// Reads `env_var`, panicking with its name if unset -- never a default, never a silent skip.
pub fn control_rfc(env_var: &str) -> String {
    std::env::var(env_var).unwrap_or_else(|_| {
        panic!(
            "{env_var} is not set -- this test needs a real control-company RFC from the \
             test environment's own .env, not a literal in source (regla 7). Set it and rerun."
        )
    })
}
