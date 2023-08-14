//! Logger
//!
//! Starts the logger so the other modules can produce logs.

use std::env;

/// Start logger so the other modules can produce logs
pub fn start() {

    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "info");
    }

    pretty_env_logger::init();

}