//! Startup
//!
//! Reads the toml config file that the user provided. Uses it to decide
//! what source tables to listen to and what destination tables to write to.

use dashmap::DashMap;

pub mod logger;
pub mod config;
pub mod poll;

/// Start Anasto
pub async fn start() {
	logger::start();
	let config = config::get();
	let catalog = DashMap::new();
	poll::start(config, catalog.into()).await;
}