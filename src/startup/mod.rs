//! Startup
//!
//! Reads the toml config file that the user provided. Uses it to decide
//! what source tables to listen to and what destination tables to write to.

use std::sync::Arc;
use dashmap::DashMap;
use datafusion::common::DFSchema;
use datafusion::prelude::DataFrame;
use tokio::sync::mpsc::UnboundedSender;

pub mod logger;
pub mod config;
pub mod poll;

/// Alias for the data catalog that Anasto maintains
pub type Catalog = Arc<DashMap<String, (Option<DFSchema>, Vec<UnboundedSender<DataFrame>>)>>;

/// Start Anasto
pub async fn start() {
	logger::start();
	let config = config::get();
	let catalog = DashMap::new();
	poll::start(config, catalog.into()).await;
}