//! Poll
//!
//! Start the source tables polling their data sources and sending the new
//! data to the specified destination tables. Also start the destination tables
//! listening for new data.

use crate::startup::config::Config;
use std::sync::Arc;
use dashmap::DashMap;
use datafusion::common::DFSchema;
use datafusion::prelude::DataFrame;
use tokio::sync::mpsc::UnboundedSender;

pub async fn start(
	config: Config,
	catalog: Arc<DashMap<String, (Option<DFSchema>, Vec<UnboundedSender<DataFrame>>)>>
) {

	let handles = Vec::new();

	for table in config.source_table {
		let handle = table.start(catalog.clone());
		handles.push(handle);
	}

	for table in config.dest_table {
		let handle = table.start();
		handles.push(handle);
	}

	for handle in handles {
		handle.await;
	}

}