//! Poll
//!
//! Start the source tables polling their data sources and sending the new
//! data to the specified destination tables. Also start the destination tables
//! listening for new data.

use crate::startup::Catalog;
use crate::startup::config::Config;

/// Start the source tables polling their source datasets and the destination tables
/// listening for new data from the sources
pub async fn start(config: Config, catalog: Catalog) {

	let mut handles = Vec::new();

	for mut table in config.source_table {
		let handle = table.start(catalog.clone()).await;
		handles.push(handle);
	}

	for mut table in config.destination_table {
		let handle = table.start(catalog.clone()).await;
		handles.push(handle);
	}

	for handle in handles {
		let _ = handle.await;
	}

}