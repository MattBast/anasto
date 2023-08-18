//! # Destination Table
//!
//! This module defines an enum whose variants represent different types of destination tables.
//! They all share the following behaviour:
//! - A table listens for new data on tokio mpsc channel
//! - This channel is added to the internal data catalog against the source table that supplies this table

use crate::startup::Catalog;

// standard, dashmap, tokio and datafusion crates 
use log::{ info, error };
use std::io::{ Error, ErrorKind };
use serde_derive::{ Serialize, Deserialize };
use dashmap::mapref::entry::Entry;
use datafusion::dataframe::DataFrame;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::{ UnboundedSender, unbounded_channel };

// crates from the internal tables module
use crate::tables::dest_files::DestFile;
use crate::tables::dest_open_table::DestOpenTable;
use crate::tables::FailAction;

/// The variants in this enum represent all the different destination tables that Anasto can process.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag="type", rename_all="snake_case")]
pub enum DestTable {
   
   /// This destination table writes files to a local or remote filesystem
   Files(DestFile),

   /// This destination table writes files to Open Table format databases like Delta Lake
   OpenTable(DestOpenTable),

}


impl DestTable {

	/// Spawn an async task that periodically reads new data from the source dataset
    pub async fn start(&mut self, catalog: Catalog) -> JoinHandle<()> {

        // create copies of the pointers needed for this task
        let mut table = self.clone();

        // create a channel
        let (tx, mut rx) = unbounded_channel();

        spawn(async move {

            // add the table to the catalog
            match table.register(catalog.clone(), tx) {
                Ok(_) => (),
                Err(_) => return,
            };

            info!(target: table.dest_table_name(), "Starting to listen for new data from table {}.", table.src_table_name());

            // start listening for new data from the source table
            while let Some(df) = rx.recv().await {
                // trigger the designated action
                match table.write_new_data(df).await {
                    
                    Ok(_) => (),
                    
                    Err(e) => {
                        
                        error!(target: table.dest_table_name(), "{}", e.to_string());
                    
                        // use the fail condition  specified in the config to decide what to do with write errors
                        match table.on_fail() {
                            FailAction::Skip => continue,
                            FailAction::Stop => break,
                        }

                    }

                }
            };

        })
        
    }

    /// Get the name of the source table that supplies this destination
	fn src_table_name(&self) -> &String {
        match self {
            Self::Files(table) => table.src_table_name(),
            Self::OpenTable(table) => table.src_table_name(),
        }
    }

    /// Get the name specified for this destination
    fn dest_table_name(&self) -> &String {
        match self {
            Self::Files(table) => table.dest_table_name(),
            Self::OpenTable(table) => table.dest_table_name(),
        }
    }

    /// Add the table to the internal data catalog
    fn register(&self, catalog: Catalog, tx: UnboundedSender<DataFrame>,) -> Result<(), Error> {
        
        match catalog.entry(self.src_table_name().to_string()) {
            
            Entry::Occupied(mut entry) => { 
                entry.get_mut().1.push(tx);
                Ok(())
            },

            Entry::Vacant(_) => {
                let error_message = format!("Source table {} could not be found in catalog.", self.src_table_name());
                error!(target: self.dest_table_name(), "{}", &error_message);
                Err(Error::new(ErrorKind::Other, error_message))
            },
        
        }

    }

    /// The logic per table type for writing new data to the destination
    async fn write_new_data(&mut self, df: DataFrame) -> Result<(), Error> {
		match self {
			Self::Files(table) => table.write_new_data(df).await,
            Self::OpenTable(table) => table.write_new_data(df).await,
		}
	}

    /// Define what to do if something goes wrong
	fn on_fail(&self) -> FailAction {
		match self {
			Self::Files(table) => table.on_fail(),
            Self::OpenTable(table) => table.on_fail(),
		}
	}

}