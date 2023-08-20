//! # Source Table
//!
//! This module defines an enum whose variants represent different types of source tables.
//! They all share the following behaviour:
//! - A table is registered in the Anasto catalog when it is created. This catalog holds the tables name, schema and destinations
//! - The SourceTable enum enforces shared behaviour by requiring each table to have variants of certain functions
//! - Each SourceTable variant is defined in it's own sub-module
//! - Sub-modules also define the configuration logic for each table

use crate::startup::Catalog;

// standard, dashmap, tokio and datafusion crates 
use log::{ info, error };
use std::io::{ Error, ErrorKind };
use serde_derive::{ Serialize, Deserialize };
use datafusion::dataframe::DataFrame;
use datafusion::common::DFSchema;
use std::time::Duration;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::time::{ interval, Interval };
use tokio::sync::mpsc::UnboundedSender;

// crates from the internal tables module
use crate::tables::source_files::SourceFiles;
use crate::tables::source_open_table::SourceOpenTable;
use crate::tables::FailAction;

/// The variants in this enum represent all the different source tables that Anasto can process.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag="type", rename_all="snake_case", deny_unknown_fields)]
pub enum SourceTable {
   
   /// This source table reads files from a local or remote filesystem
   Files(SourceFiles),

   /// This source table reads files from Open Table format databases like Delta Lake
   OpenTable(SourceOpenTable),

}


impl SourceTable {

	/// Spawn an async task that periodically reads new data from the source dataset
    pub async fn start(&mut self, catalog: Catalog) -> JoinHandle<()> {

        // Create copies of the pointers needed for this task
        let mut table = self.clone();

        // Add the table to the catalog. Only continue if another 
        // table with the same name doesn't already exist
        if table.register(catalog.clone()).is_err() {
            panic!("Failed to start a source table.");
        };

        spawn(async move {
            
            // Start the ticker to manage how often the poll is triggered
            let interval = interval(table.poll_interval());

            // Start polling the source dataset for new data
            info!(target: table.table_name(), "Starting to poll table {}.", table.table_name());
            table.start_polling(interval, catalog).await;

        })
        
    }

    /// The logic for polling a dataset for changes
    async fn start_polling(&mut self, mut interval: Interval, catalog: Catalog) {
        
        loop {
            
            // Wait for the tick before triggering the poll action
            let _ = interval.tick().await;
            
            // trigger the designated action
            match self.read_new_data().await {
                
                Ok((new_data, df)) => {
                    
                    if !new_data { continue }

                    match self.update_catalog(catalog.clone(), df.schema().clone()) {
                                    
                        Ok(dest_txs) => if let Err(e) = self.send_new_data(dest_txs, df) {
    
                            error!(target: self.table_name(), "{}", e.to_string());
    
                            // use the fail condition specified in the config to decide what to do with write errors
                            match self.on_fail() {
                                FailAction::Skip => continue,
                                FailAction::Stop => break,
                            }
    
                        },
                        
                        Err(_) => {
    
                            // use the fail condition  specified in the config to decide what to do with write errors
                            match self.on_fail() {
                                FailAction::Skip => continue,
                                FailAction::Stop => break,
                            }
    
                        }
    
                    }
                    
                },
                
                Err(e) => {
                    
                    error!(target: self.table_name(), "{}", e.to_string());
                
                    // use the fail condition  specified in the config to decide what to do with write errors
                    match self.on_fail() {
                        FailAction::Skip => continue,
                        FailAction::Stop => break,
                    }

                }

            };
        
        }
    }

    /// Get the name specified for the table
	fn table_name(&self) -> &String {
        match self {
            Self::Files(table) => table.table_name(),
            Self::OpenTable(table) => table.table_name(),
        }
    }

    // Get the frequency that the source should be polled
    fn poll_interval(&self) -> Duration {
        match self {
            Self::Files(table) => table.poll_interval(),
            Self::OpenTable(table) => table.poll_interval(),
        }
    }

    /// Add the table to the internal data catalog
    fn register(&self, catalog: Catalog) -> Result<(), Error> {
        
        let entry = (None, Vec::new());
        match catalog.insert(self.table_name().clone(), entry) {
            None => {
                info!(target: self.table_name(), "Source table registered in catalog.");
                Ok(())
            }
            Some(_) => {
                let error_message = "Source table already exists. Tables cannot have the same names.";
                error!(target: self.table_name(), "{}", &error_message);
                Err(Error::new(ErrorKind::Other, error_message))
            }
        }
        
    }

    /// The logic per table type for reading new data from source
    async fn read_new_data(&mut self) -> Result<(bool, DataFrame), Error> {
		match self {
			Self::Files(table) => table.read_new_data().await,
            Self::OpenTable(table) => table.read_new_data().await,
		}
	}

    /// Update the data catalog if the schema changes
    fn update_catalog(&self, catalog: Catalog, schema: DFSchema) -> Result<Vec<UnboundedSender<DataFrame>>, Error> {
        
        if let Some(mut entry) = catalog.clone().get_mut(self.table_name()) {
            
            let (old_schema, destinations) = entry.clone();

            match old_schema {
                
                // check new schema against old schema for compatibility
                Some(mut old_schema) => {
                    old_schema.merge(&schema);
                    entry.0 = Some(old_schema); 
                },

                // if schema does not yet exist, create it
                None => {
                    entry.0 = Some(schema);
                },
            };

            Ok(destinations)
            
        }
        else {
            
            self.register(catalog)?;
            Ok(Vec::new())

        }


    }

    /// Send new data to the destination tables who want this table
    fn send_new_data(
        &self, 
        destinations: Vec<UnboundedSender<DataFrame>>, 
        df: DataFrame
    ) -> Result<(), Error> {
        
        destinations.iter().try_for_each(|tx| {

            match tx.send(df.clone()) {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::new(ErrorKind::Other, e.to_string()))
            }

        })
            
    }

    /// Define what to do if something goes wrong
	fn on_fail(&self) -> FailAction {
		match self {
			Self::Files(table) => table.on_fail(),
            Self::OpenTable(table) => table.on_fail(),
		}
	}

}