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
use crate::tables::source_api::SourceApi;
use crate::tables::FailAction;

/// The variants in this enum represent all the different source tables that Anasto can process.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag="type", rename_all="snake_case", deny_unknown_fields)]
pub enum SourceTable {
   
   /// This source table reads files from a local or remote filesystem
   Files(SourceFiles),

   /// This source table reads files from Open Table format databases like Delta Lake
   OpenTable(SourceOpenTable),

   /// This source table gets its data by calling an api
   Api(SourceApi),

}


impl SourceTable {

	/// Spawn an async task that periodically reads new data from the source dataset
    pub async fn start(&mut self, catalog: Catalog) -> JoinHandle<()> {

        // Create copies of the pointers needed for this task
        let mut table = self.clone();

        // Add the table to the catalog. Only continue if another 
        // table with the same name doesn't already exist
        if table.register(&catalog).is_err() {
            panic!("Failed to start a source table.");
        };

        match table.one_request() {
            true => info!(target: table.table_name(), "Single request mode on. Will read table only once."),
            false => info!(target: table.table_name(), "Single request mode off. Will continuously poll table for changes."), 
        }

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

                    match self.update_catalog(&catalog, df.schema()) {
                                    
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

            if self.one_request() {
                info!(target: self.table_name(), "Request made. Shutting down table.");
                break
            }
        
        }
    }

    /// Get the name specified for the table
	fn table_name(&self) -> &String {
        match self {
            Self::Files(table) => table.table_name(),
            Self::OpenTable(table) => table.table_name(),
            Self::Api(table) => table.table_name(),
        }
    }

    // Get the frequency that the source should be polled
    fn poll_interval(&self) -> Duration {
        match self {
            Self::Files(table) => table.poll_interval(),
            Self::OpenTable(table) => table.poll_interval(),
            Self::Api(table) => table.poll_interval(),
        }
    }

    /// Add the table to the internal data catalog
    fn register(&self, catalog: &Catalog) -> Result<(), Error> {
        
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
            Self::Api(table) => table.read_new_data().await,
		}
	}

    /// Update the data catalog if the schema changes
    fn update_catalog(&self, catalog: &Catalog, schema: &DFSchema) -> Result<Vec<UnboundedSender<DataFrame>>, Error> {
        
        if let Some(mut entry) = catalog.get_mut(self.table_name()) {
            
            let (old_schema, destinations) = entry.clone();

            match old_schema {
                
                // check new schema against old schema for compatibility
                Some(mut old_schema) => {
                    old_schema.merge(schema);
                    entry.0 = Some(old_schema); 
                },

                // if schema does not yet exist, create it
                None => {
                    entry.0 = Some(schema.clone());
                },
            };

            Ok(destinations)
            
        }
        else {
            
            self.register(catalog)?;
            self.update_catalog(catalog, schema)

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
            Self::Api(table) => table.on_fail(),
		}
	}

    /// State whether the source table should be read once (true) or polled (false)
    fn one_request(&self) -> bool {
        match self {
            Self::Files(table) => table.one_request,
            Self::OpenTable(table) => table.one_request,
            Self::Api(table) => table.one_request,
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use tokio::sync::mpsc::unbounded_channel;
    use arrow_schema::{ DataType, Field };

    #[tokio::test]
    async fn register_adds_table_to_catalog() {
    
        // create a mock data catalog for Anasto
        let catalog: Catalog = DashMap::new().into();

        // load a mock files type of source table  
        let table: SourceTable = toml::from_str(&String::from(r#"
            type = "files"
            table_name = "test_csv"
            dirpath = "./tests/data/csv_table/" 
        "#)).unwrap();

        // add the mock table to the catalog
        table.register(&catalog).unwrap();

        let (schema, destinations) = catalog.get("test_csv").unwrap().clone();
        assert_eq!(schema, None);
        assert!(destinations.is_empty());

    }

    #[tokio::test]
    async fn register_rejects_duplicate_table_names() {
    
        // create a mock data catalog for Anasto
        let catalog: Catalog = DashMap::new().into();

        // load a mock files type of source table  
        let table_one: SourceTable = toml::from_str(&String::from(r#"
            type = "files"
            table_name = "test_table"
            dirpath = "./tests/data/csv_table/" 
        "#)).unwrap();

        // create second table with the same name
        let table_two: SourceTable = toml::from_str(&String::from(r#"
            type = "files"
            table_name = "test_table"
            dirpath = "./tests/data/json_table/" 
            filetype = "json"
        "#)).unwrap();

        // add the mock table to the catalog
        let register_one_result = table_one.register(&catalog);
        let register_two_result = table_two.register(&catalog);

        assert!(register_one_result.is_ok());
        assert!(register_two_result.is_err());

    }

    #[tokio::test]
    async fn update_catalog() {
    
        // create a mock data catalog for Anasto
        let catalog: Catalog = DashMap::new().into();

        // load a mock files type of source table  
        let mut table: SourceTable = toml::from_str(&String::from(r#"
            type = "files"
            table_name = "test_csv"
            dirpath = "./tests/data/csv_table/" 
        "#)).unwrap();

        // generate a dataframe for the table
        let (_read_success, df) = table.read_new_data().await.unwrap();

        // add the mock table to the catalog and update its schema
        table.register(&catalog).unwrap();
        let _ = table.update_catalog(&catalog, df.schema()).unwrap();

        let (schema, destinations) = catalog.get("test_csv").unwrap().clone();
        assert_eq!(schema, Some(df.schema().clone()));
        assert!(destinations.is_empty());

    }

    #[tokio::test]
    async fn update_catalog_performs_register_if_table_does_not_exist() {
    
        // create a mock data catalog for Anasto
        let catalog: Catalog = DashMap::new().into();

        // load a mock files type of source table  
        let mut table: SourceTable = toml::from_str(&String::from(r#"
            type = "files"
            table_name = "test_csv"
            dirpath = "./tests/data/csv_table/" 
        "#)).unwrap();

        // generate a dataframe for the table
        let (_read_success, df) = table.read_new_data().await.unwrap();

        // add the mock table to the catalog and update its schema
        let _ = table.update_catalog(&catalog, df.schema()).unwrap();

        let (schema, destinations) = catalog.get("test_csv").unwrap().clone();
        assert_eq!(schema, Some(df.schema().clone()));
        assert!(destinations.is_empty());

    }

    #[tokio::test]
    async fn update_catalog_can_update_schemas() {
    
        // create a mock data catalog for Anasto
        let catalog: Catalog = DashMap::new().into();

        // load a mock files type of source table  
        let table: SourceTable = toml::from_str(&String::from(r#"
            type = "files"
            table_name = "test_csv"
            dirpath = "./tests/data/csv_table/" 
        "#)).unwrap();

        // generate mock schema for the table
        let schema_one = DFSchema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, true).into(),
                Field::new("values", DataType::Int64, true).into(),
            ],
            (0..2).map(|i| (format!("k{i}"), format!("v{i}"))).collect()
        ).unwrap();

        // add the mock table to the catalog and update its schema
        let _ = table.update_catalog(&catalog, &schema_one).unwrap();

        let schema_two = DFSchema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, true).into(),
                Field::new("meta", DataType::Int64, true).into(),
            ],
            (0..2).map(|i| (format!("k{i}"), format!("v{i}"))).collect()
        ).unwrap();

        // update the schema
        let _ = table.update_catalog(&catalog, &schema_two).unwrap();

        let expected_schema = DFSchema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, true).into(),
                Field::new("values", DataType::Int64, true).into(),
                Field::new("meta", DataType::Int64, true).into(),
            ],
            (0..2).map(|i| (format!("k{i}"), format!("v{i}"))).collect()
        ).unwrap();

        // get tables schema and check the merge took place
        let (schema, destinations) = catalog.get("test_csv").unwrap().clone();
        assert_eq!(schema, Some(expected_schema));
        assert!(destinations.is_empty());

    }

    #[tokio::test]
    async fn new_data_is_sent_to_destinations() {
    
        // load a mock files type of source table  
        let mut table: SourceTable = toml::from_str(&String::from(r#"
            type = "files"
            table_name = "test_csv"
            dirpath = "./tests/data/csv_table/" 
        "#)).unwrap();

        // generate a dataframe for the table
        let (_read_success, df) = table.read_new_data().await.unwrap();

        // mock a destination tables channel
        let (tx, mut rx) = unbounded_channel();

        // send the data to the interested destinations
        table.send_new_data(vec![tx], df.clone()).unwrap();

        // receive the data and firmat into a vector of batches
        let recv_df = rx.recv().await.unwrap();
        let recv_batches = recv_df.collect().await.unwrap();
        let orig_batches = df.collect().await.unwrap();

        assert!(recv_batches.contains(&orig_batches[0]));
        assert!(recv_batches.contains(&orig_batches[1]));

    }

    #[tokio::test]
    async fn start_function_begins_polling_correctly() {
    
        // create a mock data catalog for Anasto
        let catalog: Catalog = DashMap::new().into();

        // load a mock files type of source table  
        let mut table: SourceTable = toml::from_str(&String::from(r#"
            type = "files"
            table_name = "test_csv"
            dirpath = "./tests/data/csv_table/" 
        "#)).unwrap();

        // add the mock table to the catalog
        let _handle = table.start(catalog.clone()).await;

        // check that table is registered in the catalog
        let (schema, destinations) = catalog.get("test_csv").unwrap().clone();
        assert_eq!(schema, None);
        assert!(destinations.is_empty());

    }

}