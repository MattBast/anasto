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
#[serde(tag="type", rename_all="snake_case", deny_unknown_fields)]
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
            match table.register(&catalog, tx) {
                Ok(_) => (),
                Err(_) => return,
            };

            info!(target: table.dest_table_name(), "Starting to listen for new data from table {}.", table.src_table_name());

            // start listening for new data from the source table
            while let Some(df) = rx.recv().await {

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
    fn register(&self, catalog: &Catalog, tx: UnboundedSender<DataFrame>,) -> Result<(), Error> {
        
        match catalog.entry(self.src_table_name().to_string()) {
            
            Entry::Occupied(mut entry) => { 
                entry.get_mut().1.push(tx);
                Ok(())
            },

            Entry::Vacant(_) => {
                let error_message = format!("Source table {} could not be found in catalog.", self.src_table_name());
                error!(target: self.dest_table_name(), "{}", &error_message);
                Err(Error::new(ErrorKind::NotFound, error_message))
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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables::test_utils::TestDir;
    use dashmap::DashMap;
    use tokio::sync::mpsc::unbounded_channel;
    use datafusion::prelude::SessionContext;
    use tokio::time::{ Duration, sleep };

    #[tokio::test]
    async fn register_adds_tables_trasmitter_to_catalog() {

        // create diectory for the destination table that tears itself down at the end of the test
        let _test_dir = TestDir::new("./tests/data/json_test_dest_table/");

        // create a mock data catalog for Anasto
        let catalog: Catalog = DashMap::new().into();

        // add mock source table to the catalog
        let entry = (None, Vec::new());
        let _ = catalog.insert("csv_table".to_string(), entry);

        // gennerate a destination table
        let table: DestTable = toml::from_str(&String::from(r#"
            type = "files"
            source_table_name = "csv_table"
            dest_table_name = "json_test_dest_table"
            dirpath = "./tests/data/json_test_dest_table/" 
            filetype = "json"
        "#)).unwrap();

        // generste a mock channel for the table
        let (tx, _rx) = unbounded_channel();

        // add the mock channel to the catalog
        table.register(&catalog, tx).unwrap();

        let (_schema, destinations) = catalog.get("csv_table").unwrap().clone();
        assert_eq!(destinations.len(), 1);

    }


    #[tokio::test]
    async fn register_throws_error_when_source_table_is_not_registered() {

        // create diectory for the destination table that tears itself down at the end of the test
        let _test_dir = TestDir::new("./tests/data/no_source_table/");

        // create a mock data catalog for Anasto
        let catalog: Catalog = DashMap::new().into();

        // gennerate a destination table
        let table: DestTable = toml::from_str(&String::from(r#"
            type = "files"
            source_table_name = "csv_table"
            dest_table_name = "no_source_table"
            dirpath = "./tests/data/no_source_table/" 
            filetype = "json"
        "#)).unwrap();

        // generste a mock channel for the table
        let (tx, _rx) = unbounded_channel();

        // add the mock channel to the catalog
        let register_result = table.register(&catalog, tx);

        assert!(register_result.is_err());
        assert_eq!(register_result.map_err(|e| e.kind()), Err(ErrorKind::NotFound));

    }


    #[tokio::test]
    async fn started_table_can_receive_new_data() {

        // create diectory for the destination table that tears itself down at the end of the test
        let _test_dir = TestDir::new("./tests/data/started_table/");

        // create a mock data catalog for Anasto
        let catalog: Catalog = DashMap::new().into();

        // add mock source table to the catalog
        let entry = (None, Vec::new());
        let _ = catalog.insert("csv_table".to_string(), entry);

        // gennerate a destination table
        let mut table: DestTable = toml::from_str(&String::from(r#"
            type = "files"
            source_table_name = "csv_table"
            dest_table_name = "started_table"
            dirpath = "./tests/data/started_table/" 
            filetype = "json"
        "#)).unwrap();

        // start the table listening for new data
        let _handle = table.start(catalog.clone()).await;

        // setup mock source table data, get transmitter from catalog and send data to destination 
        let ctx = SessionContext::new();
        let src_df = ctx.read_csv("./tests/data/csv_table/", Default::default()).await.unwrap();
        let _ = catalog.get_mut("csv_table").unwrap().clone().1[0].send(src_df.clone()).unwrap();

        // give Anasto a moment to write the data
        let wait = Duration::from_millis(50);
        let _ = sleep(wait).await;

        // check that data was written to the destination correctly
        let dest_df = ctx.read_json("./tests/data/started_table/", Default::default()).await.unwrap();
        let src_data = src_df.collect().await.unwrap();
        let dest_data = dest_df.collect().await.unwrap();
        assert!(dest_data.contains(&src_data[0]));
        assert!(dest_data.contains(&src_data[1]));

    }

}