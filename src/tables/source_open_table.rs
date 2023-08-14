//! # SourceOpenTable
//!
//! This module defines the logic for a source table read from an open table
//! database like Delta Lake.

use log::info;
use serde_derive::Deserialize;
use std::path::PathBuf;
use std::time::{SystemTime, Duration};
use crate::tables::{ FailAction, OpenTableFormat };
use crate::tables::utils::{
	five_hundred_chars_check, 
	random_table_name, 
	path_dir_check,
	start_of_time_timestamp,
	ten_secs_as_millis,
	system_time_to_string
};

use std::sync::Arc;
use datafusion::prelude::{ SessionContext, DataFrame };
use datafusion::error::Result;



/// The SourceOpenTable reads files from a local or remote filesystem
#[derive(Debug, Deserialize, Clone)]
pub struct SourceOpenTable {
	
	/// A user defined name for the table. This does not need to correlate
	/// with the directory path where the tables files are stored.
    #[serde(deserialize_with="five_hundred_chars_check", default="random_table_name")]
	pub table_name: String,

	/// The parent filepath where all data this SourceOpenTable handles will be written to
    #[serde(deserialize_with="path_dir_check")]
    pub dirpath: PathBuf,

    /// The type of file that the tables data is stored in. The table cannot be stored
    /// in multiple filetypes.
    #[serde(default)]
    pub format: OpenTableFormat,

    /// Tracks which files have been read using their created timestamp
    #[serde(default="start_of_time_timestamp")]
    pub bookmark: SystemTime,

    /// Optional field. Determines how frequently new data will be written to the destination. Provided in milliseconds.
    #[serde(default="ten_secs_as_millis")]
    pub poll_interval: u64,

    /// Optional field. Decide what to do when new data fails to be written to a destination.
    #[serde(default)]
    pub on_fail: FailAction,

}


impl SourceOpenTable {

	/// Getter function for returning the name of the table this table is reading from
	pub fn table_name(&self) -> &String {
		&self.table_name
	}

	/// Getter function for returning what to do if this table fails to read new data
	pub fn on_fail(&self) -> FailAction {
		self.on_fail
	}

	/// A getter function for getting how often the source dataset should be read
	pub fn poll_interval(&self) -> Duration {
		Duration::from_millis(self.poll_interval)
	}

	/// Get all the data that has not yet been read, add it to a Dataframe and 
	/// return the frame. If there is no new data, return an empty Dataframe with a false
	/// boolean telling the destinations not to process it.
	pub async fn read_new_data(&mut self) -> Result<(bool, DataFrame), std::io::Error> {

		let ctx = SessionContext::new();

		info!(target: &self.table_name, "Reading data created before {:?}.", system_time_to_string(self.bookmark));

		// read the files into a dataframe (table)
        let df = self.read_table(&ctx).await?;

        // update the bookmark so the same files aren't read twice
        self.bookmark = SystemTime::now();

		Ok((true, df))

	}

	/// Decide which format of open table to read from. Run the specified read 
	/// function and return a dataframe.
	async fn read_table(&self, ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
		
		let df = match self.format {
			OpenTableFormat::DeltaLake => self.read_delta(ctx).await?,
		};

		Ok(df)

	}

	/// Decide which format of open table to read from. Run the specified read 
	/// function and return a dataframe.
	async fn read_delta(&self, ctx: &SessionContext) -> Result<DataFrame, std::io::Error> {
		
		let table = deltalake::open_table(self.dirpath.to_str().unwrap()).await.unwrap();

		// Create a delta operations client pointing at an un-initialized in-memory location.
	    // In a production environment this would be created with "try_new" and point at
	    // a real storage location.
	    // let table_uri = self.dirpath.to_str().unwrap();
	    // let ops = deltalake::DeltaOps::try_from_uri(table_uri).await.unwrap();

	    // let columns = vec![
	    //     deltalake::SchemaField::new(
	    //         String::from("id"),
	    //         deltalake::SchemaDataType::primitive(String::from("integer")),
	    //         false,
	    //         Default::default(),
	    //     )
	    // ];

	    // The operations module uses a builder pattern that allows specifying several options
	    // on how the command behaves. The builders implement `Into<Future>`, so once
	    // options are set you can run the command using `.await`.
	    // let table = ops
	    //     .create()
	    //     .with_columns(columns)
	    //     .with_table_name(self.table_name.clone())
	    //     .with_configuration_property(deltalake::delta_config::DeltaConfigKey::EnableChangeDataFeed, Some("true"))
	    //     .await
	    //     .unwrap();

	    let df = ctx.read_table(Arc::new(table)).unwrap();

	    println!("{:?}", df.schema());

		Ok(df)

	}

}

// #[cfg(test)]
// mod tests {
// 	use super::*;

//     #[test]
//     fn can_read_files() {
    
//         let subscriber = File { dirpath: PathBuf::new(".") };
//         subscriber.poll_action();

//     }

// }
