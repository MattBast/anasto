//! # Destination Files
//!
//! This module defines the logic for a destination table that writes to a local
//! or remote filesystem.

use serde::*;
use log::info;
use std::io::Error;
use serde_derive::Deserialize;
use std::path::PathBuf;
use std::time::SystemTime;
use crate::tables::{ FailAction, LakeFileType };
use crate::tables::utils::{
	five_hundred_chars_check, 
	random_table_name, 
	path_dir_check,
	start_of_time_timestamp,
	system_time_to_string
};
use datafusion::prelude::DataFrame;
use datafusion::error::Result;
use convert_case::{ Case, Casing };


/// The DestFile reads files from a local or remote filesystem
#[derive(Debug, Deserialize, Clone)]
pub struct DestFile {
	
	/// A user defined name for the table. This does not need to correlate
	/// with the directory path where the tables files will be written to.
    #[serde(deserialize_with="to_snake_case")]
	pub dest_table_name: String,

	/// The name of the source table that supplies this destination table
    #[serde(deserialize_with="five_hundred_chars_check", default="random_table_name")]
	pub src_table_name: String,

	/// The parent filepath where all data this DestFile handles will be written to
    #[serde(deserialize_with="path_dir_check")]
    pub dirpath: PathBuf,

    /// The type of file that the tables data will be written to.
    #[serde(default)]
    pub filetype: LakeFileType,

    /// Tracks which files have been read using their created timestamp
    #[serde(default="start_of_time_timestamp")]
    pub bookmark: SystemTime,

    /// Optional field. Decide what to do when new data fails to be written to a destination.
    #[serde(default)]
    pub on_fail: FailAction,

}

/// Return a string as snake case
pub fn to_snake_case<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {

	let s = String::deserialize(d)?;
    Ok(s.to_case(Case::Snake))

}


impl DestFile {

	/// Get the name specified for this destination
    pub fn dest_table_name(&self) -> &String {
		&self.dest_table_name
	}

	/// Get the name of the source table that supplies this destination
    pub fn src_table_name(&self) -> &String {
		&self.src_table_name
	}

	/// Getter function for returning what to do if this table fails to read new data
	pub fn on_fail(&self) -> FailAction {
		self.on_fail
	}

	/// Get all the files that have not yet been read, add them to a Dataframe and 
	/// return the frame. If there are no new files, return an empty Dataframe with a false
	/// boolean telling the destinations not to process it.
	pub async fn write_new_data(&mut self, df: DataFrame) -> Result<(), Error> {

		info!(target: &self.dest_table_name, "Reading files from {} created before {:?}.", self.dirpath.display(), system_time_to_string(self.bookmark));

		// create the dataframe
		match self.filetype {
			LakeFileType::Csv => df.write_csv(self.dirpath.to_str().unwrap()).await?,
			LakeFileType::Json => df.write_json(self.dirpath.to_str().unwrap()).await?,
			// ************************************************************************************************
			// switch to Avro write method once one exists
			// ************************************************************************************************
			LakeFileType::Avro => df.write_json(self.dirpath.to_str().unwrap()).await?,
			LakeFileType::Parquet => df.write_parquet(self.dirpath.to_str().unwrap(), Default::default()).await?,
		};

		// update the bookmark so the same files aren't read twice
        self.bookmark = SystemTime::now();

		Ok(())

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
