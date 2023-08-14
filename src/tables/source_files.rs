//! # SourceLake
//!
//! This module defines the logic for a source table read from a local or remote
//! filesystem. It works by polling a specified file directory and reading all
//! the files that have been created or modified since the Table last read them. 
//! A timestamp bookmark is kept and compared against a files `last modified`
//! timestamp to track what files need to be read per poll.

use log::{ info, warn };
use serde_derive::Deserialize;
use std::path::PathBuf;
use std::time::{SystemTime, Duration};
use crate::tables::{ FailAction, LakeFileType };
use crate::tables::utils::{
	five_hundred_chars_check, 
	random_table_name, 
	path_dir_check,
	start_of_time_timestamp,
	ten_secs_as_millis,
	system_time_to_string
};
use datafusion::prelude::{ SessionContext, DataFrame };
use datafusion::error::Result;
use std::fs::read_dir;


/// The SourceLake reads files from a local or remote filesystem
#[derive(Debug, Deserialize, Clone)]
pub struct SourceLake {
	
	/// A user defined name for the table. This does not need to correlate
	/// with the directory path where the tables files are stored.
    #[serde(deserialize_with="five_hundred_chars_check", default="random_table_name")]
	pub table_name: String,

	/// The parent filepath where all data this SourceLake handles will be written to
    #[serde(deserialize_with="path_dir_check")]
    pub dirpath: PathBuf,

    /// The type of file that the tables data is stored in. The table cannot be stored
    /// in multiple filetypes.
    #[serde(default)]
    pub filetype: LakeFileType,

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


impl SourceLake {

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

	/// Get all the files that have not yet been read, add them to a Dataframe and 
	/// return the frame. If there are no new files, return an empty Dataframe with a false
	/// boolean telling the destinations not to process it.
	pub async fn read_new_data(&mut self) -> Result<(bool, DataFrame), std::io::Error> {

		let ctx = SessionContext::new();

		info!(target: &self.table_name, "Reading files from {} created before {:?}.", self.dirpath.display(), system_time_to_string(self.bookmark));

		// get all new or newly modified files
		let file_paths = self.get_unread_filepaths()?;
		
		// if there are no files to read, don't waste time trying to read them
		if file_paths.is_empty() { return Ok((false, ctx.read_empty().unwrap())) };

		// read the files into a dataframe (table)
        let df = self.read_files(&ctx, file_paths).await?;

        // update the bookmark so the same files aren't read twice
        self.bookmark = SystemTime::now();

		Ok((true, df))

	}

	/// Get a list of files that have been created or modified since they were last read
	fn get_unread_filepaths(&self) -> Result<Vec<String>, std::io::Error> {

		let mut paths = Vec::new();
		
		for entry in (read_dir(self.dirpath.clone())?).flatten() {
    
            if let Ok(metadata) = entry.metadata() {
                
                if self.bookmark < metadata.created().unwrap() {
                	paths.push(entry.path().into_os_string().into_string().unwrap())
                }

            } else {
                
                warn!("Skipping file {:?} as couldn't get metadata from it", entry.path());

            }

		}

        Ok(paths)

	}

	/// Read all files in a list of filepaths into a datafusion dataframe
	async fn read_files(&self, ctx: &SessionContext, paths: Vec<String>) -> Result<DataFrame, std::io::Error> {
		
		info!(target: &self.table_name, "Reading {} files.", paths.len());

		// create the dataframe
		let df = match self.filetype {
			LakeFileType::Csv => ctx.read_csv(paths, Default::default()).await?,
			LakeFileType::Json => ctx.read_json(paths, Default::default()).await?,
			LakeFileType::Avro => ctx.read_avro(paths, Default::default()).await?,
			LakeFileType::Parquet => ctx.read_parquet(paths, Default::default()).await?,
		};

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
