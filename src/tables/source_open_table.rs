//! # SourceOpenTable
//!
//! This module defines the logic for a source table read from an open table
//! database like Delta Lake.

use log::{ info, warn };
use serde_derive::Deserialize;
use std::path::PathBuf;
use std::path::Path;
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
use datafusion::logical_expr::{ col, replace, Expr };
use datafusion::scalar::ScalarValue;


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

    /// The open table format the data is stored in
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

    /// Private field. Tracks whether this table has done it's first read
    #[serde(default)]
    first_read: bool

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

		// read the files into a dataframe
        let df = self.read_table(&ctx).await?;

        // update the bookmark so the same files aren't read twice
        self.bookmark = SystemTime::now();

		Ok(df)

	}

	/// Decide which format of open table to read from. Run the specified read 
	/// function and return a dataframe.
	async fn read_table(&self, ctx: &SessionContext) -> Result<(bool, DataFrame), std::io::Error> {
		
		let df = match self.format {
			OpenTableFormat::DeltaLake => self.read_delta(ctx).await?,
		};

		Ok(df)

	}

	/// Decide which format of open table to read from. Run the specified read 
	/// function and return a dataframe.
	async fn read_delta(&self, ctx: &SessionContext) -> Result<(bool, DataFrame), std::io::Error> {
		
		match self.first_read {
			false => self.delta_first_read(ctx).await,
			true => self.delta_read_cdc(ctx).await,
		}

	}

	/// Read all rows from the latest version of a delta table and return as a dataframe
	async fn delta_first_read(&self, ctx: &SessionContext) -> Result<(bool, DataFrame), std::io::Error> {
		
		// read the table
		let table = deltalake::open_table(self.dirpath.to_str().unwrap()).await.unwrap();
	    let df = ctx.read_table(Arc::new(table)).unwrap();

	    // add mock cdc columns
	    let df = df
	    	.with_column("_change_type", Expr::Literal("insert".into()))
	    	.unwrap()
	    	.with_column("_commit_timestamp", self.dataframe_bookmark())
	    	.unwrap();

	    Ok((true, df))
	}

	/// Read all the cdc files that have not yet been read and return them as a dataframe
	async fn delta_read_cdc(&self, ctx: &SessionContext) -> Result<(bool, DataFrame), std::io::Error> {
		
		// get a list of the cdc files that have not yet been read
	    let new_cdc_paths = self.get_unread_cdc_filepaths()?;
	    if new_cdc_paths.is_empty()  { return Ok((false, ctx.read_empty().unwrap())) };

	    // read new cdc files into a dataframe
	    let cdc_df = ctx.read_parquet(new_cdc_paths, Default::default()).await?;
	    
	    // filter out the "update_preimage" changes, change the "update_postimage"
	    // change type to "update" and add a commit timestamp if it doesn't already
	    // exist
	    let cdc_df = cdc_df
	    	.filter(col("_change_type").not_eq(Expr::Literal("update_preimage".into())))?
	    	.with_column("_change_type", replace(col("_change_type"), Expr::Literal("update_postimage".into()), Expr::Literal("update".into())))
	    	.unwrap()
	    	.with_column("_commit_timestamp", self.dataframe_bookmark())
	    	.unwrap();

	    Ok((true, cdc_df))

	}

	/// Return the tables bookmark as a DataFusion Dataframe timestamp
	fn dataframe_bookmark(&self) -> Expr {
		
		let timestamp_seconds: i64 = self.bookmark.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs().try_into().unwrap();
		Expr::Literal(ScalarValue::TimestampSecond(Some(timestamp_seconds), None))

	}

	/// Get a list of files that have been created or modified since they were last read
	fn get_unread_cdc_filepaths(&self) -> Result<Vec<String>, std::io::Error> {

		let mut paths = Vec::new();
		let cdc_path = format!("{}/{}", self.dirpath.display(), "_change_data");

		if !Path::new(&cdc_path).exists() { 
			
			warn!("Could not find a change data log at the path {:?}. No change data will be read until this path is found.", cdc_path);
			return Ok(paths) 

		}
		
		for entry in (std::fs::read_dir(cdc_path)?).flatten() {
    
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
