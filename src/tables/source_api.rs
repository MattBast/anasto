//! # SourceApi
//!
//! This module defines the logic to read a source table from an API.
//! It works by calling an API on a regular schedule (the poll_interval), picking
//! a field and using the values found in the field as rows of a table. 
//! A timestamp bookmark is kept to request only the latest data from an API.

use std::sync::Arc;
use std::io::{ Error, ErrorKind };
use log::info;
use serde_derive::{ Serialize, Deserialize };
use chrono::{ DateTime, offset::Utc };
use std::time::Duration;
use crate::tables::FailAction;
use crate::tables::utils::{
	five_hundred_chars_check, 
	random_table_name, 
	ten_secs_as_millis,
	start_of_time_timestamp,
	deserialize_url,
	serialize_url,
	schema_from_json
};
use datafusion::prelude::{ SessionContext, DataFrame };
use datafusion::error::Result;

use reqwest::Url;
use serde_json::Value;
use arrow_json::ReaderBuilder;

/// The SourceApi reads files from a local or remote filesystem
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SourceApi {
	
	/// A user defined name for the table. This does not need to correlate
	/// with the directory path where the tables files are stored.
    #[serde(deserialize_with="five_hundred_chars_check", default="random_table_name")]
	pub table_name: String,

	/// The url of the endpoint to call
	#[serde(deserialize_with="deserialize_url", serialize_with="serialize_url")]
	pub endpoint_url: Url,

    /// Tracks which files have been read using their created timestamp
    #[serde(default="start_of_time_timestamp")]
    pub bookmark: DateTime<Utc>,

    /// Optional field. Determines how frequently new data will be written to the destination. Provided in milliseconds.
    #[serde(default="ten_secs_as_millis")]
    pub poll_interval: u64,

    /// Optional field. Decide what to do when new data fails to be written to a destination.
    #[serde(default)]
    pub on_fail: FailAction,

}


impl SourceApi {

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

	/// Call an api endpoint, read the response body into a dataframe and return the
	/// dataframe.
	pub async fn read_new_data(&mut self) -> Result<(bool, DataFrame), Error> {

		let ctx = SessionContext::new();

		info!(target: &self.table_name, "Calling api endpoint {}.", self.endpoint_url);

		// *******************************************************************
		// make sure to include all four methods (get, post, put, delete)
		// and make sure to handle the http status code
		// *******************************************************************
		// make the request
		let resp = match reqwest::get(self.endpoint_url.as_str()).await {
			Ok(resp) => resp,
			Err(e) => return Err(Error::new(ErrorKind::Other, e.to_string()))
		};

		// parse the response to a json object
		let body = match resp.json::<Value>().await {
			Ok(resp) => resp,
			Err(e) => return Err(Error::new(ErrorKind::Other, e.to_string()))	
		};

		// discover the response schema and generate a record batch generator from it
		let schema = schema_from_json(&body, &self.table_name);
		let mut reader = ReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();

		// parse the response to a arrow record batch
		let iter_body = match body {
			Value::Array(arr) => arr,
			_ => [body].to_vec()
		};
		reader.serialize(&iter_body).unwrap();
		let record_batch = reader.flush().unwrap().unwrap();

		// read the data into a dataframe (table)
        let df = ctx.read_batch(record_batch)?;

        // update the bookmark so future calls get new data
        self.bookmark = Utc::now();

		Ok((true, df))

	}

}