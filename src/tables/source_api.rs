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
use crate::tables::{ FailAction, HttpMethod };
use crate::tables::utils::{
	five_hundred_chars_check, 
	random_table_name, 
	ten_secs_as_millis,
	start_of_time_timestamp,
	deserialize_url,
	serialize_url,
	serialize_schema,
	schema_from_json,
	deserialize_header_map,
	serialize_header_map
};
use datafusion::prelude::{ SessionContext, DataFrame };
use datafusion::error::Result;

use reqwest::{ Url, header::HeaderMap };
use arrow_schema::Schema;
use serde_json::Value;
use arrow_array::RecordBatch;
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

	/// The HTTP method to call this endpoint with
	#[serde(default)]
	pub method: HttpMethod,

	/// Select the field in the response where the table data resides. Provide
	/// as a vector of strings to select a nested field
	#[serde(default)]
	pub select_field: Option<Vec<String>>,

	/// How long to wait for a response before cancelling the request
	#[serde(default)]
	pub timeout: Option<Duration>,

	/// Adds one or more queries to the url
	#[serde(default)]
	pub query: Option<Vec<(String, String)>>,

	/// Adds a basic (username-password) header to the request
	#[serde(default)]
	pub basic_auth: Option<(String, String)>,

	/// Adds one or more headers to the request
	#[serde(default, deserialize_with="deserialize_header_map", serialize_with="serialize_header_map")]
	pub headers: HeaderMap,

    /// Tracks which files have been read using their created timestamp
    #[serde(default="start_of_time_timestamp")]
    pub bookmark: DateTime<Utc>,

    /// Optional field. Determines how frequently new data will be written to the destination. Provided in milliseconds.
    #[serde(default="ten_secs_as_millis")]
    pub poll_interval: u64,

    /// Optional field. Decide what to do when new data fails to be written to a destination.
    #[serde(default)]
    pub on_fail: FailAction,

    /// Stores the schema so it only needs to be generated once.
    #[serde(default, skip_deserializing, serialize_with="serialize_schema")]
    pub schema: Option<Schema>,

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

		info!(target: &self.table_name, "Calling api endpoint {} with method {}.", self.endpoint_url, self.method);

		// make the request
		let resp = self.call_api().await?;

		// parse the response into an Arrow RecordBatch
		let record_batch = self.json_to_record_batch(resp);

		// read the data into a dataframe (table)
        let df = ctx.read_batch(record_batch)?;

        // update the bookmark so future calls get new data
        self.bookmark = Utc::now();

		Ok((true, df))

	}

	/// Call the API endpoint with the specified method. Return the response body.
	async fn call_api(&self) -> Result<Value, Error> {

		// *******************************************************************
		// make sure to include all four methods (get, post, put, delete)
		// and make sure to handle the http status code
		// *******************************************************************
		let client = reqwest::Client::new();

		let req = match self.method {
			HttpMethod::Get => client.get(self.endpoint_url.as_str()).send().await,
			HttpMethod::Post => client.post(self.endpoint_url.as_str()).send().await,
			HttpMethod::Put => client.put(self.endpoint_url.as_str()).send().await,
			HttpMethod::Patch => client.patch(self.endpoint_url.as_str()).send().await,
			HttpMethod::Delete => client.delete(self.endpoint_url.as_str()).send().await,
		};

		let resp = match req {
			Ok(resp) => resp,
			Err(e) => return Err(Error::new(ErrorKind::Other, e.to_string()))
		};

		// parse the response to a json object
		let json_resp = match resp.json::<Value>().await {
			Ok(resp) => resp,
			Err(e) => return Err(Error::new(ErrorKind::Other, e.to_string()))	
		};

		Ok(json_resp)

	}

	/// Parse the API json response body into an Arrow RecordBatch type
	fn json_to_record_batch(&mut self, json: Value) -> RecordBatch {

		// Get the schema for the json value
		let schema = match &self.schema {
			
			// Use the previously inferred schema
			Some(schema) => schema.clone(),
			
			// This must be the first API call so infer a schema from the json value
			None => {
				let schema = schema_from_json(&json, &self.table_name);
				self.schema = Some(schema.clone());
				schema
			}

		};

		// Create a reader that will parse the json into a RecordBatch
		let mut reader = ReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();

		// Make sure the json value is iterable
		let iter_json = match json {
			Value::Array(arr) => arr,
			_ => [json].to_vec()
		};

		// Parse the response to an Arrow RecordBatch
		reader.serialize(&iter_json).unwrap();
		reader.flush().unwrap().unwrap()

	}

}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::Utc;

	#[test]
    fn table_with_minimal_config() {
    
        let content = String::from(r#"
            table_name = "trello_board"
            endpoint_url = "https://trello.com/b/abc/board.json" 
        "#);

        let table: SourceApi = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name, "trello_board");
        assert_eq!(table.endpoint_url, Url::parse("https://trello.com/b/abc/board.json").unwrap());
        assert!(matches!(table.method, HttpMethod::Get));
        assert_eq!(table.select_field, None);
        assert_eq!(table.timeout, None);
        assert_eq!(table.query, None);
        assert_eq!(table.basic_auth, None);
        assert_eq!(table.headers, HeaderMap::new());
        assert_eq!(table.bookmark, chrono::DateTime::<Utc>::MIN_UTC);
        assert_eq!(table.poll_interval, 10_000);
        assert!(matches!(table.on_fail, FailAction::Stop));
        assert_eq!(table.schema, None);

    }

}