//! # SourceApi
//!
//! This module defines the logic to read a source table from an API.
//! It works by calling an API on a regular schedule (the poll_interval), picking
//! a field and using the values found in the field as rows of a table. 
//! A timestamp bookmark is kept to request only the latest data from an API.

use std::sync::Arc;
use std::io::{ Error, ErrorKind };
use log::{ info, warn };
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
	serialize_header_map,
	deserialize_duration,
	serialize_duration
};
use datafusion::prelude::{ SessionContext, DataFrame };
use datafusion::error::Result;

use reqwest::{ Url, header::HeaderMap, RequestBuilder, Response };
use arrow_schema::Schema;
use serde_json::{ Value, json };
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

	/// How long to wait for a response before cancelling the request (in seconds)
	#[serde(default, deserialize_with="deserialize_duration", serialize_with="serialize_duration")]
	pub timeout: Option<Duration>,

	/// Adds one or more queries to the url
	#[serde(default)]
	pub query: Vec<(String, String)>,

	/// Adds a basic (username-password) header to the request
	#[serde(default)]
	pub basic_auth: Option<(String, String)>,

	/// Adds one or more headers to the request
	#[serde(default, deserialize_with="deserialize_header_map", serialize_with="serialize_header_map")]
	pub headers: HeaderMap,

	/// Adds a json body to the request
	#[serde(default)]
	pub body: Option<Value>,

    /// Tracks which files have been read using their created timestamp
    #[serde(default="start_of_time_timestamp")]
    pub bookmark: DateTime<Utc>,

    /// Optional field. Determines how frequently new data will be written to the destination. Provided in milliseconds.
    #[serde(default="ten_secs_as_millis")]
    pub poll_interval: u64,

    /// Optional field. Decide what to do when new data fails to be written to a destination.
    #[serde(default)]
    pub on_fail: FailAction,

    /// Optional field. State if this source should call an API just once (true) or 
    /// if it should poll the API (false). Defaults to false.
    #[serde(default)]
    pub one_request: bool,

    /// Stores the schema so it only needs to be generated once.
    #[serde(default, skip_deserializing, serialize_with="serialize_schema")]
    pub schema: Option<Schema>,

    /// Optional field. States what pagination approach will be taken. 
    /// Defaults to None meaning no pagination will be performed.
    #[serde(default)]
    pub pagination: PaginationOptions,

    /// Optional field. States the name of the page number or offset parameter that will
    /// increment during pagination. Defaults to "page_size".
    #[serde(default="default_page_number_key")]
    pub pagination_page_token_key: String,

    /// Optional field. Keeps track of what page the pagination needs to request. 
    /// Defaults to 0.
    #[serde(default)]
    pub pagination_page_number: usize,

    /// Optional field. States the name of the page size parameter that will
    /// increment during pagination. Defaults to "page".
    #[serde(default="default_page_size_key")]
    pub pagination_page_size_key: String,

    /// Optional field. States how many records will be returned per page during. 
    /// pagination. Defaults to 5.
    #[serde(default="default_page_size")]
    pub pagination_page_size: usize,

    /// Optional field. States the maximum number of requests a paginated call can make. 
    /// Defaults to 100.
    #[serde(default="default_page_requests")]
    pub max_pagination_requests: usize,

    /// Optional field. Keeps track of how many records the pagination requests 
    /// have received. Defaults to 0.
    #[serde(default)]
    pub pagination_offset: usize,

    /// Optional field. Select the field that will be used to request the next page of
    /// results.
	#[serde(default)]
	pub pagination_cursor_field: Option<Vec<String>>,

	/// Optional field. Define which record in a response contains the cursor. Defaults 
	/// to the last record.
	#[serde(default)]
	pub pagination_cursor_record: PaginationCursorRecord,

	/// Stores the cursor used to make the next cursor Pagination request.
	#[serde(default)]
	pub pagination_cursor: Option<String>,

	/// Optional field. State where the pagination cursor field can be found.
	#[serde(default)]
	pub pagination_cursor_location: CursorLocation,

}

/// Returns the string "page"
pub fn default_page_number_key() -> String {
	String::from("page")
}

/// Returns the string "page_size"
pub fn default_page_size_key() -> String {
	String::from("page_size")
}

/// Returns the integer 5
pub fn default_page_size() -> usize {
	5
}

/// Returns the integer 100
pub fn default_page_requests() -> usize {
	100
}

/// This enum defines what pagination strategy (if any) to perform
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
#[serde(rename_all="snake_case")]
pub enum PaginationOptions {
   
   /// Don't use pagination
   #[default]
   None,

   /// Page increment option
   PageIncrement,

   /// Offset increment option
   OffsetIncrement,

   /// Cursor option
   Cursor,

}

/// This enum defines which record of a response the pagination cursor can be foind in
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, Eq, PartialEq)]
#[serde(rename_all="snake_case")]
pub enum PaginationCursorRecord {
   
   /// The last record
   #[default]
   Last,

   /// The first record
   First,

}


/// This enum defines where the pagination cursor field can be found
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, Eq, PartialEq)]
#[serde(rename_all="snake_case")]
pub enum CursorLocation {
   
   /// The header of the respose
   #[default]
   Body,

   /// The header of the response
   Header,

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

		// make the request
		let resp = self.call_api().await?;

		// parse the response into an Arrow RecordBatch
		// and then into a dataframe
		let record_batch = self.json_to_record_batch(resp);
        let df = ctx.read_batch(record_batch)?;

        // update the bookmark so future calls get new data
        self.bookmark = Utc::now();

		Ok((true, df))

	}

	/// Call the API endpoint. Return the response body as a JSON value.
	async fn call_api(&mut self) -> Result<Value, Error> {

		// decide if making many requests (pagination) or one request (no pagination)
		match self.pagination {
			
			PaginationOptions::None => self.single_request().await,
			PaginationOptions::PageIncrement => self.page_increment_req().await,
			PaginationOptions::OffsetIncrement => self.page_offset_req().await,
			PaginationOptions::Cursor => self.cursor_req().await,

		}

	}

	/// Make one request to the API and return the response as JSON
	async fn single_request(&self) -> Result<Value, Error> {

		// define the request
		let req = self.build_request();

		// make the request
		let resp = req.send().await;

		// handle the response
		let checked_resp = self.check_response(resp)?;
		let json_resp = self.parse_response(checked_resp).await?;

		Ok(json_resp)

	}

	/// Generate an API request from the parameters in the table config
	fn build_request(&self) -> RequestBuilder {

		let mut req = self.pick_method();
		req = self.add_query(req);
		req = self.add_headers(req);
		req = self.add_basic_auth(req);
		req = self.add_body(req);
		req = self.add_timeout(req);

		info!(target: &self.table_name, r#"
			Calling api with request: 
			{:?}
		"#, req);

		req

	}

	/// Start the request by picking the HTTP method
	fn pick_method(&self) -> RequestBuilder {

		let client = reqwest::Client::new();

		match self.method {
			HttpMethod::Get => client.get(self.endpoint_url.as_str()),
			HttpMethod::Post => client.post(self.endpoint_url.as_str()),
			HttpMethod::Put => client.put(self.endpoint_url.as_str()),
			HttpMethod::Patch => client.patch(self.endpoint_url.as_str()),
			HttpMethod::Delete => client.delete(self.endpoint_url.as_str()),
		}

	}

	/// If the table is configured to accept a query, add the query params to the url
	fn add_query(&self, req: RequestBuilder) -> RequestBuilder {

		if !self.query.is_empty() {
			return req.query(&self.query)
		};

		req

	}

	/// If the table is configured to use headers, add the headers to the request
	fn add_headers(&self, req: RequestBuilder) -> RequestBuilder {

		if !self.headers.is_empty() {
			req.headers(self.headers.clone())
		}
		else {
			req
		}

	}

	/// If the table is configured to use basic authorisation, add it to the header
	fn add_basic_auth(&self, req: RequestBuilder) -> RequestBuilder {

		match &self.basic_auth {
			Some(auth) => req.basic_auth(&auth.0, Some(&auth.1)),
			None => req
		}

	}

	/// If the table is configured to use a body, add it to the request
	fn add_body(&self, req: RequestBuilder) -> RequestBuilder {

		match &self.body {
			Some(body) => req.json(body),
			None => req
		}

	}

	/// Adjust the request timeout to be different from the default
	fn add_timeout(&self, req: RequestBuilder) -> RequestBuilder {

		match &self.timeout {
			Some(duration) => req.timeout(*duration),
			None => req
		}

	}

	// Check that the request was successful. Return an error if not.
	fn check_response(&self, resp_result: Result<Response, reqwest::Error>) -> Result<Response, Error> {

		match resp_result {
			
			Ok(resp) => {
				
				// Is the http status code in the range of 200-299
				if resp.status().is_success() {
					Ok(resp)
				}
				else {
					Err(Error::new(ErrorKind::Other, "The response http status code was not in the range of 200-299."))
				}

			},

			Err(e) => Err(Error::new(ErrorKind::Other, e.to_string()))

		}

	}

	// Parse the response type into a JSON object
	async fn parse_response(&self, resp: Response) -> Result<Value, Error> {

		match resp.json::<Value>().await {
			Ok(resp) => Ok(resp),
			Err(e) => Err(Error::new(ErrorKind::Other, e.to_string()))	
		}

	}

	/// Parse the API json response body into an Arrow RecordBatch type
	fn json_to_record_batch(&mut self, mut json: Value) -> RecordBatch {

		// Select nested data from the response.
		json = match self.filter_result(json.clone(), 0) {
			Ok(filtered_json) => filtered_json,
			Err(e) => {
				warn!(target: &self.table_name, "Got error '{:?}' while selecting field from result. Ignoring field select.", e);
				json
			}
		};
		
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
		let (_, iter_json) = self.resp_to_iter(json);

		// Parse the response to an Arrow RecordBatch
		reader.serialize(&iter_json).unwrap();
		reader.flush().unwrap().unwrap()

	}

	/// If the option is active, filter the response to include only the selected field
	fn filter_result(&self, json: Value, mut index: usize) -> Result<Value, Error> {

		match &self.select_field {
			
			Some(fields) => match json {
				
				Value::Object(json_obj) => {
					
					let filtered_obj = match json_obj.get(&fields[index]) {
						Some(filtered_obj) => filtered_obj,
						None => return Err(Error::new(ErrorKind::Other, format!("The value {:?} does not contain the key {}.", json_obj, &fields[index])))
					};

					index += 1;

					if fields.len() == index {
						Ok(filtered_obj.clone())
					}
					else {
						self.filter_result(filtered_obj.clone(), index)
					}

				},

				Value::Array(json_arr) => {

					let mut filtered_objs = Vec::new();

					for obj in json_arr {
						filtered_objs.push(
							self.filter_result(obj.clone(), 0)?
						);
					}
						
					Ok(Value::Array(filtered_objs))

				},

				_ => Err(Error::new(ErrorKind::Other, format!("The value {} is not an object.", json)))

			},

			None => Ok(json)

		}

	}

	/// Perform a series of paginated API requests following the page increment approach.
	/// This involves adding the page number to the request query and calling the API
	/// on a loop until the response contains less than the stated page_size.
	async fn page_increment_req(&mut self) -> Result<Value, Error> {
		
		let mut responses = Vec::new();
		let mut requests_count = 0;

		// Kepp calling API until it contains less records than the page_size parameter
		loop {

			requests_count += 1;

			// Add the pagination parameters to the query parameters
			self.add_pagination_params();

			// Make the request and parse the resonse
			let resp = self.single_request().await?;

			// Make sure the json value is iterable
			let (record_count, mut iter_resp) = self.resp_to_iter(resp);

			// Add response data to data from other responses
			responses.append(&mut iter_resp);

			// increment the page number
			self.pagination_page_number += 1;

			// Decide if finished making pagfinated requests
			if record_count < self.pagination_page_size || requests_count >= self.max_pagination_requests {
				break
			}

		}

		Ok(serde_json::Value::Array(responses))
			
	}

	/// Add the pagination parameters to the query parameters
	fn add_pagination_params(&mut self) {
		
		self.add_query_param(
			self.pagination_page_size_key.clone(), 
			self.pagination_page_size.to_string()
		);

		match self.pagination {
			PaginationOptions::None => (),
			PaginationOptions::PageIncrement => {
				
				self.add_query_param(
					self.pagination_page_token_key.clone(), 
					self.pagination_page_number.to_string()
				);

			},
			PaginationOptions::OffsetIncrement => {
				
				self.add_query_param(
					self.pagination_page_token_key.clone(), 
					self.pagination_offset.to_string()
				);

			},
			PaginationOptions::Cursor => {

				// add or replace the cursor in the query string
				match &self.pagination_cursor {
					Some(cursor) => {

						self.add_query_param(
							self.pagination_page_token_key.clone(), 
							cursor.clone()
						);
						
					},
					None => ()
				}

			},
		}
	}

	// Add or replace a query parameter
	fn add_query_param(&mut self, param_key: String, param_value: String) {

		match self.query.binary_search_by_key(&param_key, |(a, _b)| a.to_string()) {
			Ok(index) => {
				self.query[index] = (param_key.clone(), param_value);
			},
			Err(_) => self.query.push((param_key.clone(), param_value))
		};

	}

	/// Make sure the json value representation of an API response is iterable.
	/// Returns json array as well as length of that array.
	fn resp_to_iter(&self, resp: Value) -> (usize, Vec<Value>) {

		match resp {
			Value::Array(arr) => (arr.len(), arr),
			_ => (1, [resp].to_vec())
		}

	}

	/// Perform a series of paginated API requests following the page offset approach.
	/// This involves adding the number of records received (the offset) to the request 
	/// query and calling the API on a loop until the response contains less than the 
	/// stated page_size.
	async fn page_offset_req(&mut self) -> Result<Value, Error> {
		
		let mut responses = Vec::new();
		let mut requests_count = 0;

		// Kepp calling API until it contains less records than the page_size parameter
		loop {

			requests_count += 1;

			// Add the pagination parameters to the query parameters
			self.add_pagination_params();

			// Make the request and parse the resonse
			let resp = self.single_request().await?;

			// Make sure the json value is iterable
			let (record_count, mut iter_resp) = self.resp_to_iter(resp);

			// Add response data to data from other responses
			responses.append(&mut iter_resp);

			// increment the page number
			self.pagination_offset += self.pagination_page_size;

			// Decide if finished making paginated requests
			if record_count < self.pagination_page_size || requests_count >= self.max_pagination_requests {
				break
			}

		}

		Ok(serde_json::Value::Array(responses))
			
	}

	/// Perform a series of paginated API requests following the cursor approach.
	/// This involves finding a field in the last response (defined via 
	/// `pagination_cursor_field`) and inserting it into the query parameters of the
	/// next request (use `page_token_key` as the parameter key). Keep making requests
	/// until no cursor field is returned or the records returned are less than the 
	/// `pagination_page_size`.
	async fn cursor_req(&mut self) -> Result<Value, Error> {
		
		let mut responses = Vec::new();
		let mut requests_count = 0;

		// Kepp calling API until it contains less records than the page_size parameter
		loop {

			requests_count += 1;

			// Add the pagination parameters to the query parameters
			match self.pagination_cursor_location {
				CursorLocation::Header => self.add_pagination_params(),
				CursorLocation::Body => self.add_body_params(),
			};

			// Make the request and parse the resonse
			let resp = self.single_request().await?;

			// Get the cursor for the next request
			self.pagination_cursor = self.get_cursor_value(&resp, 0);

			// Make sure the json value is iterable
			let (_record_count, mut iter_resp) = self.resp_to_iter(resp);

			// Add response data to data from other responses
			responses.append(&mut iter_resp);

			// Decide if finished making paginated requests (if a cursor can't be found or
			// maximum requests count is reached).
			if self.pagination_cursor == None || requests_count >= self.max_pagination_requests {
				break
			}

		}

		Ok(serde_json::Value::Array(responses))

	}

	/// Add cursor to the body of the request
	fn add_body_params(&mut self) {

		// add or replace the cursor in the query string
		match &self.pagination_cursor {
			Some(cursor) => {

				match &mut self.body {
					Some(body) => match body {
						Value::Object(body_obj) => {
							let _ = body_obj.insert(self.pagination_page_token_key.clone(), json!(cursor));
							()
						},
						_ => ()
					},
					None => {
						self.body = Some(json!({self.pagination_page_token_key.clone(): cursor}))
					}
				}
				
			},
			None => ()
		}

	}

	/// Try to extract the cursor that will be used to get the next page of results.
	/// Return None if a cursor can't be found.
	fn get_cursor_value(&self, resp: &Value, mut index: usize) -> Option<String> {

		let cursor = match &self.pagination_cursor_field {
			
			Some(fields) => match resp {
				
				Value::Object(resp_obj) => {
					
					match resp_obj.get(&fields[index]) {
						
						Some(filtered_obj) => {
							
							index += 1;

							if fields.len() == index {
								Some(filtered_obj.to_string())
							}
							else {
								self.get_cursor_value(filtered_obj, index)
							}

						},
						None => None
					}

				},

				Value::Array(resp_arr) => {
					
					match self.pagination_cursor_record {
						PaginationCursorRecord::First => self.get_cursor_value(&resp_arr[0], 0),
						PaginationCursorRecord::Last => match &resp_arr.last() {
							Some(last_resp_item) => self.get_cursor_value(&last_resp_item, 0),
							None => None
						},
					}
				},

				_ => None

			},

			None => None

		};

		match cursor {
			Some(cursor) => Some(cursor),
			None => {
				info!(target: &self.table_name, "Could not find the cursor '{:?}' in '{:?}' so stopping pagination.", self.pagination_cursor_field, resp);
				None
			}
		}

	}

}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::{ Utc, TimeZone, naive::NaiveDate, naive::NaiveDateTime };
	use http::header::HOST;
	use crate::tables::test_utils::{ 
		mock_api, 
		api_resp_batch, 
		nested_api_resp_batch, 
		paginated_resp_batch,
		reduced_paginated_resp_batch,
		paginated_offset_resp_batch,
		many_nested_api_resp_batch,
		paginated_offset_resp_batch_filtered
	};

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
        assert_eq!(table.query, Vec::new());
        assert_eq!(table.basic_auth, None);
        assert_eq!(table.headers, HeaderMap::new());
        assert_eq!(table.bookmark, chrono::DateTime::<Utc>::MIN_UTC);
        assert_eq!(table.poll_interval, 10_000);
        assert!(matches!(table.on_fail, FailAction::Stop));
        assert!(matches!(table.one_request, false));
        assert_eq!(table.schema, None);
        assert!(matches!(table.pagination, PaginationOptions::None));
        assert_eq!(table.pagination_page_token_key, "page");
        assert_eq!(table.pagination_page_number, 0);
        assert_eq!(table.pagination_page_size_key, "page_size");
        assert_eq!(table.pagination_page_size, 5);
        assert_eq!(table.max_pagination_requests, 100);
        assert_eq!(table.pagination_offset, 0);
        assert_eq!(table.pagination_cursor_field, None);
		assert_eq!(table.pagination_cursor_record, PaginationCursorRecord::Last);
		assert_eq!(table.pagination_cursor, None);
		assert_eq!(table.pagination_cursor_location, CursorLocation::Body);

    }

    #[test]
    fn table_with_full_config() {
    
        let content = String::from(r#"
            table_name = "trello_board"
            endpoint_url = "https://trello.com/b/abc/board.json" 
            method = "put"
            select_field = ["cards"]
            timeout = 10
            query = [["query_key","query_value"]]
            basic_auth = ["username","password"]
            headers = [["host","world"]]
            bookmark = "2023-08-21T00:55:00z"
            poll_interval = 20000
            on_fail = "skip"
            one_request = true
            pagination = "page_increment"
            pagination_page_token_key = "page_number"
            pagination_page_number = 1
            pagination_page_size_key = "size_of_page"
            pagination_page_size = 10
            max_pagination_requests = 11
            pagination_offset = 1
            pagination_cursor_field = ["cursor"]
            pagination_cursor_record = "first"
            pagination_cursor = "1"
            pagination_cursor_location = "header"
        "#);

        let table: SourceApi = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name, "trello_board");
        assert_eq!(table.endpoint_url, Url::parse("https://trello.com/b/abc/board.json").unwrap());
        assert!(matches!(table.method, HttpMethod::Put));
        assert_eq!(table.select_field, Some(vec!["cards".to_string()]));
        assert_eq!(table.timeout, Some(Duration::from_secs(10)));
        assert_eq!(table.query, vec![("query_key".to_string(), "query_value".to_string())]);
        assert_eq!(table.basic_auth, Some(("username".to_string(), "password".to_string())));
        assert_eq!(table.poll_interval, 20_000);
        assert!(matches!(table.on_fail, FailAction::Skip));
        assert!(matches!(table.one_request, true));
        assert_eq!(table.schema, None);
        assert!(matches!(table.pagination, PaginationOptions::PageIncrement));
        assert_eq!(table.pagination_page_token_key, "page_number");
        assert_eq!(table.pagination_page_number, 1);
        assert_eq!(table.pagination_page_size_key, "size_of_page");
        assert_eq!(table.pagination_page_size, 10);
        assert_eq!(table.max_pagination_requests, 11);
        assert_eq!(table.pagination_offset, 1);
        assert_eq!(table.pagination_cursor_field, Some(vec!["cursor".to_string()]));
		assert_eq!(table.pagination_cursor_record, PaginationCursorRecord::First);
		assert_eq!(table.pagination_cursor, Some("1".to_string()));
		assert_eq!(table.pagination_cursor_location, CursorLocation::Header);

        let dt: NaiveDateTime = NaiveDate::from_ymd_opt(2023, 8, 21).unwrap().and_hms_opt(0, 55, 0).unwrap();
        let datetime_utc = Utc.from_utc_datetime(&dt);
        assert_eq!(table.bookmark, datetime_utc);

        let mut headers = HeaderMap::new();
        let _ = headers.insert(HOST, "world".parse().unwrap());
        assert_eq!(table.headers, headers);

    }

    #[test]
    fn missing_mandatory_field() {
    
        let content = String::from(r#"
            table_name = "csv_table"
        "#);

        let table: Result<SourceApi, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "missing field `endpoint_url`", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn url_is_not_a_url() {
    
        let content = String::from(r#"
            endpoint_url = "hello" 
        "#);

        let table: Result<SourceApi, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "The string 'hello' is not a url.", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn table_name_contains_too_many_characters() {
    
        let content = String::from(r#"
            table_name = "name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name"
            endpoint_url = "https://trello.com/b/abc/board.json" 
        "#);

        let table: Result<SourceApi, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(
        		e.message(), 
        		"The string: name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name is longer than 500 chars.", 
        		"Incorrect error message."
        	),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn table_name_function_returns_table_name() {
    
        let content = String::from(r#"
            table_name = "trello_board"
            endpoint_url = "https://trello.com/b/abc/board.json" 
        "#);

        let table: SourceApi = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name(), "trello_board");

    }

    #[test]
    fn on_fail_function_returns_fail_action() {
    
        let content = String::from(r#"
            endpoint_url = "https://trello.com/b/abc/board.json" 
        "#);

        let table: SourceApi = toml::from_str(&content).unwrap();

        assert!(matches!(table.on_fail(), FailAction::Stop));

    }

    #[test]
    fn poll_interval_function_returns_correct_duration() {
    
        let content = String::from(r#"
            endpoint_url = "https://trello.com/b/abc/board.json" 
        "#);

        let table: SourceApi = toml::from_str(&content).unwrap();

        assert_eq!(table.poll_interval(), Duration::from_millis(10_000));

    }

    #[test]
    fn http_method_not_a_real_method() {
    
        let content = String::from(r#"
            endpoint_url = "https://trello.com/b/abc/board.json" 
            method = "insert"
        "#);

        let table: Result<SourceApi, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "unknown variant `insert`, expected one of `get`, `post`, `put`, `patch`, `delete`", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[tokio::test]
    async fn can_make_single_get_request() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    	"#, mock_api.url("/user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_single_post_request() {
    
    	let mock_api = mock_api("POST", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		method = "post"
    	"#, mock_api.url("/user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_single_put_request() {
    
    	let mock_api = mock_api("PUT", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		method = "put"
    	"#, mock_api.url("/user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_single_patch_request() {
    
    	let mock_api = mock_api("PATCH", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		method = "patch"
    	"#, mock_api.url("/user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_single_delete_request() {
    
    	let mock_api = mock_api("DELETE", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		method = "delete"
    	"#, mock_api.url("/user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_select_fields_from_single_record_resp() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		select_field = ["address"]
    	"#, mock_api.url("/user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = nested_api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_select_fields_from_array_resp() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		select_field = ["address"]
    	"#, mock_api.url("/many_users"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = many_nested_api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_request_including_a_query() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		query = [["query", "Metallica"]]
    	"#, mock_api.url("/query_user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_request_including_a_header() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		headers = [["key", "value"]]
    	"#, mock_api.url("/header_user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_request_including_basic_auth() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		basic_auth = ["demo", "p@55w0rd"]
    	"#, mock_api.url("/auth_user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_request_including_body() {
    
    	let mock_api = mock_api("POST", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		method = "post"
    		one_request = true
    		body = {{ "name" = "Hans" }}
    	"#, mock_api.url("/body_user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = api_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn throws_error_when_400_code_is_received() {
    
    	let mock_api = mock_api("GET", 404);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    	"#, mock_api.url("/user"));

        // Create the table and make a call to the mock api
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let read_result = table.read_new_data().await;

        assert!(read_result.is_err());

    }

    #[tokio::test]
    async fn throws_error_when_500_code_is_received() {
    
    	let mock_api = mock_api("GET", 500);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    	"#, mock_api.url("/user"));

        // Create the table and make a call to the mock api
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let read_result = table.read_new_data().await;

        assert!(read_result.is_err());

    }

    #[tokio::test]
    async fn can_make_page_increment_paginated_requests() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		pagination = "page_increment"
            pagination_page_token_key = "page"
            pagination_page_number = 0
            pagination_page_size_key = "page_size"
            pagination_page_size = 5
    	"#, mock_api.url("/paged_user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = paginated_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn pagination_max_requests_can_limit_calls() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		pagination = "page_increment"
            pagination_page_token_key = "page"
            pagination_page_number = 0
            pagination_page_size_key = "page_size"
            pagination_page_size = 5
            max_pagination_requests = 1
    	"#, mock_api.url("/paged_user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = reduced_paginated_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_offset_paginated_requests() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		pagination = "offset_increment"
            pagination_page_token_key = "offset"
            pagination_page_number = 0
            pagination_page_size_key = "page_size"
            pagination_page_size = 5
    	"#, mock_api.url("/paged_offset_user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = paginated_offset_resp_batch();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_make_paginated_requests_and_select_field() {
    
    	let mock_api = mock_api("GET", 200);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		select_field = ["name"]
    		pagination = "offset_increment"
            pagination_page_token_key = "offset"
            pagination_page_number = 0
            pagination_page_size_key = "page_size"
            pagination_page_size = 5
    	"#, mock_api.url("/paged_offset_user"));

        // Create the table and read in new data from the mock api.
        // Parse the table as a vec of record batches.
        let mut table: SourceApi = toml::from_str(&config).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        let expected_batch = paginated_offset_resp_batch_filtered();

        assert!(read_success);
        assert!(df_data.contains(&expected_batch));
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

}