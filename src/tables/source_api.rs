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

use reqwest::{ Url, header::HeaderMap, RequestBuilder };
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

	/// How long to wait for a response before cancelling the request (in seconds)
	#[serde(default, deserialize_with="deserialize_duration", serialize_with="serialize_duration")]
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

		let mut req = self.pick_method();
		req = self.add_query(req);
		req = self.add_headers(req);
		req = self.add_basic_auth(req);
		req = self.add_body(req);

		// *******************************************************************
		// and make sure to handle the http status code
		// *******************************************************************
		let resp = match req.send().await {
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

		match &self.query {
			Some(query_params) => req.query(&query_params),
			None => req
		}

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

	/// Parse the API json response body into an Arrow RecordBatch type
	fn json_to_record_batch(&mut self, mut json: Value) -> RecordBatch {

		// Select nested data from the response.
		json = match self.filter_result(json.clone(), 0) {
			Ok(filtered_json) => filtered_json,
			Err(e) => {
				warn!("Got error '{:?}' while selecting field from result. Ignoring field select.", e);
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
		let iter_json = match json {
			Value::Array(arr) => arr,
			_ => [json].to_vec()
		};

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

				_ => Err(Error::new(ErrorKind::Other, format!("The value {} is not an object.", json)))

			},

			None => Ok(json)

		}

	}

}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::{ Utc, TimeZone, naive::NaiveDate, naive::NaiveDateTime };
	use http::header::HOST;
	use crate::tables::test_utils::{ basic_mock_api, api_resp_batch, nested_api_resp_batch };

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
        assert!(matches!(table.one_request, false));
        assert_eq!(table.schema, None);

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
        "#);

        let table: SourceApi = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name, "trello_board");
        assert_eq!(table.endpoint_url, Url::parse("https://trello.com/b/abc/board.json").unwrap());
        assert!(matches!(table.method, HttpMethod::Put));
        assert_eq!(table.select_field, Some(vec!["cards".to_string()]));
        assert_eq!(table.timeout, Some(Duration::from_secs(10)));
        assert_eq!(table.query, Some(vec![("query_key".to_string(), "query_value".to_string())]));
        assert_eq!(table.basic_auth, Some(("username".to_string(), "password".to_string())));
        assert_eq!(table.poll_interval, 20_000);
        assert!(matches!(table.on_fail, FailAction::Skip));
        assert!(matches!(table.one_request, true));
        assert_eq!(table.schema, None);

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
    
    	let mock_api = basic_mock_api("GET", false, false, false, false);

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
    
    	let mock_api = basic_mock_api("POST", false, false, false, false);

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
    
    	let mock_api = basic_mock_api("PUT", false, false, false, false);

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
    
    	let mock_api = basic_mock_api("PATCH", false, false, false, false);

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
    
    	let mock_api = basic_mock_api("DELETE", false, false, false, false);

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
    async fn can_select_fields_from_resp() {
    
    	let mock_api = basic_mock_api("GET", false, false, false, false);

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
    async fn can_make_request_including_a_query() {
    
    	let mock_api = basic_mock_api("GET", true, false, false, false);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		query = [["query", "Metallica"]]
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
    async fn can_make_request_including_a_header() {
    
    	let mock_api = basic_mock_api("GET", false, true, false, false);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		headers = [["key", "value"]]
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
    async fn can_make_request_including_basic_auth() {
    
    	let mock_api = basic_mock_api("GET", false, false, true, false);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		one_request = true
    		basic_auth = ["demo", "p@55w0rd"]
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
    async fn can_make_request_including_body() {
    
    	let mock_api = basic_mock_api("POST", false, false, false, true);

    	// define table config using mock servers url
    	let config = format!(r#"
    		endpoint_url = "{}"
    		method = "post"
    		one_request = true
    		body = {{ "name" = "Hans" }}
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

}