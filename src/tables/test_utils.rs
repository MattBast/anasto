// TestDir crates
use std::path::PathBuf;
use std::fs::{ create_dir_all, remove_dir_all };

// API test crates
use serde_json::json;
use std::sync::Arc;
use arrow_array::{ RecordBatch, StringArray, Int64Array };
use arrow_schema::{ Schema, Field, DataType };

/// A helper struct for creating and deleting directories needed by test functions
#[derive(Debug)]
pub struct TestDir {
    
    /// Path pointing at the test directory
    pub path: PathBuf,
}

impl TestDir {
    
    /// Creates a new directory for a path if it doesn't already exist.
    /// In other test frameworks this would be known as a "Setup" function.
    pub fn new(path: &'static str) -> Self {

        let buf = PathBuf::from(path);

        if !buf.exists() { create_dir_all(buf.to_str().unwrap()).unwrap() }
        if !buf.is_dir() { panic!("The path provided is not pointing at a diectory.") }

        TestDir { path: buf }

    }

}

impl Drop for TestDir {
    
    /// When a test function ends, delete all the files created by the function.
    /// In other test frameworks this would be known as a "Teardown" function.
    fn drop(&mut self) {
        
        remove_dir_all(self.path.to_str().unwrap()).unwrap()
    
    }

}

/// A function for creating a simple mock server
pub fn basic_mock_api(method: &str) -> httpmock::MockServer {

    // Start a mock server.
    let server = httpmock::MockServer::start();

    // Create a mock on the server.
    let _mock = server.mock(|when, then| {
        let _ = when
            .path("/user")
            .method(method);
        let _ = then
            .status(200)
            .header("content-type", "application/json")
            .json_body(json!({ "name": "Hans", "id": 1 }));
    });

    server

}

/// A function for generating a test record batch to test an api response against
pub fn api_resp_batch() -> RecordBatch {

    // create the expected contents of the dataframe (as record batches)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Hans"])),
        ],
    ).unwrap()

}