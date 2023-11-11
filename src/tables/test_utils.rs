// TestDir crates
use std::path::PathBuf;
use std::fs::{ create_dir_all, remove_dir_all };

// API test crates
use serde_json::json;
use std::sync::Arc;
use arrow_array::{ RecordBatch, StringArray, Int64Array, StructArray, ArrayRef };
use arrow_schema::{ Field, Fields, DataType };

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

/// A function for running a mock API server
pub fn mock_api(method: &str, return_status: u16) -> httpmock::MockServer {

    // Start a mock server.
    let server = httpmock::MockServer::start();

    // Define a basic response
    let basic_resp = json!({ 
        "name": "Hans", 
        "id": 1,
        "address": {
            "number": 2,
            "line": "terrace road",
            "postcode": "S001AB"
        }
    });

    // Endpoint with no specific requirements beyond the method
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/user")
            .method(method);
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(basic_resp.clone());
    });

    // Endpoint requiring query paramters
    let _mock = server.mock(|when, then| {
        
        let _ = when
            .path("/user")
            .method(method)
            .query_param("query", "Metallica");

        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(basic_resp.clone());
            
    });

    // Endpoint requiring a header
    let _mock = server.mock(|when, then| {
        
        let _ = when
            .path("/user")
            .method(method)
            .header("key", "value");

        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(basic_resp.clone());
            
    });

    // Endpoint requiring authentication
    let _mock = server.mock(|when, then| {
        
        let _ = when
            .path("/user")
            .method(method)
            .header("Authorization", "Basic ZGVtbzpwQDU1dzByZA==");

        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(basic_resp.clone());
            
    });

    // Endpoint requiring a json encoded body
    let _mock = server.mock(|when, then| {
        
        let _ = when
            .path("/user")
            .method(method)
            .header("content-type", "application/json")
            .json_body(json!({ "name": "Hans" }));

        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(basic_resp.clone());
            
    });

    // Endpoint for first page of pagination
    let _mock = server.mock(|when, then| {
        
        let _ = when
            .path("/paged_user")
            .method(method)
            .query_param("page", "0")
            .query_param("page_size", "5");
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!([
                {"id": 1},
                {"id": 2},
                {"id": 3},
                {"id": 4},
                {"id": 5}
            ]));

    });

    // Endpoint for second page of pagination
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/paged_user")
            .method(method)
            .query_param("page", "1")
            .query_param("page_size", "5");
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!([
                {"id": 6},
                {"id": 7}
            ]));

    });

    server

}


/// A function for generating a test record batch to test an api response against
pub fn api_resp_batch() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("address", DataType::Struct(Fields::from(vec![
                Field::new("line", DataType::Utf8, true),
                Field::new("number", DataType::Int64, true),
                Field::new("postcode", DataType::Utf8, true),
            ])), true)),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("line", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec!["terrace road"])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("number", DataType::Int64, true)),
                    Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("postcode", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec!["S001AB"])) as ArrayRef,
                ),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["Hans"])) as ArrayRef,
        ),
    ]).into()

}

/// Same as the `api_resp_batch` function but returns the fields in the nested 
/// `address` field
pub fn nested_api_resp_batch() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("line", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["terrace road"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("number", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("postcode", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["S001AB"])) as ArrayRef,
        ),
    ]).into()

}

/// A test record batch for the paginated requests
pub fn paginated_resp_batch() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![1,2,3,4,5,6,7])) as ArrayRef,
        )
    ]).into()

}