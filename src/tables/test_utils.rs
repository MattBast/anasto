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

    // Endpoint returning many users
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/many_users")
            .method(method);
        
        let array_resp = json!([
            { 
                "name": "Hans", 
                "id": 1,
                "address": {
                    "number": 1,
                    "line": "terrace road",
                    "postcode": "S001AB"
                }
            },
            { 
                "name": "Zimmer", 
                "id": 2,
                "address": {
                    "number": 2,
                    "line": "new road",
                    "postcode": "S002AB"
                }
            }
        ]);

        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(array_resp);
    });

    // Endpoint requiring query paramters
    let _mock = server.mock(|when, then| {
        
        let _ = when
            .path("/query_user")
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
            .path("/header_user")
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
            .path("/auth_user")
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
            .path("/body_user")
            .method(method)
            .header("content-type", "application/json")
            .json_body(json!({ "name": "Hans" }));

        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(basic_resp.clone());
            
    });

    // Endpoint for first page of page increment pagination
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

    // Endpoint for second page of page increment pagination
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

    // Endpoint for first page of offset pagination
    let _mock = server.mock(|when, then| {
        
        let _ = when
            .path("/paged_offset_user")
            .method(method)
            .query_param("offset", "0")
            .query_param("page_size", "5");
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!([
                {"id": 10, "name": {"first": "ten"}},
                {"id": 20, "name": {"first": "twenty"}},
                {"id": 30, "name": {"first": "thirty"}},
                {"id": 40, "name": {"first": "fourty"}},
                {"id": 50, "name": {"first": "fifty"}}
            ]));

    });

    // Endpoint for second page of offset pagination
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/paged_offset_user")
            .method(method)
            .query_param("offset", "5")
            .query_param("page_size", "5");
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!([
                {"id": 60, "name": {"first": "sixty"}},
                {"id": 70, "name": {"first": "seventy"}}
            ]));

    });

    // Endpoint for third page of cursor pagination
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/paged_cursor_user")
            .method(method)
            .query_param("cursor_key", "2");
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!({
                "results": [
                    {"id": 700, "name": {"first": "seventy"}}
                ]
            }));

    });

    // Endpoint for second page of cursor pagination
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/paged_cursor_user")
            .method(method)
            .query_param("cursor_key", "1");
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!({
                "next_cursor": "2", 
                "results": [
                    {"id": 400, "name": {"first": "fourty"}},
                    {"id": 500, "name": {"first": "fifty"}},
                    {"id": 600, "name": {"first": "sixty"}}
                ]
            }));

    });

    // Endpoint for first page of cursor pagination
    let _mock = server.mock(|when, then| {
        
        let _ = when
            .path("/paged_cursor_user")
            .method(method);
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!({
                "next_cursor": "1", 
                "results": [
                    {"id": 100, "name": {"first": "ten"}},
                    {"id": 200, "name": {"first": "twenty"}},
                    {"id": 300, "name": {"first": "thirty"}}
                ]
            }));

    });

    // Endpoint for second page of cursor body pagination
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/paged_cursor_body_user")
            .method(method)
            .json_body(json!({ "record_id": "300" }));
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!([
                {"record_id": 400, "name": {"first": "fourty"}},
                {"record_id": 500, "name": {"first": "fifty"}}
            ]));

    });

    // Endpoint for first page of cursor body pagination
    let _mock = server.mock(|when, then| {
        
        let _ = when
            .path("/paged_cursor_body_user")
            .method(method);
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!([
                {"record_id": 100, "name": {"first": "ten"}},
                {"record_id": 200, "name": {"first": "twenty"}},
                {"record_id": 300, "name": {"first": "thirty"}}
            ]));

    });

    // Endpoint for formatted bookmark request
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/bookmark_user")
            .method(method)
            .json_body(json!({ "updated_after": "1970-01-01T00:00:00Z" }));
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!([
                {"id": 1, "name": "The Very Hungry Caterpillar"},
                {"id": 2, "name": "Sleeping Beauty"}
            ]));

    });

    // Endpoint for first page of bookmark request
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/bookmark_user")
            .method(method)
            .json_body(json!({ "updated_after": "1970-01-01 00:00:00 UTC" }));
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!([
                {"id": 1, "name": "The Picture of Dorian Gray"},
                {"id": 2, "name": "The Lies of Locke Lamora"}
            ]));

    });

    // Endpoint for second page of bookmark request
    let _mock = server.mock(|when, then| {

        let _ = when
            .path("/bookmark_user")
            .method(method)
            .body_contains("updated_after");
            
        let _ = then
            .status(return_status)
            .header("content-type", "application/json")
            .json_body(json!([
                {"id": 3, "name": "Dune"}
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

/// Same as the `nested_api_resp_batch` function but returns 2 rows of data
pub fn many_nested_api_resp_batch() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("line", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["terrace road", "new road"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("number", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![1,2])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("postcode", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["S001AB", "S002AB"])) as ArrayRef,
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

/// A test record batch for the paginated requests
pub fn reduced_paginated_resp_batch() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![1,2,3,4,5])) as ArrayRef,
        )
    ]).into()

}

/// A test record batch for the paginated requests
pub fn paginated_offset_resp_batch() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![10,20,30,40,50,60,70])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("name", DataType::Struct(Fields::from(vec![
                Field::new("first", DataType::Utf8, true),
            ])), true)),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("first", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec!["ten","twenty","thirty","fourty","fifty","sixty","seventy"])) as ArrayRef,
                ),
            ])) as ArrayRef,
        ),
    ]).into()

}

/// A test record batch for the paginated requests
pub fn paginated_cursor_resp_batch() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![100,200,300,400,500,600,700])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("name", DataType::Struct(Fields::from(vec![
                Field::new("first", DataType::Utf8, true),
            ])), true)),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("first", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec!["ten","twenty","thirty","fourty","fifty","sixty","seventy"])) as ArrayRef,
                ),
            ])) as ArrayRef,
        ),
    ]).into()

}

/// A test record batch for the paginated requests
pub fn paginated_cursor_body_resp_batch() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Struct(Fields::from(vec![
                Field::new("first", DataType::Utf8, true),
            ])), true)),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("first", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec!["ten","twenty","thirty","fourty","fifty"])) as ArrayRef,
                ),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("record_id", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![100,200,300,400,500])) as ArrayRef,
        ),
    ]).into()

}

/// A test record batch for the filtered paginated requests
pub fn paginated_offset_resp_batch_filtered() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("first", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["ten","twenty","thirty","fourty","fifty","sixty","seventy"])) as ArrayRef,
        ),
    ]).into()

}

/// A test record batch for the paginated requests
pub fn bookmark_resp_batch() -> Vec<RecordBatch> {

    [
        StructArray::from(vec![
            (
                Arc::new(Field::new("id", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![1,2])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["The Picture of Dorian Gray","The Lies of Locke Lamora"])) as ArrayRef,
            ),
        ]).into(),
        StructArray::from(vec![
            (
                Arc::new(Field::new("id", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![3])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["Dune"])) as ArrayRef,
            ),
        ]).into()
    ].to_vec()

}

/// A test record batch for the paginated requests
pub fn formatted_bookmark_resp_batch() -> RecordBatch {

    StructArray::from(vec![
        (
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![1,2])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["The Very Hungry Caterpillar","Sleeping Beauty"])) as ArrayRef,
        ),
    ]).into()

}