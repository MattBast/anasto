use std::path::Path;
use config::bootstrap::{ init, run };
use std::{ thread, time };
use http::StatusCode;
use serde_json::{ json, Value };
use std::fs::{ remove_file, remove_dir_all };
use url::Url;
use tungstenite::{connect, Message, WebSocket};
use tungstenite::stream::MaybeTlsStream;
use std::net::TcpStream;

// helper function to teardown a directory (and child files) created by the tests
fn teardown(dir_name: &str) {

    remove_dir_all(dir_name).unwrap();

}

// helper function to initialise a new instance of Anasto, remove 
// any files from previous tests and start a client listening to
// its websocket
async fn setup(
    schema_filepaths: &[&str], 
    config_filepath: &'static str,
    address: &'static str
) -> WebSocket<MaybeTlsStream<TcpStream>> {

    // teardown files that may interfere with the tests
    schema_filepaths
        .iter()
        .for_each(|filename| { 
            
            if Path::new("does_not_exist.txt").exists() {
                remove_file(filename).unwrap();
            }
                 
        });

    // start Anasto
    tokio::spawn(async move {
        
        let (api_tx, api, bus_tx, bus) = init(
            config_filepath.to_string(), 
            false
        ).await;
        run(api_tx, api, bus_tx, bus).await;

    });

    // give Anasto a moment to start
    let wait = time::Duration::from_millis(50);
    thread::sleep(wait);

    // start a mock subscriber who listens to the Anasto websocket
    let (mut socket, _response) = connect(
        Url::parse(address).unwrap()
    ).expect("Can't connect");

    socket.write_message(Message::Text(r#"{
        "action": "listen"
    }"#.into())).expect("Failed to start listening");

    socket

}

// helper function that makes a post request to the record endpoint
// and returns the status and body (as a json value)
async fn post_request(
    address: &'static str, 
    body: Value
) -> (StatusCode, Value) {

    let client = reqwest::Client::new();

    let resp = client.post(address)
        .body(body.to_string())
        .send()
        .await
        .unwrap();

    let status = resp.status();
    let mut body: Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();

    body = match body.as_object_mut() {
        Some(obj) => {
            let _ = obj.remove("created_at");
            let _ = obj.remove("event_id");
            json!(obj)
        },
        None => body
    };
        

    (status, body)

}

// helper function to compare two json record values
fn assert_record(mut test: Value, control: Value) {

    if test.is_array() {
            
        test
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .for_each(|record| {
                
                let _ = record.as_object_mut().unwrap().remove("created_at");
                let _ = record.as_object_mut().unwrap().remove("event_id");

            });

    }
    else {
        
        let _ = test.as_object_mut().unwrap().remove("created_at");
        let _ = test.as_object_mut().unwrap().remove("event_id");

    }

    assert_eq!(test, control);

}

// helper function to read a schema file and check its contents
fn assert_schema(filepath: &'static str, expected_schema: Value) {

    let contents = std::fs::read_to_string(filepath)
        .expect("Should have been able to read the file");

    let mut file_schema: Value = serde_json::from_str(&contents).unwrap();

    let _ = file_schema.as_object_mut().unwrap().remove("created_at");
    let _ = file_schema.as_object_mut().unwrap().remove("event_id");

    assert_eq!(file_schema, expected_schema);

}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn anasto_health_can_be_checked() {
    
    let _ = setup(
        &[],
        "./config.toml",
        "ws://127.0.0.1:3030/subscribe"
    ).await;
    
    // make a request to the Anasto API
    let resp = reqwest::get("http://127.0.0.1:3030/health")
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.text().await.unwrap(), 
        "{\"code\":200,\"state\":\"good\"}".to_string()
    );

}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn can_read_config_file() {
    
    let _ = setup(
        &[],
        "./test_configs/basic.toml",
        "ws://0.0.0.0:8080/subscribe"
    ).await;
    
    // make a request to the health endpoint
    let resp = reqwest::get("http://0.0.0.0:8080/health")
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.text().await.unwrap(), 
        "{\"code\":200,\"state\":\"good\"}".to_string()
    );

    teardown("./test_schemas/basic/");

}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn post_record_creates_schema_and_sends_record_to_subscriber() {
    
    let mut socket_client = setup(
        &["./test_schemas/one_record/test_table.json"],
        "./test_configs/one_record.toml",
        "ws://0.0.0.0:8081/subscribe"
    ).await;
    
    // post record to Anasto and check for positive response
    let (resp_status, resp_body) = post_request(
        "http://0.0.0.0:8081/record",
        json!({ 
            "table_name": "test_table",
            "record": {"hello": "world"}
        })
    ).await;

    assert_eq!(resp_status, StatusCode::CREATED);
    
    assert_record(
        resp_body, 
        json!({ 
            "table_name": "test_table",
            "event_type": "RECORD",
            "record": {"hello": "world"},
            "operation": "CREATE"
        })
    );

    // check if a schema was created for the record
    assert_schema(
        "./test_schemas/one_record/test_table.json", 
        json!({
            "event_type": "SCHEMA", 
            "key_properties": [], 
            "operation": "CREATE", 
            "schema": {
                "fields": [{
                    "name": "hello", 
                    "type": "string"
                }], 
                "name": "test_table", 
                "type": "record"
            }, 
            "table_name": "test_table"
        })
    );
    
    // listen to websocket as a mock subscriber. Make sure the posted 
    // record is sent to the subscriber.
    let msg = socket_client.read_message().expect("Error reading message");

    if msg.is_binary() || msg.is_text() {
        
        let json_msg: Value = serde_json::from_slice(&msg.clone().into_data()).unwrap();
        
        assert_record(
            json_msg[0].clone(), 
            json!({ 
                "table_name": "test_table",
                "event_type": "RECORD",
                "record": {"hello": "world"},
                "operation": "CREATE"
            })
        );

    }
    else {

        assert!(false);

    }

    teardown("./test_schemas/one_record/");

}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn posting_many_records_groups_them_into_one_buffer_per_table() {
    
    let mut socket_client = setup(
        &["./test_schemas/multi_record/table_one.json", "./test_schemas/multi_record/table_two.json"],
        "./test_configs/multi_record.toml",
        "ws://0.0.0.0:8082/subscribe"
    ).await;
    
    // post record to Anasto and check for positive response
    let _ = post_request(
        "http://0.0.0.0:8082/record",
        json!({ "table_name": "table_one", "record": {"id": "1"}})
    ).await;
    let _ = post_request(
        "http://0.0.0.0:8082/record",
        json!({ "table_name": "table_one", "record": {"id": "2"}})
    ).await;
    let _ = post_request(
        "http://0.0.0.0:8082/record",
        json!({ "table_name": "table_two", "record": {"id": "1"}})
    ).await;
    let _ = post_request(
        "http://0.0.0.0:8082/record",
        json!({ "table_name": "table_two", "record": {"id": "2"}})
    ).await;
    let _ = post_request(
        "http://0.0.0.0:8082/record",
        json!({ "table_name": "table_one", "record": {"id": "3"}})
    ).await;
    
    let msg_1 = socket_client.read_message().expect("Error reading message");    
    let json_msg_1: Value = serde_json::from_slice(&msg_1.clone().into_data()).unwrap();
    
    assert_record(
        json_msg_1.clone(), 
        json!([
            { 
                "table_name": "table_one",
                "event_type": "RECORD",
                "record": {"id": "1"},
                "operation": "CREATE"
            },
            { 
                "table_name": "table_one",
                "event_type": "RECORD",
                "record": {"id": "2"},
                "operation": "CREATE"
            },
            { 
                "table_name": "table_one",
                "event_type": "RECORD",
                "record": {"id": "3"},
                "operation": "CREATE"
            }
        ])
    );

    let msg_2 = socket_client.read_message().expect("Error reading message");    
    let json_msg_2: Value = serde_json::from_slice(&msg_2.clone().into_data()).unwrap();
    
    assert_record(
        json_msg_2.clone(), 
        json!([
            { 
                "table_name": "table_two",
                "event_type": "RECORD",
                "record": {"id": "1"},
                "operation": "CREATE"
            },
            { 
                "table_name": "table_two",
                "event_type": "RECORD",
                "record": {"id": "2"},
                "operation": "CREATE"
            }
        ])
    );

    teardown("./test_schemas/multi_record/");

}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn schemas_can_be_created() {
    
    let _ = setup(
        &[
            "./test_schemas/create_schemas/record_table_one.json", 
            "./test_schemas/create_schemas/record_table_two.json"
        ],
        "./test_configs/create_schemas.toml",
        "ws://0.0.0.0:8083/subscribe"
    ).await;
    
    // create schemas and check api responses
    let (resp_status, resp_body) = post_request(
        "http://0.0.0.0:8083/schema",
        json!({ 
            "table_name": "record_table_one",
            "schema": r#"{
                "name": "record_table_one",
                "type": "record",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }"#
        })
    ).await;

    assert_eq!(resp_status, StatusCode::CREATED);
    
    assert_eq!(
        resp_body, 
        json!({
            "event_type": "SCHEMA", 
            "key_properties": [], 
            "operation": "CREATE", 
            "schema": {
                "fields": [{
                    "name": "id", 
                    "type": "string"
                }], 
                "name": "record_table_one", 
                "type": "record"
            }, 
            "table_name": "record_table_one"
        })
    );

    let (resp_status, resp_body) = post_request(
        "http://0.0.0.0:8083/schema",
        json!({ 
            "table_name": "record_table_two",
            "schema": r#"{
                "name": "record_table_two",
                "type": "record",
                "fields": [
                    {"name": "identity", "type": "string"}
                ]
            }"#
        })
    ).await;

    assert_eq!(resp_status, StatusCode::CREATED);
    
    assert_eq!(
        resp_body, 
        json!({
            "event_type": "SCHEMA", 
            "key_properties": [], 
            "operation": "CREATE", 
            "schema": {
                "fields": [{
                    "name": "identity", 
                    "type": "string"
                }], 
                "name": "record_table_two", 
                "type": "record"
            }, 
            "table_name": "record_table_two"
        })
    );

    // check that the schemas were saved to file
    assert_schema(
        "./test_schemas/create_schemas/record_table_one.json", 
        json!({
            "event_type": "SCHEMA", 
            "key_properties": [], 
            "operation": "CREATE", 
            "schema": {
                "fields": [{
                    "name": "id", 
                    "type": "string"
                }], 
                "name": "record_table_one", 
                "type": "record"
            }, 
            "table_name": "record_table_one"
        })
    );

    assert_schema(
        "./test_schemas/create_schemas/record_table_two.json", 
        json!({
            "event_type": "SCHEMA", 
            "key_properties": [], 
            "operation": "CREATE", 
            "schema": {
                "fields": [{
                    "name": "identity", 
                    "type": "string"
                }], 
                "name": "record_table_two", 
                "type": "record"
            }, 
            "table_name": "record_table_two"
        })
    );

    teardown("./test_schemas/create_schemas/");

}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn schemas_are_enforced() {

    let _ = setup(
        &[
            "./test_schemas/enforced_schemas/test_table.json", 
            "./test_schemas/enforced_schemas/record_table_two.json"
        ],
        "./test_configs/enforced_schemas.toml",
        "ws://0.0.0.0:8084/subscribe"
    ).await;
    
    // create a schema
    let _ = post_request(
        "http://0.0.0.0:8084/schema",
        json!({ 
            "table_name": "test_table",
            "schema": r#"{
                "name": "test_table",
                "type": "record",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }"#
        })
    ).await;

    // create a record that fits the schema
    let (good_status, _) = post_request(
        "http://0.0.0.0:8084/record",
        json!({ "table_name": "table_one", "record": {"id": "1"}})
    ).await;

    assert_eq!(good_status, StatusCode::CREATED);

    // create a record that violates the schema
    let (good_status, _) = post_request(
        "http://0.0.0.0:8084/record",
        json!({ "table_name": "table_one", "record": {"identity": "1"}})
    ).await;

    assert_eq!(good_status, StatusCode::BAD_REQUEST);

    teardown("./test_schemas/enforced_schemas/");

}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn can_read_existing_schemas() {
    
    let _ = setup(
        &[],
        "./test_configs/existing_schemas.toml",
        "ws://0.0.0.0:8085/subscribe"
    ).await;
    
    let resp = reqwest::get("http://0.0.0.0:8085/schema?table_name=schema_one")
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    let mut saved_schema = resp.json::<Value>().await.unwrap();
    let _ = saved_schema.as_object_mut().unwrap().remove("created_at");
    let _ = saved_schema.as_object_mut().unwrap().remove("event_id");

    assert_eq!(
        saved_schema, 
        json!({
            "event_type": "SCHEMA", 
            "key_properties": [], 
            "operation": "CREATE", 
            "schema": {
                "fields": [{
                    "name": "identity", 
                    "type": "string"
                }], 
                "name": "schema_one", 
                "type": "record"
            }, 
            "table_name": "schema_one"
        })
    );

    let resp = reqwest::get("http://0.0.0.0:8085/schema?table_name=schema_two")
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    let mut saved_schema = resp.json::<Value>().await.unwrap();
    let _ = saved_schema.as_object_mut().unwrap().remove("created_at");
    let _ = saved_schema.as_object_mut().unwrap().remove("event_id");

    assert_eq!(
        saved_schema, 
        json!({
            "event_type": "SCHEMA", 
            "key_properties": [], 
            "operation": "CREATE", 
            "schema": {
                "fields": [{
                    "name": "name", 
                    "type": "string"
                }], 
                "name": "schema_two", 
                "type": "record"
            }, 
            "table_name": "schema_two"
        })
    );

}