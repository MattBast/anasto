use repos::schema::Repo;
use warp::{ Reply, Rejection };
use warp::http::StatusCode;
use warp::test::request;
use tokio::sync::{ mpsc, RwLock };
use domains::{ Event, schema::Schema };
use serde_json::json;
use tokio::fs::{ write, create_dir_all, remove_file, remove_dir_all };
use std::{ thread, time };
use std::sync::Arc;
use std::path::PathBuf;

// helper function to setup a test version of the api
async fn setup_api(
) -> (impl warp::Filter<Extract = impl Reply, Error = Rejection> + Clone, mpsc::UnboundedReceiver<Event>) {

    // create a mock stream channel and subscribers register
    let (bus_tx, bus_rx) = mpsc::unbounded_channel::<Event>();

    // create a mock schema repo
    let schema_repo = Repo::new(PathBuf::from("./test_schemas/basic/")).unwrap();
    let schema_repo_ref = Arc::new(RwLock::new(schema_repo));

    // initialise the api
    let api = entrypoints::endpoints(bus_tx, 16000, schema_repo_ref);

    return (api, bus_rx);

}

// helper function to create a schema
async fn create_mock_schema(file_name: &str, schema: Schema) {

    let file_path = "./schemas/".to_owned() + file_name + ".json";
    let file_content = serde_json::to_string(&schema).unwrap();
    
    create_dir_all("./schemas").await.unwrap();
    write(file_path, file_content).await.unwrap();

}

#[tokio::test]
async fn new_null_schema() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "nothing",
            "schema": r#"{"type": "null"}"#
        }))
        .reply(&api)
        .await;

    assert_eq!(resp.status(), StatusCode::CREATED);
    
}

#[tokio::test]
async fn new_bool_schema() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "yes_or_no",
            "schema": r#"{"type": "boolean"}"#
        }))
        .reply(&api)
        .await;

    assert_eq!(resp.status(), StatusCode::CREATED);
    
}


#[tokio::test]
async fn new_number_schema() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "numbers",
            "schema": r#"{"type": "double"}"#
        }))
        .reply(&api)
        .await;

    assert_eq!(resp.status(), StatusCode::CREATED);
    
}


#[tokio::test]
async fn new_string_schema() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "strings",
            "schema": r#"{"type": "string"}"#
        }))
        .reply(&api)
        .await;

    assert_eq!(resp.status(), StatusCode::CREATED);
    
}


#[tokio::test]
async fn new_array_schema() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "string lists",
            "schema": r#"{"type": "array", "items": "string"}"#
        }))
        .reply(&api)
        .await;

    assert_eq!(resp.status(), StatusCode::CREATED);
    
}


#[tokio::test]
async fn new_record_schema() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "records",
            "schema": r#"{
                "name": "records",
                "type": "record",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }"#
        }))
        .reply(&api)
        .await;

    assert_eq!(resp.status(), StatusCode::CREATED);
    
}

#[tokio::test]
async fn schema_in_response_has_a_valid_uuid() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "event_log",
            "schema": r#"{"type": "null"}"#
        }))
        .reply(&api)
        .await;

    // parse the resp body to json and remove the id
    let mut resp_body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    let resp_schema_id = resp_body.as_object_mut().unwrap().remove("event_id");

    // check that the event_id field is a valid uuid
    assert!(
        uuid::Uuid::parse_str(
            resp_schema_id.unwrap().as_str().unwrap()
        ).is_ok() 
    );
    
}


#[tokio::test]
async fn record_schema_is_in_response() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "event_log",
            "schema": r#"{
                "name": "records",
                "type": "record",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "updated_at", "type": "int", "logicalType": "date"}
                ]
            }"#,
            "key_properties": ["id"]
        }))
        .reply(&api)
        .await;

    // parse the resp body to json and remove the id
    let mut resp_body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    let _ = resp_body.as_object_mut().unwrap().remove("event_id");
    let _ = resp_body.as_object_mut().unwrap().remove("created_at");
    
    assert_eq!(
        resp_body, 
        json!({ 
            "table_name": "event_log",
            "schema": {
                "name": "records",
                "type": "record",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "updated_at", "type": { 
                            "type": "int", 
                            "logicalType": "date"
                        }
                    }
                ]
            },
            "event_type": "SCHEMA",
            "key_properties": ["id"],
            "operation": "CREATE"
        })
    );
    
}


#[tokio::test]
async fn null_schema_is_in_response() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "nothing",
            "schema": r#"{"type": "string"}"#,
        }))
        .reply(&api)
        .await;

    // parse the resp body to json and remove the id
    let resp_body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    
    assert_eq!(
        resp_body["schema"], 
        "string".to_string()
    );
    
}


#[tokio::test]
async fn schema_is_added_to_message_bus() {
    
    let (api, mut rx) = setup_api().await;

    // make a mock request
    let _ = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "event_log",
            "schema": r#"{
                "name": "records",
                "type": "record",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "updated_at", "type": "int", "logicalType": "date"}
                ]
            }"#,
            "key_properties": ["id"]
        }))
        .reply(&api)
        .await;

    // check that the event was put on the mock main stream
    let received_event = rx.try_recv().unwrap();


    if let Event::Schema(schema) = received_event {
        
        let mut json_event = serde_json::to_value(schema).unwrap();
        let _ = json_event.as_object_mut().unwrap().remove("event_id");
        let _ = json_event.as_object_mut().unwrap().remove("created_at");

        assert_eq!(
            json_event, 
            json!({ 
                "table_name": "event_log",
                "schema": {
                    "name": "records",
                    "type": "record",
                    "fields": [
                        {"name": "id", "type": "string"},
                        {"name": "name", "type": "string"},
                        {"name": "updated_at", "type": { 
                                "type": "int", 
                                "logicalType": "date"
                            }
                        }
                    ]
                },
                "event_type": "SCHEMA",
                "key_properties": ["id"],
                "operation": "CREATE"
            })
        );
    } else {
        assert!(false, "A schema event was not put on the message bus.")
    }
    
}

#[tokio::test]
async fn schema_in_channel_has_a_valid_id() {
    
    let (api, mut rx) = setup_api().await;

    // make a mock request
    let _ = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "nothing",
            "schema": r#"{"type": "string"}"#,
        }))
        .reply(&api)
        .await;

    // check that the event was put on the mock main stream
    let received_event = rx.try_recv().unwrap();

    // check that the event_id field is a valid uuid
    if let Event::Schema(schema) = received_event {
        
        assert!(
            uuid::Uuid::parse_str(
                &schema.event_id.hyphenated().to_string()
            ).is_ok() 
        );

    } else {
        assert!(false, "A schema event was not put on the message bus.")
    }
    
}

#[tokio::test]
async fn key_properties_generated_as_empty_array_when_missing_in_request() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "event_log",
            "schema": r#"{"type": "string"}"#,
        }))
        .reply(&api)
        .await;

    let schema: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();    

    assert_eq!(schema["key_properties"], json!([]));
    
}

#[tokio::test]
async fn operation_is_set_to_uppercase() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "event_log",
            "schema": r#"{"type": "string"}"#,
            "operation": "update"
        }))
        .reply(&api)
        .await;

    let schema: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();    

    assert_eq!(schema["operation"], "UPDATE".to_string());

}


#[tokio::test]
async fn body_not_an_object() {
    
    let (api, _rx) = setup_api().await;

    // create a request without the required "event_body" field
    let req_body = String::from("hello world");

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&req_body)
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

}


#[tokio::test]
async fn missing_schema_field_in_body() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "event_log"
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

}


#[tokio::test]
async fn missing_table_name_field_in_body() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "schema": r#"{"type": "string"}"#
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

}


#[tokio::test]
async fn schema_field_is_wrong_type() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "event_log",
            "schema": "schema_missing_here"
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

}

#[tokio::test]
async fn table_name_is_wrong_type() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": 1,
            "schema": r#"{"type": "string"}"#
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

}


#[tokio::test]
async fn schema_field_not_avro_schema() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "event_log",
            "schema": r#"{
              "$schema": "https://json-schema.org/draft/2019-09/schema",
              "$id": "https://example.com/tree",
              "$recursiveAnchor": true,

              "type": "object",
              "properties": {
                "data": true,
                "children": {
                  "type": "array",
                  "items": { "$recursiveRef": "" }
                }
              }
            }"#
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

}


#[tokio::test]
async fn invalid_operation() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "event_log",
            "schema": r#"{"type": "string"}"#,
            "operation": "INSERT"
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

}


#[tokio::test]
async fn incompatible_schemas_are_rejected() {
    
    let (api, _rx) = setup_api().await;

    let schema: Schema = serde_json::from_value(json!({ 
        "table_name": "numbers_table",
        "schema": r#"{"type": "double"}"#
    })).unwrap(); 

    create_mock_schema(
        "numbers_table", 
        schema
    ).await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "numbers_table",
            "schema": r#"{"type": "string"}"#,
            "operation": "INSERT"
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

    remove_file("./schemas/".to_owned() + "numbers_table.json").await.unwrap();
    remove_dir_all("./schemas".to_owned()).await.unwrap();

}


#[tokio::test]
async fn compatible_schemas_are_accepted() {
    
    // a horrible hack to make sure the other test that deals with
    // files finishes before this one clashes with it
    let one_sec = time::Duration::from_secs(1);
    thread::sleep(one_sec);

    let (api, _rx) = setup_api().await;

    let schema: Schema = serde_json::from_value(json!({ 
        "table_name": "greeting_records",
        "schema": r#"{ 
            "type": "record",
            "name": "greeting_records",
            "fields": [
                {"name": "hello", "type": "string"}
            ] 
        }"#
    })).unwrap(); 

    create_mock_schema(
        "greeting_records", 
        schema
    ).await;
       

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/schema")
        .json(&json!({ 
            "table_name": "greeting_records",
            "schema": r#"{ 
                "type": "record",
                "name": "greeting_records",
                "fields": [
                    {"name": "hello", "type": "string"},
                    {"name": "world", "type": "string"}
                ] 
            }"#
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::CREATED
    );

    remove_file("./schemas/".to_owned() + "greeting_records.json").await.unwrap();
    remove_dir_all("./schemas".to_owned()).await.unwrap();

}