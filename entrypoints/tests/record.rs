use repos::schema::Repo;
use warp::{ Reply, Rejection };
use warp::http::StatusCode;
use warp::test::request;
use tokio::sync::{ mpsc, RwLock };
use domains::Event;
use serde_json::json;
use apache_avro::schema::Schema as AvroSchema;
use std::sync::Arc;
use std::path::PathBuf;

// ***************************************************************
// note that the record table names must be unique per test
// ***************************************************************

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


#[tokio::test]
async fn post_record_returns_an_object() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/record")
        .json(&json!({ 
            "table_name": "starter",
            "record": {
                "hello": "world"
            }
        }))
        .reply(&api)
        .await;

    let resp_body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();

    // check that a positive response is retured
    assert_eq!(resp.status(), StatusCode::CREATED);

    // and that the response body contains key-value pairs
    assert!(resp_body.is_object());
    
}

#[tokio::test]
async fn post_record_returns_correct_keys() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/record")
        .json(&json!({ 
            "table_name": "test_table",
            "record": {
                "hello": "world"
            }
        }))
        .reply(&api)
        .await;

    let resp_body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();

    // and that the response body contains all five of the new events keys
    assert_eq!(resp_body.as_object().unwrap().len(), 6);

    // and that the correct keys are included
    assert!(resp_body.as_object().unwrap().contains_key("table_name"));
    assert!(resp_body.as_object().unwrap().contains_key("event_type"));
    assert!(resp_body.as_object().unwrap().contains_key("record"));
    assert!(resp_body.as_object().unwrap().contains_key("operation"));
    assert!(resp_body.as_object().unwrap().contains_key("created_at"));
    assert!(resp_body.as_object().unwrap().contains_key("event_id"));
    
}


#[tokio::test]
async fn post_record_returns_correct_values() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/record")
        .json(&json!({ 
            "table_name": "greetings",
            "record": {
                "hello": "world"
            }
        }))
        .reply(&api)
        .await;

    let resp_body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();

    assert_eq!(
        resp_body.as_object().unwrap().get("table_name").unwrap(), 
        &String::from("greetings")
    );
    assert_eq!(
        resp_body.as_object().unwrap().get("event_type").unwrap(), 
        &String::from("RECORD")
    );
    assert_eq!(
        resp_body.as_object().unwrap().get("operation").unwrap(), 
        &String::from("CREATE")
    );
    assert_eq!(
        resp_body.as_object().unwrap().get("record").unwrap(), 
        &json!({"hello": "world"})
    );
    assert!(
        uuid::Uuid::parse_str(
            resp_body.as_object().unwrap().get("event_id").unwrap().as_str().unwrap()
        ).is_ok() 
    );
    
}


#[tokio::test]
async fn created_event_gets_put_on_message_bus() {
    
    let (api, mut rx) = setup_api().await;

    // make a mock request
    let _ = request()
        .method("POST")
        .path("/record")
        .json(&json!({ 
            "table_name": "hey",
            "record": {
                "hello": "world"
            }
        }))
        .reply(&api)
        .await;

    // the API should first put a new Schema on the message bus
    // before adding a Record. We're only looking at Records here
    // so ignore the first event.
    let _first_received_event = rx.try_recv().unwrap();
    let second_received_event = rx.try_recv().unwrap();
    
    match second_received_event {

        Event::Record(record) => {

            assert_eq!(record.get_name(), String::from("hey"));
            assert_eq!(record.get_type(), String::from("RECORD"));
            assert_eq!(record.get_record(), json!({"hello": "world"}));
            assert_eq!(record.get_operation(), String::from("CREATE"));

        }
        _ => {

            assert!(false, "Did not get a Event::Record type enum.");

        }
    }
    
}


#[tokio::test]
async fn new_record_creates_correct_schema() {
    
    let (api, mut rx) = setup_api().await;

    // make a mock request
    let _ = request()
        .method("POST")
        .path("/record")
        .json(&json!({ 
            "table_name": "users",
            "record": {
                "hello": "world"
            }
        }))
        .reply(&api)
        .await;

    // the API should first put a new Schema on the message bus
    // before adding a Record.
    let first_received_event = rx.try_recv().unwrap();
    
    match first_received_event {

        Event::Schema(record) => {


            assert_eq!(
                record.schema, 
                    AvroSchema::parse_str(r#"{ 
                    "type": "record",
                    "name": "users",
                    "fields": [
                        {"name": "hello", "type": "string"}
                    ] 
                }"#).unwrap()
            );
            assert_eq!(record.event_type, String::from("SCHEMA"));
            assert_eq!(record.operation, String::from("CREATE"));

        }
        _ => {

            assert!(false, "Did not get a Event::Record type enum.");

        }
    }
    
}


#[tokio::test]
async fn request_body_not_an_object() {
    
    let (api, mut _rx) = setup_api().await;

    // create a request without the required "event_body" field
    let req_body = String::from("hello world");

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/record")
        .json(&req_body)
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

    assert_eq!(
        String::from_utf8(resp.body().to_vec()).unwrap(), 
        String::from("Request body deserialize error: invalid type: string \"hello world\", expected struct RecordRequest at line 1 column 13")
    );

}


#[tokio::test]
async fn missing_table_name_field_in_body() {
    
    let (api, _) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/record")
        .json(&json!({ 
            "record": {"hello": "world"}
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

    assert_eq!(
        String::from_utf8(resp.body().to_vec()).unwrap(), 
        String::from("Request body deserialize error: missing field `table_name` at line 1 column 28")
    );

}


#[tokio::test]
async fn missing_record_field_in_body() {
    
    let (api, _) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/record")
        .json(&json!({ 
            "table_name": "page_view"
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

    assert_eq!(
        String::from_utf8(resp.body().to_vec()).unwrap(), 
        String::from("Request body deserialize error: missing field `record` at line 1 column 26")
    );

}


#[tokio::test]
async fn table_name_is_wrong_type() {
    
    let (api, _) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/record")
        .json(&json!({ 
            "table_name": 1,
            "record": {"hello": "world"}
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::BAD_REQUEST
    );

    assert_eq!(
        String::from_utf8(resp.body().to_vec()).unwrap(), 
        String::from("Request body deserialize error: invalid type: integer `1`, expected a string at line 1 column 42")
    );

}


#[tokio::test]
async fn record_field_does_not_contain_key_pairs() {
    
    let (api, _rx) = setup_api().await;

    // make a mock request
    let resp = request()
        .method("POST")
        .path("/record")
        .json(&json!({ 
            "table_name": "no_keys",
            "record": "hello world"
        }))
        .reply(&api)
        .await;

    assert_eq!(
        resp.status(), 
        StatusCode::CREATED
    );

}