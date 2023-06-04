use warp::{Filter, reply};
use warp::http::StatusCode;
use domains::{Event, record::Record, schema::Schema};
use tokio::sync::{ mpsc, RwLock };
use serde_json::{ json, Value };
use apache_avro::schema::Schema as AvroSchema;
use crate::models::RecordRequest;
use std::sync::Arc;
use repos::schema::Repo;

// *********************************************
// remember to add logic that checks that Record schema
// names are the same as the table_name field
// *********************************************

/// Function that initialises the POST endpoint at the path /record
pub fn init(
    bus_tx: mpsc::UnboundedSender<Event>,
    max_request_size: u64,
    schema_repo: Arc<RwLock<Repo>>
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    
    warp::path!("record")
        .and(warp::post())
        .and(warp::body::content_length_limit(max_request_size))
        .and(warp::body::json())
        .and(warp::any().map(move || (bus_tx.clone(), schema_repo.clone())))
        .and_then(post_event)

}


/// Convert the new record to a Event::Data type and send it to a stream
/// for processing by putting it on the streams mpsc channel
async fn post_event(
    request: RecordRequest,
    post_conf: (mpsc::UnboundedSender<Event>, Arc<RwLock<Repo>>)
) -> Result<impl warp::Reply, std::convert::Infallible> {
    
    let (bus_tx, schema_repo) = post_conf;

    // get the schema for the table this record is part of
    match get_schema(&request, bus_tx.clone(), schema_repo).await {

        Ok(schema) => {

            // create a new Record and check the record against
            // its schema while doing so
            match Record::new(
                &request.table_name,
                request.record, 
                &schema, 
                request.operation
            ) {

                // send the Record for processing and respond to user with
                // a positive message
                Ok(record) => send_event_to_stream(record, bus_tx),

                Err(error) => Ok(reply::with_status(
                    reply::json(&error),
                    StatusCode::BAD_REQUEST
                ))

            }

        },
        
        Err(error) => Ok(reply::with_status(
            reply::json(&error),
            StatusCode::BAD_REQUEST
        ))

    }

}

async fn get_schema(
    request: &RecordRequest,
    bus_tx: mpsc::UnboundedSender<Event>,
    schema_repo: Arc<RwLock<Repo>>,
) -> Result<AvroSchema, String> {

    match schema_repo.read().await.get(&request.table_name) {

        Some(schema) => Ok(schema.schema.clone()),

        // if a schema doesn't exist, create one
        None => create_new_schema(request, bus_tx.clone()),

    }

}


/// Use a Record event to generate a new schema.
fn create_new_schema(
    request: &RecordRequest,
    bus_tx: mpsc::UnboundedSender<Event>
) -> Result<AvroSchema, String> {
                
    // generate a new schema based on the record
    let new_schema = match &request.record {
        
        Value::Null => AvroSchema::parse_str(r#"{"type": "null"}"#).unwrap(),
        
        Value::Bool(_) => AvroSchema::parse_str(r#"{"type": "boolean"}"#).unwrap(),
        
        Value::Number(record) => number_schema(record),
        
        Value::String(_) => AvroSchema::parse_str(r#"{"type": "string"}"#).unwrap(),

        Value::Array(record) => array_schema(record),
        
        Value::Object(record) => record_schema(record, request),

    };
    
    // generate a new Schema event
    let schema_event = Schema {
        table_name: request.table_name.clone(),
        schema: new_schema.clone(),
        ..Default::default() // <-- fills unincluded fields with their default values
    };

    // put the event on the bus so the new Schema gets saved for future events.
    // if that succeeds, return the new schema.
    match bus_tx.send(Event::Schema(schema_event)) {

        Ok(_) => Ok(new_schema),
        Err(error) => Err(error.to_string()),

    }

}

/// Checks the number type of a json value. Returns an Avro schema conytaining 
/// double, long or float types.
fn number_schema(record: &serde_json::Number) -> AvroSchema {

    if record.is_f64() {
                
        AvroSchema::parse_str(r#"{"type": "double"}"#).unwrap()

    } 
    else if record.is_f64() {
        
        AvroSchema::parse_str(r#"{"type": "long"}"#).unwrap()

    }
    else {

        AvroSchema::parse_str(r#"{"type": "float"}"#).unwrap()

    }

}

/// Generate a record avro schema.
fn array_schema(record: &[serde_json::Value]) -> AvroSchema {

    let item_type = match &record[0] {
                
        Value::Null => "null",
        Value::Bool(_) => "boolean",

        Value::Number(item) => {
            if item.is_f64() {
                "double"
            } 
            else {
                "long"
            }
        },

        Value::String(_) => "string",

        Value::Array(_) => "array",

        Value::Object(_) => "record",
        
    };
    
    let schema_str = json!({
        "type": "array", 
        "items": item_type, 
        "default": []
    }).to_string();

    AvroSchema::parse_str(&schema_str).unwrap()

}

/// Generate a record avro schema
fn record_schema(
    record: &serde_json::Map<String, Value>, 
    request: &RecordRequest
) -> AvroSchema {

    // generate a vector of Avro field schemas
    let record_fields: Vec<serde_json::Value> = record
        .iter()
        .map(|(key, value)| {
            
            match value {
                Value::Null => json!({"name": key, "type": "null"}),
                Value::Bool(_) => json!({"name": key, "type": "boolean"}),
                Value::Number(item) => {
                    if item.is_f64() {
                        json!({"name": key, "type": "double"})
                    } 
                    else {
                        json!({"name": key, "type": "long"})
                    }
                },
                Value::String(_) => json!({"name": key, "type": "string"}),
                Value::Array(_) => json!({"name": key, "type": "array"}),
                Value::Object(_) => json!({"name": key, "type": "record"}),
            }
        })
        .collect();

    // and put them into a Record type schema
    let schema_str = json!({
        "type": "record", 
        "name": request.table_name, 
        "fields": record_fields
    }).to_string();

    AvroSchema::parse_str(&schema_str).unwrap()

}


/// Construct a new record event and send it to the streams. Then respond
/// to the user with a copy of the event.
fn send_event_to_stream(
    request: Record, 
    bus_tx: mpsc::UnboundedSender<Event>
) -> Result<reply::WithStatus<reply::Json>, std::convert::Infallible> {

    // asynchronously send the event to the source stream.
    match bus_tx.send(Event::Record(request.clone())) {

        Ok(_) => {

            // send a copy of the created event back to the client 
            // alongside a status code.
            Ok(reply::with_status(
                reply::json(&request),
                StatusCode::CREATED
            ))

        },
        Err(error) => {

            // send a copy of the created event back to the client 
            // alongside a status code.
            Ok(reply::with_status(
                reply::json(&error.to_string()),
                StatusCode::INTERNAL_SERVER_ERROR
            ))

        }

    }

}