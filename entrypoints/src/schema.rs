use warp::{Filter, reply, Reply, Rejection};
use warp::http::StatusCode;
use domains::{Event, schema::Schema};
use tokio::sync::{ mpsc, RwLock };
use serde_derive::{Deserialize};
use apache_avro::schema_compatibility::SchemaCompatibility;
use std::sync::Arc;
use repos::schema::Repo;

// *********************************************
// remember to add logic that checks that Record schema
// names are the same as the table_name field
// *********************************************

/// Function that initialises the POST endpoint at the path /schema
pub fn post(
    bus_tx: mpsc::UnboundedSender<Event>,
    max_request_size: u64,
    schema_repo: Arc<RwLock<Repo>>
) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
    
    warp::path!("schema")
        .and(warp::post())
        .and(warp::body::content_length_limit(max_request_size))
        .and(warp::body::json())
        .and(warp::any().map(move || (bus_tx.clone(), schema_repo.clone())))
        .and_then(post_event)

}


/// Convert the new record to a Event::Data type and send it to a stream
/// for processing by putting it on the streams mpsc channel
async fn post_event(
    request: Schema, 
    post_conf: (mpsc::UnboundedSender<Event>, Arc<RwLock<Repo>>)
) -> Result<impl warp::Reply, std::convert::Infallible> {
    
    let (bus_tx, schema_repo) = post_conf;

    match schema_is_compat(&request, schema_repo).await {

        Ok(_) => send_event_to_stream(request, bus_tx),
        
        Err(e) => Ok(reply::with_status(
            reply::json(&e),
            StatusCode::BAD_REQUEST
        ))

    }

}

/// Check if the schema exists already. If it does make sure the
/// change is backwards compatible
async fn schema_is_compat(
    request: &Schema, 
    schema_repo: Arc<RwLock<Repo>>
) -> Result<(), String> {

    // look for an old copy of this schema
    match schema_repo.read().await.get(&request.table_name) {

        Some(existing_schema) => match SchemaCompatibility::can_read(&request.schema, &existing_schema.schema) {
        
            true => Ok(()),

            false => Err("Schema is not backwards compatible against the existing schema.".to_string())

        },

        None => Ok(())

    }

}

/// Construct a new record event and send it to the streams. Then respond
/// to the user with a copy of the event.
fn send_event_to_stream(
    new_event: Schema, 
    bus_tx: mpsc::UnboundedSender<Event>
) -> Result<reply::WithStatus<reply::Json>, std::convert::Infallible> {

    // asynchronously send the event to the source stream.
    bus_tx
        .send(Event::Schema(new_event.clone()))
        .unwrap();

    // send a copy of the created event back to the client alongside a
    // status code.
    Ok(reply::with_status(
        reply::json(&new_event),
        StatusCode::CREATED
    ))

}


/// Function that initialises the GET endpoint at the path /schema
pub fn get(schema_repo: Arc<RwLock<Repo>>) -> impl Filter<Extract=impl Reply, Error=Rejection> + Clone {
    
    warp::path!("schema")
        .and(warp::get())
        .and(warp::query::<SchemaQuery>())
        .and(warp::any().map(move || schema_repo.clone()))
        .and_then(get_schema)

}


/// Get and return the requested schema
async fn get_schema(
    opts: SchemaQuery,
    schema_repo: Arc<RwLock<Repo>>
) -> Result<impl warp::Reply, std::convert::Infallible> {
    
    // make sure the user has included the correct `table_name` parameter
    match opts.table_name {

        // get the schema for the table this record is part of
        Some(table_name) => match schema_repo.read().await.get(&table_name) {

            Some(schema) => {

                Ok(reply::with_status(
                    reply::json(&schema),
                    StatusCode::OK
                ))

            },

            None => {

                let error_message = format!("Could not find a schema for {}", &table_name);
                
                Ok(reply::with_status(
                    reply::json(&error_message),
                    StatusCode::NOT_FOUND
                ))

            }

        }

        None => {

            let error_message = "The url parameter `table_name` was missing from your request.".to_string();

            Ok(reply::with_status(
                reply::json(&error_message),
                StatusCode::UNPROCESSABLE_ENTITY
            ))

        }

    }
    
}


/// the fields that can be used as URL query parameters
/// in a get request
#[derive(Debug, Deserialize)]
pub struct SchemaQuery {
    pub table_name: Option<String>,
}