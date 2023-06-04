#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![cfg_attr(test, deny(warnings))]

//! # Entrypoints
//!
//! Entrypoints are the api layer of Anasto that developers can use to
//! interact with the tool. Right now it contains the following endpoints:
//!
//! - ** GET /health** - an endpoint to ping to see if the API is available
//! - ** POST /record** - post new data events from a data source. These events will be sent to a destination
//! - ** POST /schema** - add and update a json schema. Any records with the same type as a schema will be validated against it
//! - ** WS /subscribe** - destinations can subscribe for all events sent to Anasto

use log::{ info, error };
use warp::Filter;
use domains::Event;
use tokio::sync::{ mpsc, oneshot, RwLock };
use std::net::{ Ipv4Addr };
use std::sync::Arc;
use repos::schema::Repo;

/// Logic for the API health check endpoint
mod health;

/// Logic for the endpoint that receives new events
mod record;

/// Logic for the endpoint that receives new schemas
mod schema;

/// Logic for the endpoint that sends new events a consumer
mod subscribe;

/// Logic for generic 4xx -> 5xx responses
mod errors;

/// Definitions for the requests and responses used by the endpoints
mod models;


/// Provides a RESTful web server for producing events to and 
/// getting events from blackbox.
pub async fn start(
    bus_tx: mpsc::UnboundedSender<Event>,
    max_request_size: u64,
    ip: Ipv4Addr,
    port: u16,
    schema_repo: Arc<RwLock<Repo>>,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    
    // set up the api endpoints
    let api = endpoints(bus_tx, max_request_size, schema_repo);

    // view access logs by setting `RUST_LOG=endpoint`.
    let routes = api
        .with(warp::log("api"))
        .recover(errors::reject_request);

    // create a channel that can be used to send a shutdown
    // request to the api server
    let (tx, rx) = oneshot::channel();

    // create the server and define what to do if it goes down
    let serve_result = warp::serve(routes)
        .try_bind_with_graceful_shutdown((ip.octets(), port), async {
            rx.await.ok();
    });

    match serve_result {
        
        Ok((addr, server)) => {

            let server_handle = tokio::task::spawn(server);
            info!(target: "api", "API started at {}", addr);
            (tx, server_handle)

        },
        Err(e) => {
            
            error!(target: "api", "{}", e);
            panic!("{:?}", "API failed to start.");

        }
    }

}

/// Initialise all the endpoints
pub fn endpoints(
    bus_tx: mpsc::UnboundedSender<Event>,
    max_request_size: u64,
    schema_repo: Arc<RwLock<Repo>>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    
    health::init()
        .or(record::init(
            bus_tx.clone(),
            max_request_size,
            schema_repo.clone()
        ))
        .or(subscribe::init(
            bus_tx.clone()
        ))
        .or(schema::post(
            bus_tx,
            max_request_size,
            schema_repo.clone()
        ))
        .or(schema::get(schema_repo))

}

