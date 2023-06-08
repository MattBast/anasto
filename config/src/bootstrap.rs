use std::env;
// use tracing::{ info, warn };
// use tracing_subscriber;
use log::{ info, warn, error };
use std::fs::read_to_string;
use crate::tables::{
    Config,
    Subscriber
};
use std::io::ErrorKind;
use repos::schema::Repo;
use tokio::{ 
    select, 
    sync::mpsc, 
    sync::oneshot, 
    sync::RwLock, 
    signal 
};
use domains::Event;
use std::sync::Arc;
use std::collections::HashMap;
use sub::sub_trait::start_subscriber;
use time::OffsetDateTime;
use uuid::Uuid;


/// Starts the Anasto message bus, API and connectors specified
/// in the config file.
pub async fn init(config_filepath: String, use_logging: bool) -> (
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
    mpsc::UnboundedSender<Event>,
    tokio::task::JoinHandle<()>,
    Vec<tokio::task::JoinHandle<()>>
) {

    // Setup and start the logger. This is configured based 
    // on RUST_LOG env var.
    if use_logging {

        if env::var_os("RUST_LOG").is_none() {
            env::set_var("RUST_LOG", "info");
        }

        pretty_env_logger::init();

    }

    info!(target: "bootstrap", "Starting Anasto ...");
    
    // Try to read the config file.
    let config = get_config(config_filepath);

    // create a repository to save schemas in
    let schema_repo = match Repo::new(config.schemas.url.clone()) {
        Ok(repo) => repo,
        Err(e) => panic!("{}", e)
    };

    // enable shared ownership across threads 
    let schema_repo_ref = Arc::new(RwLock::new(schema_repo));

    // start the message bus which also initialises the repos and the handlers
    let (bus_tx, bus_handle) = bus::start(
        (
            config.stream.max_capacity, 
            config.stream.max_memory, 
            config.stream.max_event_age
        ),
        schema_repo_ref.clone()
    ).await;

    // then start the entrypoints API which allows for subscribers and publishers
    // to interact with Anasto
    let (api_tx, api_handle) = entrypoints::start(
        bus_tx.clone(), 
        config.api.max_request_size,
        config.api.ip,
        config.api.port,
        schema_repo_ref
    ).await;

    // start the subscribers
    let subscriber_handles = start_subscribers(config, bus_tx.clone()).await;
    

    (api_tx, api_handle, bus_tx, bus_handle, subscriber_handles)

}


/// Looks for the config file. If it can't find it, start Anasto with
/// default values.
fn get_config(filepath: String) -> Config {

    match read_to_string(&filepath) {

        Ok(content) => match toml::from_str(&content) {
            
            Ok(config) => {
                
                info!(target: "bootstrap", "Starting Anasto with configurations from {}.", &filepath);
                config

            },
            Err(error) => {
                
                warn!(target: "bootstrap", "Got error while trying to parse {}", &filepath);
                warn!(target: "bootstrap", "{}", error.to_string());
                panic!("Cancelled Anasto startup.");

            }

        },
        Err(error) => match error.kind() {
            
            ErrorKind::PermissionDenied => {

                warn!(target: "bootstrap", "Anasto does not have permission to access {}.", &filepath);
                info!(target: "bootstrap", "Reverting to the default configuration for Anasto");
                toml::from_str(r#""#).unwrap()

            },

            ErrorKind::NotFound => {

                warn!(target: "bootstrap", "Could not find the file {}.", &filepath);
                info!(target: "bootstrap", "Reverting to the default configuration for Anasto");
                toml::from_str(r#""#).unwrap()

            },

            _ => {
                
                warn!(target: "bootstrap", "Unknown error while trying to read {}.", &filepath);
                info!(target: "bootstrap", "Reverting to the default configuration for Anasto");
                toml::from_str(r#""#).unwrap()

            }

        }

    }

}

/// Start all the subscribers in tokio tasks and return a vector of the task handles
async fn start_subscribers(config: Config, bus_tx: mpsc::UnboundedSender<Event>) -> Vec<tokio::task::JoinHandle<()>> {

    let mut subscriber_handles = Vec::new();

    for subscriber in config.subscriber.iter() {
        
        // create the localfile subscriber
        let sub_result = match subscriber {
            
            Subscriber::Localfile(config) => {
                
                info!(target: "bootstrap", "Localfile subscriber found in config writing {} files to {}.", &config.filetype.to_string(), &config.dirpath.display());
                sub::localfile::Localfile::new(config.name.clone(), &config.dirpath, config.filetype.to_string())

            },

        };

        // create a repo entry for the subscriber
        let (tx, rx) = mpsc::unbounded_channel();

        let sub = domains::sub::Subscriber {
            sender: Some(tx),
            state: HashMap::new(),
            operation: "CREATE".to_owned(),
            created_at: OffsetDateTime::now_utc(),
            sub_id: Uuid::new_v4()
        };

        // and send it to be placed into the subscriber Repo so the rest of Anasto knows about it
        bus_tx
            .send(domains::Event::Subscribe(sub))
            .unwrap();

        match sub_result {
            Ok(sub_result) => {
                
                let handle = start_subscriber(Box::new(sub_result), rx, false).await.unwrap();
                subscriber_handles.push(handle);
                
            },
            Err(e) => {
                error!(target: "bootstrap", "Failed to start subscriber.");
                panic!("{}", e)
            }
        };

    }

    info!(target: "bootstrap", "Subscribers started.");

    subscriber_handles

}

/// Uses the handlers to continuously check that Anasto is running
/// Will shut down Anasto if one of the handlers drops.
pub async fn run(
    api_tx: oneshot::Sender<()>,
    api_future: tokio::task::JoinHandle<()>,
    bus_tx: mpsc::UnboundedSender<Event>,
    bus: tokio::task::JoinHandle<()>
) {

    info!(target: "bootstrap", "Anasto started");

    select! {
        
        _ = api_future => {
            
            warn!("The API has failed. Starting shutdown process.");
            
            // initiate the bus showdown
            bus_tx.send(Event::Shutdown).unwrap();

        }
        
        _ = bus => {
            
            warn!("The message bus has failed. Starting shutdown process.");
            
            // initiate the api showdown
            api_tx.send(()).unwrap();

        }

        // listen for a ctrl-c keystroke i.e. the user wants
        // Anasto to shutdown
        _ = signal::ctrl_c() => {

            warn!("Shutdown request received. Starting shutdown process.");
            
            // initiate the bus showdown
            bus_tx.send(Event::Shutdown).unwrap();

            // initiate the api showdown
            api_tx.send(()).unwrap();

        }
    };

    info!("Anasto has successfully shutdown.");

}