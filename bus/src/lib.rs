#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![cfg_attr(test, deny(warnings))]

//! # Message buses
//!
//! The message bus is a queue of events that have been sent to Anasto.
//! The bus looks at each event type and forwards it on to the correct 
//! handler. It also initialises any repositories (repos) that are required
//! to store events in.

use std::collections::HashMap;
use tokio::sync::{ mpsc, RwLock };
use domains::{
    Event,
    events::State,
    events::Metric
};
use handlers::{
    record::buffer,
    record::drain,
    sub::update
};
use repos::schema::Repo;
use std::sync::Arc;

/// Create the required repos, start listening for new events and forward
/// the events to appropriate handler.
pub async fn start(
    buffer_confg: (usize, usize, u64), // (max_capacity, max_memory, max_event_age)
    schema_repo: Arc<RwLock<Repo>>,
) -> (mpsc::UnboundedSender<Event>, tokio::task::JoinHandle<()>) {

    // create a channel to receive events on
    let (tx, mut rx) = mpsc::unbounded_channel();

    // make a copy of the bus sender so the handlers can 
    // also put events on the bus
    let bus_tx = tx.clone();

    let task_handle = tokio::spawn(async move {

        // create a repository to save schemas in
        let mut record_buffers = HashMap::new();

        // create a repository to save subscribers in
        let mut sub_repo = HashMap::new();

        while let Some(event) = rx.recv().await {

            match event {
                Event::Record(record) => buffer(&mut record_buffers, record, buffer_confg, bus_tx.clone(), &mut sub_repo),
                Event::Schema(schema) => schema_repo.write().await.update(schema).await,
                Event::State(state) => handle_state(state),
                Event::Metric(metric) => handle_metric(metric),
                Event::Drain(drain_event) => drain(&mut record_buffers, &drain_event.table_name, &mut sub_repo),
                Event::Subscribe(sub) => update(&mut sub_repo, sub),
                Event::Shutdown => rx.close()
            }

        }

    });

    (tx, task_handle)

}


fn handle_state(state: State) {
    println!("{:?}", state);   
}

fn handle_metric(metric: Metric) {
    println!("{:?}", metric);   
}
