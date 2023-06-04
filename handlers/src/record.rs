use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;
use tokio::time::{sleep, Duration};
use time::OffsetDateTime;
use domains::Event;
use domains::record::Record;
use domains::events::Drain;
use domains::sub::Subscriber;
use repos::record::Repo;
use log::{ 
    warn,
    error
};


/// Receive a new event and put it in the right buffer.
pub fn buffer(
    buffers: &mut HashMap<String, Repo>,
    record: Record,
    buf_confg: (usize, usize, u64), // (capacity, memory_size, batch_window)
    bus_tx: mpsc::UnboundedSender<Event>,
    sub_repo: &mut HashMap<Uuid, Subscriber>
) {

    // unwrap the config tuple
    let capacity = buf_confg.0;
    let memory_size = buf_confg.1;
    let batch_window = buf_confg.2;
    
    // If this is the first event sent to the stream, no buffers will 
    // exist. So create one for it
    if buffers.is_empty() || !buffers.contains_key(&record.get_name()) {

        create_buffer(
        	buffers, 
        	&record.get_name(), 
        	capacity, 
        	memory_size, 
        	batch_window, 
        	bus_tx.clone()
        ).expect("Could not create new buffer.");

    }

    // put the record in the current buffer
    put(buffers, &record)
        .expect("Was unable to put new record in a buffer.");

    // check if the buffer holding this Record type is full before 
    // sending a new Record to it.
    if buffer_full(&record.get_name(), buffers) {

        // remove the buffer from the buffers and send all its events
        // to the subscribers
        drain(buffers, &record.get_name(), sub_repo);
        
        // create a new buffer
        create_buffer(
        	buffers, 
        	&record.get_name(), 
        	capacity, 
        	memory_size, 
        	batch_window, 
        	bus_tx
        ).expect("Could not create new buffer.");

    }

}

/// Create a new buffer and add it to the hashmap managing them.
fn create_buffer(
    buffers: &mut HashMap<String, Repo>,
    table_name: &str,
    capacity: usize,
    memory_size: usize,
    batch_window: u64,
    bus_tx: mpsc::UnboundedSender<Event>
) -> Result<(), std::io::Error> {
    
    let buffer = Repo::new(
        table_name.to_owned(), 
        capacity,
        memory_size,
        batch_window
    ).expect("Failed to create a new buffer");

    // Update map of event channels with new channel for
    // this event.
    buffers.insert(table_name.to_owned(), buffer);

    // Start an async task to wait for the buffer window to expire 
    // and drain it if the batching window expires.
    send_drain_event(table_name.to_owned(), bus_tx, batch_window);

    Ok(())

}


/// Decide if a buffer is ready to be drained of its events
fn buffer_full(
    table_name: &String,
    buffers: &mut HashMap<String, Repo>
) -> bool {

    match buffers.get_mut(table_name) {
        
        Some(buffer) => buffer.at_capacity() || buffer.max_age_reached(),
        None => false

    }

}


/// Put an event in the buffer that shares a name with the event type
fn put( 
    buffers: &mut HashMap<String, Repo>,
    record: &Record
) -> Result<(), String> {

    match buffers.get_mut(&record.get_name()) {
        
        Some(buffer) => {
            
            buffer.append(record.clone()).unwrap();
            Ok(())

        }
        None => {
            
            let error_message = format!("Could not get the buffer {} when trying to put an event in it.", &record.get_name());
            warn!(target: "records", "{}", error_message);
            Err(error_message)

        }

    }

}


/// Wait for the allocated time and then put a Drain event on the queue
/// telling the stream to drain a buffer.
fn send_drain_event(
    table_name: String,
    bus_tx: mpsc::UnboundedSender<Event>,
    batch_window: u64
) {

    tokio::spawn(async move {

        // put the thread to sleep from the specified period
        let delay = Duration::from_secs(batch_window);
        sleep(delay).await;

        // Put drain event on the streams channel
        let drain_event = Drain {
            table_name, 
            created_at: OffsetDateTime::now_utc(),
            event_id: Uuid::new_v4()
        };

        bus_tx.send(domains::Event::Drain(drain_event)).unwrap();

    });

}

/// Send all events in a buffer to the subscribers
pub fn drain(
    buffers: &mut HashMap<String, Repo>,
    table_name: &String,
    sub_repo: &mut HashMap<Uuid, Subscriber>
) {

    // get the buffer and remove its key from the streams list of buffers
    let (_, mut buffer) = match buffers.remove_entry(table_name) {
        
        Some(entry) => entry,
        None => {
            
            error!(target: "records", "Was not able to delete the drained buffer.");
            return

        }
        
    };


    // drain the buffer of its events
    let records = buffer
            .drain_buffer()
            .unwrap();

    // and send a copy of the events to every subscriber
    sub_repo
        .iter_mut()
        .for_each(|(_, subscriber)| {
            
            // send the new events to a subscriber
            subscriber
                .sender
                .as_ref()
                .unwrap()
                .send(records.clone())
                .unwrap();

        });

}