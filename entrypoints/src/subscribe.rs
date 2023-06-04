use futures_util::{SinkExt, StreamExt, TryFutureExt};
use futures_util::stream::SplitSink;
use warp::Filter;
use warp::ws::{Message, WebSocket};
use tokio::sync::mpsc;
use domains::{Event, sub::Subscriber, record::Record};
use time::OffsetDateTime;
use std::collections::HashMap;
use uuid::Uuid;
use serde_json::json;
use log::{ info, warn };


/// Function that initialises the websocket endpoint at the path /subscribe
pub fn init(
    bus_tx: mpsc::UnboundedSender<Event>
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {

    // re-type the subscribers register as a warp filter 
    // so it can be passed into endpoint initialisation
    let bus_handle = warp::any().map(
        move || bus_tx.clone());

    warp::path!("subscribe")
        .and(warp::ws())
        .and(bus_handle)
        .map(|ws: warp::ws::Ws, bus_handle| {
            
            // If the handshake succeeds, setup the subscriber so they
            // can start receiving processed events.
            ws.on_upgrade(move |socket| 
                setup_sub(socket, bus_handle)
            )

        })

}

/// Setup a new subscriber so that they can start receiving 
/// processed events.
async fn setup_sub(
    ws: WebSocket,
    bus_tx: mpsc::UnboundedSender<Event>
) {
    
    // create a new subscriber and send an event to let Anasto know
    // it's happened
    let (sub_id, mut sub_rx) = new_sub(bus_tx.clone());

    // Split the websocket into a sender and receiver of messages.
    // The websocker transmitter (tx) is used to send messages to
    // the subscriber. The websocket receiver is used to receive
    // messages the subscriber has sent to the websocket.
    let (mut ws_tx, mut ws_rx) = ws.split();

    // start listening to the streams channel. The stream will send filepaths
    // pointing at files that contain events to be sent to all register_sender.
    tokio::task::spawn(async move {
        
        while let Some(message) = sub_rx.recv().await {
            
            // get the name of the table the records belong to for logging purposes
            let table_name = message[0].get_name();
            
            // Send the batch of events to the subscriber in this scope
            publish(message, &mut ws_tx).await;

            info!(target: "subscribers", "Events sent to subscriber {:?} from table {:?}", sub_id, &table_name);

        }

    });

    // listen for the presence of the subscriber. Break the loop when they
    // disconnect. Also log any messages they send.
    while let Some(result) = ws_rx.next().await {
        
        match result {
            
            Ok(msg) => info!(target: "subscribers", "Subscriber {:?} sent the message: {:?}.", sub_id, msg),
            
            Err(e) => {
                warn!("Subscriber {:?} experienced the error: {}", sub_id, e);
                break;
            }
            
        };
        
    }

    // If the receive loop breaks, it means the subscriber disconnected.
    delete_sub(
        sub_id,
        bus_tx.clone()
    );

}

// create a new subscriber and send an event to let Anasto know
// it's happened
fn new_sub(
    bus_tx: mpsc::UnboundedSender<Event>
) -> (Uuid, mpsc::UnboundedReceiver<Vec<Record>>) {
    
    let sub_id = Uuid::new_v4();
    let (sub_tx, sub_rx) = mpsc::unbounded_channel();

    let sub = Subscriber {
        sender: Some(sub_tx),
        state: HashMap::new(),
        operation: "CREATE".to_owned(),
        created_at: OffsetDateTime::now_utc(),
        sub_id
    };

    bus_tx
        .send(Event::Subscribe(sub))
        .unwrap();

    (sub_id, sub_rx)

}


/// Send a batch of events to all register_sender
async fn publish(
    events: Vec<Record>, 
    tx: &mut SplitSink<WebSocket, Message>
) {
    
    // send the events
    tx.send(Message::text(json!(events).to_string()))
        .unwrap_or_else(|e| {
            eprintln!("websocket send error: {}", e);
        })
        .await;

}


fn delete_sub(
    sub_id: Uuid,
    bus_tx: mpsc::UnboundedSender<Event>
) {

    let sub = Subscriber {
        sender: None,
        state: HashMap::new(),
        operation: "DELETE".to_owned(),
        created_at: OffsetDateTime::now_utc(),
        sub_id
    };

    bus_tx
        .send(Event::Subscribe(sub))
        .unwrap();

}