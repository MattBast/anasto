use warp::{ Reply, Rejection };
use tokio::sync::{ mpsc, RwLock };
use domains::{ Event, record::Record };
use serde_json::json;
use apache_avro::schema::Schema as AvroSchema;
use std::sync::Arc;
use std::path::PathBuf;
use repos::schema::Repo;


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

// helper function to create a json file for the websocket to read.
// returns the filepath as a string
fn setup_json(seed_value: &str) -> Vec<Record> {

    let record = Record::new(
        "users",
        json!({
            "id": 0,
            "seed_value": seed_value
        }),
        &AvroSchema::parse_str(r#"{ 
            "type": "record",
            "name": "users",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "seed_value", "type": "string"}
            ] 
        }"#).unwrap(),
        "CREATE".to_string()
    ).unwrap();

    return vec![record];

}


// helper to convert a Warp message to serde_json::Value
fn message_to_record(
    message: warp::filters::ws::Message
) -> Vec<Record> {

    let json_message = serde_json::from_slice(message.as_bytes()).unwrap();
    return serde_json::from_value(json_message).unwrap();

}


#[tokio::test]
async fn subscriber_can_handshake_with_websocket() {
    
    let (api, _bus_rx) = setup_api().await;

    // attempt to do ahandshake with the websocket
    let subscriber_result = warp::test::ws()
        .path("/subscribe")
        .handshake(api)
        .await;

    assert!(
        subscriber_result.is_ok(), 
        "Could not complete handshake with the websocket endpoint."
    );

}


#[tokio::test]
async fn handshake_puts_new_subscriber_event_on_message_bus() {
    
    let (api, mut bus_rx) = setup_api().await;

    // attempt to do ahandshake with the websocket
    let _subscriber_result = warp::test::ws()
        .path("/subscribe")
        .handshake(api)
        .await;

    let bus_event = bus_rx.try_recv().unwrap();
    
    match bus_event {

        Event::Subscribe(_subscriber) => assert!(true),
        _ => assert!(false, "Did not get a Event::Subscribe type enum.")
    }

}


#[tokio::test]
async fn subscriber_can_receive_messages() {
    
    let (api, mut bus_rx) = setup_api().await;
    let data = setup_json("hello_subscriber");

    // attempt to do ahandshake with the websocket
    let mut subscriber = warp::test::ws()
        .path("/subscribe")
        .handshake(api)
        .await
        .expect("handshake completed.");

    let bus_event = bus_rx.try_recv().unwrap();
    
    match bus_event {

        Event::Subscribe(subscriber_event) => {

            let sub_tx = subscriber_event.sender.unwrap();

            // send a message to the subscriber
            sub_tx.send(data.clone()).unwrap();

            let message = subscriber.recv().await.unwrap();
            assert_eq!(message_to_record(message), data);

        },

        _ => assert!(false, "Did not get a Event::Subscribe type enum.")

    
    }
        

}


#[tokio::test]
async fn subscriber_can_receive_multiple_messages() {
    
    let (api, mut bus_rx) = setup_api().await;
    let data_1 = setup_json("event_1");
    let data_2 = setup_json("event_2");
    let data_3 = setup_json("event_3");

    // attempt to do ahandshake with the websocket
    let mut subscriber = warp::test::ws()
        .path("/subscribe")
        .handshake(api)
        .await
        .expect("handshake completed.");

    let bus_event = bus_rx.try_recv().unwrap();

    match bus_event {

        Event::Subscribe(subscriber_event) => {

            let sub_tx = subscriber_event.sender.unwrap();

            // send messages to the subscriber
            sub_tx.send(data_1.clone()).unwrap();
            sub_tx.send(data_2.clone()).unwrap();
            sub_tx.send(data_3.clone()).unwrap();

            let message_1 = subscriber.recv().await.unwrap();
            assert_eq!(message_to_record(message_1), data_1);

            let message_2 = subscriber.recv().await.unwrap();
            assert_eq!(message_to_record(message_2), data_2);

            let message_3 = subscriber.recv().await.unwrap();
            assert_eq!(message_to_record(message_3), data_3);

        },

        _ => assert!(false, "Did not get a Event::Subscribe type enum.")

    
    }

}


