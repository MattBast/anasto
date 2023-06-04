use std::collections::HashMap;
use tokio::sync::mpsc;
use domains::{ Event, record::Record, sub::Subscriber };
use handlers::record::buffer;
use serde_json::json;
use std::{ thread, time as std_time };
use uuid::Uuid;
use time::OffsetDateTime;
use apache_avro::schema::Schema as AvroSchema;


// Helper function to generate a test event
fn test_record() -> Record {

    Record::new(
        "test_table",
        json!({"id": "1"}), 
        &AvroSchema::parse_str(r#"{ 
            "type": "record",
            "name": "test_table",
            "fields": [
                {"name": "id", "type": "string"}
            ] 
        }"#).unwrap(), 
        "CREATE".to_string()
    ).unwrap()

}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_records_are_added_to_a_buffer() {
    
    // setup dependencies
    let (bus_tx, _bus_rx) = mpsc::unbounded_channel();
    let mut record_buffers = HashMap::new();
    let mut sub_repo = HashMap::new();
    
    // buffer a new Record
    buffer(
    	&mut record_buffers, 
    	test_record(), 
    	(100, 1000000, 60), 
    	bus_tx, 
    	&mut sub_repo
    );

    assert!(
    	record_buffers.contains_key("test_table"),
    	"the hashmap of buffers does not contain a buffer for the new event."
    );

    assert_eq!(
    	record_buffers.len(),
    	1,
    	"the hashmap of buffers contains more than one buffer."
    );

    assert_eq!(
    	record_buffers.get("test_table").unwrap().get_events().unwrap().len(),
    	1,
    	"the buffer does not contain the single event added to it."
    );

}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_event_put_on_bus() {

	// setup dependencies
    let (bus_tx, mut bus_rx) = mpsc::unbounded_channel();
    let mut record_buffers = HashMap::new();
    let mut sub_repo = HashMap::new();
    
    // buffer a new Record
    buffer(
    	&mut record_buffers, 
    	test_record(), 
    	(100, 1000000, 1), 
    	bus_tx, 
    	&mut sub_repo
    );

    let two_seconds = std_time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    let received_event = bus_rx.try_recv().unwrap();

    match received_event {

    	Event::Drain(event) => {

    		assert_eq!(event.table_name, String::from("test_table"));

    	}
    	
    	_ => {

    		assert!(false, "Did not get a Event::Drain type enum.");

    	}
    }

}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffer_gets_drained_when_at_capacity() {
    
    // setup dependencies
    let (bus_tx, _bus_rx) = mpsc::unbounded_channel();
    let mut record_buffers = HashMap::new();
    let mut sub_repo = HashMap::new();
    
    // buffer a new Record
    buffer(
    	&mut record_buffers, 
    	test_record(), 
    	(2, 1000000, 60), 
    	bus_tx.clone(), 
    	&mut sub_repo
    );

    assert_eq!(
    	record_buffers.get("test_table").unwrap().get_events().unwrap().len(),
    	1,
    	"the buffer drained after the first event."
    );

    buffer(
    	&mut record_buffers, 
    	test_record(), 
    	(2, 1000000, 60), 
    	bus_tx.clone(), 
    	&mut sub_repo
    );

    assert_eq!(
    	record_buffers.get("test_table").unwrap().get_events().unwrap().len(),
    	0,
    	"the buffer did not drain when it hit capacity."
    );

}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffer_draining_sends_records_to_subscribers() {
    
    // setup dependencies
    let (bus_tx, _bus_rx) = mpsc::unbounded_channel();
    let mut record_buffers = HashMap::new();
    let mut sub_repo = HashMap::new();

    // add subscriber
    let sub_id = Uuid::new_v4();
    let (tx, mut rx) = mpsc::unbounded_channel();
    sub_repo.insert(
    	sub_id.clone(),
    	Subscriber {
    		sender: Some(tx),
    		state: HashMap::new(),
    		operation: "CREATE".to_string(),
    		created_at: OffsetDateTime::now_utc(),
    		sub_id
    	}
    );
    
    // buffer a new Record and trigger a drain
    buffer(
    	&mut record_buffers, 
    	test_record(), 
    	(1, 1000000, 60), 
    	bus_tx.clone(), 
    	&mut sub_repo
    );

    buffer(
    	&mut record_buffers, 
    	test_record(), 
    	(1, 1000000, 60), 
    	bus_tx.clone(), 
    	&mut sub_repo
    );

    let recv_records = rx.try_recv().unwrap();

    match &recv_records[..] {

    	[_record_1, _record_2] => assert!(true, "There should only be one record in the received events."),
    	[_record_1] => assert!(true),
    	_ => assert!(false, "Did not get the right count of records.")

    }

}