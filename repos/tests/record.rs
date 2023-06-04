use repos::record::Repo;
use domains::record::Record;
use serde_json::json;
use std::{ thread, time };
use apache_avro::schema::Schema as AvroSchema;


// Helper function to generate a test event
fn test_event(
    table_name: &str, 
    record: serde_json::Value,
    schema: &str
) -> Record {

    Record::new(
        table_name,
        record, 
        &AvroSchema::parse_str(schema).unwrap(), 
        "CREATE".to_string()
    ).unwrap()

}



#[test]
fn get_attributes() {
    
    let buffer = Repo::new("test_table".to_string(), 1, 100, 1)
    	.expect("Failed to create a new buffer");

    assert_eq!(buffer.get_name(), "test_table");
    assert_eq!(buffer.get_memory_cap(), &100);
    assert_eq!(buffer.get_capacity_cap(), &1);

}


#[test]
fn add_events() {
    
    let test_event_schema = r#"{ 
        "type": "record",
        "name": "test_table",
        "fields": [
            {"name": "id", "type": "string"}
        ] 
    }"#;

    let mut buffer = Repo::new("test_table".to_string(), 3, 1000, 1)
    	.expect("Failed to create a new buffer");

    // Buffer was not initiated with 0 events in it.
    assert_eq!(buffer.event_count(), 0);

    // Add event to Buffer and make sure it is saved in the Buffer
    buffer.append(test_event("test_table", json!({"id": "1"}), test_event_schema)).unwrap();
    assert_eq!(buffer.event_count(), 1);
    
    // Add another event to Buffer and make sure it is saved in the Buffer
    buffer.append(test_event("test_table", json!({"id": "1"}), test_event_schema)).unwrap();
    assert_eq!(buffer.event_count(), 2);

}


#[test]
fn count_of_events_capacity() {
    
    let test_event_schema = r#"{ 
        "type": "record",
        "name": "test_table",
        "fields": [
            {"name": "id", "type": "string"}
        ] 
    }"#;
    let mut buffer = Repo::new("test_table".to_string(), 3, 1000, 1)
    	.expect("Failed to create a new buffer");

    assert_eq!(
    	buffer.at_capacity(), 
    	false, 
    	"Buffer is at capacity upon initiation."
    );

    buffer.append(test_event("test_table", json!({"id": "1"}), test_event_schema)).unwrap();
    buffer.append(test_event("test_table", json!({"id": "1"}), test_event_schema)).unwrap();
    buffer.append(test_event("test_table", json!({"id": "1"}), test_event_schema)).unwrap();
    
    assert_eq!(
    	buffer.at_capacity(), 
    	true, 
    	"Buffer is not at capacity after 3 events were put in a buffer with only 3 capacity."
    );

    assert_eq!(
    	buffer.append(test_event("test_table", json!({"id": "1"}), test_event_schema)),
    	Err("Buffer is at capacity so cannot include any new events.".to_owned()),
    	"Appending an event to a buffer at event count capacity did not throw the correct error."
    );

}


#[test]
fn memory_capacity() {
    
    let test_event_schema = r#"{ 
        "type": "record",
        "name": "test_table",
        "fields": [
            {"name": "id", "type": "string"}
        ] 
    }"#;

    let mut buffer = Repo::new("test_table".to_string(), 3, 1, 1)
    	.expect("Failed to create a new buffer");

    assert_eq!(
    	buffer.at_capacity(), 
    	false, 
    	"Buffer is at capacity upon initiation."
    );

    buffer.append(test_event("test_table", json!({"id": "1"}), test_event_schema)).unwrap();
    
    assert_eq!(
    	buffer.at_capacity(), 
    	true, 
    	"Buffer is not at capacity after 3 events were put in a buffer with only 100 bytes capacity."
    );

    assert_eq!(
    	buffer.append(test_event("test_table", json!({"id": "1"}), test_event_schema)),
    	Err("Buffer is at capacity so cannot include any new events.".to_owned()),
    	"Appending an event to a buffer at memory capacity did not throw the correct error."
    )
}


#[test]
fn get_events() {
    
    let test_event_schema = r#"{ 
        "type": "record",
        "name": "test_table",
        "fields": [
            {"name": "id", "type": "int"}
        ] 
    }"#;

    let mut buffer = Repo::new("test_table".to_string(), 2, 1000, 1)
    	.expect("Failed to create a new buffer");

    let event_1 = test_event("test_table", json!({"id": 1}), test_event_schema);
    let event_2 = test_event("test_table", json!({"id": 2}), test_event_schema);

    buffer.append(event_1.clone()).unwrap();
    buffer.append(event_2.clone()).unwrap();

    let events = buffer.get_events().expect("Could not get events from buffer");

    assert_eq!(
    	buffer.event_count(), 
    	2, 
    	"Getting the events from the buffer need not maintain a copy of the events in the buffer."
    );

    assert_eq!(
    	events.len(), 
    	2, 
    	"The 2 events in the buffer were not returned."
    );
    assert_eq!(
    	events[0], 
    	event_1.clone(), 
    	"The first returned event is not the event that was put in the buffer."
    );
    assert_eq!(
    	events[1], 
    	event_2.clone(), 
    	"The second returned event is not the event that was put in the buffer."
    );

}


#[test]
fn drain_buffer() {
    
    let test_event_schema = r#"{ 
        "type": "record",
        "name": "test_table",
        "fields": [
            {"name": "id", "type": "int"}
        ] 
    }"#;

    let mut buffer = Repo::new("test_table".to_string(), 2, 1000, 1)
    	.expect("Failed to create a new buffer");

    let initial_last_drained = buffer.last_drained();

    let event_1 = test_event("test_table", json!({"id": 1}), test_event_schema);
    let event_2 = test_event("test_table", json!({"id": 2}), test_event_schema);

    buffer.append(event_1.clone()).unwrap();
    buffer.append(event_2.clone()).unwrap();

    // drain the buffer
    let json_events = buffer
        .drain_buffer()
        .expect("Could not drain events from buffer");

    assert_eq!(
        buffer.event_count(), 
        0, 
        "The buffer still has events in it."
    );

    assert!(
        buffer.last_drained() > initial_last_drained, 
        "The buffer did not log a timestamp to state when the draining took place."
    );

    assert_eq!(
        json_events,
        Vec::from([event_1, event_2]),
        "The events drained from the buffer do not match what was put in the buffer."
    );

}


#[test]
fn check_if_max_age_reached() {
    
    let test_event_schema = r#"{ 
        "type": "record",
        "name": "test_table",
        "fields": [
            {"name": "id", "type": "int"}
        ] 
    }"#;

    let mut buffer = Repo::new("test_table".to_string(), 2, 100, 1)
    	.expect("Failed to create a new buffer");

    assert_eq!(
    	buffer.max_age_reached(), 
    	false, 
    	"The buffer has initiated and already at its max age."
    );

    buffer.append(test_event("test_table", json!({"id": 1}), test_event_schema)).unwrap();

    let two_seconds = time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    assert_eq!(
    	buffer.max_age_reached(), 
    	true, 
    	"The buffer did not realise it had reached its max age."
    );

}


#[test]
fn events_from_another_table_are_rejected() {
    
    let test_event_schema = r#"{ 
        "type": "record",
        "name": "test_table",
        "fields": [
            {"name": "id", "type": "string"}
        ] 
    }"#;

    let mut buffer = Repo::new("test_table".to_string(), 3, 1000, 1)
    	.expect("Failed to create a new buffer");

    assert!(
    	buffer.append(test_event("test_table_2", json!({"id": "1"}), test_event_schema)).is_err(),
    	"the buffer did not reject records from different tables."
    );

}