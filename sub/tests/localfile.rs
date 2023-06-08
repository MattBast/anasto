use tokio::sync::mpsc;
use std::path::PathBuf;

use std::boxed::Box;
use sub::localfile::Localfile;
use sub::sub_trait::start_subscriber;

use domains::record::Record;
use serde_json::json;
use apache_avro::schema::Schema as AvroSchema;
use apache_avro::{Reader};

use std::{ thread, time };

use parquet::file::reader::SerializedFileReader;
use std::convert::TryFrom;

async fn setup(
    path: &'static str,
    filetype: String, 
) -> (tokio::sync::mpsc::UnboundedSender<Vec<domains::record::Record>>, tokio::task::JoinHandle<()>) {
    
    // create a mpsc channel to mock Anasto
    let (tx, rx) = mpsc::unbounded_channel();

    // create the subscriber and start it listening for records
    let subscriber = Localfile::new("localfile_sub".to_string(), &PathBuf::from(path), filetype).unwrap();
    let handle = start_subscriber(Box::new(subscriber), rx, false).await.unwrap();

    (tx, handle)

}

// helper function to create records 
fn test_records(table_name: &'static str, records_count: u32) -> Vec<Record> {

    // ************************************************************************************
    // take out this default and add some tests for none record schema types like strings and booleans
    // ************************************************************************************
    let schema = format!("{{  \"type\": \"record\",\"name\": \"{}\",\"fields\": [{{\"name\": \"id\", \"type\": \"string\"}}, {{\"name\": \"value\", \"type\": \"string\"}}] }}", table_name);

    let mut test_records: Vec<Record> = Vec::with_capacity(records_count as usize);
    
    for i in 0..records_count {
        test_records.push(Record::new(
            table_name, 
            json!({ "id": i.to_string(), "value": format!("{} value", i) }), 
            &AvroSchema::parse_str(&schema).unwrap(), 
            "CREATE".to_string()
        ).unwrap());
    }

    test_records

}

// helper function to read a schema file and check its contents
fn assert_file(path: &'static str, expected_content: String) {

    let mut content = String::new();
    let paths = std::fs::read_dir(path).unwrap();

    for path in paths {
        
        let file_contents = std::fs::read_to_string(path.unwrap().path())
            .expect("Should have been able to read the file");
        
        content.push_str(&file_contents);

    }

    assert_eq!(content, expected_content);

}

// helper function to read a schema file and check its contents
fn assert_avro_file(path: &'static str, expected_content: Vec<apache_avro::types::Value>) {

    let mut content = Vec::new();
    let paths = std::fs::read_dir(path).unwrap();

    for path in paths {

        let file = std::fs::File::open(path.unwrap().path()).unwrap();

        for value in Reader::new(file).unwrap() {
            match value {
                Ok(v) => content.push(v),
                Err(e) => assert!(false, "{}", e.to_string()),
            };
        }

    }

    assert_eq!(content, expected_content);

}

fn assert_parquet_file(path: &'static str, expected_content: Vec<serde_json::Value>) {
    
    let mut content = Vec::new();
    let paths = std::fs::read_dir(path).unwrap();

    // Create a reader for each file and flat map rows
    let rows = paths
        .map(|p| SerializedFileReader::try_from(p.unwrap().path().to_str().unwrap()).unwrap())
        .flat_map(|r| r.into_iter());

    for row in rows {
        content.push(row.to_json_value());
    }

    assert_eq!(content, expected_content);

}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn one_jsonl() {
    
    let (tx, _handle) = setup("./test_tables/", String::from("jsonl")).await;

    let records = test_records("test_table_one", 1);
    tx.send(records).unwrap();

    let two_seconds = time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    assert_file(
        "./test_tables/test_table_one/", 
        "{\"id\":\"0\",\"value\":\"0 value\"}\n".to_string()
    );

    // teardown
    std::fs::remove_dir_all("./test_tables/test_table_one/").unwrap();

}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn two_jsonl() {
    
    let (tx, _handle) = setup("./test_tables/", String::from("jsonl")).await;

    let records = test_records("test_table_two", 2);
    tx.send(records).unwrap();

    let two_seconds = time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    assert_file(
        "./test_tables/test_table_two/", 
        "{\"id\":\"0\",\"value\":\"0 value\"}\n{\"id\":\"1\",\"value\":\"1 value\"}\n".to_string()
    );

    // teardown
    std::fs::remove_dir_all("./test_tables/test_table_two/").unwrap();

}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn one_csv() {
    
    let (tx, _handle) = setup("./test_tables/", String::from("csv")).await;

    let records = test_records("test_table_three", 1);
    tx.send(records).unwrap();

    let two_seconds = time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    assert_file(
        "./test_tables/test_table_three/", 
        "id,value\n0,0 value\n".to_string()
    );

    // teardown
    std::fs::remove_dir_all("./test_tables/test_table_three/").unwrap();

}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn two_csv() {
    
    let (tx, _handle) = setup("./test_tables/", String::from("csv")).await;

    let records = test_records("test_table_four", 2);
    tx.send(records).unwrap();

    let two_seconds = time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    assert_file(
        "./test_tables/test_table_four/", 
        "id,value\n0,0 value\n1,1 value\n".to_string()
    );

    // teardown
    std::fs::remove_dir_all("./test_tables/test_table_four/").unwrap();

}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn one_avro() {
    
    let (tx, _handle) = setup("./test_tables/", String::from("avro")).await;

    let records = test_records("test_table_five", 1);
    tx.send(records).unwrap();

    let two_seconds = time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    assert_avro_file(
        "./test_tables/test_table_five/", 
        vec![
            apache_avro::types::Value::Record(vec![
                ("id".to_string(), apache_avro::types::Value::String("0".to_string())), 
                ("value".to_string(), apache_avro::types::Value::String("0 value".to_string()))
            ])
        ]
    );

    // teardown
    std::fs::remove_dir_all("./test_tables/test_table_five/").unwrap();

}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn one_parquet() {
    
    let (tx, _handle) = setup("./test_tables/", String::from("parquet")).await;

    let records = test_records("test_table_six", 1);
    tx.send(records).unwrap();

    let two_seconds = time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    assert_parquet_file(
        "./test_tables/test_table_six/", 
        Vec::from([json!({"id":"\"0\"", "value":"\"0 value\""})])
    );

    // teardown
    std::fs::remove_dir_all("./test_tables/test_table_six/").unwrap();

}

// ************************************************************************
// add tests for larger volumes of records and including more varied records.
// could include more complex records such as integer, boolean and nested fields.
// ************************************************************************

// ************************************************************************
// add tests for bad config. The connector should throw an error.
// ************************************************************************