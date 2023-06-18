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
use ::time::format_description;

async fn setup(
    path: &'static str,
    filetype: String, 
    keep_headers: bool,
) -> (tokio::sync::mpsc::UnboundedSender<Vec<domains::record::Record>>, tokio::task::JoinHandle<()>) {
    
    // create a mpsc channel to mock Anasto
    let (tx, rx) = mpsc::unbounded_channel();

    // create the subscriber and start it listening for records
    let subscriber = Localfile::new("localfile_sub".to_string(), &PathBuf::from(path), filetype, keep_headers).unwrap();
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
    
    let (tx, _handle) = setup("./test_tables/", String::from("jsonl"), false).await;

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
    
    let (tx, _handle) = setup("./test_tables/", String::from("jsonl"), false).await;

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
    
    let (tx, _handle) = setup("./test_tables/", String::from("csv"), false).await;

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
    
    let (tx, _handle) = setup("./test_tables/", String::from("csv"), false).await;

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
    
    let (tx, _handle) = setup("./test_tables/", String::from("avro"), false).await;

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
    
    let (tx, _handle) = setup("./test_tables/", String::from("parquet"), false).await;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn one_jsonl_with_headers() {
    
    let (tx, _handle) = setup("./test_tables/", String::from("jsonl"), true).await;

    let records = test_records("test_table_seven", 1);
    tx.send(records.clone()).unwrap();

    let two_seconds = time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    // get table files
    let mut content = String::new();
    let paths = std::fs::read_dir("./test_tables/test_table_seven/").unwrap();

    for path in paths {
        
        let file_contents = std::fs::read_to_string(path.unwrap().path())
            .expect("Should have been able to read the file");
        
        content.push_str(&file_contents);

    }

    let test_record: serde_json::Value = serde_json::from_str(&content).unwrap();

    assert_eq!(test_record["table_name"], records[0].get_name());
    assert_eq!(test_record["event_type"], records[0].get_type());
    assert_eq!(test_record["record"], records[0].get_record());
    assert_eq!(test_record["operation"], records[0].get_operation());

    // teardown
    std::fs::remove_dir_all("./test_tables/test_table_seven/").unwrap();

}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn one_csv_with_headers() {
    
    let (tx, _handle) = setup("./test_tables/", String::from("csv"), true).await;

    let records = test_records("test_table_eight", 1);
    tx.send(records.clone()).unwrap();

    let two_seconds = time::Duration::from_secs(2);
    thread::sleep(two_seconds);

    // get table files
    let mut content = String::new();
    let paths = std::fs::read_dir("./test_tables/test_table_eight/").unwrap();

    for path in paths {
        
        let file_contents = std::fs::read_to_string(path.unwrap().path())
            .expect("Should have been able to read the file");
        
        content.push_str(&file_contents);

    }

    let lines: Vec<&str> = content.split("\n").collect();
    let headers: Vec<&str> = lines[0].split(",").collect();
    let values: Vec<&str> = lines[1].split(",").collect();
    
    assert_eq!(headers.len(), 7);
    assert!(headers.contains(&"table_name"));
    assert!(headers.contains(&"event_type"));
    assert!(headers.contains(&"record.id"));
    assert!(headers.contains(&"record.value"));
    assert!(headers.contains(&"operation"));
    assert!(headers.contains(&"event_id"));
    assert!(headers.contains(&"created_at"));

    assert_eq!(values.len(), 7);
    assert!(values.contains(&records[0].get_name().as_str()));
    assert!(values.contains(&records[0].get_type().as_str()));
    assert!(values.contains(&"0"));
    assert!(values.contains(&"0 value"));
    assert!(values.contains(&records[0].get_operation().as_str()));
    assert!(values.contains(&records[0].get_id().to_string().as_str()));

    // define the string format that the created_at field will be in
    let date_format = format_description::parse(
        "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]",
    ).unwrap();

    assert!(values.contains(&records[0].get_created_at().format(&date_format).unwrap().as_str()));

    // teardown
    std::fs::remove_dir_all("./test_tables/test_table_eight/").unwrap();

}

// ************************************************************************
// add tests for keep headers
// ************************************************************************

// ************************************************************************
// add tests for larger volumes of records and including more varied records.
// could include more complex records such as integer, boolean and nested fields.
// ************************************************************************

// ************************************************************************
// add tests for bad config. The connector should throw an error.
// ************************************************************************