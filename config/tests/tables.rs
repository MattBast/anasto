use config::tables::Config;
use std::path::PathBuf;

#[test]
fn basic_config() {
    
    let config: Config = toml::from_str(r#"
       [api]
       ip = '127.0.0.0'
       port = 8080
       max_request_size = 16000

       [stream]
       max_capacity = 300
       max_memory = 2000000
       max_event_age = 120

       [schemas]
        url = './test_schemas/'
    "#).unwrap();

    assert_eq!(Ok(config.api.ip), "127.0.0.0".parse());
    assert_eq!(config.api.port, 8080);
    assert_eq!(config.api.max_request_size, 16000);

    assert_eq!(config.stream.max_capacity, 300);
    assert_eq!(config.stream.max_memory, 2000000);
    assert_eq!(config.stream.max_event_age, 120);

    assert_eq!(config.schemas.url, PathBuf::from("./test_schemas/"));

}


#[test]
fn default_config() {
    
    let config: Config = toml::from_str(r#""#).unwrap();

    assert_eq!(Ok(config.api.ip), "127.0.0.1".parse());
    assert_eq!(config.api.port, 3030);
    assert_eq!(config.api.max_request_size, 16384);

    assert_eq!(config.stream.max_capacity, 100);
    assert_eq!(config.stream.max_memory, 1000000);
    assert_eq!(config.stream.max_event_age, 60);

    assert_eq!(config.schemas.url, PathBuf::from("./schemas/"));

}


#[test]
fn part_default_config() {
    
    let config: Config = toml::from_str(r#"
       [api]
       ip = '127.0.0.0'

       [stream]
       max_capacity = 300
       max_memory = 2000000
    "#).unwrap();

    assert_eq!(Ok(config.api.ip), "127.0.0.0".parse());
    assert_eq!(config.api.port, 3030);
    assert_eq!(config.api.max_request_size, 16384);

    assert_eq!(config.stream.max_capacity, 300);
    assert_eq!(config.stream.max_memory, 2000000);
    assert_eq!(config.stream.max_event_age, 60);

    assert_eq!(config.schemas.url, PathBuf::from("./schemas/"));

}


#[test]
fn one_localfile_subscriber() {
    
    let config: Config = toml::from_str(r#"
       [[subscriber.localfile]]
       dirpath_str = "./destination/"
       filetype = "avro"
    "#).unwrap();

    assert_eq!(config.subscribers.unwrap().len(), 1);
    assert_eq!(config.subscribers.unwrap()[0].dirpath_str, "./destination/".parse());
    assert_eq!(config.subscribers.unwrap()[0].filetype, "avro".parse());

}

#[test]
fn two_localfile_subscriber() {
    
    let config: Config = toml::from_str(r#"
       [[subscriber.localfile]]
       dirpath_str = "./avro_destination/"
       filetype = "avro"

       [[subscriber.localfile]]
       dirpath_str = "./json_destination/"
       filetype = "json"
    "#).unwrap();

    assert_eq!(config.subscribers.unwrap().len(), 2);
    assert_eq!(Ok(config.subscribers.unwrap()[0].dirpath_str), "./avro_destination/".parse());
    assert_eq!(Ok(config.subscribers.unwrap()[0].filetype), "avro".parse());

    assert_eq!(Ok(config.subscribers.unwrap()[0].dirpath_str), "./json_destination/".parse());
    assert_eq!(Ok(config.subscribers.unwrap()[0].filetype), "json".parse());

}