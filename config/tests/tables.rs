use config::tables::{Config, Subscriber};
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
       [[subscriber]]
       type = "Localfile"
       dirpath = "./destination/"
       filetype = "avro"
    "#).unwrap();

    assert_eq!(config.subscriber.len(), 1);

    match &config.subscriber[0] {
        
        Subscriber::Localfile(sub) => {
            assert_eq!(sub.dirpath, PathBuf::from("./destination/"));
            assert_eq!(sub.filetype, "avro".to_string());
        }

    }
            

}

#[test]
fn two_localfile_subscriber() {
    
    let config: Config = toml::from_str(r#"
       [[subscriber]]
       type = "Localfile"
       dirpath = "./avro_destination/"
       filetype = "avro"

       [[subscriber]]
       type = "Localfile"
       dirpath = "./json_destination/"
       filetype = "jsonl"
    "#).unwrap();

    assert_eq!(config.subscriber.len(), 2);

    match &config.subscriber[0] {
        
        Subscriber::Localfile(sub) => {
            assert_eq!(sub.dirpath, PathBuf::from("./avro_destination/"));
            assert_eq!(sub.filetype, "avro".to_string());
        }
        
    }

    match &config.subscriber[1] {
        
        Subscriber::Localfile(sub) => {
            assert_eq!(sub.dirpath, PathBuf::from("./json_destination/"));
            assert_eq!(sub.filetype, "jsonl".to_string());
        }
        
    }

            

}

#[test]
fn localfile_subscriber_path_not_a_directory() {
    
    let config: Result<Config, toml::de::Error> = toml::from_str(r#"
       [[subscriber]]
       type = "Localfile"
       dirpath = "./destination/filename.json"
       filetype = "avro"
    "#);

    assert!(config.is_err());

}

#[test]
fn localfile_subscriber_default_filetype() {
    
    let config: Config = toml::from_str(r#"
       [[subscriber]]
       type = "Localfile"
       dirpath = "./destination/"
    "#).unwrap();

    match &config.subscriber[0] {
        
        Subscriber::Localfile(sub) => {
            assert_eq!(sub.dirpath, PathBuf::from("./destination/"));
            assert_eq!(sub.filetype, "jsonl".to_string());
        }
        
    }

}


#[test]
fn localfile_subscriber_filetype_not_available() {
    
    let config: Result<Config, toml::de::Error> = toml::from_str(r#"
       [[subscriber]]
       type = "Localfile"
       dirpath = "./destination/"
       filetype = "rs"
    "#);

    assert!(config.is_err());

}