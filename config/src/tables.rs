use serde::de::Error;
use serde::*;
use serde_derive::{Deserialize};
use std::net::{ Ipv4Addr };
use std::path::PathBuf;
use std::fs::create_dir_all;

/// Top level container of all the fields in the config file
#[derive(Debug, Deserialize)]
pub struct Config {
   
   /// settings for the API
   #[serde(default="default_api")]
   pub api: Api,

   /// settings for the Stream
   #[serde(default="default_stream")]
   pub stream: Stream,

   /// settings for the Schemas
   #[serde(default="default_schemas")]
   pub schemas: Schemas,

   /// settings for the Subscribers
   #[serde(default)]
   pub subscriber: Vec<Subscriber>,

}

/// Settings for the API
#[derive(Debug, Deserialize)]
pub struct Api {
   
   /// the ip that the API will listen for requests on
   #[serde(default="default_ip")]
   pub ip: Ipv4Addr,
   
   /// the port number that the API will listen for requests on
   #[serde(default="default_port")]
   pub port: u16,

   /// the maximum size of an api request measured in bytes
   #[serde(default="default_request_size")]
   pub max_request_size: u64,

}


/// enable default values when declaring the API config
impl Default for Api {
    
    fn default() -> Api {
        
      Api {
         ip: Ipv4Addr::new(127, 0, 0, 1),
         port: 3030,
         max_request_size: 1024 * 16
      }

    }

}


/// default the api field to {ip: "127.0.0.1", port: 3030}
fn default_api() -> Api {
   Api {
      ip: Ipv4Addr::new(127, 0, 0, 1),
      port: 3030,
      max_request_size: 1024 * 16
   }
}


/// default the ip field to "127.0.0.1"
fn default_ip() -> Ipv4Addr {
	Ipv4Addr::new(127, 0, 0, 1)
}


/// default the port field to 3030
fn default_port() -> u16 {
	3030
}


/// default the port field to 3030
fn default_request_size() -> u64 {
   1024 * 16
}


/// Settings for the Streams (message bus)
#[derive(Debug, Deserialize)]
pub struct Stream {
   
   /// How many events a buffer can include before it needs to be read
   #[serde(default="default_capacity")]
   pub max_capacity: usize,
   
   /// How much memory a buffer can consume in bytes before it needs
   /// to be read
   #[serde(default="default_memory")]
   pub max_memory: usize,
   
   /// The maximum age in seconds that an event can live to until its buffer 
   /// should be drained
   #[serde(default="default_event_age")]
   pub max_event_age: u64,

}

/// enable default values when declaring the Stream config
impl Default for Stream {
    
    fn default() -> Stream {
        
        Stream {
            max_capacity: 100,
            max_memory: 1000000,
            max_event_age: 60,
        }

    }

}

/// default the stream field to {max_capacity: 100, max_memory: 1000000, max_event_age: 60}
fn default_stream() -> Stream {
   Stream { ..Default::default() }
}


/// default the max_capacity field to 100 records
fn default_capacity() -> usize {
   100
}


/// default the max_memory field to 1000000 bytes
fn default_memory() -> usize {
   1000000
}


/// default the max_event_age field to 60 bytes
fn default_event_age() -> u64 {
   60
}


/// Settings for the Schemas
#[derive(Debug, Deserialize)]
pub struct Schemas {
   
   /// URL location of the schemas directory in the users filesystem
   #[serde(default="default_schemas_url", deserialize_with="de_schemas_url")]
   pub url: PathBuf,

}

/// enable default values when declaring the Stream config
impl Default for Schemas {
    
    fn default() -> Schemas {
        
        Schemas {
            url: PathBuf::from("./schemas/"),
        }

    }

}

/// default the Schemas table to include a url equal to "./schemas/"
fn default_schemas() -> Schemas {
   Schemas { ..Default::default() }
}


/// default the stream field to "./schemas/"
fn default_schemas_url() -> PathBuf {
    
    let filepath = PathBuf::from("./schemas/");
            
    if filepath.exists() {

        filepath

    }
    else {
        
        create_dir_all("./schemas/").unwrap();
        filepath

    }

}


/// Check that the path points at a directory and ends with a / character
/// Create the directory if it doesn't exist
fn de_schemas_url<'de, D: Deserializer<'de>>(d: D) -> Result<PathBuf, D::Error> {

    let s = String::deserialize(d)?;
    let filepath = PathBuf::from(s.clone());

    if s.is_empty() {
        let error_message = format!("The [schemas] url: {} has no characters in it.", s);
        return Err(D::Error::custom(error_message));
    }

    if !s.ends_with('/') {
        let error_message = format!("The [schemas] url: {} did not end with a / character.", s);
        return Err(D::Error::custom(error_message));
    }

    match filepath.try_exists() {
            
        Ok(filepath_exists) => {
            
            if filepath_exists {
                
                Ok(filepath)

            }
            else {

                create_dir_all(&s).unwrap();
                Ok(filepath)

            }

        },

        Err(e) => Err(D::Error::custom(e.to_string())),

    }

}

/// The Subscriber variations
#[derive(Debug, Deserialize)]
#[serde(tag="type")]
pub enum Subscriber {
   
   /// Creates a subscriber that writes Records to localfiles
   Localfile(LocalfileSubscriber),

}


/// Settings for the Localfile Subscriber
#[derive(Debug, Deserialize)]
pub struct LocalfileSubscriber {
   
    /// The parent filepath where all data this Subscriber handles will
    /// be written to
    #[serde(deserialize_with="localfile_path")]
    pub dirpath: PathBuf,
    /// The type of file to write. Defaults to JSON.
    #[serde(default="default_local_filetype", deserialize_with="local_filetype")]
    pub filetype: String,

}

/// Check that the path points at a directory and ends with a / character
/// Create the directory if it doesn't exist
fn localfile_path<'de, D: Deserializer<'de>>(d: D) -> Result<PathBuf, D::Error> {

    let s = String::deserialize(d)?;
    let filepath = PathBuf::from(s.clone());

    if s.is_empty() {
        let error_message = format!("The [subscriber] dirpath: {} has no characters in it.", s);
        return Err(D::Error::custom(error_message));
    }

    if !s.ends_with('/') {
        let error_message = format!("The [subscriber] dirpath: {} did not end with a / character.", s);
        return Err(D::Error::custom(error_message));
    }
    
    Ok(filepath)

}

fn default_local_filetype() -> String {

    "jsonl".to_string()

}

fn local_filetype<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    
    let mut filetype = String::deserialize(d)?;
    filetype.make_ascii_lowercase();
    
    if !["jsonl".to_owned(), "csv".to_owned(), "parquet".to_owned(), "avro".to_owned()].contains(&filetype) {
        return Err(D::Error::custom("The filetype was not one of: ['jsonl', 'csv', 'parquet', 'avro']"));
    }

    Ok(filetype)

}