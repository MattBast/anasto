//! Config
//!
//! Read the user supplied config file and check it for invalid content. Validity
//! rules are defined in the enums and structs referenced by the Config struct.
//!
//! The first arg in the command line is expected to be the path to the config file.

use log::info;
use std::env;
use std::io::ErrorKind;
use std::fs::read_to_string;
use serde_derive::{Deserialize};
use std::path::PathBuf;

use crate::tables::{ source_tables::SourceTable, dest_tables::DestTable };


/// Top level container of all the fields in the config file
#[derive(Debug, Deserialize)]
pub struct Config {
   
   /// the source tables that listen to a source dataset and reads all changes
   pub source_table: Vec<SourceTable>,

   /// the destination tables that get all changes from a specifc source table
   /// and write to a specified destination
   pub dest_table: Vec<DestTable>,

}

/// Looks for the config file. If it can't find it, start Anasto with
/// default values.
pub fn get() -> Config {

    let filepath = get_config_filepath();

    match read_to_string(&filepath) {

        Ok(content) => match toml::from_str(&content) {
            
            Ok(config) => {
                
                info!(target: "startup", "Starting Anasto with configurations from {}.", &filepath);
                config

            },
            Err(error) => panic!("Got error while trying to parse {}", &filepath)

        },
        Err(error) => match error.kind() {
                
            ErrorKind::PermissionDenied => panic!("Anasto does not have permission to access {}.", &filepath),
            ErrorKind::NotFound => panic!("Could not find the file {}.", &filepath),
            _ => panic!("Unknown error while trying to read {}.", &filepath)

        }

    }

}

// Get the config filepath from the command line. Panic if one can't be found.
fn get_config_filepath() -> String {

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        panic!("Could not find a filepath in the first argument. Try running something like `anasto config.toml`.")
    }

    let filepath = PathBuf::from(args[1]);

    if !filepath.exists() {
        panic!("The filepath: {} does not exist.", args[1]);
    }

    if filepath.extension().unwrap() != "toml" {
        panic!("The filepath: {} does not have the toml extension.", args[1]);
    }

    filepath.canonicalize().unwrap().to_str().unwrap().to_string()

}