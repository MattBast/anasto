//! Config
//!
//! Read the user supplied config file and check it for invalid content. Validity
//! rules are defined in the enums and structs referenced by the Config struct.
//!
//! The first arg in the command line is expected to be the path to the config file.

use log::info;
use std::env;
use std::io::{ ErrorKind, Error };
use std::fs::read_to_string;
use serde_derive::{ Serialize, Deserialize };
use std::path::PathBuf;

use crate::tables::{ source_tables::SourceTable, dest_tables::DestTable };


/// Top level container of all the fields in the config file
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
   
   /// the source tables that listen to a source dataset and reads all changes
   pub source_table: Vec<SourceTable>,

   /// the destination tables that get all changes from a specifc source table
   /// and write to a specified destination
   #[serde(default)]
   pub destination_table: Vec<DestTable>,

}

/// Gets path to config from command line args and loads resources specified
/// in it 
pub fn get() -> Config {

    let args: Vec<String> = env::args().collect();
    let filepath = get_config_filepath(args);
    let file_content = read_to_string(&filepath);
    read_config(file_content, filepath)

}

/// Loads resources specified in config file 
fn read_config(file_content: Result<String, Error>, filepath: String) -> Config {

    match file_content {

        Ok(content) => match toml::from_str(&content) {
            
            Ok(config) => {
                
                info!(target: "startup", "Starting Anasto with configurations from {}.", &filepath);
                config

            },
            Err(error) => panic!("{}", error)

        },
        Err(error) => match error.kind() {
                
            ErrorKind::PermissionDenied => panic!("Anasto does not have permission to access {}.", &filepath),
            ErrorKind::NotFound => panic!("Could not find the file {}.", &filepath),
            _ => panic!("Unknown error while trying to read {}.", &filepath)

        }

    }

}

// Get the config filepath from the command line. Panic if one can't be found.
fn get_config_filepath(args: Vec<String>) -> String {

    if args.len() < 2 {
        panic!("Could not find a filepath in the first argument. Try running something like `anasto config.toml`.")
    }

    let filepath = PathBuf::from(args[1].clone());

    if !filepath.exists() {
        panic!("The filepath: {} does not exist.", args[1]);
    }

    if filepath.extension().unwrap() != "toml" {
        panic!("The filepath: {} does not have the toml extension.", args[1]);
    }

    filepath.canonicalize().unwrap().to_str().unwrap().to_string()

}


#[cfg(test)]
mod tests {
 use super::*;

    #[test]
    fn can_read_tables_from_config() {
    
        let config_content = String::from(r#"
            [[source_table]]
            type = "files"
            table_name = "csv_table"
            dirpath = "./tests/data/csv_table/"

            [[destination_table]]
            type = "files"
            source_table_name = "csv_table"
            dest_table_name = "json_table"
            dirpath = "./tests/data/json_table/"
            filetype = "json"
        "#);

        let config = read_config(Ok(config_content), ".".into());

        assert_eq!(config.source_table.len(), 1);
        assert_eq!(config.destination_table.len(), 1);

    }

    #[test]
    fn missing_destinations_do_not_cause_a_panic() {
    
        // dirpath points at file instead of directory
        let config_content = String::from(r#"
            [[source_table]]
            type = "files"
            table_name = "csv_table"
            dirpath = "./tests/data/csv_table/" 
        "#);

        let _config = read_config(Ok(config_content), "./config.toml".into());

    }

    #[test]
    #[should_panic(expected = "TOML parse error at line 1, column 1\n  |\n1 | \n  | ^\nmissing field `source_table`\n")]
    fn missing_sources_do_cause_a_panic() {
    
        // dirpath points at file instead of directory
        let config_content = String::from(r#"
            [[destination_table]]
            type = "files"
            source_table_name = "csv_table"
            dest_table_name = "json_table"
            dirpath = "./tests/data/json_table/"
            filetype = "json"
        "#);

        let _config = read_config(Ok(config_content), "./config.toml".into());

    }

    #[test]
    #[should_panic(expected = "The path: ./tests/data/csv_table.csv does not exist.")]
    fn bad_table_causes_panic() {
    
        // dirpath points at file instead of directory
        let config_content = String::from(r#"
            [[source_table]]
            type = "files"
            table_name = "csv_table"
            dirpath = "./tests/data/csv_table.csv" 
        "#);

        let _config = read_config(Ok(config_content), "./config.toml".into());

    }

    #[test]
    #[should_panic(expected = "The path: ./tests/data/csv_table.csv does not exist.")]
    fn bad_table_with_good_table_causes_panic() {
    
        // source table is bad, destination is good
        let config_content = String::from(r#"
            [[source_table]]
            type = "files"
            table_name = "csv_table"
            dirpath = "./tests/data/csv_table.csv" 

            [[destination_table]]
            type = "files"
            source_table_name = "csv_table"
            dest_table_name = "json_table"
            dirpath = "./tests/data/json_table/"
            filetype = "json"
        "#);

        let _config = read_config(Ok(config_content), ".".into());

    }

    #[test]
    #[should_panic(expected = "Could not find the file ./config.toml.")]
    fn error_reading_file_causes_panic() {
    
        let config_content_error = Error::from(ErrorKind::NotFound);
        let _config = read_config(Err(config_content_error), "./config.toml".into());

    }

}