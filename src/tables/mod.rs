//! # Tables
//!
//! This module defines the logic for each of the source tables that Anasto can read from
//! and the destination tables that it writes to. It does this through:
//! - A single enum whose (Table) variants represent the different tables
//! - The Table enum enforces shared behaviour by requiring each table to have variants of certain functions
//! - Each Table variant is defined in it's own sub-module
//! - Sub-modules also define the configuration logic for each table

use serde_derive::{ Serialize, Deserialize };

// Some handy functions that are used across different Tables
pub mod utils;

// Each of these module represents a different type of table
pub mod source_files;
pub mod source_open_table;
pub mod source_api;
pub mod dest_files;
pub mod dest_open_table;

pub mod source_tables;
pub mod dest_tables;

/// A helper struct for creating and deleting files needed by test functions
pub mod test_utils;


/// This enum defines the actions that a Table can take when it handles an error
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
#[serde(rename_all="snake_case")]
pub enum FailAction {
   
   /// This action tells the Table to skip the data it had issues with
   Skip,

   /// This action tells the Table to stop processing new data and effectively 
   /// shutdown (the other tables remain unaffected)
   #[default]
   Stop

}

/// This enum defines the file types that the lake tables can work with
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
#[serde(rename_all="snake_case")]
pub enum LakeFileType {
   
   /// csv files
   #[default]
   Csv,

   /// json files
   Json,

   /// avro files
   Avro,

   /// parquet files
   Parquet,

}


/// This enum defines the open table types that the lake tables can work with
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
#[serde(rename_all="snake_case")]
pub enum OpenTableFormat {
   
   /// Delta Lake
   #[default]
   DeltaLake,

}