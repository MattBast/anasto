//! # Destination Open Table
//!
//! This module defines the logic for a destination table that writes to an Open Table.

use std::collections::HashMap;
use serde::*;
use log::info;
use std::io::{ Error, ErrorKind };
use serde_derive::{ Serialize, Deserialize };
use std::path::PathBuf;
use std::time::SystemTime;
use crate::tables::{ FailAction, OpenTableFormat };
use crate::tables::utils::{
	five_hundred_chars_check, 
	random_table_name, 
	path_dir_check,
	start_of_time_timestamp
};
use datafusion::prelude::DataFrame;
use datafusion::error::Result;
use datafusion::common::DFSchema;
use convert_case::{ Case, Casing };


// delta lake crates
use deltalake::DeltaTable;
use deltalake::operations::DeltaOps;
// use deltalake::arrow::record_batch::RecordBatch;
use deltalake::errors::DeltaTableError;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::schema::{ SchemaField, SchemaDataType , SchemaTypeStruct, SchemaTypeArray, SchemaTypeMap };
use parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};


/// The DestOpenTable reads files from a local or remote filesystem
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DestOpenTable {
	
	/// A user defined name for the table. This does not need to correlate
	/// with the directory path where the tables files will be written to.
    #[serde(deserialize_with="to_snake_case")]
	pub dest_table_name: String,

	/// The name of the source table that supplies this destination table
    #[serde(deserialize_with="five_hundred_chars_check", default="random_table_name")]
	pub source_table_name: String,

	/// The parent filepath where all data this DestOpenTable handles will be written to
    #[serde(deserialize_with="path_dir_check")]
    pub dirpath: PathBuf,

    /// The open table format the data will be written to
    #[serde(default)]
    pub format: OpenTableFormat,

    /// Tracks which files have been read using their created timestamp
    #[serde(default="start_of_time_timestamp")]
    pub bookmark: SystemTime,

    /// Optional field. Decide what to do when new data fails to be written to a destination.
    #[serde(default)]
    pub on_fail: FailAction,

}

/// Return a string as snake case
pub fn to_snake_case<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {

	let s = String::deserialize(d)?;
    Ok(s.to_case(Case::Snake))

}


impl DestOpenTable {

	/// Get the name specified for this destination
    pub fn dest_table_name(&self) -> &String {
		&self.dest_table_name
	}

	/// Get the name of the source table that supplies this destination
    pub fn src_table_name(&self) -> &String {
		&self.source_table_name
	}

	/// Getter function for returning what to do if this table fails to read new data
	pub fn on_fail(&self) -> FailAction {
		self.on_fail
	}

	/// Decide what format of delta table to write to
	pub async fn write_new_data(&mut self, df: DataFrame) -> Result<(), Error> {

		info!(target: &self.dest_table_name, "Writing files to {}.", self.dirpath.display());

		// write the dataframe to file
		let write_result = match self.format {
			OpenTableFormat::DeltaLake => self.write_delta(df).await,
		};

		match write_result {
			Ok(_) => {
				self.bookmark = SystemTime::now();
				Ok(())
			},
			Err(e) => Err(Error::new(ErrorKind::Other, e.to_string()))
		}

	}

	/// Create the table if it doesn't already exist and then write a batch to it
	async fn write_delta(&self, df: DataFrame) -> Result<(), DeltaTableError> {
		
		// open the destination table or create it doesn't yet exist
	    let mut table = match deltalake::open_table(&self.dirpath.to_str().unwrap()).await {
	        
	        Ok(table) => table,
	        
	        Err(DeltaTableError::NotATable(_)) => {
	            info!(target: &self.dest_table_name, "Creating table for first batch");
	            let columns = self.get_columns(df.schema());
	            self.new_delta_table(columns).await?
	        },
	        
	        Err(err) => Err(err).unwrap(),
	    };

	    // Setup compression settings for the tables files
	    let writer_properties = WriterProperties::builder()
	        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
	        .build();

        // Create a writer for the table
	    let mut writer = RecordBatchWriter::for_table(&table)
	        .expect("Failed to make RecordBatchWriter")
	        .with_writer_properties(writer_properties);

		// **********************************************************************
		// Once the functionality is released, add upsert logic here. 
		// It's append only for now.
		// **********************************************************************
	    for batch in df.collect().await? { 
	    	writer.write(batch).await?; 
	    }

	    // Confirm all the data changes and make them available at the destination
	    let _ = writer
	        .flush_and_commit(&mut table)
	        .await
	        .expect("Failed to flush write");

	    Ok(())

	}

	async fn new_delta_table(&self, columns: Vec<SchemaField>) -> Result<DeltaTable, DeltaTableError> {
	    
	    DeltaOps::try_from_uri(&self.dirpath.to_str().unwrap())
	        .await
	        .unwrap()
	        .create()
	        .with_columns(columns)
	        .with_table_name(self.dest_table_name.clone())
	        .with_configuration_property(deltalake::delta_config::DeltaConfigKey::EnableChangeDataFeed, Some("true"))
	        .with_save_mode(deltalake::action::SaveMode::Append)
	        .await

	}

	fn get_columns(&self, schema: &DFSchema) -> Vec<SchemaField> {
		
		schema
			.fields()
			.iter()
			.map(|field| SchemaField::new(
				field.name().to_string(),
                self.get_delta_data_type(field.data_type()).unwrap(),
                true,
                HashMap::new()
			))
			.collect()

	}

	fn get_delta_data_type(&self, arrow_type: &arrow_schema::DataType) -> Result<SchemaDataType, Error> {

		match arrow_type {
			arrow_schema::DataType::Boolean => Ok(SchemaDataType::primitive("boolean".to_string())),
			arrow_schema::DataType::Int8 => Ok(SchemaDataType::primitive("byte".to_string())),
			arrow_schema::DataType::Int16 => Ok(SchemaDataType::primitive("short".to_string())),
			arrow_schema::DataType::Int32 => Ok(SchemaDataType::primitive("integer".to_string())),
			arrow_schema::DataType::Int64 => Ok(SchemaDataType::primitive("long".to_string())),
			arrow_schema::DataType::UInt8 => Ok(SchemaDataType::primitive("byte".to_string())),
			arrow_schema::DataType::UInt16 => Ok(SchemaDataType::primitive("short".to_string())),
			arrow_schema::DataType::UInt32 => Ok(SchemaDataType::primitive("integer".to_string())),
			arrow_schema::DataType::UInt64 => Ok(SchemaDataType::primitive("long".to_string())),
			arrow_schema::DataType::Float16 => Ok(SchemaDataType::primitive("float".to_string())),
			arrow_schema::DataType::Float32 => Ok(SchemaDataType::primitive("float".to_string())),
			arrow_schema::DataType::Float64 => Ok(SchemaDataType::primitive("double".to_string())),
			arrow_schema::DataType::Timestamp(_, _) => Ok(SchemaDataType::primitive("timestamp".to_string())),
			arrow_schema::DataType::Date32 => Ok(SchemaDataType::primitive("date".to_string())),
			arrow_schema::DataType::Date64 => Ok(SchemaDataType::primitive("date".to_string())),
			arrow_schema::DataType::Binary => Ok(SchemaDataType::primitive("binary".to_string())),
			arrow_schema::DataType::FixedSizeBinary(_) => Ok(SchemaDataType::primitive("binary".to_string())),
			arrow_schema::DataType::LargeBinary => Ok(SchemaDataType::primitive("binary".to_string())),
			arrow_schema::DataType::Utf8 => Ok(SchemaDataType::primitive("string".to_string())),
			arrow_schema::DataType::LargeUtf8 => Ok(SchemaDataType::primitive("string".to_string())),
			arrow_schema::DataType::List(field) => {
                Ok(SchemaDataType::array(SchemaTypeArray::new(
                    Box::new((*field).data_type().try_into().unwrap()),
                    (*field).is_nullable(),
                )))
            },
			arrow_schema::DataType::FixedSizeList(field, _) => {
                Ok(SchemaDataType::array(SchemaTypeArray::new(
                    Box::new((*field).data_type().try_into().unwrap()),
                    (*field).is_nullable(),
                )))
            },
			arrow_schema::DataType::LargeList(field) => {
                Ok(SchemaDataType::array(SchemaTypeArray::new(
                    Box::new((*field).data_type().try_into().unwrap()),
                    (*field).is_nullable(),
                )))
            },
			arrow_schema::DataType::Struct(fields) => {
                let converted_fields: Result<Vec<SchemaField>, _> = fields
                    .iter()
                    .map(|field| field.as_ref().try_into())
                    .collect();
                
                Ok(SchemaDataType::r#struct(
                    SchemaTypeStruct::new(converted_fields.unwrap()),
                ))
            },
			arrow_schema::DataType::Map(field, _) => {
                if let arrow_schema::DataType::Struct(struct_fields) = field.data_type() {
                    let key_type = struct_fields[0].data_type().try_into().unwrap();
                    let value_type = struct_fields[1].data_type().try_into().unwrap();
                    let value_type_nullable = struct_fields[1].is_nullable();
                    Ok(SchemaDataType::map(SchemaTypeMap::new(
                        Box::new(key_type),
                        Box::new(value_type),
                        value_type_nullable,
                    )))
                } else {
                    panic!("DataType::Map should contain a struct field child");
                }
            },
			s => Err(Error::new(ErrorKind::Other, format!(
                "Invalid data type for Delta Lake: {s}"
            ))),
		}
	}

}

// #[cfg(test)]
// mod tests {
// 	use super::*;

//     #[test]
//     fn can_read_files() {
    
//         let subscriber = File { dirpath: PathBuf::new(".") };
//         subscriber.poll_action();

//     }

// }
