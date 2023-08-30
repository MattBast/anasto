//! # Destination Open Table
//!
//! This module defines the logic for a destination table that writes to an Open Table.

use std::collections::HashMap;
use serde::*;
use log::info;
use std::io::{ Error, ErrorKind };
use serde_derive::{ Serialize, Deserialize };
use std::path::PathBuf;
use chrono::{ DateTime, offset::Utc };
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
    #[serde(deserialize_with="to_snake_case", default="random_table_name")]
	pub dest_table_name: String,

	/// The name of the source table that supplies this destination table
    #[serde(deserialize_with="five_hundred_chars_check")]
	pub source_table_name: String,

	/// The parent filepath where all data this DestOpenTable handles will be written to
    #[serde(deserialize_with="path_dir_check")]
    pub dirpath: PathBuf,

    /// The open table format the data will be written to
    #[serde(default)]
    pub format: OpenTableFormat,

    /// Tracks which files have been read using their created timestamp
    #[serde(default="start_of_time_timestamp")]
    pub bookmark: DateTime<Utc>,

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
				self.bookmark = Utc::now();
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

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::{ Utc, TimeZone, naive::NaiveDate, naive::NaiveDateTime };
	use crate::tables::test_utils::TestDir;
	use datafusion::prelude::SessionContext;
	use std::sync::Arc;
	

    #[test]
    fn table_from_toml_with_minimal_config() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
			dirpath = "./tests/data/delta_table/"
        "#);

        let table: DestOpenTable = toml::from_str(&content).unwrap();

        assert!(!table.dest_table_name.is_empty());
        assert_eq!(table.source_table_name, "json_table");
        assert_eq!(table.dirpath, PathBuf::from("./tests/data/delta_table/").canonicalize().unwrap());
        assert!(matches!(table.format, OpenTableFormat::DeltaLake));
        assert_eq!(table.bookmark, chrono::DateTime::<Utc>::MIN_UTC);
        assert!(matches!(table.on_fail, FailAction::Stop));

    }

    #[test]
    fn table_from_toml_with_maximum_config() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dest_table_name = "delta_table"
            dirpath = "./tests/data/delta_table/"
            format = "delta_lake"
            bookmark = "2023-08-21T00:55:00z"
            on_fail = "skip"
        "#);

        let table: DestOpenTable = toml::from_str(&content).unwrap();

        assert_eq!(table.dest_table_name, "delta_table");
        assert_eq!(table.source_table_name, "json_table");
        assert_eq!(table.dirpath, PathBuf::from("./tests/data/delta_table/").canonicalize().unwrap());
        assert!(matches!(table.format, OpenTableFormat::DeltaLake));
        assert!(matches!(table.on_fail, FailAction::Skip));

        let dt: NaiveDateTime = NaiveDate::from_ymd_opt(2023, 8, 21).unwrap().and_hms_opt(0, 55, 0).unwrap();
		let datetime_utc = Utc.from_utc_datetime(&dt);
        assert_eq!(table.bookmark, datetime_utc);

    }

    #[test]
    fn missing_mandatory_field() {
    
        let content = String::from(r#"
            dest_table_name = "delta_table"
            dirpath = "./tests/data/delta_table/"
        "#);

        let table: Result<DestOpenTable, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "missing field `source_table_name`", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn dirpath_does_not_exist() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dirpath = "./tests/data/does_not_exist/" 
        "#);

        let table: Result<DestOpenTable, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "The path: ./tests/data/does_not_exist/ does not exist.", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn dirpath_is_not_a_directory() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dirpath = "./tests/data/delta_table/part-00000-7444aec4-710a-4a4c-8abe-3323499043e9.c000.snappy.parquet" 
        "#);

        let table: Result<DestOpenTable, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "The path: ./tests/data/delta_table/part-00000-7444aec4-710a-4a4c-8abe-3323499043e9.c000.snappy.parquet is not a directory.", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

     #[test]
    fn typo_in_fieldname() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dirpath = "./tests/data/delta_table/"
            filepath = "json"
        "#);

        let table: Result<DestOpenTable, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "unknown field `filepath`, expected one of `dest_table_name`, `source_table_name`, `dirpath`, `format`, `bookmark`, `on_fail`", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn table_name_contains_too_many_characters() {
    
        let content = String::from(r#"
            source_table_name = "name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name"
            dirpath = "./tests/data/delta_table/part-00000-7444aec4-710a-4a4c-8abe-3323499043e9.c000.snappy.parquet" 
        "#);

        let table: Result<DestOpenTable, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(
        		e.message(), 
        		"The string: name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name is longer than 500 chars.", 
        		"Incorrect error message."
        	),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn source_table_name_function_returns_source_table_name() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dirpath = "./tests/data/delta_table/"
        "#);

        let table: DestOpenTable = toml::from_str(&content).unwrap();

        assert_eq!(table.src_table_name(), "json_table");

    }

    #[test]
    fn dest_table_name_function_returns_dest_table_name() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dest_table_name = "delta_table"
            dirpath = "./tests/data/delta_table/"
        "#);

        let table: DestOpenTable = toml::from_str(&content).unwrap();

        assert_eq!(table.dest_table_name(), "delta_table");

    }

    #[test]
    fn on_fail_function_returns_fail_action() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dirpath = "./tests/data/delta_table/"
        "#);

        let table: DestOpenTable = toml::from_str(&content).unwrap();

        assert!(matches!(table.on_fail(), FailAction::Stop));

    }

    #[tokio::test]
    async fn can_write_delta_table() {
    
    	// create diectory for destinsation table that tears itself down at the end of the test
        let _test_dir = TestDir::new("./tests/data/test_delta_dest/");

        // create the destination table
        let mut table: DestOpenTable = toml::from_str(&String::from(r#"
            source_table_name = "csv_table"
            dirpath = "./tests/data/test_delta_dest/"
        "#)).unwrap();

        // setup mock source table
        let ctx = SessionContext::new();
        let src_df = ctx.read_csv("./tests/data/csv_table/", Default::default()).await.unwrap();

        // create received source data to a destination
        table.write_new_data(src_df.clone()).await.unwrap();

        // read what was written into a dataframe and compare with the source dataframe
		let dest_delta_table = deltalake::open_table("./tests/data/test_delta_dest")
		    .await
		    .unwrap();
		let dest_df = ctx.read_table(Arc::new(dest_delta_table)).unwrap();
        let dest_data = dest_df.collect().await.unwrap();

        let mut ids: Vec<i64> = dest_data[0]
        	.column_by_name("id")
        	.unwrap()
        	.as_any()
    		.downcast_ref::<arrow_array::array::PrimitiveArray<arrow_array::types::Int64Type>>()
    		.unwrap()
    		.values()
    		.to_vec();
        ids.sort();

       	let values_array = dest_data[0]
        	.column_by_name("value")
        	.unwrap()
        	.as_any()
    		.downcast_ref::<arrow_array::array::StringArray>()
    		.unwrap();
        let mut values = vec![values_array.value(0), values_array.value(1), values_array.value(2)];
        values.sort();

        assert_eq!(ids, vec![1,2,3]);
        assert_eq!(values, vec!["hello world", "hey there", "hi"]);

        // make sure the bookmark has progressed
		assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

}
