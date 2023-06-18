use std::path::Path;
use domains::record::Record;
use crate::sub_trait::Subscriber;
use std::fs::{ create_dir_all, File };
use std::io::{Error, ErrorKind, Write, LineWriter};
use uuid::Uuid;

// csv crates
use csv::Writer as CsvWriter;
use flatten_json_object::ArrayFormatting;
use flatten_json_object::Flattener;
use json_objects_to_csv::Json2Csv;

// avro crates
use apache_avro::Writer as AvroWriter;
use apache_avro::Codec;
use apache_avro::Schema;
use std::sync::Arc;

// parquet crates
use arrow_array::{
	// Int32Array, 
	// Int64Array, 
	// ListArray, 
	// MapArray, 
	StringArray, 
	ArrayRef
	};
use arrow_array::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap};

use log::{ 
	info, 
	error 
};


/// A Subscriber that writes records to files in the local filesystem.
/// The files are written into a directory per table/schema so that
/// the data can be read into tables by other systems.
#[derive(Debug)]
pub struct Localfile {
	
	/// A unique name for the subscriber. This helps to identify the logs relating to this subscriber.
	name: String,

	/// The parent filepath where all data this Subscriber handles will
	/// be written to
	dirpath_str: String,
	
	/// The type of file to write. Defaults to JSON.
	filetype: String,

	/// If the user would like the Record event headers to be included in the write
	keep_headers: bool,

}

impl Localfile {

	/// Create a new localfile subscriber
	pub fn new(
		name: String,
		dirpath: &Path, 
		filetype: String,
		keep_headers: bool, 
	) -> Result<Localfile, std::io::Error> {
		
		// Convert the directory path into a string for easy access in future functions
		let dirpath_str = match dirpath.to_path_buf().into_os_string().into_string() {
			
			Ok(dirpath) => dirpath,
			Err(_) => return Err(Error::new(ErrorKind::Other, "The dirpath did not contain valid unicode characters."))

		};

		// Create the Subscriber
		let connector = Localfile { name, dirpath_str, filetype, keep_headers };

		// do a check that the dirpath is a directory and that it ends with a `/` char
		connector.check_dirpath()?;

		info!(target: &connector.name, "Initialised Localfile subscriber called {} writing {} type files to {}", &connector.name, connector.filetype, connector.dirpath_str);

		Ok(connector)

	}

	/// make sure that the path points at a directory
	fn check_dirpath(&self) -> Result<(), std::io::Error> {

	    if self.dirpath_str.is_empty() {
	        let error_message = format!("The dirpath url: {} has no characters in it.", &self.dirpath_str);
	        return Err(Error::new(ErrorKind::Other, error_message));
	    }

	    if !&self.dirpath_str.ends_with('/') {
	        let error_message = format!("The dirpath url: {} did not end with a / character.", &self.dirpath_str);
	        return Err(Error::new(ErrorKind::Other, error_message));
	    }

		Ok(())  

	}

	// create a json file from a vector of records
	fn create_json_records(&self, table_name: String, records: Vec<Record>) -> Result<String, std::io::Error> {

		let file_path = format!("{}{}/{}.jsonl", self.dirpath_str, table_name, Uuid::new_v4());
		let file = File::create(&file_path)?;
		let mut file = LineWriter::new(file);

		// write the records to the file
	    records.into_iter().try_for_each(|record| {
	    	
	    	let line = match self.keep_headers {
	    		true => format!("{}{}", record.get_record_with_headers(), "\n"),
	    		false => format!("{}{}", record.get_record(), "\n"),
	    	};
	    	file.write_all(&line.into_bytes())?;

	    	Ok::<(), std::io::Error>(())
	    })?;

	    file.flush()?;

		Ok(file_path)

	}

	// create a csv file from a vector of records
	fn create_csv_records(&self, table_name: String, records: Vec<Record>) -> Result<String, std::io::Error> {

		let file_path = format!("{}{}/{}.csv", self.dirpath_str, table_name, Uuid::new_v4());
		let csv_writer = CsvWriter::from_path(&file_path)?;

		let json_records: Vec<serde_json::Value> = match self.keep_headers {

			true => records.into_iter().map(|record| record.get_record_with_headers()).collect(),
			false => records.into_iter().map(|record| record.get_record()).collect(),
    	
    	};

		let flattener = Flattener::new()
			.set_key_separator(".")
		    .set_array_formatting(ArrayFormatting::Plain)
		    .set_preserve_empty_arrays(true)
		    .set_preserve_empty_objects(true);

		let write_result = Json2Csv::new(flattener).convert_from_array(&json_records, csv_writer);

		match write_result {
			Ok(_) => Ok(file_path),
			Err(error_message) => Err(Error::new(ErrorKind::Other, error_message))
		}

	}


	// create an avro file from a vector of records
	fn create_avro_records(&self, table_name: String, records: Vec<Record>) -> Result<String, std::io::Error> {

		// get the schema from the first record in the vector
		let schema = match self.keep_headers {
			true => Schema::parse_str(&records[0].get_raw_schema_with_headers()).unwrap(),
    		false => Schema::parse_str(&records[0].get_raw_schema()).unwrap(),
		};
		let mut writer = AvroWriter::with_codec(&schema, Vec::new(), Codec::Snappy);

		// write the records to the file
	    records.into_iter().try_for_each(|record| {
	    	
	    	match self.keep_headers {
	    		true => writer.append_value_ref(&record.get_avro_record_with_headers()).unwrap(),
	    		false => writer.append_value_ref(&record.get_avro_record()).unwrap(),
	    	};
	    	Ok::<(), std::io::Error>(())

	    })?;

		// create and write all content to the file
	    let file_path = format!("{}{}/{}.avro", self.dirpath_str, table_name, Uuid::new_v4());
	    std::fs::write(&file_path, writer.into_inner().unwrap())?;

		Ok(file_path)

	}


	// create a parquet file from a vector of records
	fn create_parquet_records(&self, table_name: String, records: Vec<Record>) -> Result<String, std::io::Error> {

		// re-arrange rows into columns and return as Parquet RecordBatch type
		let batch = self.create_batch(records);

		// create a file for the batch
		let file_path = format!("{}{}/{}.parquet", self.dirpath_str, &table_name, Uuid::new_v4());
		let path = Path::new(&file_path);
		let file = File::create(path).unwrap();

		// create a Parquet writer with the default properties
		let props = WriterProperties::builder().build();
		let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

		// write the batch to the file
		writer.write(&batch).expect("Writing batch");

		// write the required Parquet footer to the file and close it
		writer.close().unwrap();

		Ok(file_path)

	}

	// re-arrange rows into columns and return as Parquet RecordBatch type
	fn create_batch(&self, records: Vec<Record>) -> RecordBatch {
		
		// a hashmap where each element is a column of values. 
		// The key is the column name
		let mut columns: HashMap<String, Vec<String>> = HashMap::new();

		// loop through each record organising them into columns
		for record in records {
			
			let record_json = match self.keep_headers {
		    		
	    		true => record.get_record_with_headers(),
	    		
	    		false => record.get_record(),

	    	};

			if let serde_json::Value::Object(record_json) = record_json {
					
				for key in record_json.keys() {
					
					// If this is the first time the loop has seen a column, create a key for it.
					columns.entry(key.to_string()).or_insert_with(Vec::new);
					// Then put the value in the column vector.
					columns.get_mut(&key.to_string()).unwrap().push(record_json[key].to_string());
					
				}

			};

		}

		// merge the columns (vectors of values) into one vector of Parquet ArrayRefs
		let mut record_batch = Vec::new();

		for (key, values) in columns {
			
			record_batch.push((key, Arc::new(StringArray::from(values)) as ArrayRef));

		}

		// change type from vector to Parquet RecordBatch and return the batch
		RecordBatch::try_from_iter(record_batch).unwrap()

	}

}

impl Subscriber for Localfile {

	fn create_records(&self, records: Vec<Record>) -> Result<(), std::io::Error> {
		
		info!(target: &self.name, "Received {} records.", records.len());

		// check if a directory for the table exists. If not, create it.
		let table_name = records[0].get_name();
		match self.check_table(&table_name[..]) {

			Ok(_) => (),
			Err(error) => {
				error!(target: &self.name, "Failed to create the table {}", &table_name);
				return Err(error)
			},

		};
		
		// check what type of file the user wants 
		let write_result = match self.filetype.to_lowercase().as_str() {
			
			"jsonl" => self.create_json_records(table_name, records),
			"csv" => self.create_csv_records(table_name, records),
			"avro" => self.create_avro_records(table_name, records),
			"parquet" => self.create_parquet_records(table_name, records),
			_ => self.create_json_records(table_name, records),

		};

		match write_result {
			
			Ok(filepath) => {
				info!(target: &self.name, "Wrote batch of records to {}", filepath);
				Ok(())
			},
			Err(error) => {
				error!(target: &self.name, "Failed to write batch of records");
				Err(error)
			},

		}

	}

	fn upsert_records(&self, records: Vec<Record>) -> Result<(), std::io::Error> {
		
		// there is no upsert mode for localfiles so just create the records
		self.create_records(records)

	}

	fn delete_records(&self, records: Vec<Record>) -> Result<(), std::io::Error> {
		
		// there is no delete mode for localfiles so just create the records
		self.create_records(records)

	}

    fn check_table(&self, table_name: &str) -> Result<(), std::io::Error> {
    	
    	// generate a path for the table
    	let table_path = format!("{}{}", self.dirpath_str, table_name);
		
    	// try to create the directory for the table
		match create_dir_all(table_path) {
			Ok(_) => Ok(()),
			Err(e) => Err(Error::new(ErrorKind::Other, e))
		}

    }

}