use std::path::Path;
use domains::record::Record;
use crate::sub_trait::Subscriber;
use std::path::PathBuf;
use std::fs::{ create_dir_all, File };
use std::io::{Error, ErrorKind, Write, LineWriter};
use uuid::Uuid;
use csv::Writer as CsvWriter;
use apache_avro::Writer as AvroWriter;
use apache_avro::Codec;
use apache_avro::Schema;
use std::sync::Arc;


// use tokio::runtime::Runtime;
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
// use log::{ 
	// info, 
	// warn, 
	// error 
// };


/// A Subscriber that writes records to files in the local filesystem.
/// The files are written into a directory per table/schema so that
/// the data can be read into tables by other systems.
#[derive(Debug)]
pub struct Localfile {
	/// The parent filepath where all data this Subscriber handles will
	/// be written to
	dirpath_str: String,
	/// The type of file to write. Defaults to JSON.
	filetype: String,
}

impl Localfile {

	/// Create a new localfile subscriber
	pub fn new(
		dirpath: &PathBuf, 
		filetype: String, 
	) -> Result<Localfile, std::io::Error> {
		
		// Convert the directory path into a string for easy access in future functions
		let dirpath_str = match dirpath.clone().into_os_string().into_string() {
			
			Ok(dirpath) => dirpath,
			Err(_) => return Err(Error::new(ErrorKind::Other, "The dirpath did not contain valid unicode characters."))

		};

		// Create the Subscriber
		let connector = Localfile { dirpath_str, filetype };

		// do a check that the dirpath is a directory and that it ends with a `/` char
		connector.check_dirpath()?;

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
	fn create_json_records(&self, table_name: String, records: Vec<Record>) -> Result<(), std::io::Error> {

		let file_path = format!("{}{}/{}.jsonl", self.dirpath_str, table_name, Uuid::new_v4());
		let file = File::create(file_path)?;
		let mut file = LineWriter::new(file);

		// write the records to the file
	    records.into_iter().try_for_each(|record| {
	    	let line = format!("{}{}", record.get_record(), "\n");
	    	file.write_all(&line.into_bytes())?;

	    	Ok::<(), std::io::Error>(())
	    })?;

	    file.flush()?;

		Ok(())

	}

	// create a csv file from a vector of records
	fn create_csv_records(&self, table_name: String, records: Vec<Record>) -> Result<(), std::io::Error> {

		let file_path = format!("{}{}/{}.csv", self.dirpath_str, table_name, Uuid::new_v4());
		let mut wtr = CsvWriter::from_path(file_path)?;
		let mut headers_written = false;

		// write the records to the file
	    records.into_iter().try_for_each(|record| {
	    	
	    	if !headers_written {
	    		
	    		record.get_record().as_object().unwrap().keys().try_for_each(|key| {
		    		wtr.write_field(key.as_str())?;
		    		Ok::<(), std::io::Error>(())
		    	})?;

		    	wtr.write_record(None::<&[u8]>)?;

		    	headers_written = true;
		    	
	    	}
		    	

	    	record.get_record().as_object().unwrap().values().try_for_each(|value| {
	    		wtr.write_field(value.as_str().unwrap())?;
	    		Ok::<(), std::io::Error>(())
	    	})?;

	    	wtr.write_record(None::<&[u8]>)?;
	    	Ok::<(), std::io::Error>(())

	    })?;

	    wtr.flush()?;

		Ok(())

	}


	// create an avro file from a vector of records
	fn create_avro_records(&self, table_name: String, records: Vec<Record>) -> Result<(), std::io::Error> {

		// let schema = match self.schemas.read().await.get(&table_name) {
		// 	Some(schema) => schema.schema.clone(),
		// 	None => return Err(Error::new(ErrorKind::Other, "Table schema does not exist."))
		// };

		let schema = Schema::parse_str(&records[0].get_raw_schema()).unwrap();
		let mut writer = AvroWriter::with_codec(&schema, Vec::new(), Codec::Snappy);

		// write the records to the file
	    records.into_iter().try_for_each(|record| {
	    	
	    	writer.append_value_ref(&record.get_avro_record()).unwrap();
	    	Ok::<(), std::io::Error>(())

	    })?;

		// create and write all content to the file
	    let file_path = format!("{}{}/{}.avro", self.dirpath_str, table_name, Uuid::new_v4());
	    std::fs::write(file_path, writer.into_inner().unwrap())?;

		Ok(())

	}


	// create a parquet file from a vector of records
	fn create_parquet_records(&self, table_name: String, records: Vec<Record>) -> Result<(), std::io::Error> {

		let batch = self.create_batch(records);

		let file_path = format!("{}{}/{}.parquet", self.dirpath_str, &table_name, Uuid::new_v4());
		let path = Path::new(&file_path);

		// let ids = Int32Array::from(vec![1, 2, 3, 4]);
		// let vals = Int32Array::from(vec![5, 6, 7, 8]);
		// let batch = RecordBatch::try_from_iter(vec![
		// 	("id", Arc::new(ids) as ArrayRef),
		// 	("val", Arc::new(vals) as ArrayRef),
		// ]).unwrap();

		let file = File::create(&path).unwrap();

		// Default writer properties
		let props = WriterProperties::builder().build();

		let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

		writer.write(&batch).expect("Writing batch");

		// writer must be closed to write footer
		writer.close().unwrap();

		Ok(())

	}


	fn create_batch(&self, records: Vec<Record>) -> RecordBatch {
		
		let mut columns: HashMap<String, Vec<String>> = HashMap::new();

		for record in records {
			
			let record_json = record.get_record();

			match record_json {
				
				serde_json::Value::Object(record_json) => {
					
					for key in record_json.keys() {
						
						if !columns.contains_key(&key.to_string()) {
			                columns.insert(key.to_string(), Vec::new());
			            }
						
						columns.get_mut(&key.to_string()).unwrap().push(record_json[key].to_string());
						
					}

				},
				
				_ => ()

			};

		}

		let mut record_batch = Vec::new();

		for (key, values) in columns {
			
			// let arrow_array = match values[0] {
			// 	serde_json::Value::Null => StringArray::from(values),
			//     serde_json::Value::Bool(_) => StringArray::from(values),
			//     serde_json::Value::Number(_) => Int64Array::from(values),
			//     serde_json::Value::String(_) => StringArray::from(values),
			//     serde_json::Value::Array(_) => ListArray::from(values),
			//     serde_json::Value::Object(_) => MapArray::from(values),
			// };

			record_batch.push((key, Arc::new(StringArray::from(values)) as ArrayRef));

		}

		// let vals = Int32Array::from(vec![5, 6, 7, 8]);
		RecordBatch::try_from_iter(record_batch).unwrap()

	}

}

impl Subscriber for Localfile {

	fn create_records(&self, records: Vec<Record>) -> Result<(), std::io::Error> {
		
		// check if a directory for the table exists. If not, create it.
		let table_name = records[0].get_name();
		self.check_table(&table_name[..]).unwrap();
		
		// check what type of file the user wants 
		match self.filetype.to_lowercase().as_str() {
			"jsonl" => self.create_json_records(table_name, records),
			"csv" => self.create_csv_records(table_name, records),
			"avro" => self.create_avro_records(table_name, records),
			"parquet" => self.create_parquet_records(table_name, records),
			_ => self.create_json_records(table_name, records),
		}


	}

	fn upsert_records(&self, records: Vec<Record>) -> Result<(), std::io::Error> {
		
		// check if a directory for the table exists. If not, create it.
		let table_name = records[0].get_name();
		self.check_table(&table_name[..]).unwrap();

		Ok(())

	}

	fn delete_records(&self, records: Vec<Record>) -> Result<(), std::io::Error> {
		
		// check if a directory for the table exists. If not, create it.
		let table_name = records[0].get_name();
		self.check_table(&table_name[..]).unwrap();

		Ok(())

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