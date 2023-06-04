use std::collections::HashMap;
use domains::schema::Schema;
use serde_json;
use tokio::fs::{ write, remove_file };
use std::fs::{ read_to_string, read_dir, create_dir_all };
use std::io::ErrorKind;
use std::path::PathBuf;
use log::{ info, warn, error };

/// This repository stores Schema events to keep track of all the 
/// that should be getting enforced on the Record events. It saves
/// all the records to file so that they can be accessed asynchronously
/// by other threads.
#[derive(Debug)]
pub struct Repo {
    
    /// All the schemas available in this repository
    schemas: HashMap<String, Schema>,

    /// The location where the schemas are saved as files
    filepath: String

}


impl Default for Repo {
	
	fn default() -> Self {
		
		Self::new(PathBuf::new()).unwrap()
	
	}

}


impl Repo {
	
	/// Create a new Schema repo
	pub fn new(dir: PathBuf) -> Result<Repo, std::io::Error> {
		
		// create the schemas directory path if it doesn't already exist
		match create_dir_all(&dir) {
			Ok(_) => (),
			Err(e) => {
				error!(target: "schemas", "Failed to create the directory {}", &dir.display());
				return Err(e);
			}
		};

		let schemas = get_schemas(&dir)?; 
		let filepath = dir.into_os_string().into_string();
		
		if filepath.is_err() {
			
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData, 
				"The schema directory path contains invalid Unicode data."
			));

		};

		Ok(Repo { schemas, filepath: filepath.unwrap() })

	}

	/// Receive a Schema event, update the Repo and write the
	/// changes to file
	pub async fn update(&mut self, schema_event: Schema) {

		let table_name = schema_event.table_name.clone();
		let operation = schema_event.operation.to_ascii_lowercase();

		if operation == *"create" {

	        self.schemas.insert(
	            table_name.clone(), 
	            schema_event.clone()
	        );

	        self.write_file(schema_event).await;

	    }

	    else if operation == *"update" {

	        if let Some(old_schema) = self.schemas.get_mut(&table_name) {
	                
	            *old_schema = schema_event.clone();

	        } else {

	            self.schemas.insert(
	                table_name.clone(), 
	                schema_event.clone()
	            );

	        }

	        self.write_file(schema_event).await;

	    }

	    else if operation == *"delete" {

	        self.schemas.remove(&table_name);
	        self.remove_file(&table_name).await;

	    }

		info!(target: "schemas", "{}d the schema '{}'", &operation, &table_name);

	}

	/// Write the Repos state to file
	async fn write_file(
		&self, 
		schema: Schema
	) {

		// prepare a path for the file
		let file_path = self.filepath.to_string() + &schema.table_name + ".json";

		// prepare schema to become file content
		let file_content = match serde_json::to_string(&schema) {
			
			Ok(file_content) => file_content,
			
			Err(e) => {
				
				warn!(target: "schemas", "Failed to convert the schema {} into a string when writing to file.", &schema.table_name);
				warn!(target: "schemas", "{:?}", e);
				return;

			}

		};
		
		// create or update the file at the specified path
		match write(file_path, file_content).await {

			Ok(_) => (),

        	Err(error) => match error.kind() {

        		ErrorKind::PermissionDenied => warn!(target: "schemas", "Anasto does not have permission to create schema files at {}*.json.", self.filepath),

        		_ => warn!(target: "schemas", "An unexpected error happened when trying to create or update a schema file. The received error was: {:?}", error),

        	}

		};

	}


	/// Update the Repos state after a delete event
	async fn remove_file(
		&self, 
		table_name: &String
	) {

		match remove_file(self.filepath.to_string() + table_name + ".json").await {

        	Ok(_) => (),

        	Err(error) => match error.kind() {

        		ErrorKind::NotFound => (),

        		ErrorKind::PermissionDenied => warn!(target: "schemas", "Anasto does not have permission to create schema files at {}*.json.", self.filepath),        		
        		
        		_ => (),

        	}
        	
        };

	}

	/// Check if a schema for a specified table is in the repo
	pub fn contains_key(&self, table_name: &String) -> bool {

		self.schemas.contains_key(table_name)

	}

	/// Get the schema for a specified table in the repo
	pub fn get(&self, table_name: &String) -> Option<&Schema> {

		self.schemas.get(table_name)

	}

	/// Get the schema for a specified table in the repo
	pub fn get_all(&self) -> Vec<Schema> {

		self
			.schemas
			.values()
			.into_iter()
			.map(|schema| schema.clone())
			.collect()

	}

}

/// Read all files in the path provided and populate the Repo
/// with the schemas that are found.
fn get_schemas(dir: &PathBuf) -> Result<HashMap<String, Schema>, std::io::Error> {
    
    let mut schemas = HashMap::new();

    // if the path provided is a directory, read all files
    // in the directory
    if dir.is_dir() {
        
        read_dir(dir)?.try_for_each(|entry| -> Result<(), std::io::Error> {

		    let path = entry?.path();

		    // including the files in child directories
		    if path.is_dir() {
		    	
		    	let child_schemas = get_schemas(&path)?;
		    	schemas.extend(child_schemas);

		    } else {

		    	let (table_name, schema) = read_schema_file(&path);
				schemas.insert(table_name, schema);

		    }

		    Ok(())

		})?;

    }

    // or just read one file
    if dir.is_file() {

    	let (table_name, schema) = read_schema_file(dir);
		schemas.insert(table_name, schema);

    }

	Ok(schemas)

}

/// Read a file containing a schema and return the Schema
fn read_schema_file(filepath: &PathBuf) -> (String, Schema) {

	let schema: Schema = match read_to_string(filepath) {
		
		Ok(file_content) => match serde_json::from_str::<serde_json::Value>(&file_content) {
			
			Ok(mut schema) => {
				
				if !schema["schema"].is_string() {
					schema["schema"] = serde_json::Value::String(schema["schema"].to_string());
				}

				match serde_json::from_value::<Schema>(schema) {
					
					Ok(schema) => schema,
					
					Err(e) => {

						error!(target: "schemas", "Failed to parse the file at {} to a schema", &filepath.display());
						panic!("{:?}", e);

					}

				}
			},
			
			Err(e) => {
				
				error!(target: "schemas", "Failed to parse the file at {} to a json value", &filepath.display());
				panic!("{:?}", e);

			}

		},
		
		Err(e) => {
			
			error!(target: "schemas", "Failed to open the file at {}", &filepath.display());
			panic!("{:?}", e);

		}

	};

	info!(target: "schemas", "Found schema {} at path {}.", &schema.table_name, &filepath.display());

	(schema.table_name.clone(), schema)

}