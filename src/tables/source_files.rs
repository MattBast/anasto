//! # SourceFiles
//!
//! This module defines the logic for a source table read from a local or remote
//! filesystem. It works by polling a specified file directory and reading all
//! the files that have been created or modified since the Table last read them. 
//! A timestamp bookmark is kept and compared against a files `last modified`
//! timestamp to track what files need to be read per poll.

use log::{ info, warn };
use serde_derive::{ Serialize, Deserialize };
use std::path::PathBuf;
use chrono::{ DateTime, offset::Utc };
use std::time::Duration;
use crate::tables::{ FailAction, LakeFileType };
use crate::tables::utils::{
	five_hundred_chars_check, 
	random_table_name, 
	path_dir_check,
	ten_secs_as_millis,
};
use datafusion::prelude::{ SessionContext, DataFrame };
use datafusion::error::Result;
use std::fs::read_dir;

/// The SourceFiles reads files from a local or remote filesystem
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SourceFiles {
	
	/// A user defined name for the table. This does not need to correlate
	/// with the directory path where the tables files are stored.
    #[serde(deserialize_with="five_hundred_chars_check", default="random_table_name")]
	pub table_name: String,

	/// The parent filepath where all data this SourceFiles handles will be written to
    #[serde(deserialize_with="path_dir_check")]
    pub dirpath: PathBuf,

    /// The type of file that the tables data is stored in. The table cannot be stored
    /// in multiple filetypes.
    #[serde(default)]
    pub filetype: LakeFileType,

    /// Tracks which files have been read using their created timestamp
    #[serde(default="start_of_time_timestamp")]
    pub bookmark: DateTime<Utc>,

    /// Optional field. Determines how frequently new data will be written to the destination. Provided in milliseconds.
    #[serde(default="ten_secs_as_millis")]
    pub poll_interval: u64,

    /// Optional field. Decide what to do when new data fails to be written to a destination.
    #[serde(default)]
    pub on_fail: FailAction,

}


/// Return the timestamp “1970-01-01 00:00:00 UTC”
pub fn start_of_time_timestamp() -> DateTime<Utc> {
	chrono::DateTime::<Utc>::MIN_UTC
}


impl SourceFiles {

	/// Getter function for returning the name of the table this table is reading from
	pub fn table_name(&self) -> &String {
		&self.table_name
	}

	/// Getter function for returning what to do if this table fails to read new data
	pub fn on_fail(&self) -> FailAction {
		self.on_fail
	}

	/// A getter function for getting how often the source dataset should be read
	pub fn poll_interval(&self) -> Duration {
		Duration::from_millis(self.poll_interval)
	}

	/// Get all the files that have not yet been read, add them to a Dataframe and 
	/// return the frame. If there are no new files, return an empty Dataframe with a false
	/// boolean telling the destinations not to process it.
	pub async fn read_new_data(&mut self) -> Result<(bool, DataFrame), std::io::Error> {

		let ctx = SessionContext::new();

		info!(target: &self.table_name, "Reading files from {} created before {:?}.", self.dirpath.display(), self.bookmark);

		// get all new or newly modified files
		let file_paths = self.get_unread_filepaths()?;
		
		// if there are no files to read, don't waste time trying to read them
		if file_paths.is_empty() { return Ok((false, ctx.read_empty().unwrap())) };

		// read the files into a dataframe (table)
        let df = self.read_files(&ctx, file_paths).await?;

        // update the bookmark so the same files aren't read twice
        self.bookmark = Utc::now();

		Ok((true, df))

	}

	/// Get a list of files that have been created or modified since they were last read
	fn get_unread_filepaths(&self) -> Result<Vec<String>, std::io::Error> {

		let mut paths = Vec::new();
		
		for entry in (read_dir(self.dirpath.clone())?).flatten() {
    
            if let Ok(metadata) = entry.metadata() {
                
                let file_created: DateTime<Utc> = metadata.created().unwrap().into();

                if self.bookmark < file_created {
                	paths.push(entry.path().into_os_string().into_string().unwrap())
                }

            } else {
                
                warn!("Skipping file {:?} as couldn't get metadata from it", entry.path());

            }

		}

        Ok(paths)

	}

	/// Read all files in a list of filepaths into a datafusion dataframe
	async fn read_files(&self, ctx: &SessionContext, paths: Vec<String>) -> Result<DataFrame, std::io::Error> {
		
		info!(target: &self.table_name, "Reading {} files.", paths.len());

		// create the dataframe
		let df = match self.filetype {
			LakeFileType::Csv => ctx.read_csv(paths, Default::default()).await?,
			LakeFileType::Json => ctx.read_json(paths, Default::default()).await?,
			LakeFileType::Avro => ctx.read_avro(paths, Default::default()).await?,
			LakeFileType::Parquet => ctx.read_parquet(paths, Default::default()).await?,
		};

		Ok(df)

	}

}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Arc;
	use std::matches;
	use arrow_array::{ RecordBatch, StringArray, Int64Array, Int32Array };
	use arrow_schema::{ Schema, Field, DataType };
	use chrono::naive::NaiveDate;
	

    #[test]
    fn table_from_toml_with_minimal_config() {
    
        let content = String::from(r#"
            table_name = "csv_table"
            dirpath = "./tests/data/csv_table/" 
        "#);

        let table: SourceFiles = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name, "csv_table");
        assert_eq!(table.dirpath, PathBuf::from("./tests/data/csv_table/").canonicalize().unwrap());
        assert!(matches!(table.filetype, LakeFileType::Csv));
        assert_eq!(table.bookmark, chrono::DateTime::<Utc>::MIN_UTC);
        assert_eq!(table.poll_interval, 10_000);
        assert!(matches!(table.on_fail, FailAction::Stop));

    }

    #[test]
    fn table_from_toml_with_maximum_config() {
    
        let content = String::from(r#"
            table_name = "csv_table"
            dirpath = "./tests/data/csv_table/" 
            filetype = "csv"
            bookmark = "2023-08-21T00:55:00z"
            poll_interval = 5000
            on_fail = "skip"
        "#);

        let table: SourceFiles = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name, "csv_table");
        assert_eq!(table.dirpath, PathBuf::from("./tests/data/csv_table/").canonicalize().unwrap());
        assert!(matches!(table.filetype, LakeFileType::Csv));
        assert_eq!(table.poll_interval, 5000);
        assert!(matches!(table.on_fail, FailAction::Skip));

        let naivedatetime_utc = NaiveDate::from_ymd_opt(2023, 8, 21).unwrap().and_hms_opt(0, 55, 0).unwrap();
		let datetime_utc = DateTime::<Utc>::from_utc(naivedatetime_utc, Utc);
        assert_eq!(table.bookmark, datetime_utc);

    }

    #[test]
    fn missing_mandatory_field() {
    
        let content = String::from(r#"
            table_name = "csv_table"
        "#);

        let table: Result<SourceFiles, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "missing field `dirpath`", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn dirpath_does_not_exist() {
    
        let content = String::from(r#"
            dirpath = "./tests/data/does_not_exist/" 
        "#);

        let table: Result<SourceFiles, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "The path: ./tests/data/does_not_exist/ does not exist.", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn dirpath_is_not_a_directory() {
    
        let content = String::from(r#"
            dirpath = "./tests/data/csv_table/test_1.csv" 
        "#);

        let table: Result<SourceFiles, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "The path: ./tests/data/csv_table/test_1.csv is not a directory.", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

     #[test]
    fn typo_in_fieldname() {
    
        let content = String::from(r#"
            dirpath = "./tests/data/json_table/"
            filepath = "json"
        "#);

        let table: Result<SourceFiles, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "unknown field `filepath`, expected one of `table_name`, `dirpath`, `filetype`, `bookmark`, `poll_interval`, `on_fail`", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn table_name_contains_too_many_characters() {
    
        let content = String::from(r#"
            table_name = "name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name"
            dirpath = "./tests/data/csv_table/test_1.csv" 
        "#);

        let table: Result<SourceFiles, toml::de::Error> = toml::from_str(&content);

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
    fn table_name_function_returns_table_name() {
    
        let content = String::from(r#"
            table_name = "csv_table"
            dirpath = "./tests/data/csv_table/" 
        "#);

        let table: SourceFiles = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name(), "csv_table");

    }

    #[test]
    fn on_fail_function_returns_fail_action() {
    
        let content = String::from(r#"
            dirpath = "./tests/data/csv_table/" 
        "#);

        let table: SourceFiles = toml::from_str(&content).unwrap();

        assert!(matches!(table.on_fail(), FailAction::Stop));

    }

    #[test]
    fn poll_interval_function_returns_correct_duration() {
    
        let content = String::from(r#"
            dirpath = "./tests/data/csv_table/" 
        "#);

        let table: SourceFiles = toml::from_str(&content).unwrap();

        assert_eq!(table.poll_interval(), Duration::from_millis(10_000));

    }

    #[tokio::test]
    async fn can_read_csv_table() {
    
    	// define table config (mostly default config pointing at data in csv files)
        let content = String::from(r#"
            dirpath = "./tests/data/csv_table/" 
        "#);

        // create the table and read its data as a dataframe
        let mut table: SourceFiles = toml::from_str(&content).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        // create the expected contents of the dataframe (as record batches)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let batch_one = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["hello world", "hey there"])),
            ],
        ).unwrap();

        let batch_two = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![3])),
                Arc::new(StringArray::from(vec!["hi"])),
            ],
        ).unwrap();

        assert!(read_success);
        assert_eq!(df_data[0], batch_one);
        assert_eq!(df_data[1], batch_two);
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_read_json_table() {
    
    	// define table config (mostly default config pointing at data in csv files)
        let content = String::from(r#"
            dirpath = "./tests/data/json_table/" 
            filetype = "json"
        "#);

        // create the table and read its data as a dataframe
        let mut table: SourceFiles = toml::from_str(&content).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        // create the expected contents of the dataframe (as record batches)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let batch_one = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["hello world", "hey there"])),
            ],
        ).unwrap();

        let batch_two = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![3])),
                Arc::new(StringArray::from(vec!["hi"])),
            ],
        ).unwrap();

        assert!(read_success);
        assert_eq!(df_data[0], batch_one);
        assert_eq!(df_data[1], batch_two);

    }

    #[tokio::test]
    async fn can_read_avro_table() {
    
    	// define table config (mostly default config pointing at data in csv files)
        let content = String::from(r#"
            dirpath = "./tests/data/avro_table/" 
            filetype = "avro"
        "#);

        // create the table and read its data as a dataframe
        let mut table: SourceFiles = toml::from_str(&content).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        // create the expected contents of the dataframe (as record batches)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch_one = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["hello world", "hey there", "hi"])),
            ],
        ).unwrap();

        assert!(read_success);
        assert_eq!(df_data[0], batch_one);

    }

    #[tokio::test]
    async fn can_read_parquet_table() {
    
    	// define table config (mostly default config pointing at data in csv files)
        let content = String::from(r#"
            dirpath = "./tests/data/parquet_table/" 
            filetype = "parquet"
        "#);

        // create the table and read its data as a dataframe
        let mut table: SourceFiles = toml::from_str(&content).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        // create the expected contents of the dataframe (as record batches)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let batch_one = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["hello world", "hey there"])),
            ],
        ).unwrap();

        let batch_two = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![3])),
                Arc::new(StringArray::from(vec!["hi"])),
            ],
        ).unwrap();

        assert!(read_success);
        assert_eq!(df_data[1], batch_one);
        assert_eq!(df_data[0], batch_two);

    }

}
