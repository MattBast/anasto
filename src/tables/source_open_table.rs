//! # SourceOpenTable
//!
//! This module defines the logic for a source table read from an open table
//! database like Delta Lake.

use log::{ info, warn };
use serde_derive::{ Serialize, Deserialize };
use std::path::PathBuf;
use std::path::Path;
use chrono::{ DateTime, offset::Utc };
use std::time::Duration;
use crate::tables::{ FailAction, OpenTableFormat };
use crate::tables::utils::{
	five_hundred_chars_check, 
	random_table_name, 
	path_dir_check,
	start_of_time_timestamp,
	ten_secs_as_millis,
};

use std::sync::Arc;
use datafusion::prelude::{ SessionContext, DataFrame };
use datafusion::error::Result;
use datafusion::logical_expr::{ col, replace, Expr };
use datafusion::scalar::ScalarValue;


/// The SourceOpenTable reads files from a local or remote filesystem
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SourceOpenTable {
	
	/// A user defined name for the table. This does not need to correlate
	/// with the directory path where the tables files are stored.
    #[serde(deserialize_with="five_hundred_chars_check", default="random_table_name")]
	pub table_name: String,

	/// The parent filepath where all data this SourceOpenTable handles will be written to
    #[serde(deserialize_with="path_dir_check")]
    pub dirpath: PathBuf,

    /// The open table format the data is stored in
    #[serde(default)]
    pub format: OpenTableFormat,

    /// Tracks which files have been read using their created timestamp
    #[serde(default="start_of_time_timestamp")]
    pub bookmark: DateTime<Utc>,

    /// Optional field. Determines how frequently new data will be written to the destination. Provided in milliseconds.
    #[serde(default="ten_secs_as_millis")]
    pub poll_interval: u64,

    /// Optional field. Decide what to do when new data fails to be written to a destination.
    #[serde(default)]
    pub on_fail: FailAction,

    /// Private field. Tracks whether this table has done it's first read
    #[serde(default)]
    first_read: bool

}


impl SourceOpenTable {

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

	/// Get all the data that has not yet been read, add it to a Dataframe and 
	/// return the frame. If there is no new data, return an empty Dataframe with a false
	/// boolean telling the destinations not to process it.
	pub async fn read_new_data(&mut self) -> Result<(bool, DataFrame), std::io::Error> {

		let ctx = SessionContext::new();

		info!(target: &self.table_name, "Reading data created before {:?}.", self.bookmark);

		// read the files into a dataframe
        let df = self.read_table(&ctx).await?;

        // update the bookmark so the same files aren't read twice
        self.bookmark = Utc::now();

		Ok(df)

	}

	/// Decide which format of open table to read from. Run the specified read 
	/// function and return a dataframe.
	async fn read_table(&self, ctx: &SessionContext) -> Result<(bool, DataFrame), std::io::Error> {
		
		let df = match self.format {
			OpenTableFormat::DeltaLake => self.read_delta(ctx).await?,
		};

		Ok(df)

	}

	/// Decide which format of open table to read from. Run the specified read 
	/// function and return a dataframe.
	async fn read_delta(&self, ctx: &SessionContext) -> Result<(bool, DataFrame), std::io::Error> {
		
		match self.first_read {
			false => self.delta_first_read(ctx).await,
			true => self.delta_read_cdc(ctx).await,
		}

	}

	/// Read all rows from the latest version of a delta table and return as a dataframe
	async fn delta_first_read(&self, ctx: &SessionContext) -> Result<(bool, DataFrame), std::io::Error> {
		
		// read the table
		let table = deltalake::open_table(self.dirpath.to_str().unwrap()).await.unwrap();
	    let df = ctx.read_table(Arc::new(table)).unwrap();

	    // add mock cdc columns
	    let df = df
	    	.with_column("_change_type", Expr::Literal("insert".into()))
	    	.unwrap()
	    	.with_column("_commit_timestamp", self.dataframe_bookmark())
	    	.unwrap();

	    Ok((true, df))
	}

	/// Read all the cdc files that have not yet been read and return them as a dataframe
	async fn delta_read_cdc(&self, ctx: &SessionContext) -> Result<(bool, DataFrame), std::io::Error> {
		
		// get a list of the cdc files that have not yet been read
	    let new_cdc_paths = self.get_unread_cdc_filepaths()?;
	    if new_cdc_paths.is_empty()  { return Ok((false, ctx.read_empty().unwrap())) };

	    // read new cdc files into a dataframe
	    let cdc_df = ctx.read_parquet(new_cdc_paths, Default::default()).await?;
	    
	    // filter out the "update_preimage" changes, change the "update_postimage"
	    // change type to "update" and add a commit timestamp if it doesn't already
	    // exist
	    let cdc_df = cdc_df
	    	.filter(col("_change_type").not_eq(Expr::Literal("update_preimage".into())))?
	    	.with_column("_change_type", replace(col("_change_type"), Expr::Literal("update_postimage".into()), Expr::Literal("update".into())))
	    	.unwrap()
	    	.with_column("_commit_timestamp", self.dataframe_bookmark())
	    	.unwrap();

	    Ok((true, cdc_df))

	}

	/// Return the tables bookmark as a DataFusion Dataframe timestamp
	fn dataframe_bookmark(&self) -> Expr {
		
		let timestamp_seconds: i64 = (self.bookmark - chrono::DateTime::<Utc>::MIN_UTC).num_seconds();
		Expr::Literal(ScalarValue::TimestampSecond(Some(timestamp_seconds), None))

	}

	/// Get a list of files that have been created or modified since they were last read
	fn get_unread_cdc_filepaths(&self) -> Result<Vec<String>, std::io::Error> {

		let mut paths = Vec::new();
		let cdc_path = format!("{}/{}", self.dirpath.display(), "_change_data");

		if !Path::new(&cdc_path).exists() { 
			
			warn!("Could not find a change data log at the path {:?}. No change data will be read until this path is found.", cdc_path);
			return Ok(paths) 

		}
		
		for entry in (std::fs::read_dir(cdc_path)?).flatten() {
    
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

}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Arc;
	use std::matches;
	use arrow_array::{ RecordBatch, StringArray, Int32Array, PrimitiveArray, types::TimestampSecondType };
	use arrow_schema::{ Schema, Field, DataType, TimeUnit };
	use chrono::naive::NaiveDate;
	

    #[test]
    fn table_from_toml_with_minimal_config() {
    
        let content = String::from(r#"
            table_name = "delta_table"
            dirpath = "./tests/data/delta_table/" 
        "#);

        let table: SourceOpenTable = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name, "delta_table");
        assert_eq!(table.dirpath, PathBuf::from("./tests/data/delta_table/").canonicalize().unwrap());
        assert!(matches!(table.format, OpenTableFormat::DeltaLake));
        assert_eq!(table.bookmark, chrono::DateTime::<Utc>::MIN_UTC);
        assert_eq!(table.poll_interval, 10_000);
        assert!(matches!(table.on_fail, FailAction::Stop));

    }

    #[test]
    fn table_from_toml_with_maximum_config() {
    
        let content = String::from(r#"
            table_name = "delta_table"
            dirpath = "./tests/data/delta_table/" 
            format = "delta_lake"
            bookmark = "2023-08-21T00:55:00z"
            poll_interval = 5000
            on_fail = "skip"
        "#);

        let table: SourceOpenTable = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name, "delta_table");
        assert_eq!(table.dirpath, PathBuf::from("./tests/data/delta_table/").canonicalize().unwrap());
        assert!(matches!(table.format, OpenTableFormat::DeltaLake));
        assert_eq!(table.poll_interval, 5000);
        assert!(matches!(table.on_fail, FailAction::Skip));

        let naivedatetime_utc = NaiveDate::from_ymd_opt(2023, 8, 21).unwrap().and_hms_opt(0, 55, 0).unwrap();
		let datetime_utc = DateTime::<Utc>::from_utc(naivedatetime_utc, Utc);
        assert_eq!(table.bookmark, datetime_utc);

    }

    #[test]
    fn missing_mandatory_field() {
    
        let content = String::from(r#"
            table_name = "delta_table"
        "#);

        let table: Result<SourceOpenTable, toml::de::Error> = toml::from_str(&content);

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

        let table: Result<SourceOpenTable, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "The path: ./tests/data/does_not_exist/ does not exist.", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn dirpath_is_not_a_directory() {
    
        let content = String::from(r#"
            dirpath = "./tests/data/delta_table/part-00000-7444aec4-710a-4a4c-8abe-3323499043e9.c000.snappy.parquet" 
        "#);

        let table: Result<SourceOpenTable, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "The path: ./tests/data/delta_table/part-00000-7444aec4-710a-4a4c-8abe-3323499043e9.c000.snappy.parquet is not a directory.", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

     #[test]
    fn typo_in_fieldname() {
    
        let content = String::from(r#"
            dirpath = "./tests/data/json_table/"
            filetype = "parquet"
        "#);

        let table: Result<SourceOpenTable, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "unknown field `filetype`, expected one of `table_name`, `dirpath`, `format`, `bookmark`, `poll_interval`, `on_fail`, `first_read`", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn table_name_contains_too_many_characters() {
    
        let content = String::from(r#"
            table_name = "name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name"
            dirpath = "./tests/data/delta_table/" 
        "#);

        let table: Result<SourceOpenTable, toml::de::Error> = toml::from_str(&content);

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
            table_name = "delta_table"
            dirpath = "./tests/data/delta_table/" 
        "#);

        let table: SourceOpenTable = toml::from_str(&content).unwrap();

        assert_eq!(table.table_name(), "delta_table");

    }

    #[test]
    fn on_fail_function_returns_fail_action() {
    
        let content = String::from(r#"
            dirpath = "./tests/data/delta_table/" 
        "#);

        let table: SourceOpenTable = toml::from_str(&content).unwrap();

        assert!(matches!(table.on_fail(), FailAction::Stop));

    }

    #[test]
    fn poll_interval_function_returns_correct_duration() {
    
        let content = String::from(r#"
            dirpath = "./tests/data/delta_table/" 
        "#);

        let table: SourceOpenTable = toml::from_str(&content).unwrap();

        assert_eq!(table.poll_interval(), Duration::from_millis(10_000));

    }

    #[tokio::test]
    async fn can_read_delta_table() {
    
    	// define table config (mostly default config pointing at data in csv files)
        let content = String::from(r#"
            dirpath = "./tests/data/delta_table/" 
        "#);

        // create the table and read its data as a dataframe
        let mut table: SourceOpenTable = toml::from_str(&content).unwrap();
        let (read_success, df) = table.read_new_data().await.unwrap();
        let df_data = df.collect().await.unwrap();

        // create the expected contents of the dataframe (as record batches)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("_change_type", DataType::Utf8, false),
            Field::new("_commit_timestamp", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(StringArray::from(vec!["Mino"])),
                Arc::new(StringArray::from(vec!["insert"])),
                Arc::new(PrimitiveArray::<TimestampSecondType>::from(vec![0])),
            ],
        ).unwrap();

        assert!(read_success);
        assert_eq!(df_data[0], batch);
        assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

}
