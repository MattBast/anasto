//! # Destination Files
//!
//! This module defines the logic for a destination table that writes to a local
//! or remote filesystem.

use serde::*;
use log::info;
use std::io::Error;
use serde_derive::{ Serialize, Deserialize };
use std::path::{PathBuf, Path };
use chrono::{ DateTime, offset::Utc };
use crate::tables::{ FailAction, LakeFileType };
use crate::tables::utils::{
	five_hundred_chars_check, 
	random_table_name, 
	path_dir_check,
	start_of_time_timestamp,
	create_avro_file,
};
use datafusion::prelude::DataFrame;
use datafusion::error::Result;
use convert_case::{ Case, Casing };
use uuid::Uuid;
use std::fs::{ read_dir, copy, create_dir_all, remove_dir_all, remove_file };

/// The DestFile reads files from a local or remote filesystem
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DestFile {
	
	/// A user defined name for the table. This does not need to correlate
	/// with the directory path where the tables files will be written to.
    #[serde(deserialize_with="to_snake_case", default="random_table_name")]
	pub dest_table_name: String,

	/// The name of the source table that supplies this destination table
    #[serde(deserialize_with="five_hundred_chars_check")]
	pub source_table_name: String,

	/// The parent filepath where all data this DestFile handles will be written to
    #[serde(deserialize_with="path_dir_check")]
    pub dirpath: PathBuf,

    /// The type of file that the tables data will be written to.
    #[serde(default)]
    pub filetype: LakeFileType,

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


impl DestFile {

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

	/// Get all the files that have not yet been read, add them to a Dataframe and 
	/// return the frame. If there are no new files, return an empty Dataframe with a false
	/// boolean telling the destinations not to process it.
	pub async fn write_new_data(&mut self, df: DataFrame) -> Result<(), Error> {

		// generate unique name for the new file
		let sub_dirpath = format!("{}/temp_{}", self.dirpath.display(), Uuid::new_v4());

		// make sure the directory for the table exists
		std::fs::create_dir_all(&self.dirpath)?;

		info!(target: &self.dest_table_name, "Writing files to {}.", &sub_dirpath);

		// write the dataframe to file
		match self.filetype {
			LakeFileType::Csv => df.write_csv(&sub_dirpath).await?,
			LakeFileType::Json => df.write_json(&sub_dirpath).await?,
			LakeFileType::Avro => self.write_avro(df, sub_dirpath.clone()).await?,
			LakeFileType::Parquet => df.write_parquet(&sub_dirpath, Default::default()).await?,
		};

		// move the new data files out of their temporary folder
		move_files(sub_dirpath, self.dirpath.clone())?;

        self.bookmark = Utc::now();

		Ok(())

	}

	/// Write the contents of a dataframe to an Avro file
	async fn write_avro(&self, df: DataFrame, sub_dirpath: String) -> Result<(), Error> {
	    
	    let df_schema = df.schema().clone();
		df.write_json(&sub_dirpath).await?;
		create_avro_file(df_schema, &self.dest_table_name, &sub_dirpath)?;

		read_dir(sub_dirpath)?
			.try_for_each(|entry| {

				let path = entry?.path();
				let extension = path.extension().unwrap();
				
				if extension == "json" {
				    remove_file(path)?;
				}

				Ok::<(), Error>(())

			})?;
		
		Ok(())

	}

}

/// Move the new data files out of their temporary folder
fn move_files(src: impl AsRef<Path> + Clone, dest: impl AsRef<Path>) -> Result<(), Error> {
    
    create_dir_all(&dest)?;

    for entry in read_dir(src.clone())? {
        
        let entry = entry?;
        let ty = entry.file_type()?;
        
        if ty.is_dir() {
            move_files(entry.path(), dest.as_ref().join(entry.file_name()))?;
        } else {
            let new_filename = format!("{}.{}", Uuid::new_v4(), entry.path().extension().unwrap().to_str().unwrap());
            let _ = copy(entry.path(), dest.as_ref().join(new_filename))?;
        }

    }

    remove_dir_all(src)?;

    Ok(())

}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::naive::NaiveDate;
	use crate::tables::test_utils::TestDir;
	use datafusion::prelude::SessionContext;


    #[test]
    fn table_from_toml_with_minimal_config() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
			dirpath = "./tests/data/avro_table/"
        "#);

        let table: DestFile = toml::from_str(&content).unwrap();

        assert!(!table.dest_table_name.is_empty());
        assert_eq!(table.source_table_name, "json_table");
        assert_eq!(table.dirpath, PathBuf::from("./tests/data/avro_table/").canonicalize().unwrap());
        assert!(matches!(table.filetype, LakeFileType::Csv));
        assert_eq!(table.bookmark, chrono::DateTime::<Utc>::MIN_UTC);
        assert!(matches!(table.on_fail, FailAction::Stop));

    }

    #[test]
    fn table_from_toml_with_maximum_config() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dest_table_name = "avro_table"
            dirpath = "./tests/data/avro_table/"
            filetype = "avro"
            bookmark = "2023-08-21T00:55:00z"
            on_fail = "skip"
        "#);

        let table: DestFile = toml::from_str(&content).unwrap();

        assert_eq!(table.dest_table_name, "avro_table");
        assert_eq!(table.source_table_name, "json_table");
        assert_eq!(table.dirpath, PathBuf::from("./tests/data/avro_table/").canonicalize().unwrap());
        assert!(matches!(table.filetype, LakeFileType::Avro));
        assert!(matches!(table.on_fail, FailAction::Skip));

        let naivedatetime_utc = NaiveDate::from_ymd_opt(2023, 8, 21).unwrap().and_hms_opt(0, 55, 0).unwrap();
		let datetime_utc = DateTime::<Utc>::from_utc(naivedatetime_utc, Utc);
        assert_eq!(table.bookmark, datetime_utc);

    }

    #[test]
    fn missing_mandatory_field() {
    
        let content = String::from(r#"
            dest_table_name = "avro_table"
            dirpath = "./tests/data/avro_table/"
        "#);

        let table: Result<DestFile, toml::de::Error> = toml::from_str(&content);

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

        let table: Result<DestFile, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "The path: ./tests/data/does_not_exist/ does not exist.", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn dirpath_is_not_a_directory() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dirpath = "./tests/data/avro_table/test.avro" 
        "#);

        let table: Result<DestFile, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "The path: ./tests/data/avro_table/test.avro is not a directory.", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

     #[test]
    fn typo_in_fieldname() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dirpath = "./tests/data/avro_table/"
            filepath = "json"
        "#);

        let table: Result<DestFile, toml::de::Error> = toml::from_str(&content);

        match table {
        	Err(e) => assert_eq!(e.message(), "unknown field `filepath`, expected one of `dest_table_name`, `source_table_name`, `dirpath`, `filetype`, `bookmark`, `on_fail`", "Incorrect error message."),
        	Ok(_) => assert!(false, "Table config parse should have returned an error."),
        }

    }

    #[test]
    fn table_name_contains_too_many_characters() {
    
        let content = String::from(r#"
            source_table_name = "name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name_name"
            dirpath = "./tests/data/avro_table/test.avro" 
        "#);

        let table: Result<DestFile, toml::de::Error> = toml::from_str(&content);

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
            dirpath = "./tests/data/avro_table/"
        "#);

        let table: DestFile = toml::from_str(&content).unwrap();

        assert_eq!(table.src_table_name(), "json_table");

    }

    #[test]
    fn dest_table_name_function_returns_dest_table_name() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dest_table_name = "avro_table"
            dirpath = "./tests/data/avro_table/"
        "#);

        let table: DestFile = toml::from_str(&content).unwrap();

        assert_eq!(table.dest_table_name(), "avro_table");

    }

    #[test]
    fn on_fail_function_returns_fail_action() {
    
        let content = String::from(r#"
            source_table_name = "json_table"
            dirpath = "./tests/data/avro_table/"
        "#);

        let table: DestFile = toml::from_str(&content).unwrap();

        assert!(matches!(table.on_fail(), FailAction::Stop));

    }

    #[tokio::test]
    async fn can_write_csv_table() {
    
    	// create diectory for destination table that tears itself down at the end of the test
        let _test_dir = TestDir::new("./tests/data/test_csv_dest/");

        // create the destination table
        let mut table: DestFile = toml::from_str(&String::from(r#"
            source_table_name = "csv_table"
            dirpath = "./tests/data/test_csv_dest/"
        "#)).unwrap();

        // setup mock source table
        let ctx = SessionContext::new();
        let src_df = ctx.read_csv("./tests/data/csv_table/", Default::default()).await.unwrap();

        // create received source data to a destination
        table.write_new_data(src_df.clone()).await.unwrap();

        // read what was written into a dataframe and compare with the source dataframe
        let dest_df = ctx.read_csv("./tests/data/test_csv_dest/", Default::default()).await.unwrap();
        let src_data = src_df.collect().await.unwrap();
        let dest_data = dest_df.collect().await.unwrap();
        assert_eq!(src_data.len(), dest_data.len());
        assert!(src_data.contains(&dest_data[0]));
        assert!(src_data.contains(&dest_data[1]));

        // make sure the bookmark has progressed
		assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_write_json_table() {
    
    	// create diectory for destinsation table that tears itself down at the end of the test
        let _test_dir = TestDir::new("./tests/data/test_json_dest/");

        // create the destination table
        let mut table: DestFile = toml::from_str(&String::from(r#"
            source_table_name = "csv_table"
            dirpath = "./tests/data/test_json_dest/"
            filetype = "json"
        "#)).unwrap();

        // setup mock source table
        let ctx = SessionContext::new();
        let src_df = ctx.read_csv("./tests/data/csv_table/", Default::default()).await.unwrap();

        // create received source data to a destination
        table.write_new_data(src_df.clone()).await.unwrap();

        // read what was written into a dataframe and compare with the source dataframe
        let dest_df = ctx.read_json("./tests/data/test_json_dest/", Default::default()).await.unwrap();
        let src_data = src_df.collect().await.unwrap();
        let dest_data = dest_df.collect().await.unwrap();
        assert_eq!(src_data.len(), dest_data.len());
        assert!(src_data.contains(&dest_data[0]));
        assert!(src_data.contains(&dest_data[1]));

        // make sure the bookmark has progressed
		assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_write_parquet_table() {
    
    	// create diectory for destinsation table that tears itself down at the end of the test
        let _test_dir = TestDir::new("./tests/data/test_parquet_dest/");

        // create the destination table
        let mut table: DestFile = toml::from_str(&String::from(r#"
            source_table_name = "csv_table"
            dirpath = "./tests/data/test_parquet_dest/"
            filetype = "parquet"
        "#)).unwrap();

        // setup mock source table
        let ctx = SessionContext::new();
        let src_df = ctx.read_csv("./tests/data/csv_table/", Default::default()).await.unwrap();

        // create received source data to a destination
        table.write_new_data(src_df.clone()).await.unwrap();

        // read what was written into a dataframe and compare with the source dataframe
        let dest_df = ctx.read_parquet("./tests/data/test_parquet_dest/", Default::default()).await.unwrap();
        let src_data = src_df.collect().await.unwrap();
        let dest_data = dest_df.collect().await.unwrap();
        assert_eq!(src_data.len(), dest_data.len());
        assert!(src_data.contains(&dest_data[0]));
        assert!(src_data.contains(&dest_data[1]));

        // make sure the bookmark has progressed
		assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_write_avro_table() {
    
    	// create diectory for destinsation table that tears itself down at the end of the test
        let _test_dir = TestDir::new("./tests/data/test_avro_dest/");

        // create the destination table
        let mut table: DestFile = toml::from_str(&String::from(r#"
            source_table_name = "csv_table"
            dirpath = "./tests/data/test_avro_dest/"
            filetype = "avro"
        "#)).unwrap();

        // setup mock source table
        let ctx = SessionContext::new();
        let src_df = ctx.read_csv("./tests/data/csv_table/", Default::default()).await.unwrap();

        // create received source data to a destination
        table.write_new_data(src_df.clone()).await.unwrap();

        // read what was written into a dataframe and compare with the source dataframe
        let dest_df = ctx.read_avro("./tests/data/test_avro_dest/", Default::default()).await.unwrap();
        let dest_data = dest_df.collect().await.unwrap();

        let mut dest_ids: Vec<i64> = dest_data[0]
        	.column_by_name("id")
        	.unwrap()
        	.as_any()
    		.downcast_ref::<arrow_array::array::PrimitiveArray<arrow_array::types::Int64Type>>()
    		.unwrap()
            .values()
            .to_vec();
        dest_ids.sort();

        let values_array = dest_data[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::array::StringArray>()
            .unwrap();
        let mut dest_values = vec![values_array.value(0), values_array.value(1), values_array.value(2)];
        dest_values.sort();

        assert_eq!(dest_ids, &[1,2,3]);
        assert_eq!(dest_values, vec!["hello world", "hey there", "hi"]);

        // make sure the bookmark has progressed
		assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);

    }

    #[tokio::test]
    async fn can_write_two_batches_to_csv_table() {

    	// create diectory for destinsation table that tears itself down at the end of the test
        let _test_dir = TestDir::new("./tests/data/test_multi_csv_dest/");

        // create the destination table
        let mut table: DestFile = toml::from_str(&String::from(r#"
            source_table_name = "csv_table"
            dirpath = "./tests/data/test_multi_csv_dest/"
        "#)).unwrap();

        // setup mock source tables
        let ctx = SessionContext::new();
        let src_df = ctx.read_csv("./tests/data/csv_table/", Default::default()).await.unwrap();
        let src_df_two = ctx.read_csv("./tests/data/csv_table_two/", Default::default()).await.unwrap();

        // create received source data to a destination
        table.write_new_data(src_df.clone()).await.unwrap();
        table.write_new_data(src_df_two.clone()).await.unwrap();

        // read what was written into a dataframe and compare with the source dataframe
        let dest_df = ctx.read_csv("./tests/data/test_multi_csv_dest/", Default::default()).await.unwrap();
        let src_data = src_df.collect().await.unwrap();
        let src_data_two = src_df_two.collect().await.unwrap();
        let dest_data = dest_df.collect().await.unwrap();
        assert_eq!(src_data.len() + src_data_two.len(), dest_data.len());
        assert!(dest_data.contains(&src_data[0]));
        assert!(dest_data.contains(&src_data[1]));
        assert!(dest_data.contains(&src_data_two[0]));

        // make sure the bookmark has progressed
		assert!(table.bookmark > chrono::DateTime::<Utc>::MIN_UTC);
    }

}
