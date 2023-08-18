//! # Utils
//!
//! Re-usable functions that are used across many modules.

use std::path::PathBuf;
use serde::de::Error;
use serde::*;
use std::time::{ SystemTime, UNIX_EPOCH };
use rnglib::{RNG, Language};
use convert_case::{Case, Casing};
use time::OffsetDateTime;


/// Return an error if the string provided is more than 500 characters long
pub fn five_hundred_chars_check<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {

	let s = String::deserialize(d)?;

    if &s.chars().count() > &500 {
        let error_message = format!("The string: {} is longer than 500 chars.", &s);
        return Err(D::Error::custom(error_message));
    }

    Ok(s)

}

/// Generate a random name in snake case
pub fn random_table_name() -> String {
	
	let rng = RNG::try_from(&Language::Fantasy).unwrap();
	rng.generate_name().to_case(Case::Snake)

}

/// Check that the path provided points at a directory
pub fn path_dir_check<'de, D: Deserializer<'de>>(d: D) -> Result<PathBuf, D::Error> {

	let s = String::deserialize(d)?;
	let dirpath = PathBuf::from(&s);

    if !dirpath.exists() {
        let error_message = format!("The path: {} does not exist.", &s);
        return Err(D::Error::custom(error_message));
    }

    if !dirpath.is_dir() {
        let error_message = format!("The path: {} is not a directory.", &s);
        return Err(D::Error::custom(error_message));
    }

    Ok(dirpath.canonicalize().unwrap())

}

/// Return the timestamp “1970-01-01 00:00:00 UTC”
pub fn start_of_time_timestamp() -> SystemTime {
	UNIX_EPOCH
}

/// Returns 10 seconds as 10,000 milliseconds
pub fn ten_secs_as_millis() -> u64 {
	10_000
}

/// Convert a system time type into a string that can be printed
pub fn system_time_to_string<T>(dt: T) -> String
   where T: Into<OffsetDateTime>
{
    dt.into().to_string()
}