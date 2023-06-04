use serde_derive::{Deserialize, Serialize};
use serde::de::Error;
use serde::*;

/// The body of the POST request handled by the /record endpoint
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub struct RecordRequest {

    /// The name of the table this record belongs to.
    pub table_name: String,
    
    /// The key value pairs of the record.
    pub record: serde_json::Value,
    
    /// Whether this event represents the event getting 
    /// created, updated or deleted
    #[serde(default="create_operation", deserialize_with="valid_operation")]
    pub operation: String,

}

/// If the operation isn't included, default it to "CREATE"
fn create_operation() -> String {
    "CREATE".to_string()
}


/// Make sure the operation field is either ["CREATE", "UPDATE", "DELETE"]
fn valid_operation<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    
    let mut operation = String::deserialize(d)?;
    operation.make_ascii_uppercase();
    
    if !["CREATE".to_owned(), "UPDATE".to_owned(), "DELETE".to_owned()].contains(&operation) {
        return Err(D::Error::custom("The operation was not one of: ['CREATE', 'UPDATE', 'DELETE']"));
    }

    Ok(operation)

}