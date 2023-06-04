use serde::de::Error;
use serde::*;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::mpsc;
use crate::record::Record;
use std::collections::HashMap;
use time::OffsetDateTime;
use uuid::Uuid;

/// A subscriber profile including a channel to send events to and a 
/// state to track what events they currently have
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all="snake_case")]
pub struct Subscriber {

    /// An MPSC channel that receives a copy of all events for this subscriber
    /// to receive.
    #[serde(skip)]
    pub sender: Option<mpsc::UnboundedSender<Vec<Record>>>,
    
    /// A HashMap detailing all the tables this subscriber has been listening to
    /// and which events it is yet to process.
    #[serde(default)]
    pub state: HashMap<String, std::time::SystemTime>,
    
    /// Whether this event represents the event getting 
    /// created, updated or deleted
    #[serde(default="create_operation", deserialize_with="ensure_valid_operation")]
    pub operation: String,
    
    /// The time and date when this event was first created.
    /// Defaults to time and date it entered Anasto if not provided.
    #[serde(with="time::serde::rfc3339", default="now_timestamp")]
    pub created_at: OffsetDateTime,
    
    /// The unique identifier of the subscriber this event relates to.
    #[serde(default="new_uuid")]
    pub sub_id: Uuid,

}


impl PartialEq for Subscriber {
    
    fn eq(&self, other: &Self) -> bool {
        self.sub_id == other.sub_id
    }

}


impl Eq for Subscriber {}


fn create_operation() -> String {
    "CREATE".to_string()
}


fn ensure_valid_operation<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    
    let mut operation = String::deserialize(d)?;
    operation.make_ascii_uppercase();
    
    if !["CREATE".to_owned(), "UPDATE".to_owned(), "DELETE".to_owned()].contains(&operation) {
        return Err(D::Error::custom("The operation was not one of: ['CREATE', 'UPDATE', 'DELETE']"));
    }

    Ok(operation)

}


fn now_timestamp() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}


fn new_uuid() -> Uuid {
    Uuid::new_v4()
}