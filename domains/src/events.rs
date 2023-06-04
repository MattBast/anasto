use serde_derive::{Deserialize, Serialize};
use std::cmp::Eq;
use time::OffsetDateTime;
use uuid::Uuid;
use std::collections::HashMap;


/// State represents a point in time that tells us how which
/// events have been sent to destinations and which haven't.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub struct State {

    table_name: String,
    value: serde_json::Value,
    #[serde(with="time::serde::rfc3339", default="now_timestamp")]
    created_at: OffsetDateTime,
    #[serde(default="new_uuid")]
    event_id: Uuid

}

/// A statistic giving an aggregated measure of something happening
/// in Anasto such as how many events have been processed.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub struct Metric {
    
    metric: String, 
    value: i32, 
    tags: HashMap<String, String>,
    #[serde(with="time::serde::rfc3339", default="now_timestamp")]
    created_at: OffsetDateTime,
    #[serde(default="new_uuid")]
    event_id: Uuid,

}

/// A command telling Anasto to send the events in a buffer (a type
/// of repository) to all subscribed destinations.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub struct Drain {

    /// The name of the table whose buffer this event needs to drain
    pub table_name: String,
    /// The time and date when this event was first created.
    /// Defaults to time and date it entered Anasto if not provided.
    #[serde(with="time::serde::rfc3339", default="now_timestamp")]
    pub created_at: OffsetDateTime,
    /// The unique identifier of this event.
    #[serde(default="new_uuid")]
    pub event_id: Uuid,

}



fn new_uuid() -> Uuid {
    Uuid::new_v4()
}


fn now_timestamp() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}