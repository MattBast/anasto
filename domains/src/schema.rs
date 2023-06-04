use serde::de::Error;
use serde::*;
use serde_derive::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;
use apache_avro::schema::Schema as AvroSchema;


/// A schema defines the key names and data types of the values
/// in a Record.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all="snake_case")]
pub struct Schema {

    /// The name of the table this schema defines
    pub table_name: String,
    /// A JSON schema compatible description of a record event type
    #[serde(deserialize_with="compile_schema")]
    pub schema: AvroSchema,
    /// Will always equal "SCHEMA".
    #[serde(default="schema_type", deserialize_with="valid_schema_type")]
    pub event_type: String,
    /// The time and date when this event was first created.
    /// Defaults to time and date it entered Anasto if not provided.
    #[serde(with="time::serde::rfc3339", default="now_timestamp")]
    pub created_at: OffsetDateTime,
    /// The field names that make up the records primary key
    #[serde(default)]
    pub key_properties: Vec<String>,
    /// The unique identifier of this event.
    #[serde(default="new_uuid")]
    pub event_id: Uuid,
    /// Whether this event represents the event getting 
    /// created, updated or deleted
    #[serde(default="create_operation", deserialize_with="ensure_valid_operation")]
    pub operation: String,

}

// enable default values when declaring a schema
impl Default for Schema {
    
    fn default() -> Schema {
        
        let null_avro_schema = r#"{"type": "null"}"#;

        Schema {
            table_name: "default".to_string(),
            schema: AvroSchema::parse_str(null_avro_schema).unwrap(),
            event_type: "SCHEMA".to_string(),
            created_at: OffsetDateTime::now_utc(),
            key_properties: Vec::new(),
            event_id: Uuid::new_v4(),
            operation: "CREATE".to_string()
        }

    }

}


fn compile_schema<'de, D: Deserializer<'de>>(d: D) -> Result<AvroSchema, D::Error> {
    
    let s = String::deserialize(d)?;
    AvroSchema::parse_str(&s[..]).map_err(de::Error::custom)

}


fn schema_type() -> String {
    
    "SCHEMA".to_string()

}

fn valid_schema_type<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    
    let mut event_type = String::deserialize(d)?;
    event_type.make_ascii_uppercase();
    
    if event_type != *"SCHEMA" {
        return Err(D::Error::custom("The event_type must equal \"SCHEMA\""));
    }

    Ok(event_type)

}


fn now_timestamp() -> OffsetDateTime {
    
    OffsetDateTime::now_utc()

}


fn new_uuid() -> Uuid {
    
    Uuid::new_v4()

}


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

