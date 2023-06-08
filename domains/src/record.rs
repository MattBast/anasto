use serde_derive::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;
use apache_avro::{
    // to_value, 
    schema::Schema as AvroSchema, 
    types::Value as AvroValue
};


/// A record is a data change event representing a row of data
/// getting created, updated or deleted.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub struct Record {

    /// The name of the table this record belongs to.
    table_name: String,
    
    /// Will always equal "RECORD".
    event_type: String,
    
    /// The key value pairs of the record.
    record: serde_json::Value,

    /// The schema this record abides by in an Avro String format
    raw_schema: String,
    
    /// Whether this event represents the event getting 
    /// created, updated or deleted
    operation: String,
    
    /// The time and date when this event was first created.
    /// Defaults to time and date it entered Anasto if not provided.
    created_at: OffsetDateTime,
    
    /// The unique identifier of this event.
    event_id: Uuid,

}

impl Record {

    /// Create a new Record. Checks that the record matches a schema
    /// during creation.
    pub fn new(
        table_name: &str,
        record: serde_json::Value,
        schema: &AvroSchema,
        operation: String
    ) -> Result<Record, String> {

        // parse record to Avro
        let avro_value = json_to_avro(&record);

        // let avro_value = to_value(&record).unwrap();

        // validate the record against the schema
        match avro_value.resolve(schema) {
            
            // return a new Anasto Record type if it validates against the schema
            Ok(_) => Ok(Record {
                table_name: table_name.to_string(),
                event_type: "RECORD".to_string(),
                record,
                raw_schema: schema.canonical_form(),
                operation: valid_operation(operation)?,
                created_at: OffsetDateTime::now_utc(),
                event_id: Uuid::new_v4()
            }),

            // return an error containing what's wrong with the record if it is invalid
            Err(e) => Err(e.to_string())

        }

    }

    /// Getter for the table_name field
    pub fn get_name(&self) -> String {

        self.table_name.clone()

    }

    /// Getter for the type field
    pub fn get_type(&self) -> String {

        self.event_type.clone()

    }

    /// Getter for the record field
    pub fn get_record(&self) -> serde_json::Value {

        self.record.clone()

    }

    /// Getter for the record field converted to Avro format
    pub fn get_avro_record(&self) -> apache_avro::types::Value {

        json_to_avro(&self.record)

    }

    /// Getter for the record field
    pub fn get_raw_schema(&self) -> String {

        self.raw_schema.clone()

    }

    /// Getter for the operation field
    pub fn get_operation(&self) -> String {

        self.operation.clone()

    }

    /// Getter for the created_at field
    pub fn get_created_at(&self) -> OffsetDateTime {

        self.created_at

    }

    /// Getter for the event_id field
    pub fn get_id(&self) -> Uuid {

        self.event_id

    }

}


fn valid_operation(mut operation: String) -> Result<String, String> {
    
    operation.make_ascii_uppercase();
    
    if !["CREATE".to_owned(), "UPDATE".to_owned(), "DELETE".to_owned()].contains(&operation) {
        return Err("The operation was not one of: ['CREATE', 'UPDATE', 'DELETE']".to_string());
    }

    Ok(operation)

}

fn json_to_avro(json: &serde_json::Value) -> AvroValue {

    match json {
        serde_json::Value::Null => AvroValue::Null,
        serde_json::Value::Bool(value) => AvroValue::Boolean(*value),
        serde_json::Value::Number(value) => {

            if value.is_f64() {     
                
                AvroValue::Double(value.as_f64().unwrap())

            } 
            else if value.is_i64() {
                
                AvroValue::Long(value.as_i64().unwrap())

            }
            else {

                AvroValue::Double(value.as_f64().unwrap())

            }
        },
        serde_json::Value::String(value) => AvroValue::String(value.to_string()),
        serde_json::Value::Array(value) => {
            
            let avro_value_array: Vec<AvroValue> = value
                .iter()
                .map(json_to_avro)
                .collect();

            AvroValue::Array(avro_value_array)

        },
        serde_json::Value::Object(value) => {
            
            let key_value_vec: Vec<(String, AvroValue)> = value
                .into_iter()
                .map(|(key, value)| (key.to_string(), json_to_avro(value)))
                .collect();

            AvroValue::Record(key_value_vec)

        }
    }

}