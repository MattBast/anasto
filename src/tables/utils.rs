//! # Utils
//!
//! Re-usable functions that are used across many modules.

use std::path::PathBuf;
use serde::de::Error as SerdeError;
use std::io::{Error, ErrorKind};
use serde::*;
use chrono::{ DateTime, TimeZone, offset::Utc };
use rnglib::{RNG, Language};
use convert_case::{Case, Casing};
use reqwest::{ 
    Url, 
    header::HeaderMap 
};
use std::collections::HashMap;
use http::Error as HttpError;
use std::time::Duration;

// Datafusion to Avro conversion crates
use datafusion::common::DFSchema;
use arrow_schema::{ Field, Fields, Schema, DataType as ArrowDataType };
use apache_avro::types::Value as AvroValue;
use apache_avro::schema::{
    Schema as AvroSchema,
    Name as AvroSchemaName,
    RecordSchema as AvroRecordSchema,
    RecordField as AvroRecordField,
    RecordFieldOrder,
    DecimalSchema as AvroDecimalSchema,
    UnionSchema as AvroUnionSchema
};
use apache_avro::{ Writer as AvroWriter, Codec };
use uuid::Uuid;
use std::collections::BTreeMap;

// JSON to Datafusion schema crates
use serde_json::Value;
use std::sync::Arc;


// **************************************************************************************
// Config check code
// ************************************************************************************** 

/// Return an error if the string provided is more than 500 characters long
pub fn five_hundred_chars_check<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {

	let s = String::deserialize(d)?;

    if s.chars().count() > 500 {
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
pub fn start_of_time_timestamp() -> DateTime<Utc> {
    chrono::Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap()
}

/// Returns 10 seconds as 10,000 milliseconds
pub fn ten_secs_as_millis() -> u64 {
	10_000
}

/// Parse a Url type as a str
pub fn serialize_url<S>(url: &Url, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(url.as_str())
}

/// Make sure that string is a valid url
pub fn deserialize_url<'de, D: Deserializer<'de>>(d: D) -> Result<Url, D::Error> {

    let s = String::deserialize(d)?;
    let url_result = Url::parse(&s);

    match url_result {
        Ok(url) => Ok(url),
        Err(_e) => {
            let error_message = format!("The string '{}' is not a url.", &s);
            Err(D::Error::custom(error_message))
        }
    }

}

/// Parse an Arrow Schema type as a String
pub fn serialize_schema<S>(schema: &Option<Schema>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match schema {
        Some(schema) => serializer.serialize_str(&schema.to_string()),
        None => serializer.serialize_str("")
    }
}

/// Make sure that headers are valid
pub fn deserialize_header_map<'de, D: Deserializer<'de>>(d: D) -> Result<HeaderMap, D::Error> {

    let headers: Vec<(String, String)> = Vec::deserialize(d)?;

    let mut map: HashMap<String, String> = HashMap::new();

    headers.iter().try_for_each(|(key, value)| { 
        let _ = map.insert(key.to_string(), value.to_string()); 
        Ok(())
    })?;

    let header_map: Result<HeaderMap, HttpError> = (&map).try_into();

    match header_map {
        Ok(header_map) => Ok(header_map),
        Err(e) => Err(D::Error::custom(e.to_string()))
    }

}

/// Parse a HeaderMap as a vector of string tuples
pub fn serialize_header_map<S>(header_map: &HeaderMap, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{    
    let raw_headers: Vec<String> = header_map.iter().map(|(key, value)| format!("({}{})", key.as_str(), value.to_str().unwrap())).collect();
    serializer.serialize_str(&raw_headers.join(","))
}

/// Parse number as a duration
pub fn deserialize_duration<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {

    let secs: u64 = match u64::deserialize(d) {
        Ok(secs) => secs,
        Err(e) => return Err(D::Error::custom(e.to_string()))
    };
    Ok(Some(Duration::from_secs(secs)))

}

/// Parse a Duration into a u64 type
pub fn serialize_duration<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{    
    match duration {
        Some(duration) =>  serializer.serialize_str(&duration.as_secs().to_string()),
        None =>  serializer.serialize_str("0")
    }
}

// **************************************************************************************
// Datafusion to Avro conversion code
// ************************************************************************************** 

/// Read all json files under the provided dirpath and write their content to an Avro file
pub fn create_avro_file(df_schema: DFSchema, table_name: &str, dirpath: &String) -> Result<(), std::io::Error> {

    // get the schema from the first record in the vector
    let schema = avro_schema_from(df_schema, table_name.to_owned());

    let mut writer = AvroWriter::with_codec(&schema, Vec::new(), Codec::Snappy);

    for entry in std::fs::read_dir(dirpath)? {

        let path = entry?.path();

        for line in std::fs::read_to_string(path)?.lines() {
            
            // Read the JSON contents of the file as an instance of `User`.
            let json_value: serde_json::Value = serde_json::from_str(line)?;
            let avro_value = json_to_avro(&json_value);

            // write the records to the file
            let _ = writer.append_value_ref(&avro_value).unwrap();

        }

    }

    // create and write all content to the file
    let file_path = format!("{}/{}.avro", dirpath, Uuid::new_v4());
    std::fs::write(file_path, writer.into_inner().unwrap())?;

    Ok(())

}

fn avro_schema_from(df_schema: DFSchema, table_name: String) -> AvroSchema {

    let df_fields: Vec<(&arrow_schema::DataType, &String)> = df_schema
        .fields()
        .iter()
        .map(|field| (field.data_type(), field.name()))
        .collect();

    avro_record_from(df_fields, table_name)

}

fn avro_field_from(data_type: &arrow_schema::DataType, name: &String) -> Result<AvroSchema, std::io::Error> {

    let schema = match data_type {
        ArrowDataType::Null => AvroSchema::Null,
        ArrowDataType::Boolean => AvroSchema::Boolean,
        ArrowDataType::Int8 => AvroSchema::Int,
        ArrowDataType::Int16 => AvroSchema::Int,
        ArrowDataType::Int32 => AvroSchema::Int,
        ArrowDataType::Int64 => AvroSchema::Long,
        ArrowDataType::UInt8 => AvroSchema::Int,
        ArrowDataType::UInt16 => AvroSchema::Int,
        ArrowDataType::UInt32 => AvroSchema::Int,
        ArrowDataType::UInt64 => AvroSchema::Long,
        ArrowDataType::Float16 => AvroSchema::Float,
        ArrowDataType::Float32 => AvroSchema::Float,
        ArrowDataType::Float64 => AvroSchema::Double,
        ArrowDataType::Timestamp(timeunit, _timezone) => match timeunit {
            arrow_schema::TimeUnit::Second => return Err(Error::new(ErrorKind::Other, "Parse error")),
            arrow_schema::TimeUnit::Millisecond => AvroSchema::TimestampMillis,
            arrow_schema::TimeUnit::Microsecond => AvroSchema::TimestampMicros,
            arrow_schema::TimeUnit::Nanosecond => return Err(Error::new(ErrorKind::Other, "Parse error")),
        },
        ArrowDataType::Date32 => AvroSchema::Date,
        ArrowDataType::Date64 => AvroSchema::Date,
        ArrowDataType::Time32(timeunit) => match timeunit {
            arrow_schema::TimeUnit::Second => return Err(Error::new(ErrorKind::Other, "Parse error")),
            arrow_schema::TimeUnit::Millisecond => AvroSchema::TimeMillis,
            arrow_schema::TimeUnit::Microsecond => AvroSchema::TimeMicros,
            arrow_schema::TimeUnit::Nanosecond => return Err(Error::new(ErrorKind::Other, "Parse error")),
        },
        ArrowDataType::Time64(timeunit) => match timeunit {
            arrow_schema::TimeUnit::Second => return Err(Error::new(ErrorKind::Other, "Parse error")),
            arrow_schema::TimeUnit::Millisecond => AvroSchema::TimeMillis,
            arrow_schema::TimeUnit::Microsecond => AvroSchema::TimeMicros,
            arrow_schema::TimeUnit::Nanosecond => return Err(Error::new(ErrorKind::Other, "Parse error")),
        },
        ArrowDataType::Duration(_timeunit) => AvroSchema::Duration,
        ArrowDataType::Interval(_interval_unit) => AvroSchema::Duration,
        ArrowDataType::Binary => AvroSchema::Bytes,
        ArrowDataType::FixedSizeBinary(_size) => AvroSchema::Bytes,
        ArrowDataType::LargeBinary => AvroSchema::Bytes,
        ArrowDataType::Utf8 => AvroSchema::String,
        ArrowDataType::LargeUtf8 => AvroSchema::String,
        ArrowDataType::List(field) => AvroSchema::Array(Box::new(avro_field_from(field.data_type(), field.name())?)),
        ArrowDataType::FixedSizeList(field, _size) => AvroSchema::Array(Box::new(avro_field_from(field.data_type(), field.name())?)),
        ArrowDataType::LargeList(field) => AvroSchema::Array(Box::new(avro_field_from(field.data_type(), field.name())?)),
        ArrowDataType::Struct(fields) => {
            let struct_fields: Vec<(&arrow_schema::DataType, &String)> = fields
                .iter()
                .map(|field| (field.data_type(), field.name()))
                .collect();

            avro_record_from(struct_fields, name.to_string())
        },
        ArrowDataType::Union(union_fields, _mode) => {
            let avro_fields = union_fields
                .iter()
                .map(|field| avro_field_from(field.1.data_type(), field.1.name()).unwrap())
                .collect();
            AvroSchema::Union(AvroUnionSchema::new(avro_fields).unwrap())
        }
        ArrowDataType::Dictionary(_key_type, _value_type) => return Err(Error::new(ErrorKind::Other, "Parse error")),
        ArrowDataType::Decimal128(precision, _scale) => AvroSchema::Decimal(AvroDecimalSchema{
            precision: (*precision).into(), 
            scale: 0, 
            inner: Box::new(AvroSchema::Bytes)
        }),
        ArrowDataType::Decimal256(precision, _scale) => AvroSchema::Decimal(AvroDecimalSchema{
            precision: (*precision).into(), 
            scale: 0, 
            inner: Box::new(AvroSchema::Bytes)
        }),
        ArrowDataType::Map(field, _sorted) => AvroSchema::Map(Box::new(avro_field_from(field.data_type(), field.name())?)),
        ArrowDataType::RunEndEncoded(_run_ends, _values) => return Err(Error::new(ErrorKind::Other, "Parse error")),
    };

    Ok(schema)

}


fn avro_record_from(fields: Vec<(&arrow_schema::DataType, &String)>, record_name: String) -> AvroSchema {

    let mut avro_fields = Vec::new();
    let mut lookup_map = BTreeMap::new();

    // generate list of avro record fields and a lookup map
    for (pos, field) in fields.iter().enumerate() {
        
        let (data_type, field_name) = field;

        avro_fields.push(AvroRecordField {
            name: field_name.to_string(),
            doc: None,
            aliases: None,
            default: None,
            schema: avro_field_from(data_type, field_name).unwrap(),
            order: RecordFieldOrder::Ignore,
            position: pos,
            custom_attributes: BTreeMap::new(),
        });

        let _ = lookup_map.insert(field_name.to_string(), pos);

    }

    let record_schema = AvroRecordSchema {
        name: AvroSchemaName::new(&record_name).unwrap(),
        aliases: None,
        doc: None,
        fields: avro_fields,
        lookup: lookup_map,
        attributes: BTreeMap::new(),
    };

    AvroSchema::Record(record_schema)

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

// **************************************************************************************
// JSON to Datafusion schema code
// ************************************************************************************** 

/// Generate an arrow schema from a josn value
pub fn schema_from_json(json: &serde_json::Value, table_name: &String) -> Schema {
    
    let fields = fields_from_json(json, table_name);
    Schema::new(fields)

}

/// Infer arrow fields type from a json value
fn fields_from_json(json: &serde_json::Value, field_name: &String) -> Fields {
    
    match json {
        
        Value::Object(obj) => {
            
            let fields_vec: Vec<Field> = obj
                .into_iter()
                .map(|(key, value)| field_from_json(value, key))
                .collect();

            Fields::from(fields_vec)

        },
        Value::Array(arr) => fields_from_json(&arr[0], field_name),
        _ => Fields::from(vec![field_from_json(json, field_name)]),
    }

}

/// Infer an arrow field and datatype from a json value
fn field_from_json(json: &serde_json::Value, field_name: &String) -> Field {
    
    match json {
        Value::Null => Field::new(field_name, ArrowDataType::Null, true),
        Value::Bool(_) => Field::new(field_name, ArrowDataType::Boolean, true),
        Value::Number(value) => {

            if value.is_f64() {     
                Field::new(field_name, ArrowDataType::Float64, true)
            } 
            else if value.is_i64() {
                Field::new(field_name, ArrowDataType::Int64, true)
            }
            else {
                Field::new(field_name, ArrowDataType::Float64, true)
            }
        },
        Value::String(_) => Field::new(field_name, ArrowDataType::Utf8, true),
        Value::Array(arr) => {
            
            let list_field = field_from_json(&arr[0], field_name);
            Field::new(field_name, ArrowDataType::List(Arc::new(list_field)), true)

        },
        Value::Object(obj) => {
            
            let fields_vec: Vec<Field> = obj
                .into_iter()
                .map(|(key, value)| field_from_json(value, key))
                .collect();

            let object_fields = Fields::from(fields_vec);
            Field::new(field_name, ArrowDataType::Struct(object_fields), true)

        }
    }

}