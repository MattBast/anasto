use domains::schema::Schema;
use apache_avro::schema::Schema as AvroSchema;
use serde_json::json;
use uuid::Uuid;

#[test]
fn create_schema_from_json_adds_default_values() {
    
    let json_event = json!({
        "table_name": "users",
        "schema": r#"{ 
            "type": "record",
            "name": "users",
            "fields": [
                {"name": "id", "type": "string"}
            ] 
        }"#
    });

    let event: Schema = serde_json::from_value(json_event).unwrap();

    assert_eq!(event.schema, AvroSchema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "string"}
        ] 
    }"#).unwrap());
    assert_eq!(event.event_type, String::from("SCHEMA"));
    assert_eq!(event.key_properties, Vec::<String>::new());
    assert!(Uuid::parse_str(&event.event_id.to_string()).is_ok());
    assert_eq!(event.operation, String::from("CREATE"));

}


#[test]
fn not_required_field_is_not_mutated() {
    
    let json_event = json!({
        "table_name": "users",
        "schema": r#"{ 
            "type": "record",
            "name": "users",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "value", "type": ["null", "long"]}
            ] 
        }"#
    });

    let event: Schema = serde_json::from_value(json_event).unwrap();

    assert_eq!(event.schema, AvroSchema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "value", "type": ["null", "long"]}
        ] 
    }"#).unwrap());

}


#[test]
fn create_record_with_nested_schema() {
    
    let json_event = json!({
        "table_name": "users",
        "schema": r#"{ 
            "type": "record",
            "name": "users",
            "fields": [
                {"name": "id", "type": "string"},
                {
                    "name": "address",
                    "type": {
                        "type" : "record",
                        "name" : "AddressUSRecord",
                        "fields" : [
                            {"name": "streetaddress", "type": "string"},
                            {"name": "city", "type": "string"}
                        ]
                    }
                }
            ] 
        }"#
    });

    let event: Schema = serde_json::from_value(json_event).unwrap();

    assert_eq!(event.schema, AvroSchema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "string"},
            {
                "name": "address",
                "type": {
                    "type" : "record",
                    "name" : "AddressUSRecord",
                    "fields" : [
                        {"name": "streetaddress", "type": "string"},
                        {"name": "city", "type": "string"}
                    ]
                }
            }
        ] 
    }"#).unwrap());

}

#[test]
fn schema_changes_operation_to_uppercase() {
    
    let json_event = json!({
        "table_name": "users",
        "schema": r#"{ 
            "type": "record",
            "name": "users",
            "fields": [
                {"name": "id", "type": "string"}
            ] 
        }"#,
        "operation": "create"
    });

    let event: Schema = serde_json::from_value(json_event).unwrap();
    assert_eq!(event.operation, String::from("CREATE"));

}


#[test]
fn schema_rejects_invalid_operation() {
    
    let json_event = json!({
        "table_name": "users",
        "schema": r#"{ 
            "type": "record",
            "name": "users",
            "fields": [
                {"name": "id", "type": "string"}
            ] 
        }"#,
        "operation": "NEW"
    });

    assert!(serde_json::from_value::<Schema>(json_event).is_err());

}


#[test]
fn schema_changes_event_type_to_uppercase() {
    
    let json_event = json!({
        "table_name": "users",
        "schema": r#"{ 
            "type": "record",
            "name": "users",
            "fields": [
                {"name": "id", "type": "string"}
            ] 
        }"#,
        "event_type": "schema"
    });

    let event: Schema = serde_json::from_value(json_event).unwrap();
    assert_eq!(event.event_type, String::from("SCHEMA"));

}


#[test]
fn schema_type_must_equal_schema() {
    
    let json_event = json!({
        "table_name": "users",
        "schema": r#"{ 
            "type": "record",
            "name": "users",
            "fields": [
                {"name": "id", "type": "string"}
            ] 
        }"#,
        "event_type": "RECORD"
    });

    assert!(serde_json::from_value::<Schema>(json_event).is_err());

}


#[test]
fn schema_is_not_an_object() {
    
    let json_event = json!({
        "table_name": "users",
        "record": "properties=0"
    });

    assert!(serde_json::from_value::<Schema>(json_event).is_err());

}


#[test]
fn schema_must_comply_to_avro() {
    
    let json_event = json!({
        "table_name": "users",
        "schema": r#"{ 
            "properties": {
                "id": {
                    "type": "string"
                }
            } 
        }"#
    });

    assert!(serde_json::from_value::<Schema>(json_event).is_err());

}