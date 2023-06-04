use domains::record::Record;
use apache_avro::Schema;
use serde_json::json;
use uuid::Uuid;


#[test]
fn create_new_record() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "count", "type": "long"}
        ] 
    }"#).unwrap();

    let json_record = json!({"id": 4, "count": 7});

    let record = Record::new(
        "users",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_ok());

}


#[test]
fn schema_type_resolution_error() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "string"}
        ] 
    }"#).unwrap();

    let json_record = json!({"id": 4});

    let record = Record::new(
        "users",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_err());

}


#[test]
fn schema_field_name_resolution_error() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "identity", "type": "long"}
        ] 
    }"#).unwrap();

    let json_record = json!({"id": 4});

    let record = Record::new(
        "users",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_err());

}


#[test]
fn create_record_generates_correct_fields() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "count", "type": "long"}
        ] 
    }"#).unwrap();

    let json_record = json!({"id": 4, "count": 7});

    let record = Record::new(
        "users",
        json_record, 
        &schema, 
        "CREATE".to_string()
    ).unwrap();

    assert_eq!(record.get_name(), String::from("users"));
    assert_eq!(record.get_type(), String::from("RECORD"));
    assert_eq!(record.get_record(), json!({ "id": 4, "count": 7, }));
    assert_eq!(record.get_operation(), String::from("CREATE"));
    assert!(Uuid::parse_str(&record.get_id().to_string()).is_ok());

}


#[test]
fn create_record_with_nested_object() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "nested_record",
        "aliases": ["nested_record"],
        "fields": [
            {"name": "nest_1", "type": "record", "fields": [
                {"name": "nest_2", "type": "record", "fields": [
                    {"name": "nest_3", "type": "long"}
                ]}
            ]}
        ] 
    }"#).unwrap();

    let json_record = json!({
        "nest_1": {
            "nest_2": {
                "nest_3": 3
            }
        }
    });

    let record = Record::new(
        "users",
        json_record, 
        &schema, 
        "CREATE".to_string()
    ).unwrap();

    assert_eq!(record.get_record(), json!({ 
    	"nest_1": {
    		"nest_2": {
    			"nest_3": 3
    		}
    	} 
    }));

}


#[test]
fn record_operations_are_all_valid() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "long"}
        ] 
    }"#).unwrap();

    let json_record = json!({"id": 1});

    let create_record = Record::new(
        "users",
        json_record.clone(), 
        &schema, 
        "CREATE".to_string()
    );

    let update_record = Record::new(
        "users",
        json_record.clone(), 
        &schema, 
        "UPDATE".to_string()
    );

    let delete_record = Record::new(
        "users",
        json_record.clone(), 
        &schema, 
        "DELETE".to_string()
    );

    assert!(create_record.is_ok());
    assert!(update_record.is_ok());
    assert!(delete_record.is_ok());

}


#[test]
fn record_changes_operation_to_uppercase() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "long"}
        ] 
    }"#).unwrap();

    let json_record = json!({"id": 1});

    let record = Record::new(
        "users",
        json_record, 
        &schema, 
        "create".to_string()
    ).unwrap();

    assert_eq!(record.get_operation(), "CREATE".to_string());

}


#[test]
fn record_rejects_invalid_operation() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "long"}
        ] 
    }"#).unwrap();

    let json_record = json!({"id": 1});

    let record = Record::new(
        "users",
        json_record, 
        &schema, 
        "new".to_string()
    );

    assert!(record.is_err());

}


#[test]
fn record_is_not_an_object() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "long"}
        ] 
    }"#).unwrap();

    let json_record = json!("id=1");

    let record = Record::new(
        "users",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_err());

}

#[test]
fn record_type_must_have_at_least_one_field() {
    
    let schema = Schema::parse_str(r#"{ 
        "type": "record",
        "name": "users",
        "fields": [
            {"name": "id", "type": "long"}
        ] 
    }"#).unwrap();

    let json_record = json!({});

    let record = Record::new(
        "users",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_err());

}


#[test]
fn create_new_string_type_record() {
    
    let schema = Schema::parse_str(r#"{"type": "string"}"#).unwrap();

    let json_record = json!("hello world!");

    let record = Record::new(
        "greeting",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_ok());

}

#[test]
fn create_new_number_type_record() {
    
    let schema = Schema::parse_str(r#"{"type": "double"}"#).unwrap();

    let json_record = json!(12.00);

    let record = Record::new(
        "count",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_ok());

}

#[test]
fn create_new_null_type_record() {
    
    let schema = Schema::parse_str(r#"{"type": "null"}"#).unwrap();

    let json_record = json!(());

    let record = Record::new(
        "nothing",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_ok());

}


#[test]
fn create_new_bool_type_record() {
    
    let schema = Schema::parse_str(r#"{"type": "boolean"}"#).unwrap();

    let json_record = json!(true);

    let record = Record::new(
        "yes_or_no",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_ok());

}

#[test]
fn create_new_array_type_record() {
    
    let schema = Schema::parse_str(r#"{
        "type": "array", 
        "items" : "string"
    }"#).unwrap();

    let json_record = json!(["hello", "world", "!"]);

    let record = Record::new(
        "list_of_stuff",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_ok());

}


#[test]
fn number_items_are_wrong_type() {
    
    let schema = Schema::parse_str(r#"{
        "type": "array", 
        "items" : "string"
    }"#).unwrap();

    let json_record = json!(["hello", 2, false]);

    let record = Record::new(
        "list_of_stuff",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_err());

}


#[test]
fn number_record_fails_against_string_schema() {
    
    let schema = Schema::parse_str(r#"{"type": "string"}"#).unwrap();

    let json_record = json!(2);

    let record = Record::new(
        "greeting",
        json_record, 
        &schema, 
        "CREATE".to_string()
    );

    assert!(record.is_err());

}