use std::convert::Infallible;
use std::error::Error;
use warp::{reject, Rejection};
use serde_derive::{Deserialize, Serialize};
use warp::http::StatusCode;
use warp::filters::body::BodyDeserializeError;
// use log::{ warn };

pub async fn reject_request(err: Rejection) -> Result<impl warp::Reply, Infallible> {
    let code;
    let message;

    if err.is_not_found() {
        
        code = StatusCode::NOT_FOUND;
        message = String::from("Sorry, the endpoint you made a request to does not exist.");

    } else if let Some(e) = err.find::<BodyDeserializeError>() {
        
        code = StatusCode::BAD_REQUEST;
        message = body_deserialize_error(e);

    } else if err.find::<reject::MethodNotAllowed>().is_some() {
        
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = String::from("Sorry, the HTTP method you used isn't allowed on this endpoint.");

    } else {
        
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = String::from("Sorry, it looks like something unexpected went wrong so this error message can't give you any further details.");

    }

    let json = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message,
    });

    Ok(warp::reply::with_status(json, code))

}


/// When the body of a request could not be deserialized correctly, call this
/// function to generate a error message to help the user get it right next time
fn body_deserialize_error(err: &BodyDeserializeError) -> String {

    let message = match err.source() {
        
        Some(cause) => {
            
            let cause_string = cause.to_string();

            if cause_string.contains("missing field") {
                
                let field_name = cause_string.chars().skip(14).take(100).collect::<String>();
                format!("Your request body is missing the field: {}", field_name)

            } else if cause_string.contains("invalid type:") {

                let mut error_message_words: Vec<&str> = cause_string.rsplit(' ').collect();
                error_message_words.reverse();

                let request_type = error_message_words[2];
                let expected_type = error_message_words[6];
                
                format!(
                    "One of the fields in your request body had the wrong data type. You passed a {} while the expected type was {}.", 
                    request_type, 
                    expected_type
                )

            } else {
                
                String::from("BAD_REQUEST")

            }

        }

        None => String::from("BAD_REQUEST"),

    };

    message

}


/// An API error response serializable to JSON.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ErrorMessage {
    code: u16,
    message: String,
}