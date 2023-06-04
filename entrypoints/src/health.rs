use warp::Filter;
use serde_derive::{Deserialize, Serialize};

/// Function that initialises the GET endpoint at the path /health
pub fn init() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    
    warp::path!("health")
        .and(warp::get())
        // .and_then(handler);
        .map(|| {
            
            let response = Response {
                code: warp::http::StatusCode::OK.as_u16(), 
                state: String::from("good")
            };
            
            warp::reply::json(&response)

        })
}


/// An API health check response serializable to JSON.
#[derive(Debug, Deserialize, Serialize, Clone)]
struct Response {
    code: u16,
    state: String,
}