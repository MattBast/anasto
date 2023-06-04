use warp::{ Reply, Rejection };
use warp::http::StatusCode;
use warp::test::request;
use tokio::sync::{ mpsc, RwLock };
use domains::Event;
use std::sync::Arc;
use std::path::PathBuf;
use repos::schema::Repo;

// helper function to setup a test version of the api
async fn new_api() -> impl warp::Filter<Extract = impl Reply, Error = Rejection> + Clone {

    // create a mock stream channel and subscribers register
    let (bus_tx, _) = mpsc::unbounded_channel::<Event>();

    // create a mock schema repo
    let schema_repo = Repo::new(PathBuf::from("./test_schemas/basic/")).unwrap();
    let schema_repo_ref = Arc::new(RwLock::new(schema_repo));

    // initialise the api
    let api = entrypoints::endpoints(bus_tx, 16000, schema_repo_ref);

    return api;

}


#[tokio::test]
async fn test_health() {
    
    let api = new_api().await;

    // make a mock request
    let resp = request()
        .method("GET")
        .path("/health")
        .reply(&api)
        .await;

    let resp_body = resp.body();

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(!resp.body().is_empty());
    assert_eq!(&resp_body[..], b"{\"code\":200,\"state\":\"good\"}");

}