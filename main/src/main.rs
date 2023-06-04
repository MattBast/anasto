use std::env;
use config::bootstrap::{ init, run };

#[tokio::main]
async fn main() {

    let config_filepath = get_config_filepath();
    let (api_tx, api, bus_tx, bus, _sub_handles) = init(config_filepath, true).await;
    run(api_tx, api, bus_tx, bus).await;

}

/// Allow the user to specify where the config file is with a
/// command line argument. If one is not provided the default
/// "config.toml" is returned.
fn get_config_filepath() -> String {
    
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        "./config.toml".to_string()
    }
    else {
        args[1].clone()
    }

}