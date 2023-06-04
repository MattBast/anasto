#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![cfg_attr(test, deny(warnings))]

//! # Config
//!
//! The config crate is responsible for orchestrating all the 
//! crates so that together they initialise and start Anasto. 
//! There are also a number of configurations that the user can 
//! specify through a config file that is read at runtime by this 
//! crate. If a configuration is not provided, default values are used.

/// Tables are the blocks of config that will be searched for
/// and parsed from the config file.
pub mod tables;

/// Starts Anasto configuring it with the values found in the config file.
pub mod bootstrap;