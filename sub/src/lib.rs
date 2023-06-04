#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![cfg_attr(test, deny(warnings))]

//! # Subscribers
//!
//! A subscriber listens for events that are flowing through Anasto
//! and writes them to a destination system.

/// The `sub_trait` defines shared behaviour for all the subscribers
pub mod sub_trait;

/// A subscriber that writes the data as local files
pub mod localfile;