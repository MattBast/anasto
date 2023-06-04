#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![cfg_attr(test, deny(warnings))]

//! # Handlers
//!
//! Handlers are functions that are called by the message bus to process
//! different types of events. They are purely logic and should not be
//! instantiating any repos or storage themselves.

/// This module is responsible for processing new events. It groups them
/// together by table_name before releasing mini batches of them to
/// subscribers.
pub mod record;

// pub mod state;
// pub mod metric;

/// This module handlers subscriber events including the creation, updating
/// and deletion of subscribers
pub mod sub;

