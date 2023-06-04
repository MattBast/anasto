#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![cfg_attr(test, deny(warnings))]

//! # Repositories
//!
//! A repository holds and (in the future) persists groups of events
//! that have been sent to Anasto. They also perform other management
//! tasks like keeping track of how old events are and when it's time
//! to send certain events to subscribers.

/// This repository stores Record events grouped by their table_name.
/// It also contains metric like fields informing users when it's time
/// to drain the repo and send the Records to the subscribers.
pub mod record;

/// This repository stores Schema events to keep track of all the 
/// that should be getting enforced on the Record events. It saves
/// all the records to file so that they can be accessed asynchronously
/// by other threads.
pub mod schema;