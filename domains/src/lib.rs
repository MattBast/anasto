#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![cfg_attr(test, deny(warnings))]

//! # Domains
//!
//! Domains tend t be used to model business processes in code. 
//! Anasto doesn't really model a business but it does use domain
//! like code to define the events and objects that exist outside
//! the tool and are processed within it. As well as defining them
//! this crate lays out the rules for creating them to make sure no
//! data corruption takes place.

/// A record is a data change event representing a row of data
/// getting created, updated or deleted.
pub mod record;

/// A schema defines the key names and data types of the values
/// in a Record.
pub mod schema;

/// A collection of less complex events that can be passed to the message bus.
pub mod events;

/// An event informing Anasto that a subscriber has started listening
/// to a webhook for Record and Schema type events
pub mod sub;

/// An event represents something that has happened in a source system
/// or within Anasto itself. These events are intended for a message bus
/// will send them to the appropriate handler.
#[derive(Debug)]
pub enum Event {
    
    /// A record is a data change event representing a row of data
    /// getting created, updated or deleted.
    Record(record::Record),
    /// A schema defines the key names and data types of the values
    /// in a Record.
    Schema(schema::Schema),
    /// State represents a point in time that tells us how which
    /// events have been sent to destinations and which haven't.
    State(events::State),
    /// A statistic giving an aggregated measure of something happening
    /// in Anasto such as how many events have been processed.
    Metric(events::Metric),
    /// A command telling Anasto to send the events in a buffer (a type
    /// of repository) to all subscribed destinations.
    Drain(events::Drain),
    /// An event informing Anasto that a subscriber has started listening
    /// to a webhook for Record and Schema type events
    Subscribe(sub::Subscriber),
    /// An event telling Anasto to begin shutting down
    Shutdown

}