//! # Anasto
//!
//! The idea of Anasto is to make it easy to collect real time streaming events 
//! and to have them reliably written to various storage destinations. The 
//! intention is to produce something that is easy for developers to deploy and 
//! integrate with the tech they're currently using.
//! 
//! The best way to think of Anasto is as a middleman between source and destination 
//! data stores. It will listen to one or more sources and copy all the data it finds 
//! to one or more destinations. Anasto favours near realtime so changes in sources systems 
//! should be heard quickly and copied to the destination systems soon after.

#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![deny(unused_results)]
#![deny(unused_import_braces)]
#![deny(non_ascii_idents)]
#![cfg_attr(test, deny(warnings))]

pub mod tables;
pub mod startup;

/// Start Anasto
pub async fn start() {
	startup::start().await;
}