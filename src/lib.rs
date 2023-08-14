#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![cfg_attr(test, deny(warnings))]

pub mod tables;
pub mod startup;

pub fn start() {
	startup::start();
}