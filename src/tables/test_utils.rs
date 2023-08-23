use std::path::PathBuf;
use std::fs::{ create_dir_all, remove_dir_all };

/// A helper struct for creating and deleting directories needed by test functions
#[derive(Debug)]
pub struct TestDir {
    
    /// Path pointing at the test directory
    pub path: PathBuf,
}

impl TestDir {
    
    /// Creates a new directory for a path if it doesn't already exist.
    /// In other test frameworks this would be known as a "Setup" function.
    pub fn new(path: &'static str) -> Self {

        let buf = PathBuf::from(path);

        if !buf.exists() { create_dir_all(buf.to_str().unwrap()).unwrap() }
        if !buf.is_dir() { panic!("The path provided is not pointing at a diectory.") }

        TestDir { path: buf }

    }

}

impl Drop for TestDir {
    
    /// When a test function ends, delete all the files created by the function.
    /// In other test frameworks this would be known as a "Teardown" function.
    fn drop(&mut self) {
        
        remove_dir_all(self.path.to_str().unwrap()).unwrap()
    
    }

}