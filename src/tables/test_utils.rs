use std::path::PathBuf;
use std::fs::{ create_dir_all, remove_dir_all, remove_file };

/// A helper struct for creating and deleting files needed by test functions
#[derive(Debug)]
pub struct TestFile {
    path: PathBuf,
}

impl TestFile {
    
    /// Creates a new directory for a path if it doesn't already exist.
    /// In other test frameworks this would be known as a "Setup" function.
    pub fn new(path: &'static str) -> Self {

        let buf = PathBuf::from(path);

        if !buf.exists() { create_dir_all(buf.to_str().unwrap()).unwrap() }

        TestFile { path: buf }

    }

}

impl Drop for TestFile {
    
    /// When a test function ends, delete all the files created by the function.
    /// In other test frameworks this would be known as a "Teardown" function.
    fn drop(&mut self) {
        
        if self.path.is_dir() { remove_dir_all(self.path.to_str().unwrap()).unwrap() }
        if self.path.is_file() { remove_file(self.path.to_str().unwrap()).unwrap() }
    
    }

}