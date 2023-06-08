use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use domains::record::Record;
use log::error;

/// Defines the shared behaviour for all subscribers as well as a default implementation
/// for listening for events
pub trait Subscriber {
    
    /// Write new records to a destination data store
    fn create_records(&self, records: Vec<Record>) -> Result<(), std::io::Error>;

    /// Update records in a destination data store
    fn upsert_records(&self, records: Vec<Record>) -> Result<(), std::io::Error>;

    /// Delete records in a destination data store
    fn delete_records(&self, records: Vec<Record>) -> Result<(), std::io::Error>;

    /// Check if a table exists. If not, create it in the destination data store
    fn check_table(&self, table_name: &str) -> Result<(), std::io::Error>;

}

/// Start listening to Anasto for new records
pub async fn start_subscriber(
    subscriber: Box<dyn Subscriber + Send + Sync>, 
    mut rx: mpsc::UnboundedReceiver<Vec<Record>>,
    upsert: bool,
) -> Result<JoinHandle<()>, std::io::Error> {

    // start a new task so the connector doesn't disrupt the rest of Anasto
    let handle = tokio::spawn(async move {

        // start listening for new Records that Anasto wants to be written to destination data stores
        while let Some(records) = rx.recv().await {

            // if in upsert mode, split out the records into upsert and deletions
            if upsert {
    
                let (upserts, deletions):(_,Vec<_>) = records
                    .into_iter()
                    .partition(|x| x.get_operation() == *"DELETE" );

                match subscriber.upsert_records(upserts) {
                    
                    Ok(_) => (),
                    Err(error_message) => {
                        error!(target: "subscribers", "A subscriber failed while upserting records with the error:\n{}", error_message);
                        break
                    },

                };

                match subscriber.delete_records(deletions) {
                    
                    Ok(_) => (),
                    Err(error_message) => {
                        error!(target: "subscribers", "A subscriber failed while deleting records with the error:\n{}", error_message);
                        break
                    },

                };

            }
            // otherwise just write them as an event log
            else {
                
                match subscriber.create_records(records) {
                    
                    Ok(_) => (),
                    Err(error_message) => {
                        error!(target: "subscribers", "A subscriber failed while creating records with the error:\n{}", error_message);
                        break
                    },

                };

            }

        }

    });
    
    Ok(handle)

}
