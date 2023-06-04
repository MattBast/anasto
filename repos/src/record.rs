use std::time::SystemTime;
use std::mem::size_of_val;
use uuid::Uuid;
use domains::record::Record;
use log::info;


/// This repository stores Record events grouped by their table_name.
/// It also contains metric like fields informing users when it's time
/// to drain the repo and send the Records to the subscribers.
#[derive(Debug)]
pub struct Repo {
    
    /// A unique id for a single use of this buffer
    id: uuid::Uuid,
    /// The table_name of the events in this buffer
    name: String,
    /// A list of the events in this buffer
    events: Vec<Record>,
    /// How many events this buffer can include before it needs to be read
    capacity_cap: usize,
    /// How much memory this buffer can consume in bytes before it needs
    /// to be read
    memory_cap: usize,
    /// A timestamp tracking when the oldest event in the buffer entered the buffer
    oldest_event_timestamp: SystemTime,
    /// The maximum age in seconds that an event can live to until the buffer should be drained
    max_event_age: u64,
    /// A timestamp representing the last time the buffer was drained
    drained_timestamp: SystemTime,

}

impl Repo {
    
    /// Create a new buffer defining its bounded size (number of elements
    /// and memory usage). Also state what event type will be included in
    /// the buffer.
    pub fn new(
        name: String, 
        capacity: usize,
        memory_cap: usize,
        max_event_age: u64
    ) -> Result<Repo, std::io::Error> {
        
        let events: Vec<Record> = Vec::with_capacity(capacity);

        info!(target: "records", "New buffer created for records from the table {}", &name);
        
        Ok(Repo{
            id: Uuid::new_v4(),
            name,
            events,
            capacity_cap: capacity,
            memory_cap,
            oldest_event_timestamp: SystemTime::now(),
            max_event_age,
            drained_timestamp: SystemTime::now()
        })

    }

    /// Getter that returns [Repo](Repo.id) field
    pub fn get_id(&self) -> String {
    
        self.id.to_string()

    }    

    /// Getter that returns [Repo](Repo.name) field
    pub fn get_name(&self) -> String {
    
        self.name.clone()

    }

    /// Getter that returns [Repo](Repo.name) field
    pub fn get_capacity_cap(&self) -> &usize {
    
        &self.capacity_cap

    }

    /// Getter that returns [Repo](Repo.name) field
    pub fn get_memory_cap(&self) -> &usize {
    
        &self.memory_cap

    }

    /// Check if the buffer has hit its capacity and can't receive any new
    /// events.
    pub fn at_capacity(&self) -> bool {

        (self.events.len() >= self.capacity_cap) || (size_of_val(&*self.events) >= self.memory_cap)

    }

    /// Add a new event to the buffer
    pub fn append(&mut self, event: Record) -> Result<(), String> {

        if self.at_capacity() {
            
            Err("Buffer is at capacity so cannot include any new events.".to_owned())

        } else if self.name != event.get_name() {

            Err(format!(
                "Buffer can only contain records for the table {}. You passed a record from the table {}.", 
                self.name, 
                event.get_name()
            ))

        } else {

            if self.events.is_empty() {

                self.oldest_event_timestamp = SystemTime::now();

            }
            
            self.events.push(event);
            Ok(())

        }

    }

    /// Clear the buffer and return an iterator of all events in the buffer 
    // that a connector can consume.
    pub fn drain_buffer(&mut self) -> Result<Vec<Record>, std::io::Error> {

        // Drain the vector holding the buffers events so it is left empty
        let events: Vec<Record> = self.events.drain(..).collect();

        // set a new last drained timestamp
        self.drained_timestamp = SystemTime::now();

        // also set a new id so the buffer can be re-used without confusion
        self.id = Uuid::new_v4();

        info!(target: "records", "The buffer containing records from table {} has been drained.", &self.name);

        Ok(events)

    }


    /// Clear the buffer and return an iterator of all events in the buffer 
    // that a connector can consume.
    pub fn get_events(&self) -> Result<Vec<Record>, std::io::Error> {

        Ok(self.events.clone())

    }


    /// Returns a count of the number of events in the buffer.
    pub fn event_count(&self) -> usize {

        self.events.len()

    }


    /// Returns in seconds the duration of time between now and 
    /// when the oldest event entered the buffer.
    pub fn buffer_age_in_seconds(&self) -> u64 {

        if self.events.is_empty() {

            0
        
        } else {

            self
                .oldest_event_timestamp
                .elapsed()
                .expect("Could not calculate the time elapsed since the oldest event entered the buffer.")
                .as_secs()

        }

    }

    /// Returns a boolean stating if the buffer has reached its maximum age
    pub fn max_age_reached(&self) -> bool {

        self.buffer_age_in_seconds() >= self.max_event_age

    }

    /// Getter for the drained_timestamp field
    pub fn last_drained(&self) -> SystemTime {

        self.drained_timestamp

    }

}
