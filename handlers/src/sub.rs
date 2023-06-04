use domains::sub::Subscriber;
use std::collections::HashMap;
use uuid::Uuid;
use log::info;

/// This function handlers subscriber events including the creation, updating
/// and deletion of subscribers
pub fn update(
	sub_repo: &mut HashMap<Uuid, Subscriber>,
	sub_event: Subscriber
) {

    let sub_id = sub_event.sub_id;

	if sub_event.operation == *"CREATE" {

        sub_repo.insert(
            sub_event.sub_id, 
            sub_event
        );

        info!(target: "subscribers", "Created subscriber with id {:?}.", &sub_id);

    }

    else if sub_event.operation == *"UPDATE" {

        if let Some(old_sub) = sub_repo.get_mut(&sub_event.sub_id) {
                
            *old_sub = sub_event;
            info!(target: "subscribers", "Updated subscriber with id {:?}.", &sub_id);

        } else {

            sub_repo.insert(
                sub_event.sub_id, 
                sub_event
            );

            info!(target: "subscribers", "Created subscriber with id {:?}.", &sub_id);

        }


    }

    else if sub_event.operation == *"DELETE" {

        sub_repo.remove(&sub_event.sub_id);
        info!(target: "subscribers", "Deleted subscriber with id {:?}.", &sub_id);
        
    }

}