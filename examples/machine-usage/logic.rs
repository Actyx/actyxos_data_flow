use crate::model::{MachineEvent, UsageEntry};
use abomonation_derive::Abomonation;
use actyxos_data_flow::flow::{Flow, Input, Scope, Stateful};
use actyxos_sdk::event::{Event, LamportTimestamp, TimeStamp};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Abomonation)]
struct Excerpt {
    lamport: LamportTimestamp, // place first to sort in ascending (causal) order
    machine: String,
    event: MachineEvent,
    timestamp: TimeStamp,
}

pub fn usage_logic<'a>(
    scope: &mut Scope<'a>,
) -> (Input<Event<MachineEvent>>, Flow<'a, UsageEntry, Stateful>) {
    let (injector, events) = Flow::<Event<MachineEvent>, _>::new(scope);

    let out = events
        .filter(|ev| ev.stream.name.as_str().starts_with("Drill"))
        .map(|ev| Excerpt {
            lamport: ev.lamport,
            machine: ev.stream.name.to_string(),
            event: ev.payload,
            timestamp: ev.timestamp,
        })
        .group_by(|excerpt| excerpt.machine.clone())
        .reduce(|_machine, inputs, outputs| {
            let mut started_events = BTreeMap::new();
            for (excerpt, _) in inputs {
                // the inputs are in ascending order, so we can assume that stop comes after start
                match &excerpt.event {
                    MachineEvent::Started { order } => {
                        started_events.insert(
                            order,
                            UsageEntry {
                                machine: excerpt.machine.clone(),
                                order: order.clone(),
                                started: excerpt.timestamp,
                                duration_micros: 0,
                            },
                        );
                    }
                    MachineEvent::Stopped { order } => {
                        if let Some(mut usage) = started_events.remove(&order) {
                            usage.duration_micros = excerpt.timestamp - usage.started;
                            outputs.push((usage, 1));
                        }
                    }
                }
            }
        })
        .ungroup();

    (injector, out)
}
