use crate::model::{DashboardEntry, MachineEvent};
use actyxos_data_flow::flow::{Flow, Input, Scope, Stateful};
use actyxos_sdk::event::Event;
use std::time::Duration;

pub fn dashboard_logic<'a>(
    scope: &mut Scope<'a>,
) -> (
    Input<Event<MachineEvent>>,
    Flow<'a, DashboardEntry, Stateful>,
) {
    let one_year = Duration::from_secs(365 * 86400);
    let (injector, events) = Flow::<Event<MachineEvent>, _>::new_limited(scope, one_year);

    let out = events
        .filter(|ev| ev.stream.name.as_str().starts_with("Drill"))
        .map(|ev| match ev.payload {
            MachineEvent::Started { order } => {
                DashboardEntry::working(ev.stream.name.to_string(), order, ev.timestamp)
            }
            MachineEvent::Stopped { .. } => {
                DashboardEntry::idle(ev.stream.name.to_string(), ev.timestamp)
            }
        })
        .group_by(|entry| entry.machine.clone())
        .max_by(|entry| entry.since)
        .ungroup();

    (injector, out)
}
