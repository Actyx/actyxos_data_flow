use crate::model::{FinishedGoods, ProductionSummary};
use abomonation_derive::Abomonation;
use actyxos_data_flow::flow::{Flow, Input, Scope, Stateful};
use actyxos_sdk::event::{Event, LamportTimestamp, TimeStamp};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Abomonation)]
pub struct FinishedGoodsEvent {
    lamport: LamportTimestamp, // place first to sort in ascending (causal) order
    payload: FinishedGoods,
    timestamp: TimeStamp,
}

pub fn usage_logic<'a>(
    scope: &mut Scope<'a>,
) -> (
    Input<Event<FinishedGoods>>,
    Flow<'a, ProductionSummary, Stateful>,
) {
    let (injector, events) = Flow::<Event<FinishedGoods>, _>::new(scope);

    let extracts = events.map(|ev| FinishedGoodsEvent {
        lamport: ev.lamport,
        payload: ev.payload,
        timestamp: ev.timestamp,
    });

    let out = extracts
        .group_by(|e| (e.payload.article_id.clone(), e.payload.workstation.clone()))
        .reduce(|(article_id, workstation), inputs, outputs| {
            // same article_id should mean same article_name
            let article_name = inputs
                .get(0)
                .map(|e| e.0.payload.article_name.clone())
                .unwrap_or_default();
            let total_pcs: i64 = inputs
                .iter()
                // note we multiply by count below!
                .map(|(e, count)| *count as i64 * e.payload.pcs)
                .sum();
            outputs.push((
                ProductionSummary {
                    article_id: article_id.clone(),
                    article_name,
                    workstation: workstation.clone(),
                    total_pcs,
                },
                1,
            ));
        })
        .ungroup();

    (injector, out)
}
