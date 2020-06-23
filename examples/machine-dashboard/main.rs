use actyxos_data_flow::{
    db::{SqliteDB, Union},
    machine::Machine,
    runner::{run_event_machine_on_channel, run_with_db_channel},
};
use actyxos_sdk::{event_service::Subscription, semantics};
use anyhow::Result;
use logic::dashboard_logic;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

mod logic;
mod model;

fn main() -> Result<()> {
    let mut db = SqliteDB::<Union<_>>::new("", "db_name")?;
    let subscriptions = vec![Subscription::wildcard(semantics!("machineFish"))];

    // create runtime for executing the EventServiceClient and business logic
    let runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(1)
        .max_threads(2)
        .enable_all()
        .build()?;

    // set up logging
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    run_with_db_channel(
        runtime.handle().clone(), // runtime to use for running async tasks
        &mut db,                  // DB to store results in
        "dashboard",              // name for logging
        move |offsets, to_db| {
            run_event_machine_on_channel(
                Machine::new(&dashboard_logic),
                subscriptions,     // which events we need
                offsets,           // where we left off last time
                to_db,             // sending channel towards DB
                "dashboard_logic", // name for logging
                1_000,             // events per transaction
            )
        },
    )
}
