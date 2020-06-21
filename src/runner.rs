use crate::{
    coll::Coll,
    db::{DbMechanics, DbRecordExt, DB},
    flow::NeedsState,
    machine::{Inputs, Machine},
};
use actyxos_sdk::{
    event::{Event, OffsetMap, Payload},
    event_service::{EventService, Order, Subscription},
};
use anyhow::{anyhow, Result};
use differential_dataflow::ExchangeData;
use futures::{
    future::ready,
    stream::{self, StreamExt},
};
use std::{
    fmt::Write,
    future::Future,
    sync::mpsc::{sync_channel, SyncSender, TrySendError},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::{interval, Duration};

// TODO This should probably come from the settings, since it depends on what the DB is doing
pub const EVENTS_PER_TXN: usize = 1_000;

pub fn run_logic_on_db<D, M, Fut, Out, F>(db: &mut D, name: String, logic: F) -> Result<()>
where
    M: DbMechanics,
    D: DB<Mechanics = M, Record = Out>,
    F: FnOnce(OffsetMap, SyncSender<(OffsetMap, Vec<(Out, isize)>)>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>>,
    Out: ExchangeData + DbRecordExt<M>,
{
    let offsets = db.get_offsets()?;

    // limit the machine to run at most two batches ahead of the database
    // (1 in the channel, 1 in hand)
    let (to_db, from_machine) = sync_channel(1);

    let runtime = tokio::runtime::Builder::new()
        .enable_all()
        .threaded_scheduler()
        .build()
        .unwrap();

    let handle = runtime.handle().clone();

    std::thread::spawn(move || {
        handle.block_on(logic(offsets, to_db)).unwrap();
    });

    loop {
        // will kill this loop if the sender went away
        let (offsets, out) = from_machine.recv()?;
        println!("{}:     received {} deltas from machine", name, out.len());
        db.advance_offsets(&offsets, out)?;
    }
}

enum TickStream<T> {
    Item(T),
    Stop,
    Tick,
}

pub async fn run_event_machine_on_channel<I, O, R, St: NeedsState>(
    mut machine: Machine<I, O, St>,
    subscriptions: Vec<Subscription>,
    mut offsets: OffsetMap,
    to_db: SyncSender<(OffsetMap, Vec<(R, isize)>)>,
    name: String,
) -> Result<()>
where
    I: Inputs<Elem = Event<Payload>>,
    O: ExchangeData,
    R: From<O>,
{
    println!("{}: starting with subscriptions {:?}", name, subscriptions);

    // The plan here is:
    // - get previous offsets O and present P
    // - get all events up to O and ignore the output
    // - get events between O and P and write output in batches of 10'000 events
    // - get events after P and write output every 5sec
    let client = EventService::default();
    let present = client.get_offsets().await?;

    let mut progress: usize = 0;
    let mut errors: usize = 0;

    let feed = |ev: &Event<Payload>,
                machine: &mut Machine<I, O, St>,
                progress: &mut usize,
                errors: &mut usize| match machine.inputs().feed(ev) {
        Ok(_) => {
            *progress += 1;
        }
        Err(err) => {
            *errors += 1;
            println!(
                "{}: Unrecognized event {} @ {:?}: {:?}",
                name,
                *progress + *errors,
                ev,
                err
            );
        }
    };

    let send = |offsets: &OffsetMap, deltas: Coll<O, isize>| match to_db
        .try_send((offsets.clone(), deltas.to_vec()))
    {
        Ok(_) => Ok(()),
        Err(TrySendError::Full(x)) => {
            println!("{}:   waiting to send to DB, {} deltas", name, deltas.len());
            to_db
                .send(x)
                .map_err(|x| anyhow!("cannot send to DB: {}", x))?;
            Ok(())
        }
        Err(_) => Err(anyhow!("channel to DB was closed")),
    };

    // If the machine is stateless, it will just skip all previously seen events.
    // If not, it may say “I need only the last two weeks” or so, otherwise start at the beginning.
    if machine.needs_state() {
        let start_at = if let Some(look_back) = machine.look_back() {
            go_back(
                &name,
                &offsets,
                OffsetMap::empty(),
                look_back,
                &subscriptions,
                &client,
            )
            .await?
        } else {
            OffsetMap::empty()
        };
        println!(
            "{}: getting matches from {} previously ingested events",
            name,
            &offsets - &start_at
        );
        let mut events = client
            .query_between(
                start_at,
                offsets.clone(),
                subscriptions.clone(),
                Order::Lamport,
            )
            .await?;
        while let Some(event) = events.next().await {
            feed(&event, &mut machine, &mut progress, &mut errors);
            if progress % EVENTS_PER_TXN == 0 {
                let deltas = machine.drain_deltas();
                println!("{}:   got {}, {} deltas", name, progress, deltas.len());
            }
        }
        let deltas = machine.drain_deltas();
        println!(
            "{}:   ignored previous output of {} matching events, {} deltas",
            name,
            progress,
            deltas.len()
        );
    } else {
        println!(
            "{}: logic is stateless, skipping {} previously seen events",
            name,
            offsets.size()
        );
    }
    let ignored = progress;

    // For a stateless machine we may not need to start at the previous offsets if it
    // tells us that it is only interested “in the last two weeks“, i.e. gaps are okay.
    let start_at = if machine.needs_state() {
        offsets.clone()
    } else if let Some(look_back) = machine.look_back() {
        // need to ensure that previous offsets are the minimum to avoid duplicates
        go_back(
            &name,
            &present,
            offsets.clone(),
            look_back,
            &subscriptions,
            &client,
        )
        .await?
    } else {
        offsets.clone()
    };
    println!(
        "{}: getting {} events to catch up to live data",
        name,
        &present - &start_at
    );
    let mut events = client
        .query_between(
            start_at,
            present.clone(),
            subscriptions.clone(),
            Order::Lamport,
        )
        .await?;
    while let Some(event) = events.next().await {
        offsets += &event;
        feed(&event, &mut machine, &mut progress, &mut errors);
        if progress % EVENTS_PER_TXN == 0 {
            let deltas = machine.drain_deltas();
            let d = deltas.len();
            println!("{}:   got {}, {} deltas", name, progress - ignored, d);
            send(&offsets, deltas)?;
        }
    }
    let deltas = machine.drain_deltas();
    println!(
        "{}:   got {}, {} deltas",
        name,
        progress - ignored,
        deltas.len()
    );
    send(&offsets, deltas)?;
    println!(
        "{}:   sent output from {} matching events",
        name,
        progress - ignored
    );

    println!("{}: switching to live events", name);
    let events = client
        .subscribe_from(present, subscriptions.to_vec())
        .await?
        .map(TickStream::Item)
        .chain(stream::iter(vec![TickStream::Stop]));
    let ticks = interval(Duration::from_secs(5)).map(|_| TickStream::Tick);

    let mut input = stream::select(events, ticks);

    let mut last_reported = progress;
    while let Some(elem) = input.next().await {
        match elem {
            TickStream::Item(event) => {
                offsets += &event;
                feed(&event, &mut machine, &mut progress, &mut errors);
            }
            TickStream::Tick => {
                if progress > last_reported {
                    // will kill this loop if the receiver went away
                    let deltas = machine.drain_deltas();
                    let d = deltas.len();
                    send(&offsets, deltas)?;
                    println!(
                        "{}:   sent {} events, {} deltas",
                        name,
                        progress - last_reported,
                        d
                    );
                    last_reported = progress;
                }
            }
            TickStream::Stop => {
                print!("{}: event stream stopped, exiting", name);
                break;
            }
        }
    }

    Ok(())
}

fn print_duration(d: Duration) -> String {
    let mut res = String::new();
    let mut secs = d.as_secs();
    if secs >= 86400 {
        let days = secs / 86400;
        secs %= 86400;
        write!(res, "{}d", days).unwrap();
    }
    if secs >= 3600 || !res.is_empty() {
        let hours = secs / 3600;
        secs %= 3600;
        write!(res, "{}h", hours).unwrap();
    }
    if secs >= 60 || !res.is_empty() {
        let mins = secs / 60;
        secs %= 60;
        write!(res, "{}m", mins).unwrap();
    }
    write!(res, "{}s", secs).unwrap();
    res
}

async fn go_back(
    name: &str,
    offsets: &OffsetMap,
    lower_bound: OffsetMap,
    duration: Duration,
    subscriptions: &[Subscription],
    client: &EventService,
) -> Result<OffsetMap> {
    // the logic is telling us that it only needs events since `look_back` ago, so let’s find those
    println!(
        "{}: determining set of events looking back {}",
        name,
        print_duration(duration)
    );
    let mut offsets = offsets.clone();
    let cutoff = (SystemTime::now() - duration)
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;

    let mut events = 0;
    client
        .query_between(
            lower_bound,
            offsets.clone(),
            subscriptions.to_vec(),
            Order::LamportReverse,
        )
        .await?
        .take_while(|ev| ready(u64::from(ev.timestamp) > cutoff))
        .for_each(|ev| {
            offsets -= &ev;
            events += 1;
            ready(())
        })
        .await;

    println!("{}:  went back {} events", name, events);
    Ok(offsets)
}
