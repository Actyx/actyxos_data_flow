# Live data export tool for ActyxOS

[ActyxOS](https://developer.actyx.com/docs/os/introduction) is a decentralized event sourcing
system for the factory shop-floor. One of the purposes of deploying apps in factories is to
gain business insights by reporting and visualizing what is going on on the shop-floor.

With this library it is straight-forward to export event data into SQL databases with code like
the following:

```rust
pub fn dashboard_logic<'a>(
    scope: &mut Scope<'a>,
) -> (
    Input<Event<MachineEvent>>,
    Flow<'a, DashboardEntry, Stateful>,
) {
    let (injector, events) = Flow::<Event<MachineEvent>, _>::new(scope);

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
```

The full project is in the `example` folder, the sample webapp that generates the
underlying events can be found in the `webapp` folder. Both can be deployed to
an ActyxOS node or run in developer mode, see the [quickstart guide](https://developer.actyx.com/docs/quickstart).

## Building the Docker app

Build the binary and docker image on Linux with

    cd machine-dashboard-app
    ./build.sh

then package it into an ActyxOS app and deploy it with

    ax apps package
    ax apps deploy --local com.actyx.data_flow.machine-dashboard-1.0.0-x86_64.tar.gz localhost
    ax apps start --local com.actyx.data_flow.machine-dashboard localhost
    ax logs tail --local -f localhost

## Further steps

From the example you can go and explore more dataflow transformations offered by the
library as well as connect to PostgreSQL or Microsoft SQL Server databases to store
the transformation results.

The DB driver code is also commented and allows extensions for other databases.

If you want to build on Windows or for other architectures, you may take a look at the
[Rust Docker image](https://hub.docker.com/_/rust).