/*
 * Copyright 2020 Actyx AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//! Live data export tool for ActyxOS
//!
//! [ActyxOS](https://developer.actyx.com/docs/os/introduction) is a decentralized event sourcing
//! system for the factory shop-floor. One of the purposes of deploying apps in factories is to
//! gain business insights by reporting and visualizing what is going on on the shop-floor.
//!
//! With this library it is straight-forward to export event data into SQL databases with code like
//! the following:
//!
//! ```rust
//! pub fn dashboard_logic<'a>(
//!     scope: &mut Scope<'a>,
//! ) -> (
//!     Input<Event<MachineEvent>>,
//!     Flow<'a, DashboardEntry, Stateful>,
//! ) {
//!     let (injector, events) = Flow::<Event<MachineEvent>, _>::new(scope);
//!
//!     let out = events
//!         .filter(|ev| ev.stream.name.as_str().starts_with("Drill"))
//!         .map(|ev| match ev.payload {
//!             MachineEvent::Started { order } => {
//!                 DashboardEntry::working(ev.stream.name.to_string(), order, ev.timestamp)
//!             }
//!             MachineEvent::Stopped { .. } => {
//!                 DashboardEntry::idle(ev.stream.name.to_string(), ev.timestamp)
//!             }
//!         })
//!         .group_by(|entry| entry.machine.clone())
//!         .max_by(|entry| entry.since)
//!         .ungroup();
//!
//!     (injector, out)
//! }
//! ```
//!
//! The full project is in the `example` folder, the sample webapp that generates the
//! underlying events can be found in the `webapp` folder. Both can be deployed to
//! an ActyxOS node or run in developer mode, see the [quickstart guide](https://developer.actyx.com/docs/quickstart).
//!
//! See also the [blog post](http://developer.actyx.com/blog/2020/06/25/differential-dataflow) for more background information.

pub mod coll;
pub mod db;
pub mod flow;
pub mod machine;
pub mod runner;
