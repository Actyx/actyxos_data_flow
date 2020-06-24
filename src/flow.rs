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
//! Opinionated simplification of the differential-dataflow API
//!
//! Differential Dataflow provides great flexibility in terms of time tracking, multiplicity
//! tracking, etc. This comes at the cost of many type parameters and lower quality type
//! inference and tab completions, in particular in IDEs. This module fixes most of the type
//! parameters, leaving open only the type of data in a collection. It also contrains the
//! signature of the user-provided closures to reject unsuitable data (for example non-static
//! references) at their place of introduction instead of presenting the error when trying to
//! transform the resultant collection.
//!
//! The general shape of a differential dataflow remains the same:
//!
//! ```rust
//! use actyxos_data_flow::flow::{Scope, Flow, Input, Stateless};
//!
//! fn mk_logic<'a>(scope: &mut Scope<'a>) -> (Input<String>, Flow<'a, usize, Stateless>) {
//!     let (input, flow) = Flow::new(scope);
//!     let out = flow.map(|s: String| s.len());
//!     (input, out)
//! }
//! ```
//!
//! Note how the returned flow tracks the information of whether stateful combinators are
//! used. There is a [`.map_mut()`](struct.Flow.html#method.map_mut) method that allows a
//! stateful closure to be passed in, which will make the example not compile unless also
//! switching the declared output type to `Stateful`.
use differential_dataflow::{
    collection::{AsCollection, Collection},
    input::{Input as _, InputSession},
    operators::{
        arrange::{
            agent::TraceAgent,
            arrangement::{ArrangeByKey, Arranged},
        },
        count::CountTotal,
        join::JoinCore,
        reduce::ReduceCore,
        threshold::ThresholdTotal,
    },
    trace::implementations::{ord::OrdValBatch, spine_fueled_neu::Spine},
    ExchangeData, Hashable,
};
use std::{
    collections::BTreeMap, marker::PhantomData, rc::Rc, sync::mpsc::Receiver, time::Duration,
};
use timely::{
    communication::allocator::Thread,
    dataflow::{
        operators::{capture::Event, probe::Handle, Capture, Map},
        scopes::Child,
    },
    worker::Worker,
};

/// Top-level scope type where flows usually are created in.
pub type Scope<'a> = Child<'a, Worker<Thread>, usize>;

/// An input to a Flow
///
/// An `Input` is the entry point by which data records enter a [`Flow`](struct.Flow.html).
/// Both of these are created together by the [`Flow::new`](struct.Flow.html#method.new) method.
pub struct Input<T: ExchangeData>(InputSession<usize, T, isize>, Option<Duration>);

impl<T: ExchangeData> Input<T> {
    /// Advance the input timestamp to the given value
    ///
    /// This is usually done after ingesting a batch of data, followed by a [`.flush()`](#method.flush)
    /// to send the ingested collection elements through the flow and have them generate their
    /// deltas.
    pub fn advance_to(&mut self, time: usize) {
        self.0.advance_to(time)
    }
    /// Flush this input’s inserted elements into the collection
    ///
    /// The worker executing this flow can then be stepped until the resulting deltas
    /// reach their designated outputs.
    pub fn flush(&mut self) {
        self.0.flush()
    }
    /// Insert a new element into the collection
    pub fn insert(&mut self, value: T) {
        self.0.insert(value)
    }
    /// Remove an element from the collection
    pub fn remove(&mut self, value: T) {
        self.0.remove(value)
    }
    /// Query this input’s desired look back interval
    ///
    /// In general, correct function of a flow requires that all data are inserted so that all desired
    /// outputs are computed. When restarting this process, ingesting all data from the beginning can
    /// be quite time-consuming. It is not uncommon that the logic expressed by the flow does not care
    /// about elements of arbitrary age to correctly compute deltas for new (current) elements.
    ///
    /// For example, in a factory each production order is only relevant for a few days or weeks,
    /// matching the timespan needed to fulfil that order. Or an operations dashboard may focus on
    /// the behavior of the system over the past 24 hours and thus inputs from one week ago have no
    /// influence anymore on what shall be on the screens.
    ///
    /// In these cases, limited look back is a suitable performance optimization to speed up system
    /// restarts. When full precision is paramount, this shall be set to `None` to always recompute
    /// the full state of the flow after a restart.
    ///
    /// This parameter can be set by using [`Flow::new_limited`](struct.Flow.html#method.new_limited).
    pub fn look_back(&self) -> Option<Duration> {
        self.1
    }
}

/// An output of a dataflow graph
///
/// This handle gives access to the stream of updates emerging from the [`Flow`](struct.Flow.html)
/// that produced this output with [`Flow::output`](struct.Flow.html#method.output).
pub struct Output<T: ExchangeData>(Receiver<Event<usize, (T, usize, isize)>>);

impl<T: ExchangeData> Output<T> {
    /// Drain all deltas accumulated in this output
    ///
    /// This is typically called after advancing the time on all inputs and flushing the
    /// changes through the flow by stepping the worker until the new time appears on the
    /// outputs.
    pub fn msgs<'a>(&'a mut self) -> impl Iterator<Item = (T, isize)> + 'a {
        self.0.try_iter().flat_map(|elem| {
            if let Event::Messages(_, msgs) = elem {
                msgs.into_iter().map(|(msg, _, mult)| (msg, mult)).collect()
            } else {
                vec![]
            }
        })
    }
}

/// A probe measuring the propagation of progress within the dataflow
///
/// It is created by the [`Flow::probe`](struct.Flow.html#method.probe) method.
pub struct Probe(Handle<usize>);

impl Probe {
    /// Check whether this probe has not yet seen the given time
    pub fn less_than(&self, time: usize) -> bool {
        self.0.less_than(&time)
    }
}

/// Marker trait that tracks whether a Flow needs to keep state.
pub trait NeedsState {
    fn needs_state() -> bool;
}
/// Marker for stateless flows that do not need to be warmed up with previous inputs
pub struct Stateless {}
impl NeedsState for Stateless {
    fn needs_state() -> bool {
        false
    }
}
/// Marker for stateful flows that need to see previous inputs again after restart
pub struct Stateful {}
impl NeedsState for Stateful {
    fn needs_state() -> bool {
        true
    }
}

/// Differential dataflow [`Collection`](https://docs.rs/differential-dataflow/latest/differential_dataflow/collection/struct.Collection.html)
/// wrapper
///
/// This wrapper type fixes the timestamp type to `usize`, the multiplicity to `isize`
/// and the scope to the level below a `Worker`. These choices present restrictions that
/// we find commonly useful for a certain class of problems when ingesting [ActyxOS](https://developer.actyx.com/docs/os/introduction)
/// events and turning them into database rows.
///
/// Flows are constructed from a scope like this:
/// ```
/// use actyxos_data_flow::flow::{Scope, Flow, Input, Stateless};
///
/// fn mk_logic<'a>(scope: &mut Scope<'a>) -> (Input<String>, Flow<'a, usize, Stateless>) {
///     let (input, flow) = Flow::<String, _>::new(scope);
///     let out = flow.map(|s| s.len());
///     (input, out)
/// }
/// ```
///
/// When the flow’s calculations depend only on a limited amount of historical data after a
/// restart, you may use the [`look_back`](struct.Input.html#method.look_back) feature of
/// then input collection:
/// ```
/// # use actyxos_data_flow::flow::{Scope, Flow, Input, Stateless};
/// use std::time::Duration;
/// # fn mk_logic<'a>(scope: &mut Scope<'a>) -> (Input<String>, Flow<'a, usize, Stateless>) {
/// let (input, flow) = Flow::<String, _>::new_limited(scope, Duration::from_secs(3600));
/// #    let out = flow.map(|s| s.len());
/// #    (input, out)
/// # }
/// ```
pub struct Flow<'a, T: ExchangeData, St: NeedsState>(
    Collection<Child<'a, Worker<Thread>, usize>, T, isize>,
    PhantomData<St>,
);

impl<'a, T: ExchangeData> Flow<'a, T, Stateless> {
    /// Create a new flow within the given scope
    pub fn new(scope: &mut Child<'a, Worker<Thread>, usize>) -> (Input<T>, Flow<'a, T, Stateless>) {
        let (input, collection) = scope.new_collection();
        (Input(input, None), Flow(collection, PhantomData))
    }

    /// Create a new flow with limited look_back period within the given scope
    ///
    /// see also [`Input::look_back`](struct.Input.html#method.look_back)
    pub fn new_limited(
        scope: &mut Child<'a, Worker<Thread>, usize>,
        look_back: Duration,
    ) -> (Input<T>, Flow<'a, T, Stateless>) {
        let (input, collection) = scope.new_collection();
        (Input(input, Some(look_back)), Flow(collection, PhantomData))
    }
}

impl<'a, T: ExchangeData, St: NeedsState> Flow<'a, T, St> {
    /// Filter this collection with the given predicate
    pub fn filter(&self, f: impl Fn(&T) -> bool + 'static) -> Self {
        Self(self.0.filter(f), PhantomData)
    }

    /// Filter this collection with the given stateful predicate
    pub fn filter_mut(&self, f: impl FnMut(&T) -> bool + 'static) -> Flow<'a, T, Stateful> {
        Flow(self.0.filter(f), PhantomData)
    }

    /// Transform this collection’s elements 1:1
    pub fn map<U: ExchangeData>(&self, f: impl Fn(T) -> U + 'static) -> Flow<'a, U, St> {
        Flow(self.0.map(f), PhantomData)
    }

    /// Transform this collection’s elements 1:1 with a stateful function
    pub fn map_mut<U: ExchangeData>(
        &self,
        f: impl FnMut(T) -> U + 'static,
    ) -> Flow<'a, U, Stateful> {
        Flow(self.0.map(f), PhantomData)
    }

    /// Transform this collection’s elements 1:1 while keeping the same type
    pub fn map_in_place(&self, f: impl Fn(&mut T) + 'static) -> Self {
        Self(self.0.map_in_place(f), PhantomData)
    }

    /// Transform this collection’s elements 1:1 with a stateful function while keeping the same type
    pub fn map_in_place_mut(&self, f: impl FnMut(&mut T) + 'static) -> Flow<'a, T, Stateful> {
        Flow(self.0.map_in_place(f), PhantomData)
    }

    /// Transform this collection’s elements 1:many
    pub fn flat_map<U, I>(&self, f: impl Fn(T) -> I + 'static) -> Flow<'a, U, St>
    where
        U: ExchangeData,
        I: IntoIterator<Item = U>,
    {
        Flow(self.0.flat_map(f), PhantomData)
    }

    /// Transform this collection’s elements 1:many with a stateful function
    pub fn flat_map_mut<U, I>(&self, f: impl FnMut(T) -> I + 'static) -> Flow<'a, U, Stateful>
    where
        U: ExchangeData,
        I: IntoIterator<Item = U>,
    {
        Flow(self.0.flat_map(f), PhantomData)
    }

    /// Retain only the maximum element for each key computed by the given function
    ///
    /// This function is an optimization over using `.group().max()` in that it does
    /// not retain the elements previously added to the collection. Therefore it cannot
    /// deal with the situation that the currently known maximum for a group is removed.
    pub fn monotonic_max_by<K: ExchangeData>(
        &self,
        f: impl Fn(&T) -> K + 'static,
    ) -> Flow<'a, T, Stateful> {
        let mut highest = BTreeMap::new();
        Flow(
            self.0
                .inner
                .flat_map(move |(mut data, time, delta)| {
                    let key = f(&data);
                    if let Some(max) = highest.get_mut(&key) {
                        if &data > max {
                            std::mem::swap(&mut data, max);
                            vec![(data, time, -1), (max.clone(), time, 1)]
                        } else {
                            assert!(
                                &data != max || delta >= 0,
                                "cannot remove max element {:?} from monotonic_max_by",
                                data,
                            );
                            vec![]
                        }
                    } else {
                        highest.insert(key, data.clone());
                        vec![(data, time, 1)]
                    }
                })
                .as_collection(),
            PhantomData,
        )
    }

    /// Retain only one representative for each key computed by the given function
    ///
    /// This function is an optimization over using `.group().min()` in that it does
    /// not retain the elements previously added to the collection. Therefore it cannot
    /// deal with the situation that the chosen representative is removed.
    ///
    /// The chosen representative is the first element to be seen for each key.
    pub fn monotonic_representative_by<K: ExchangeData>(
        &self,
        f: impl Fn(&T) -> K + 'static,
    ) -> Flow<'a, T, Stateful> {
        let mut repr = BTreeMap::<K, (T, isize)>::new();
        Flow(
            self.0
                .inner
                .flat_map(move |(data, time, delta)| {
                    let key = f(&data);
                    if let Some(repr) = repr.get_mut(&key) {
                        let (prev, mult) = repr;
                        if prev == &data {
                            *mult += delta;
                            assert!(
                                *mult != 0,
                                "cannot remove representative {:?} from collection",
                                data
                            );
                            vec![]
                        } else {
                            vec![]
                        }
                    } else {
                        repr.insert(key, (data.clone(), 1));
                        vec![(data, time, 1)]
                    }
                })
                .as_collection(),
            PhantomData,
        )
    }

    /// Turn additions into removals and vice versa
    pub fn negate(&self) -> Self {
        Self(self.0.negate(), PhantomData)
    }

    /// Arrange this collection according to the computed keys
    ///
    /// This function is used to access the join, reduce, etc. methods of the [`Grouped`](struct.Grouped.html)
    /// type, it has no inherent value by itself.
    pub fn group_by<K: ExchangeData + Hashable>(
        &self,
        mut f: impl FnMut(&T) -> K + 'static,
    ) -> Grouped<'a, K, T> {
        Grouped(
            self.0.map(move |t| (f(&t), t)).arrange_by_key(),
            PhantomData,
        )
    }

    /// Inspect elements as they flow through the underlying timely dataflow stream
    pub fn inspect(&self, f: impl Fn(&(T, usize, isize)) + 'static) -> Self {
        Self(self.0.inspect(f), PhantomData)
    }

    /// Inspect elements as they flow through the underlying timely dataflow stream
    /// using a stateful function
    pub fn inspect_mut(
        &self,
        f: impl FnMut(&(T, usize, isize)) + 'static,
    ) -> Flow<'a, T, Stateful> {
        Flow(self.0.inspect(f), PhantomData)
    }

    /// Attach a probe to this collection to check the propagation of input timestamps
    pub fn probe(&self) -> Probe {
        Probe(self.0.probe())
    }

    /// Turn this flow into an output to be consumed by a machine
    ///
    /// see also [`Machine`](../machine/struct.Machine.html)
    pub fn output(&self) -> Output<T> {
        Output(self.0.inner.capture())
    }
}

impl<'a, T: ExchangeData> Flow<'a, T, Stateless> {
    /// Compute the union with the other flow
    pub fn concat<St: NeedsState>(&self, other: &Flow<'a, T, St>) -> Flow<'a, T, St> {
        Flow(self.0.concat(&other.0), PhantomData)
    }

    /// Compute the union with many other flows
    pub fn concat_many<St: NeedsState>(
        &self,
        others: impl IntoIterator<Item = Flow<'a, T, St>>,
    ) -> Flow<'a, T, St> {
        Flow(
            self.0.concatenate(others.into_iter().map(|x| x.0)),
            PhantomData,
        )
    }
}

impl<'a, T: ExchangeData> Flow<'a, T, Stateful> {
    /// Compute the union with the other flow
    pub fn concat<St: NeedsState>(&self, other: &Flow<'a, T, St>) -> Flow<'a, T, Stateful> {
        Flow(self.0.concat(&other.0), PhantomData)
    }

    /// Compute the union with many other flows
    pub fn concat_many<St: NeedsState>(
        &self,
        others: impl IntoIterator<Item = Flow<'a, T, St>>,
    ) -> Flow<'a, T, Stateful> {
        Flow(
            self.0.concatenate(others.into_iter().map(|x| x.0)),
            PhantomData,
        )
    }
}

impl<'a, T: ExchangeData + Hashable, St: NeedsState> Flow<'a, T, St> {
    /// Reduce the multiplicity of each element in this flow to 1
    pub fn distinct(&self) -> Flow<'a, T, Stateful> {
        Flow(self.0.distinct_total(), PhantomData)
    }

    /// Transform the multiplicity of each element in this flow with the given function
    pub fn threshold(
        &self,
        mut f: impl FnMut(&T, isize) -> isize + 'static,
    ) -> Flow<'a, T, Stateful> {
        Flow(self.0.threshold_total(move |k, r| f(k, *r)), PhantomData)
    }

    /// Count the number of elements in this collection
    pub fn count(&self) -> Flow<'a, (T, isize), Stateful> {
        Flow(self.0.count_total(), PhantomData)
    }
}

impl<'a, K: ExchangeData + Hashable, V: ExchangeData, St: NeedsState> Flow<'a, (K, V), St> {
    /// Group this flow of K-V pairs by the first element (the key) of the pair
    pub fn group(&self) -> Grouped<'a, K, V> {
        Grouped(self.0.arrange_by_key(), PhantomData)
    }
}

/// An arrangement of a collection by key
///
/// The collection is partitioned by key and stored in-memory so that it can be manipulated
#[allow(clippy::type_complexity)]
pub struct Grouped<'a, K, V>(
    Arranged<
        Child<'a, Worker<Thread>, usize>,
        TraceAgent<Spine<K, V, usize, isize, Rc<OrdValBatch<K, V, usize, isize>>>>,
    >,
    PhantomData<(K, V)>,
)
where
    K: ExchangeData + Hashable,
    V: ExchangeData;

impl<'a, K, V> Grouped<'a, K, V>
where
    K: ExchangeData + Hashable,
    V: ExchangeData,
{
    /// Join this collection with another that uses the same key, combining values with the given 1:many function
    pub fn join<V2, L, D, I>(&self, other: &Grouped<'a, K, V2>, f: L) -> Flow<'a, D, Stateful>
    where
        V2: ExchangeData,
        D: ExchangeData,
        I: IntoIterator<Item = D>,
        L: FnMut(&K, &V, &V2) -> I + 'static,
    {
        Flow(self.0.join_core(&other.0, f), PhantomData)
    }

    /// Join this collection with another that uses the same key, combining values with the given 1:1 function
    pub fn join_single<V2, L, D>(
        &self,
        other: &Grouped<'a, K, V2>,
        mut f: L,
    ) -> Flow<'a, D, Stateful>
    where
        V2: ExchangeData,
        D: ExchangeData,
        L: FnMut(&K, &V, &V2) -> D + 'static,
    {
        Flow(
            self.0
                .join_core(&other.0, move |k, v, v2| std::iter::once(f(k, v, v2))),
            PhantomData,
        )
    }

    /// Reduce each per-key collection to a vector of output values
    pub fn reduce<V2, L>(&self, f: L) -> Grouped<'a, K, V2>
    where
        V2: ExchangeData,
        L: FnMut(&K, &[(&V, isize)], &mut Vec<(V2, isize)>) + 'static,
    {
        Grouped(self.0.reduce_abelian("Reduce", f), PhantomData)
    }

    /// Transform the multiplicity of each collection element
    pub fn threshold(&self, mut f: impl FnMut(&K, &V, isize) -> isize + 'static) -> Self {
        self.reduce(move |k, i, o| o.extend(i.iter().map(|(v, m)| ((**v).clone(), f(k, v, *m)))))
    }

    /// Set the multiplicity of each collection element to 1
    pub fn distinct(&self) -> Self {
        self.threshold(|_, _, _| 1)
    }

    /// Count the number of elements per key
    pub fn count(&self) -> Grouped<'a, K, isize> {
        self.reduce(|_, i, o| o.push((i.iter().map(|x| x.1).sum(), 1)))
    }

    /// Compute the minimum element per key
    pub fn min(&self) -> Self {
        self.reduce(|_, i, o| o.push((i[0].0.clone(), 1)))
    }

    /// Compute the maximum element per key
    pub fn max(&self) -> Self {
        self.reduce(|_, i, o| o.push((i[i.len() - 1].0.clone(), 1)))
    }

    /// Compute the maximum element per key, sorting by the result of applying the given function to each value
    pub fn max_by<T, F>(&self, f: F) -> Self
    where
        F: Fn(&V) -> T + 'static + Clone,
        T: Ord,
    {
        self.reduce(move |_, i, o| {
            o.push((
                i.iter().map(|x| x.0.clone()).max_by_key(f.clone()).unwrap(),
                1,
            ))
        })
    }

    /// Ungroup by discarding the key
    pub fn ungroup(&self) -> Flow<'a, V, Stateful> {
        self.ungroup_with(|_, v| v.clone())
    }

    /// Ungroup by computing the new element from key and value
    pub fn ungroup_with<T: ExchangeData>(
        &self,
        f: impl FnMut(&K, &V) -> T + 'static,
    ) -> Flow<'a, T, Stateful> {
        Flow(self.0.as_collection(f), PhantomData)
    }

    /// Ungroup to a collection of key-value pairs
    pub fn ungroup_both(&self) -> Flow<'a, (K, V), Stateful> {
        self.ungroup_with(|k, v| (k.clone(), v.clone()))
    }

    /// Rearrange this collection by a different key
    pub fn regroup<K2, V2, L>(&self, f: L) -> Grouped<'a, K2, V2>
    where
        K2: ExchangeData + Hashable,
        V2: ExchangeData,
        L: FnMut(&K, &V) -> (K2, V2) + 'static,
    {
        self.ungroup_with(f).group()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine::{Inputs, Machine};
    use anyhow::Result;

    impl Inputs for Input<i32> {
        type Elem = i32;
        fn advance_clock(&mut self, time: usize) {
            self.advance_to(time);
            self.flush();
        }
        fn feed(&mut self, input: &Self::Elem) -> Result<()> {
            self.insert(*input);
            Ok(())
        }
    }

    #[test]
    fn monotonic_max_by() {
        let mut machine = Machine::new(|scope| {
            let (handle, coll) = Flow::<i32, _>::new(scope);
            let out = coll.monotonic_max_by(|x| *x % 5);
            (handle, out)
        });
        machine.assert(&[1], &[(1, 1)]);
        machine.assert(&[1], &[]);
        machine.assert(&[11, 2], &[(1, -1), (2, 1), (11, 1)]);
        machine.assert(&[6, 7], &[(2, -1), (7, 1)]);
    }

    #[test]
    fn monotonic_representative_by() {
        let mut machine = Machine::new(|scope| {
            let (handle, coll) = Flow::<i32, _>::new(scope);
            let out = coll.monotonic_representative_by(|x| *x % 5);
            (handle, out)
        });
        machine.assert(&[1], &[(1, 1)]);
        machine.assert(&[1], &[]);
        machine.assert(&[11, 2], &[(2, 1)]);
        machine.assert(&[6, 7], &[]);
    }
}
