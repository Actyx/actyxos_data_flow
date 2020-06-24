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
//! A wrapper for dataflow logic that knows how to feed inputs and get out deltas
use anyhow::Result;
use timely::communication::allocator::thread::Thread;
use timely::dataflow::scopes::Child;
use timely::worker::Worker;

use differential_dataflow::ExchangeData;

use super::{
    coll::Coll,
    flow::{Flow, NeedsState},
};
use crate::flow::{Input, Output, Probe};
use actyxos_sdk::event::{Event, Payload};
use serde::de::DeserializeOwned;
use std::{marker::PhantomData, time::Duration};

/// Encapsulation of a differential dataflow, managing inputs and outputs
///
/// The machine is parameterised with a function creating the business logic
/// which is run within the scope of a thread-based worker; the machine is
/// thus single-threaded.
///
/// The usage principle is that the inputs are used to feed data into the
/// machine. From time to time [`drain_deltas`](#method.drain_deltas) needs
/// to be called to flush inputs through the dataflow and extract the resulting
/// deltas. To this end, the machine manages the dataflow’s logical time:
/// each call to `drain_deltas` increments the time by one unit and then
/// steps the dataflow worker until the new time is seen at the output
/// [`Flow`](../flow/struct.Flow.html).
pub struct Machine<In: Inputs, Out: ExchangeData, St: NeedsState> {
    inputs: In,
    output: Output<Out>,
    probe: Probe,
    worker: Worker<Thread>,
    time: usize,
    ph: PhantomData<St>,
}

pub trait Inputs {
    type Elem;

    fn advance_clock(&mut self, time: usize);
    fn feed(&mut self, input: &Self::Elem) -> Result<()>;
    fn look_back(&self) -> Option<Duration> {
        None
    }
}

impl<T: ExchangeData + DeserializeOwned> Inputs for Input<Event<T>> {
    type Elem = Event<Payload>;
    fn advance_clock(&mut self, time: usize) {
        self.advance_to(time);
        self.flush();
    }
    fn feed(&mut self, input: &Event<Payload>) -> Result<()> {
        let ev = input.extract::<T>()?;
        self.insert(ev);
        Ok(())
    }
    fn look_back(&self) -> Option<Duration> {
        self.look_back()
    }
}

impl<I, T: Inputs<Elem = I>, U: Inputs<Elem = I>> Inputs for (T, U) {
    type Elem = I;
    fn advance_clock(&mut self, time: usize) {
        self.0.advance_clock(time);
        self.1.advance_clock(time);
    }
    fn feed(&mut self, input: &I) -> Result<()> {
        if self.0.feed(input).is_err() {
            self.1.feed(input)?;
        }
        Ok(())
    }
    fn look_back(&self) -> Option<Duration> {
        self.0
            .look_back()
            .and_then(|d0| self.1.look_back().map(|d1| d0.max(d1)))
    }
}

impl<I, T1: Inputs<Elem = I>, T2: Inputs<Elem = I>, T3: Inputs<Elem = I>> Inputs for (T1, T2, T3) {
    type Elem = I;
    fn advance_clock(&mut self, time: usize) {
        self.0.advance_clock(time);
        self.1.advance_clock(time);
        self.2.advance_clock(time);
    }
    fn feed(&mut self, input: &I) -> Result<()> {
        if self.0.feed(input).is_ok() {
            return Ok(());
        }
        if self.1.feed(input).is_ok() {
            return Ok(());
        }
        self.2.feed(input)?;
        Ok(())
    }
    fn look_back(&self) -> Option<Duration> {
        self.0.look_back().and_then(|d0| {
            self.1
                .look_back()
                .and_then(|d1| self.2.look_back().map(|d2| d0.max(d1.max(d2))))
        })
    }
}

impl<In: Inputs, Out: ExchangeData, St: NeedsState> Machine<In, Out, St> {
    /// Construct a new machine
    ///
    /// The given function receives a reference to a dataflow scope with limited
    /// lifetime, it can therefore only return the flow and not store it in external
    /// locations.
    pub fn new<F>(f: F) -> Self
    where
        F: for<'a> FnOnce(&mut Child<'a, Worker<Thread>, usize>) -> (In, Flow<'a, Out, St>),
    {
        let mut worker = Worker::new(Thread::new());
        let (inputs, output, probe) = worker.dataflow(|scope| {
            let (inp, outp) = f(scope);
            (inp, outp.output(), outp.probe())
        });
        Self {
            inputs,
            output,
            probe,
            worker,
            time: 0,
            ph: PhantomData,
        }
    }

    /// Returns true if the captured Flow uses at least one stateful operator
    pub fn needs_state(&self) -> bool {
        St::needs_state()
    }

    /// Returns the minimum look-back duration in case all input collections are limited
    ///
    /// cf. [`Flow::new_limited`](../flow/struct.Flow.html#method.new_limited)
    pub fn look_back(&self) -> Option<Duration> {
        self.inputs.look_back()
    }

    /// Get a reference to the inputs in order to feed them
    pub fn inputs(&mut self) -> &mut In {
        &mut self.inputs
    }

    /// Process all inputs fed since the last call to this method and return the resulting deltas
    pub fn drain_deltas(&mut self) -> Coll<Out, isize> {
        self.time += 1;
        let now = self.time;
        self.inputs.advance_clock(now);
        let worker = &mut self.worker;
        let probe = &self.probe;
        worker.step_while(|| probe.less_than(now));
        let mut deltas = Coll::new();
        for (ts, mult) in self.output.msgs() {
            deltas += (ts, mult);
        }
        deltas
    }

    #[cfg(test)]
    pub fn assert(&mut self, input: &[In::Elem], output: &[(Out, isize)]) {
        for i in input {
            self.inputs().feed(i).unwrap();
        }
        assert_eq!(self.drain_deltas().to_vec(), output);
    }
}
