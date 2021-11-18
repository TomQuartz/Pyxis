/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use std::cell::Cell;
use std::ops::{Generator, GeneratorState};
use std::pin::Pin;

use super::cycles;
use super::task::TaskState::*;
use super::task::{Task, TaskPriority, TaskState};

use e2d2::common::EmptyMetadata;
use e2d2::headers::UdpHeader;
use e2d2::interface::Packet;

use sandstorm::common::PACKET_UDP_LEN;
use std::{mem, slice};
use wireformat::GetResponse;

// The expected type signature on a generator for a native operation (ex: get()). The return
// value is an optional tuple consisting of a request and response packet parsed/deparsed upto
// their UDP headers. This is to allow for operations that might not require a response packet
// such as garbage collection, logging etc. to be run as generators too.
type NativeGenerator = Pin<
    Box<
        Generator<
            Yield = u64,
            Return = Option<(
                Packet<UdpHeader, EmptyMetadata>,
                Vec<Packet<UdpHeader, EmptyMetadata>>,
            )>,
        >,
    >,
>;

/// A task corresponding to a native operation (like get() and put() requests).
pub struct Native {
    // The current execution state of the task. Required to determine if the task has completed
    // execution, or whether it needs to be scheduled to run on the CPU.
    state: TaskState,

    // The total amount of time for which the task has run on the CPU in cycles.
    time: u64,

    // The total amount of time for which the task has spent in DB in cycles.
    db_time: u64,

    // The priority of the task. Required to determine when the task must be allowed to run next.
    priority: TaskPriority,

    // The underlying generator for the task. Running the task effectively runs this generator.
    gen: NativeGenerator,

    // The result (if any) returned by the generator once it completes execution.
    req_res: Cell<
        Option<(
            Packet<UdpHeader, EmptyMetadata>,
            Vec<Packet<UdpHeader, EmptyMetadata>>,
        )>,
    >,
}

// Implementation of methods on Native.
impl Native {
    /// Constructs a new task for native operations.
    ///
    /// # Arguments:
    ///
    /// * `prio`:      The priority of the created task. Required by the scheduler.
    /// * `generator`: The generator for the task. Will be executed when the task is running.
    ///
    /// # Return:
    ///
    /// A Task containing a native operation that can be handed off to, and run by the scheduler.
    pub fn new(prio: TaskPriority, generator: NativeGenerator) -> Native {
        // The res field is initialized to None. It will be populated when the task has completed
        // execution.
        Native {
            state: INITIALIZED,
            time: 0,
            db_time: 0,
            priority: prio,
            gen: generator,
            req_res: Cell::new(None),
        }
    }
}

// Implementation of the Task trait on Native.
impl Task for Native {
    /// Refer to the Task trait for documentation.
    fn run(&mut self) -> (TaskState, u64) {
        let start = cycles::rdtsc();

        // Run the generator if need be.
        if self.state == INITIALIZED || self.state == YIELDED {
            self.state = RUNNING;

            match self.gen.as_mut().resume(()) {
                GeneratorState::Yielded(time) => {
                    self.db_time += time;
                    self.state = YIELDED;
                }

                GeneratorState::Complete(req_res) => {
                    self.req_res.set(req_res);
                    self.state = COMPLETED;
                }
            }
        }

        // Get the continuous time this task executed for.
        let exec = cycles::rdtsc() - start;

        // Update the total time this task has executed for and return.
        self.time += exec;

        return (self.state.clone(), exec);
    }

    /// Refer to the Task trait for documentation.
    fn state(&self) -> TaskState {
        self.state.clone()
    }

    /// Refer to the Task trait for documentation.
    fn time(&self) -> u64 {
        self.time.clone()
    }

    /// Refer to the Task trait for documentation.
    fn db_time(&self) -> u64 {
        self.db_time.clone()
    }

    /// Refer to the Task trait for documentation.
    fn priority(&self) -> TaskPriority {
        self.priority.clone()
    }

    /// Refer to the Task trait for documentation.
    unsafe fn tear(
        &mut self,
    ) -> Option<(
        Packet<UdpHeader, EmptyMetadata>,
        Vec<Packet<UdpHeader, EmptyMetadata>>,
    )> {
        let (req, resps) = self.req_res.replace(None).unwrap();
        let resps = resps
            .into_iter()
            .map(|p| {
                let mut p = p.parse_header::<GetResponse>();
                p.get_mut_header().common_header.duration = self.time;
                p.deparse_header(PACKET_UDP_LEN as usize)
            })
            .collect();
        // let mut get_resp = res.parse_header::<GetResponse>();
        // // add task duration to resp
        // get_resp.get_mut_header().common_header.duration = self.time;
        // // let time_ptr = &self.time as *const _ as *const u8;
        // // let time_u8 = unsafe { slice::from_raw_parts(time_ptr, mem::size_of::<u64>()) };
        // // get_resp
        // //     .add_to_payload_tail(time_u8.len(), time_u8)
        // //     .unwrap();
        // let res = get_resp.deparse_header(PACKET_UDP_LEN as usize);
        Some((req, resps))
    }

    /// Refer to the `Task` trait for Documentation.
    fn set_state(&mut self, state: TaskState) {
        self.state = state;
    }

    /// Refer to the `Task` trait for Documentation.
    fn update_cache(&mut self, _record: &[u8], _seg_id: usize, _num_segs: usize) -> bool {
        true
    }

    /// Refer to the `Task` trait for Documentation.
    fn get_id(&self) -> u64 {
        0
    }

    fn set_time(&mut self, overhead: u64) {
        // assert_eq!(self.state,INITIALIZED,"setting overhead for task not in INITIALIZED state");
        self.time = overhead;
    }
}
