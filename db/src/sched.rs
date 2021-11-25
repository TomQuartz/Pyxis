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

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

use super::cycles;
use super::rpc;
use super::task::Task;
use super::task::TaskPriority;
use super::task::TaskState::*;

use e2d2::common::EmptyMetadata;
use e2d2::headers::IpHeader;
use e2d2::interface::Packet;

use spin::RwLock;

// use super::dispatch::Resp;

use e2d2::allocators::CacheAligned;
use e2d2::interface::{PacketTx, PortQueue};
use std::rc::Rc;

// /// The number of resp packets to send in one go
// const TX_PACKETS_THRESH: usize = 8;

// /// The maximum interval between two resp packets transmission, 8us
// const TX_CYCLES_THRESH: u64 = 2400 * 8;

/// The maximum number of tasks the dispatcher can take in one go.
const MAX_RX_PACKETS: usize = 32;

/// Interval in microsecond which each task can use as credit to perform CPU work.
/// Under load shedding, the task which used more than this credit will be pushed-back.
const CREDIT_LIMIT_US: f64 = 0.5f64;

// #[cfg(feature = "queue_len")]
// struct Avg {
//     counter: f64,
//     lastest: f64,
//     E_x: f64,
//     E_x2: f64,
// }
// #[cfg(feature = "queue_len")]
// impl Avg {
//     fn new() -> Avg {
//         Avg {
//             counter: 0.0,
//             lastest: 0.0,
//             E_x: 0.0,
//             E_x2: 0.0,
//         }
//     }
//     fn update(&mut self, delta: f64) {
//         self.counter += 1.0;
//         self.lastest = delta;
//         self.E_x = self.E_x * ((self.counter - 1.0) / self.counter) + delta / self.counter;
//         self.E_x2 =
//             self.E_x2 * ((self.counter - 1.0) / self.counter) + delta * delta / self.counter;
//     }
// }

struct TimeAvg {
    time_avg: f64,
    elapsed: f64,
    last_data: f64,
    last_time: u64,
    // #[cfg(feature = "queue_len")]
    // avg: Avg,
}

impl TimeAvg {
    fn new() -> TimeAvg {
        TimeAvg {
            time_avg: 0.0,
            elapsed: 0.0,
            last_data: 0.0,
            last_time: 0,
            // #[cfg(feature = "queue_len")]
            // avg: Avg::new(),
        }
    }
    fn update(&mut self, current_time: u64, delta: f64) {
        let elapsed = (current_time - self.last_time) as f64;
        self.last_time = current_time;
        let delta_avg = (self.last_data + delta) / 2.0;
        self.last_data = delta;
        self.elapsed += elapsed;
        let update_ratio = elapsed / self.elapsed;
        self.time_avg = self.time_avg * (1.0 - update_ratio) + delta_avg * update_ratio;
        // #[cfg(feature = "queue_len")]
        // self.avg.update(delta_avg);
    }
    fn avg(&self) -> f64 {
        self.time_avg
    }
}

struct MovingTimeAvg {
    moving_avg: f64,
    elapsed: f64,
    last_data: f64,
    last_time: u64,
    norm: f64,
    moving_exp: f64,
    // #[cfg(feature = "queue_len")]
    // avg: Avg,
}
impl MovingTimeAvg {
    fn new(moving_exp: f64) -> MovingTimeAvg {
        MovingTimeAvg {
            moving_avg: 0.0,
            elapsed: 0.0,
            last_data: 0.0,
            last_time: 0,
            norm: 0.0,
            moving_exp: moving_exp,
            // #[cfg(feature = "queue_len")]
            // avg: Avg::new(),
        }
    }
    fn update(&mut self, current_time: u64, delta: f64) {
        let elapsed = (current_time - self.last_time) as f64;
        self.last_time = current_time;
        let delta_avg = (self.last_data + delta) / 2.0;
        self.last_data = delta;
        self.elapsed += elapsed;
        let update_ratio = self.moving_exp * elapsed / self.elapsed;
        self.moving_avg = self.moving_avg * (1.0 - update_ratio) + delta_avg * update_ratio;
        self.norm = self.norm * (1.0 - update_ratio) + update_ratio;
        // #[cfg(feature = "queue_len")]
        // self.avg.update(delta_avg);
    }
    fn avg(&self) -> f64 {
        self.moving_avg / self.norm
    }
}

/// A simple round robin scheduler for Tasks in Sandstorm.
pub struct RoundRobin {
    // The time-stamp at which the scheduler last ran. Required to identify whether there is an
    // uncooperative task running on the scheduler.
    latest: AtomicUsize,

    // Atomic flag indicating whether there is a malicious/long running procedure on this
    // scheduler. If true, the scheduler must return down to Netbricks on the next call to poll().
    compromised: AtomicBool,

    // Identifier of the thread this scheduler is running on. Required for pre-emption.
    thread: AtomicUsize,

    // Identifier of the core this scheduler is running on. Required for pre-emption.
    core: AtomicIsize,

    // Run-queue of tasks waiting to execute. Tasks on this queue have either yielded, or have been
    // recently enqueued and never run before.
    waiting: RwLock<VecDeque<Box<Task>>>,

    // Response packets returned by completed tasks. Will be picked up and sent out the network by
    // the Dispatch task.
    responses: RwLock<Vec<Packet<IpHeader, EmptyMetadata>>>,

    // /// last tx
    // pub last_tx: Cell<u64>,

    // task_completed is incremented after the completion of each task. Reset to zero
    // after every 1M tasks.
    task_completed: RefCell<u64>,
    // // the dispatcher in this roundrobin(FCFS) scheduler
    // dispatcher_port: CacheAligned<PortQueue>,
    queue_length: RefCell<TimeAvg>,
    #[cfg(feature = "queue_len")]
    pub timestamp: RefCell<Vec<u64>>,
    #[cfg(feature = "queue_len")]
    pub raw_length: RefCell<Vec<usize>>,
    #[cfg(feature = "queue_len")]
    pub terminate: AtomicBool,
}

// Implementation of methods on RoundRobin.
impl RoundRobin {
    /// Creates and returns a round-robin scheduler that can run tasks implementing the `Task`
    /// trait.
    ///
    /// # Arguments
    ///
    /// * `thread`: Identifier of the thread this scheduler will run on.
    /// * `core`:   Identifier of the core this scheduler will run on.
    pub fn new(thread: u64, core: i32) -> RoundRobin {
        RoundRobin {
            latest: AtomicUsize::new(cycles::rdtsc() as usize),
            compromised: AtomicBool::new(false),
            thread: AtomicUsize::new(thread as usize),
            core: AtomicIsize::new(core as isize),
            waiting: RwLock::new(VecDeque::new()),
            responses: RwLock::new(Vec::new()),
            task_completed: RefCell::new(0),
            // dispatcher_port: portq.clone(),
            // last_tx: Cell::new(cycles::rdtsc()),
            queue_length: RefCell::new(TimeAvg::new()),
            #[cfg(feature = "queue_len")]
            timestamp: RefCell::new(Vec::with_capacity(12800000 as usize)),
            #[cfg(feature = "queue_len")]
            raw_length: RefCell::new(Vec::with_capacity(12800000 as usize)),
            #[cfg(feature = "queue_len")]
            terminate: AtomicBool::new(false),
        }
    }

    /// Enqueues a task onto the scheduler. The task is enqueued at the end of the schedulers
    /// queue.
    ///
    /// # Arguments
    ///
    /// * `task`: The task to be added to the scheduler. Must implement the `Task` trait.
    #[inline]
    pub fn enqueue(&self, task: Box<Task>) {
        self.waiting.write().push_back(task);
    }

    /// Enqueues multiple tasks onto the scheduler.
    ///
    /// # Arguments
    ///
    /// * `tasks`: A deque of tasks to be added to the scheduler. These tasks will be run in the
    ///            order that they are provided in, and must implement the `Task` trait.
    #[inline]
    pub fn enqueue_many(&self, mut tasks: VecDeque<Box<Task>>) {
        self.waiting.write().append(&mut tasks);
    }

    /// Dequeues all waiting tasks from the scheduler.
    ///
    /// # Return
    ///
    /// A deque of all waiting tasks in the scheduler. This tasks might be in various stages of
    /// execution. Some might have run for a while and yielded, and some might have never run
    /// before. If there are no tasks waiting to run, then an empty vector is returned.
    #[inline]
    pub fn dequeue_all(&self) -> VecDeque<Box<Task>> {
        let mut tasks = self.waiting.write();
        return tasks.drain(..).collect();
    }

    /// Returns a list of pending response packets.
    ///
    /// # Return
    ///
    /// A vector of response packets that were returned by tasks that completed execution. This
    /// packets should be sent out the network. If there are no pending responses, then an empty
    /// vector is returned.
    #[inline]
    pub fn responses(&self) -> Vec<Packet<IpHeader, EmptyMetadata>> {
        let mut responses = self.responses.write();
        return responses.drain(..).collect();
    }

    // #[inline]
    // fn pending_resps(&self) -> Option<Vec<Packet<IpHeader, EmptyMetadata>>> {
    //     let pending = self.responses.read().len();
    //     if pending >= TX_PACKETS_THRESH {
    //         self.last_tx.set(cycles::rdtsc());
    //         Some(self.responses.write().drain(..).collect())
    //     } else if pending > 0 {
    //         let now = cycles::rdtsc();
    //         if now - self.last_tx.get() > TX_CYCLES_THRESH {
    //             self.last_tx.set(now);
    //             Some(self.responses.write().drain(..).collect())
    //         } else {
    //             None
    //         }
    //     } else {
    //         None
    //     }
    // }

    // // no statistics, so that we may use &self instead of mut self
    // fn send_resps(&self, mut packets: Vec<Packet<IpHeader, EmptyMetadata>>) {
    //     unsafe {
    //         let mut mbufs = vec![];
    //         let num_packets = packets.len();

    //         // Extract Mbuf's from the batch of packets.
    //         while let Some(packet) = packets.pop() {
    //             mbufs.push(packet.get_mbuf());
    //         }

    //         // Send out the above MBuf's.
    //         match self.dispatcher_port.send(&mut mbufs) {
    //             Ok(sent) => {
    //                 if sent < num_packets as u32 {
    //                     warn!("Was able to send only {} of {} packets.", sent, num_packets);
    //                 }

    //                 // self.responses_sent += mbufs.len() as u64;
    //             }

    //             Err(ref err) => {
    //                 error!("Error on packet send: {}", err);
    //             }
    //         }
    //     }
    // }

    /// Appends a list of responses to the scheduler.
    ///
    /// # Arguments
    ///
    /// * `resps`: A vector of response packets parsed upto their IP headers.
    pub fn append_resps(&self, resps: &mut Vec<Packet<IpHeader, EmptyMetadata>>) {
        self.responses.write().append(resps);
    }

    /// Returns the time-stamp at which the latest scheduling decision was made.
    #[inline]
    pub fn latest(&self) -> u64 {
        self.latest.load(Ordering::Relaxed) as u64
    }

    /// Sets the compromised flag on the scheduler.
    #[inline]
    pub fn compromised(&self) {
        self.compromised.store(true, Ordering::Relaxed);
    }

    /// Returns the identifier of the thread this scheduler was configured to run on.
    #[inline]
    pub fn thread(&self) -> u64 {
        self.thread.load(Ordering::Relaxed) as u64
    }

    /// Returns the identifier of the core this scheduler was configured to run on.
    #[inline]
    pub fn core(&self) -> i32 {
        self.core.load(Ordering::Relaxed) as i32
    }

    /// Picks up a task from the waiting queue, and runs it until it either yields or completes.
    pub fn poll(&self) {
        let mut total_time: u64 = 0;
        let mut db_time: u64 = 0;
        let credit = (CREDIT_LIMIT_US / 1000000f64) * (cycles::cycles_per_second() as f64);
        let mut overhead_per_req: u64 = 0;

        // XXX: Trigger Pushback if the two dispatcher invocation is 20 us apart.
        let time_trigger: u64 = 2000 * credit as u64;
        let mut previous: u64 = cycles::rdtsc();
        let mut interval: u64 = 0;
        let mut avg_queue_len: f64 = -1.0;
        loop {
            // Set the time-stamp of the latest scheduling decision.
            let current = cycles::rdtsc();
            self.latest.store(current as usize, Ordering::Relaxed);

            // If the compromised flag was set, then return.
            if self.compromised.load(Ordering::Relaxed) {
                return;
            }

            #[cfg(feature = "queue_len")]
            // set by dispatcher upon recving terminate rpc
            if self.terminate.load(Ordering::Relaxed) {
                return;
            }

            // If there are tasks to run, then pick one from the head of the queue, and run it until it
            // either completes or yields back.
            let task = self.waiting.write().pop_front();
            // TODO: run dispatch first, profile dispatch time, and evenly distribute to all tasks with state=INITIALIZED
            if let Some(mut task) = task {
                let mut is_dispatcher: bool = false;
                let mut difference: u64 = 0;
                match task.priority() {
                    TaskPriority::DISPATCH => {
                        is_dispatcher = true;

                        // The time difference include the dispatcher time to account the native
                        // operations.
                        difference = current - previous;
                        interval = difference;
                        previous = current;
                    }

                    _ => {}
                }

                if is_dispatcher {
                    overhead_per_req = task.run().1;
                    // update queue length
                    let current_time = cycles::rdtsc();
                    let queue_length = self.waiting.read().len();
                    self.queue_length
                        .borrow_mut()
                        .update(current_time, queue_length as f64);
                    avg_queue_len = self.queue_length.borrow().avg();
                    self.waiting.write().push_back(task);
                    #[cfg(feature = "queue_len")]
                    if queue_length > 0 || self.timestamp.borrow().len() > 0 {
                        self.timestamp.borrow_mut().push(current_time);
                        // #[cfg(feature = "queue_len")]
                        self.raw_length.borrow_mut().push(queue_length);
                    }
                    continue;
                }
                // handle requests
                if task.state() == INITIALIZED {
                    task.set_time(overhead_per_req);
                }
                if task.run().0 == COMPLETED {
                    // The task finished execution, check for request and response packets. If they
                    // exist, then free the request packet, and enqueue the response packet.
                    if let Some((req, resps)) = unsafe { task.tear(&mut avg_queue_len) } {
                        trace!("task complete");
                        req.free_packet();
                        for resp in resps.into_iter() {
                            self.responses
                                .write()
                                .push(rpc::fixup_header_length_fields(resp));
                        }
                        // self.responses
                        //     .write()
                        //     .push(rpc::fixup_header_length_fields(res));
                    }
                    if cfg!(feature = "execution") {
                        total_time += task.time();
                        db_time += task.db_time();
                        let mut count = self.task_completed.borrow_mut();
                        *count += 1;
                        let every = 1000000;
                        if *count >= every {
                            info!("Total {}, DB {}", total_time / (*count), db_time / (*count));
                            *count = 0;
                            total_time = 0;
                            db_time = 0;
                        }
                    }
                } else {
                    // The task did not complete execution. EITHER add it back to the waiting list so that it
                    // gets to run again OR run the pushback mechanism. The pushback starts only after that
                    // dispatcher task execution. Trigger pushback:-
                    //
                    // if there are MAX_RX_PACKETS /4 yeilded tasks in the queue, OR
                    // if two dispatcher invocations are 2000 us apart, AND
                    // if the current dispatcher invocation received MAX_RX_PACKETS /4 new tasks.
                    /*
                    if cfg!(feature = "pushback")
                        && is_dispatcher == true
                        && (queue_length >= MAX_RX_PACKETS / 8 || difference > time_trigger)
                        && ((self.waiting.read().len() - queue_length) > 0)
                    {
                        for _i in 0..queue_length {
                            let mut yeilded_task = self.waiting.write().pop_front().unwrap();

                            // Compute Ranking/Credit on the go for each task to pushback
                            // some of the tasks whose rank/credit is more than the threshold.
                            if (yeilded_task.state() == YIELDED)
                                && ((yeilded_task.time() - yeilded_task.db_time()) > credit as u64)
                            {
                                yeilded_task.set_state(STOPPED);
                                if let Some((req, resps)) = unsafe { yeilded_task.tear() } {
                                    req.free_packet();
                                    for resp in resps.into_iter() {
                                        self.responses
                                            .write()
                                            .push(rpc::fixup_header_length_fields(resp));
                                    }
                                    // self.responses
                                    //     .write()
                                    //     .push(rpc::fixup_header_length_fields(res));
                                }
                            } else {
                                self.waiting.write().push_front(yeilded_task);
                            }
                        }
                    }
                    */
                    self.waiting.write().push_back(task);
                }
            }
        }
    }
}

// RoundRobin uses atomics and RwLocks. Hence, it is thread-safe. Need to explicitly mark it as
// Send and Sync here because the compiler does not do so. This is because Packet contains a *mut
// MBuf which is not Send and Sync. Similarly, the compiler appears to be having trouble with the
// "Task" trait object.
unsafe impl Send for RoundRobin {}
unsafe impl Sync for RoundRobin {}
