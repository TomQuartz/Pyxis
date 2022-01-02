/* Copyright (c) 2019 University of Utah
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

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

use super::container::Container;
use super::dispatch::*;
use super::proxy::ProxyDB;

use db::config::{ComputeConfig, NetConfig};
use db::dispatch::{PacketHeaders, Queue, Sender};
use db::master::Master;
use db::task::{Task, TaskPriority, TaskState, TaskState::*};

use sandstorm::common::{self, *};
use util::model::GLOBAL_MODEL;

use db::cycles;
use db::e2d2::allocators::*;
use db::e2d2::common::EmptyMetadata;
use db::e2d2::headers::*;
use db::e2d2::interface::*;
use db::e2d2::scheduler::Executable;
use db::rpc::{self, *};
use db::sched::{CoeffOfVar, MovingTimeAvg};
use db::wireformat::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

const TASK_BUCKETS: usize = 32;

/// TaskManager handles the information for a pushed-back extension on the client side.
pub struct TaskManager {
    // A ref counted pointer to a master service. The master service
    // implements the primary interface to the database.
    master: Arc<Master>,

    // The reference to the task generator, which is used to suspend/resume the generator.
    pub ready: VecDeque<Box<Task>>,

    ///  The HashMap containing the waiting tasks.
    pub waiting: HashMap<u64, Box<Task>>,

    /// responses
    pub responses: Vec<Packet<IpHeader, EmptyMetadata>>,
}

impl TaskManager {
    /// This function creates and returns the TaskManager for the pushed back task. The client uses
    /// this object to resume the extension on the client side.
    ///
    /// # Arguments
    ///
    /// * `master_service`: A reference to a Master which will be used to construct tasks from received
    ///                    response.
    /// * `req`: A reference to the request sent by the client, it will be helpful in task creation
    ///          if the requested is pushed back.
    /// * `tenant_id`: Tenant id will be needed reuqest generation.
    /// * `name_len`: This will be useful in parsing the request and find out the argument for consecutive requests.
    /// * `timestamp`: This is unique-id for the request and consecutive requests will have same id.
    ///
    /// # Return
    ///
    /// A TaskManager for generator creation and task execution on the client.
    pub fn new(master_service: Arc<Master>) -> TaskManager {
        TaskManager {
            master: master_service,
            ready: VecDeque::with_capacity(32),
            waiting: HashMap::with_capacity(32),
            responses: vec![],
        }
    }

    /// This method creates a task for the extension on the client-side and add
    /// it to the task-manager.
    ///
    /// # Arguments
    /// * `sender_service`: A reference to the service which helps in the RPC request generation.
    pub fn create_task(
        &mut self,
        id: u64,
        // req: &[u8],
        req: Packet<InvokeRequest, EmptyMetadata>,
        resp: Packet<InvokeResponse, EmptyMetadata>,
        tenant: u32,
        name_length: usize,
        sender_service: Rc<Sender>,
        // ) -> Option<Box<Task>> {
    ) {
        let tenant_id: TenantId = tenant as TenantId;
        let name_length: usize = name_length as usize;

        // Read the extension's name from the request payload.
        let mut name = Vec::new();
        name.extend_from_slice(req.get_payload().split_at(name_length).0);
        let name: String = String::from_utf8(name).expect("ERROR: Failed to get ext name.");

        // Get the model for the given extension.
        let mut model = None;
        // If the extension doesn't need an ML model, don't waste CPU cycles in lookup.
        if cfg!(feature = "ml-model") {
            GLOBAL_MODEL.with(|a_model| {
                if let Some(a_model) = (*a_model).borrow().get(&name) {
                    model = Some(Arc::clone(a_model));
                }
            });
        }

        if let Some(ext) = self.master.extensions.get(tenant_id, name.clone()) {
            let db = Rc::new(ProxyDB::new(
                tenant,
                id,
                req,
                resp,
                name_length as usize,
                sender_service,
                model,
            ));
            // self.waiting.insert(
            //     id,
            //     Box::new(Container::new(TaskPriority::REQUEST, db, ext, id)),
            // );
            self.ready
                .push_back(Box::new(Container::new(TaskPriority::REQUEST, db, ext, id)))
            // return Some(Box::new(Container::new(TaskPriority::REQUEST, db, ext, id)));
        } else {
            info!("Unable to create a generator for this ext of name {}", name);
        }
        // None
    }

    /// Delete a waiting task from the scheduler.
    ///
    /// # Arguments
    /// *`id`: The unique identifier for the task.
    pub fn delete_task(&mut self, id: u64) {
        if self.waiting.remove(&id).is_none() == true {
            info!("No task to delete with id {}", id);
        }
    }

    /// Find the number of tasks waiting in the ready queue.
    ///
    /// # Return
    ///
    /// The current length of the task queue.
    pub fn get_queue_len(&self) -> usize {
        self.ready.len()
    }

    /// This method updates the RW set for the extension.
    ///
    /// # Arguments
    /// * `records`: A reference to the RWset sent back by the server when the extension is
    ///             pushed back.
    pub fn update_rwset(
        &mut self,
        id: u64,
        // table_id: usize,
        record: &[u8],
        // recordlen: usize,
        // segment_id: usize,
        // num_segments: usize,
        // value_len: usize,
        // ) -> Option<Box<Task>> {
    ) {
        if let Some(mut task) = self.waiting.remove(&id) {
            if cfg!(feature = "checksum") {
                // let (keys, records) = records.split_at(377);
                // task.update_cache(keys, 8);
                // for record in records.chunks(recordlen) {
                //     task.update_cache(record, keylen);
                // }
                // self.ready.push_back(task);
            } else {
                // let mut ready = false;
                // for record in records.chunks(recordlen) {
                //     // task.update_cache(record, table_id);
                //     if task.update_cache(record, segment_id, num_segments, value_len) {
                //         ready = true;
                //     }
                // }
                let ready = task.update_cache(record/*, segment_id, num_segments, value_len*/);
                if ready {
                    trace!("ext id {} all segments recvd", id);
                    self.ready.push_back(task);
                    // return Some(task);
                } else {
                    trace!("ext id {} still waiting for kv resps", id);
                    self.waiting.insert(id, task);
                }
            }
            // self.ready.push_back(task);
        } else {
            info!("No waiting task with id {}", id);
        }
        // None
    }
    /*
    /// This method run the task associated with an extension. And on the completion
    /// of the task, it tear downs the task.
    ///
    /// # Return
    ///
    /// The taskstate on the completion, yielding, or waiting of the task.
    pub fn execute_task(&mut self) -> (TaskState, u64) {
        let task = self.ready.pop_front();
        let mut taskstate: TaskState = INITIALIZED;
        let mut time: u64 = 0;
        if let Some(mut task) = task {
            if task.run().0 == COMPLETED {
                taskstate = task.state();
                time = task.time();
                unsafe {
                    task.tear();
                    // Do something for commit(Transaction commit?)
                }
            } else {
                taskstate = task.state();
                time = task.time();
                if taskstate == YIELDED {
                    self.ready.push_back(task);
                } else if taskstate == WAITING {
                    self.waiting.insert(task.get_id(), task);
                }
            }
        }
        (taskstate, time)
    }
    */
    pub fn run_task(&mut self, mut task: Box<Task>, queue_len: &mut f64, task_duration_cv: f64) {
        // let mut task = self.ready.pop_front().unwrap();
        if task.run().0 == COMPLETED {
            trace!("task complete");
            if let Some((req, resps)) = unsafe { task.tear(queue_len, task_duration_cv) } {
                req.free_packet();
                trace!("push resps");
                for resp in resps.into_iter() {
                    self.responses.push(rpc::fixup_header_length_fields(resp));
                }
            }
        } else {
            let taskstate = task.state();
            if taskstate == YIELDED {
                self.ready.push_back(task);
            } else if taskstate == WAITING {
                self.waiting.insert(task.get_id(), task);
            }
        }
    }
    // pub fn execute_tasks(&mut self, mut queue_len: f64) {
    //     // let mut taskstate: TaskState;
    //     // let mut time: u64;
    //     while let Some(mut task) = self.ready.pop_front() {
    //         if task.run().0 == COMPLETED {
    //             trace!("task complete");
    //             // taskstate = task.state();
    //             // time = task.time();
    //             if let Some((req, resps)) = unsafe { task.tear(&mut queue_len) } {
    //                 req.free_packet();
    //                 trace!("push resps");
    //                 for resp in resps.into_iter() {
    //                     self.responses.push(rpc::fixup_header_length_fields(resp));
    //                 }
    //             }
    //         } else {
    //             let taskstate = task.state();
    //             // time = task.time();
    //             if taskstate == YIELDED {
    //                 self.ready.push_back(task);
    //             } else if taskstate == WAITING {
    //                 self.waiting.insert(task.get_id(), task);
    //             }
    //         }
    //     }
    //     // (taskstate, time)
    // }
}

/*
/// TaskManager handles the information for a pushed-back extension on the client side.
pub struct TaskManager {
    // A ref counted pointer to a master service. The master service
    // implements the primary interface to the database.
    master: Master,

    // The reference to the task generator, which is used to suspend/resume the generator.
    // pub ready: VecDeque<Box<Task>>,
    ///  The HashMap containing the waiting tasks.
    pub waiting: [RwLock<HashMap<u64, Box<Task>>>; TASK_BUCKETS],
    // /// responses
    // pub responses: Vec<Packet<IpHeader, EmptyMetadata>>,
}

impl TaskManager {
    /// This function creates and returns the TaskManager for the pushed back task. The client uses
    /// this object to resume the extension on the client side.
    ///
    /// # Arguments
    ///
    /// * `master_service`: A reference to a Master which will be used to construct tasks from received
    ///                    response.
    /// * `req`: A reference to the request sent by the client, it will be helpful in task creation
    ///          if the requested is pushed back.
    /// * `tenant_id`: Tenant id will be needed reuqest generation.
    /// * `name_len`: This will be useful in parsing the request and find out the argument for consecutive requests.
    /// * `timestamp`: This is unique-id for the request and consecutive requests will have same id.
    ///
    /// # Return
    ///
    /// A TaskManager for generator creation and task execution on the client.
    pub fn new(master_service: Master) -> TaskManager {
        TaskManager {
            master: master_service,
            // ready: VecDeque::with_capacity(32),
            // waiting: HashMap::with_capacity(32),
            waiting: [
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
            ],
            // responses: vec![],
        }
    }

    /// This method creates a task for the extension on the client-side and add
    /// it to the task-manager.
    ///
    /// # Arguments
    /// * `sender_service`: A reference to the service which helps in the RPC request generation.
    pub fn create_task(
        &self,
        id: u64,
        // req: &[u8],
        req: Packet<InvokeRequest, EmptyMetadata>,
        resp: Packet<InvokeResponse, EmptyMetadata>,
        tenant: u32,
        name_length: usize,
        sender_service: Rc<Sender>,
    ) -> Option<Box<Task>> {
        let tenant_id: TenantId = tenant as TenantId;
        let name_length: usize = name_length as usize;

        // Read the extension's name from the request payload.
        let mut name = Vec::new();
        name.extend_from_slice(req.get_payload().split_at(name_length).0);
        let name: String = String::from_utf8(name).expect("ERROR: Failed to get ext name.");

        // Get the model for the given extension.
        let mut model = None;
        // If the extension doesn't need an ML model, don't waste CPU cycles in lookup.
        if cfg!(feature = "ml-model") {
            GLOBAL_MODEL.with(|a_model| {
                if let Some(a_model) = (*a_model).borrow().get(&name) {
                    model = Some(Arc::clone(a_model));
                }
            });
        }

        if let Some(ext) = self.master.extensions.get(tenant_id, name.clone()) {
            let db = Rc::new(ProxyDB::new(
                tenant,
                id,
                req,
                resp,
                name_length as usize,
                sender_service,
                model,
            ));
            // self.waiting.insert(
            //     id,
            //     Box::new(Container::new(TaskPriority::REQUEST, db, ext, id)),
            // );
            Some(Box::new(Container::new(TaskPriority::REQUEST, db, ext, id)))
            // self.ready
            //     .push_back(Box::new(Container::new(TaskPriority::REQUEST, db, ext, id)))
        } else {
            info!("Unable to create a generator for this ext of name {}", name);
            None
        }
    }

    /// Delete a waiting task from the scheduler.
    ///
    /// # Arguments
    /// *`id`: The unique identifier for the task.
    pub fn delete_task(&self, id: u64) {
        let bucket_id = id as usize & (TASK_BUCKETS - 1);
        if self.waiting[bucket_id]
            .write()
            .unwrap()
            .remove(&id)
            .is_none()
        {
            info!("No task to delete with id {}", id);
        }
    }

    // /// Find the number of tasks waiting in the ready queue.
    // ///
    // /// # Return
    // ///
    // /// The current length of the task queue.
    // pub fn get_queue_len(&self) -> usize {
    //     self.ready.len()
    // }

    /// This method updates the RW set for the extension.
    ///
    /// # Arguments
    /// * `records`: A reference to the RWset sent back by the server when the extension is
    ///             pushed back.
    pub fn update_rwset(
        &self,
        id: u64,
        // table_id: usize,
        records: &[u8],
        recordlen: usize,
        segment_id: usize,
        num_segments: usize,
    ) -> Option<Box<Task>> {
        let bucket_id = id as usize & (TASK_BUCKETS - 1);
        let mut ready = false;
        // NOTE: change remove to get
        if let Some(task) = self.waiting[bucket_id].read().unwrap().get(&id) {
            if cfg!(feature = "checksum") {
                // let (keys, records) = records.split_at(377);
                // task.update_cache(keys, 8);
                // for record in records.chunks(recordlen) {
                //     task.update_cache(record, keylen);
                // }
                // self.ready.push_back(task);
            } else {
                for record in records.chunks(recordlen) {
                    // task.update_cache(record, table_id);
                    if task.update_cache(record, segment_id, num_segments) {
                        ready = true;
                    }
                }
            }
            // self.ready.push_back(task);
        } else {
            info!("No waiting task with id {}", id);
        }
        // check if all resps has been recvd
        if ready {
            trace!("ext id {} all segments recvd", id);
            // self.ready.push_back(task);
            self.waiting[bucket_id].write().unwrap().remove(&id)
        } else {
            trace!("ext id {} still waiting for kv resps", id);
            // self.waiting.insert(id, task);
            None
        }
    }
    pub fn execute_task(
        &self,
        mut task: Box<Task>,
        mut queue_len: f64,
        task_duration_cv: f64,
    ) -> Option<Vec<Packet<IpHeader, EmptyMetadata>>> {
        // let mut task = self.ready.pop_front().unwrap();
        if task.run().0 == COMPLETED {
            trace!("task complete");
            if let Some((req, resps)) = unsafe { task.tear(&mut queue_len, task_duration_cv) } {
                req.free_packet();
                trace!("push resps");
                // for resp in resps.into_iter() {
                //     self.responses.push(rpc::fixup_header_length_fields(resp));
                // }
                return Some(
                    resps
                        .into_iter()
                        .map(|resp| rpc::fixup_header_length_fields(resp))
                        .collect::<Vec<_>>(),
                );
            }
        } else {
            let taskstate = task.state();
            if taskstate == YIELDED {
                // self.ready.push_back(task);
            } else if taskstate == WAITING {
                // self.waiting.insert(task.get_id(), task);
                let id = task.get_id();
                let bucket_id = id as usize & (TASK_BUCKETS - 1);
                self.waiting[bucket_id].write().unwrap().insert(id, task);
            }
        }
        None
    }
}

unsafe impl Send for TaskManager {}
unsafe impl Sync for TaskManager {}
*/
/// sender&receiver for compute node
// 1. does not precompute mac and ip addr for LB and storage
// 2. repeated parse and reparse header
pub struct ComputeNodeWorker {
    dispatcher: Dispatcher,
    // this will bottleneck since all workers attempts to write
    // task_duration_cv: Arc<RwLock<CoeffOfVar>>,
    task_duration_cv: CoeffOfVar,
    // manager: Arc<TaskManager>,
    manager: TaskManager,
    // this is dynamic, i.e. resp dst is set as req src upon recving new reqs
    resp_hdr: PacketHeaders,
    // id: usize,
    queue_length: MovingTimeAvg,
    // reset: Arc<AtomicBool>,
    // last_run: u64,
    // #[cfg(feature = "queue_len")]
    // timestamp: Arc<RwLock<Vec<u64>>>,
    // #[cfg(feature = "queue_len")]
    // raw_length: Arc<RwLock<Vec<usize>>>,
    // #[cfg(feature = "queue_len")]
    // terminate: Arc<AtomicBool>,
}

// TODO: move req_ports& into config?
impl ComputeNodeWorker {
    pub fn new(
        config: &ComputeConfig,
        net_port: CacheAligned<PortQueue>,
        // sib_port: Option<CacheAligned<PortQueue>>,
        queue: Arc<Queue>,
        sib_queue: Option<Arc<Queue>>,
        // reset: Arc<AtomicBool>,
        // reset: Vec<Arc<AtomicBool>>,
        masterservice: Arc<Master>,
        // manager: Arc<TaskManager>,
        // #[cfg(feature = "queue_len")] timestamp: Arc<RwLock<Vec<u64>>>,
        // #[cfg(feature = "queue_len")] raw_length: Arc<RwLock<Vec<usize>>>,
        // #[cfg(feature = "queue_len")] terminate: Arc<AtomicBool>,
        // id: usize,
    ) -> ComputeNodeWorker {
        ComputeNodeWorker {
            resp_hdr: PacketHeaders::create_hdr(
                &config.compute.server,
                net_port.txq() as u16,
                &NetConfig::default(),
            ),
            dispatcher: Dispatcher::new(
                net_port,
                // sib_port,
                &config.compute.server,
                &config.storage,
                config.compute.max_rx_packets,
                queue,
                sib_queue,
                // reset,
            ),
            task_duration_cv: CoeffOfVar::new(),
            // sender: Rc::new(Sender::new(
            //     net_port.clone(),
            //     &config.src,
            //     &config.storage,
            //     // shared_credits.clone(),
            // )),
            // receiver: Receiver {
            //     stealing: !sib_port.is_none(),
            //     net_port: net_port,
            //     sib_port: sib_port,
            //     max_rx_packets: config.max_rx_packets,
            //     ip_addr: u32::from(
            //         Ipv4Addr::from_str(&config.src.ip_addr).expect("missing src ip for LB"),
            //     ),
            // },
            manager: TaskManager::new(Arc::clone(&masterservice)),
            // manager: manager,
            // id: id,
            queue_length: MovingTimeAvg::new(config.moving_exp),
            // last_run: 0,
            // #[cfg(feature = "queue_len")]
            // timestamp: timestamp,
            // #[cfg(feature = "queue_len")]
            // raw_length: raw_length,
            // #[cfg(feature = "queue_len")]
            // terminate: terminate,
        }
    }

    // fn poll(&mut self) {
    //     if let Some(packet) = self.dispatcher.poll_self() {
    //         self.handle_rpc(packet);
    //     } else if let Some(packet) = self.dispatcher.poll_sib() {
    //         self.handle_rpc(packet);
    //     } else if let Err(_) = self.dispatcher.recv() {
    //         if self.dispatcher.receiver.stealing {
    //             self.dispatcher.steal();
    //         }
    //     }

    //     // if let Some(mut packets) = self.receiver.recv() {
    //     //     trace!("worker recv {} resps", packets.len());
    //     //     while let Some((packet, _)) = packets.pop() {
    //     //         self.dispatch(packet);
    //     //     }
    //     //     if self.manager.ready.len() > 0 {
    //     //         let queue_len = self.queue.length.swap(-1.0, Ordering::Relaxed) as f64;
    //     //         // return queue_len + 1.0;
    //     //         return queue_len;
    //     //     }
    //     // }
    //     // // let (request, queue_len) = {
    //     // //     let mut queue = self.queue.queue.write().unwrap();
    //     // //     let queue_len = queue.len();
    //     // //     let request = queue.pop_front();
    //     // //     (request, queue_len)
    //     // // };
    //     // let request = self.queue.queue.write().unwrap().pop_front();
    //     // if let Some(request) = request {
    //     //     self.dispatch(request);
    //     //     // return queue_len as f64;
    //     //     return self.queue.length.swap(-1.0, Ordering::Relaxed) as f64;
    //     // }
    //     // return -1.0;
    // }

    // fn poll_response(&mut self) -> usize {
    //     // let mut num_dispatched: u32 = 0;
    //     if let Some(mut packets) = self.receiver.recv() {
    //         trace!("worker recv {} resps", packets.len());
    //         while let Some((packet, _)) = packets.pop() {
    //             self.dispatch(packet);
    //         }
    //     }
    // }

    // fn poll_request(&mut self) {
    //     let packet = self.queue.write().unwrap().pop_front();
    //     if let Some(packet) = packet {
    //         self.dispatch(packet);
    //     }
    // }

    // pub fn run_extensions(&mut self, queue_len: f64) {
    //     self.manager.execute_tasks(queue_len);
    // }

    pub fn send_response(&mut self) {
        let resps = &mut self.manager.responses;
        if resps.len() > 0 {
            self.dispatcher.sender.send_pkts(resps);
        }
    }
    pub fn run_tasks(&mut self, queue_length: &mut f64) {
        while let Some(task) = self.manager.ready.pop_front() {
            let start = cycles::rdtsc();
            let cv = self.task_duration_cv.cv();
            self.manager.run_task(task, queue_length, cv);
            self.send_response();
            let end = cycles::rdtsc();
            let duration = (end - start) as f64;
            self.task_duration_cv.update(duration);
        }
    }
    // pub fn handle_rpc(&mut self, packet: Packet<UdpHeader, EmptyMetadata>, queue_length: &mut f64) {
    //     let start = cycles::rdtsc();
    //     let (task, op) = self.dispatch(packet);
    //     if let Some(task) = task {
    //         let cv = self.task_duration_cv.cv();
    //         // TODO: add dispatch overhead to task time
    //         // NOTE: we do not report the queue len of sib queue
    //         // TODO: add multi-packet transmission overhead in send_response
    //         // if let Some(mut resps) = self.manager.execute_task(task, ql, cv) {
    //         //     self.dispatcher.sender.send_pkts(&mut resps);
    //         // }
    //         self.manager.execute_task(task, queue_length, cv);
    //         self.send_response();
    //         let end = cycles::rdtsc();
    //         let duration = (end - start) as f64;
    //         // if self.dispatcher.reset[self.id].load(Ordering::Relaxed) {
    //         //     self.task_duration_cv.reset();
    //         //     self.dispatcher.reset[self.id].store(false, Ordering::Relaxed);
    //         // }
    //         // let type_id = if op == OpCode::SandstormInvokeRpc {
    //         //     0
    //         // } else {
    //         //     2
    //         // };
    //         self.task_duration_cv.update(duration /*, type_id*/);
    //     } else if op == OpCode::SandstormGetRpc {
    //         let end = cycles::rdtsc();
    //         let duration = (end - start) as f64;
    //         // if self.dispatcher.reset[self.id].load(Ordering::Relaxed) {
    //         //     self.task_duration_cv.reset();
    //         //     self.dispatcher.reset[self.id].store(false, Ordering::Relaxed);
    //         // }
    //         self.task_duration_cv.update(duration /*, 1*/);
    //     }
    // }

    // 1. invoke_req: lb to compute
    // 2. get_resp: storage to compute
    pub fn dispatch(
        &mut self,
        packet: Packet<UdpHeader, EmptyMetadata>,
        // ) -> (Option<Box<Task>>, OpCode) {
    ) {
        match parse_rpc_opcode(&packet) {
            op @ OpCode::SandstormInvokeRpc => {
                if parse_rpc_service(&packet) == Service::MasterService {
                    // (self.dispatch_invoke(packet), op)
                    self.dispatch_invoke(packet)
                } else {
                    trace!("invalid rpc req, ignore");
                    packet.free_packet();
                    // (None, op)
                }
            }
            op @ OpCode::SandstormGetRpc => self.process_get_resp(packet), //(self.process_get_resp(packet), op),
            // #[cfg(feature = "queue_len")]
            // OpCode::TerminateRpc => {
            //     self.terminate.store(true, Ordering::Relaxed);
            //     packet.free_packet();
            //     println!("core {} terminate", self.sender.net_port.rxq());
            //     Ok(())
            // }
            // OpCode::ResetRpc => {
            //     packet.free_packet();
            //     // println!("core {} resets", self.sender.net_port.rxq());
            //     self.task_duration_cv.write().unwrap().reset();
            //     // TODO: reset dispatcher moving avg queue len
            //     // self.queue.moving_avg.reset()
            //     Ok(())
            // }
            _ => {
                trace!("pkt is not for compute node, ignore");
                packet.free_packet();
                // (None, OpCode::InvalidOperation)
            }
        }
    }

    fn process_get_resp(
        &mut self,
        response: Packet<UdpHeader, EmptyMetadata>,
        // ) -> Option<Box<Task>> {
    ) {
        let p = response.parse_header::<GetResponse>();
        let hdr = p.get_header();
        let timestamp = hdr.common_header.stamp; // this is the timestamp when this ext is inserted in taskmanager
        // let num_segments = hdr.num_segments as usize;
        // let segment_id = hdr.segment_id as usize;
        // let value_len = hdr.value_len as usize;
        // let table_id = hdr.table_id as usize;
        let record = p.get_payload();
        // let recordlen = records.len();
        let task = if p.get_header().common_header.status == RpcStatus::StatusOk {
            // self.manager
            //     .update_rwset(timestamp, table_id, records, recordlen);
            self.manager.update_rwset(
                timestamp,
                record,
                // recordlen,
                // segment_id,
                // num_segments,
                // value_len,
            )
            // self.sender.return_credit();
        } else {
            warn!(
                "kv req of ext id: {} failed with status {:?}",
                timestamp,
                p.get_header().common_header.status
            );
            // None
        };
        p.free_packet();
        // task
    }

    fn dispatch_invoke(&mut self, request: Packet<UdpHeader, EmptyMetadata>) /*-> Option<Box<Task>>*/
    {
        self.resp_hdr
            .udp_header
            .set_dst_port(request.get_header().src_port());
        let ip = request.deparse_header(common::IP_HDR_LEN);
        // self.resp_hdr.ip_header.set_src(ip.get_header().dst());
        self.resp_hdr.ip_header.set_dst(ip.get_header().src());

        // Set the destination mac address on the response MAC header.
        let mac = ip.deparse_header(common::MAC_HDR_LEN);
        // self.resp_hdr.mac_header.set_src(mac.get_header().dst());
        self.resp_hdr.mac_header.set_dst(mac.get_header().src());

        let request = mac.parse_header::<IpHeader>().parse_header::<UdpHeader>();

        if let Some(response) = new_packet() {
            let mut response = response
                .push_header(&self.resp_hdr.mac_header)
                .expect("ERROR: Failed to add response MAC header")
                .push_header(&self.resp_hdr.ip_header)
                .expect("ERROR: Failed to add response IP header")
                .push_header(&self.resp_hdr.udp_header)
                .expect("ERROR: Failed to add response UDP header");

            // Set the destination port on the response UDP header.
            // response
            //     .get_mut_header()
            //     .set_src_port(request.get_header().dst_port());
            // response
            //     .get_mut_header()
            //     .set_dst_port(request.get_header().src_port());
            self.add_to_manager(request, response)
        } else {
            println!("ERROR: Failed to allocate packet for response");
            request.free_packet();
            // None
        }
    }

    fn add_to_manager(
        &mut self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
        // ) -> Option<Box<Task>> {
    ) {
        let req = req.parse_header::<InvokeRequest>();
        let hdr = req.get_header();
        let tenant_id = hdr.common_header.tenant;
        let name_length = hdr.name_length as usize;
        let args_length = hdr.args_length as usize;
        let rpc_stamp = hdr.common_header.stamp;

        let res = res
            .push_header(&InvokeResponse::new(
                rpc_stamp,
                OpCode::SandstormInvokeRpc,
                tenant_id,
            ))
            .expect("Failed to push InvokeResponse");
        self.manager.create_task(
            cycles::rdtsc(),
            req,
            res,
            tenant_id,
            name_length,
            self.dispatcher.sender.clone(),
        )
    }
}

impl Executable for ComputeNodeWorker {
    fn execute(&mut self) {
        // if let Some(packet) = self.dispatcher.poll_self() {
        //     self.handle_rpc(packet);
        // } else if let Some(packet) = self.dispatcher.poll_sib() {
        //     self.handle_rpc(packet);
        // } else if let Err(_) = self.dispatcher.recv() {
        //     if self.dispatcher.receiver.stealing {
        //         self.dispatcher.steal();
        //     }
        // }
        loop {
            // let mut waiting = self.manager.waiting.len() as f64;
            // get resp processing is not considered
            let get_resps = self.dispatcher.recv();
            if self.dispatcher.reset() {
                self.task_duration_cv.reset();
                // self.queue_length.reset();
            }
            if let Some(resps) = get_resps {
                for resp in resps.into_iter() {
                    self.dispatch(resp);
                }
            }
            let mut waiting = self.manager.waiting.len() as f64;
            // self.queue_length.update(cycles::rdtsc(), waiting);
            // let mut waiting_mean = self.queue_length.moving();
            while let Some(packet) = self.dispatcher.poll() {
                self.dispatch(packet);
            }
            // let mut ql = self.manager.ready.len() as f64;
            // self.queue_length.update(cycles::rdtsc(), ql);
            // let mut ql_mean = self.queue_length.avg();
            if self.manager.ready.len() > 0 {
                self.run_tasks(&mut waiting);
                // self.run_tasks(&mut waiting_mean);
                // self.run_tasks(&mut ql);
                // self.run_tasks(&mut ql_mean);
            } else if self.dispatcher.length == 0.0 {
                if let Some(packet) = self.dispatcher.poll_sib() {
                    self.dispatch(packet);
                }
            }
        }
        // TODO: put only invoke req in queue
        // loop {
        //     // if let Some(mut packets) = self.dispatcher.recv() {
        //     //     while let Some(packet) = packets.pop() {
        //     //         self.handle_rpc(packet);
        //     //     }
        //     // }
        //     let get_resps = self.dispatcher.recv();
        //     if self.dispatcher.reset() {
        //         self.task_duration_cv.reset();
        //         self.queue_length.reset();
        //     }
        //     let ql = self.dispatcher.length;
        //     self.queue_length.update(cycles::rdtsc(), ql);
        //     let mut queue_length = self.queue_length.avg();
        //     if let Some(resps) = get_resps {
        //         for resp in resps.into_iter() {
        //             self.handle_rpc(resp, &mut queue_length);
        //         }
        //     }
        //     if ql > 0.0 {
        //         while let Some(packet) = self.dispatcher.poll() {
        //             self.handle_rpc(packet, &mut queue_length);
        //         }
        //     } else if let Some(packet) = self.dispatcher.poll_sib() {
        //         self.handle_rpc(packet, &mut queue_length);
        //     }
        //     // if let Ok(_) = self.dispatcher.recv() {
        //     //     loop {
        //     //         let packet = self.dispatcher.queue.queue.write().unwrap().pop_front();
        //     //         if let Some(packet) = packet {
        //     //             self.handle_rpc(packet);
        //     //         } else {
        //     //             break;
        //     //         }
        //     //     }
        //     // } else if let Some(packet) = self.dispatcher.poll_sib() {
        //     //     self.handle_rpc(packet);
        //     // }
        // }
    }
    /*
    fn execute(&mut self) {
        // #[cfg(feature = "queue_len")]
        // if self.terminate.load(Ordering::Relaxed) {
        //     return;
        // }
        // let current = cycles::rdtsc();
        // let eventloop_interval = if self.last_run > 0 {
        //     current - self.last_run
        // } else {
        //     0
        // };
        // self.last_run = current;
        // event loop
        // TODO: smooth queue_len
        let queue_len = self.poll();
        if self.manager.ready.len() > 0 {
            // let cv = self.task_duration_cv.read().unwrap().cv();
            let cv = self.task_duration_cv.cv();
            let start = cycles::rdtsc();
            self.manager.execute_task(queue_len, cv);
            // TODO: add transmission overhead in send_response(in case of multi-packet resp)
            self.send_response();
            let end = cycles::rdtsc();
            let task_time = (end - start) as f64;
            if self.reset.load(Ordering::Relaxed) {
                self.task_duration_cv.reset();
                self.reset.store(false, Ordering::Relaxed);
            }
            // self.task_duration_cv.write().unwrap().update(task_time);
            self.task_duration_cv.update(task_time);
        }
        // TODO: pass in queue_len
        // self.manager.execute_tasks(queue_len);
        // self.run_extensions(0.0);
        // let current_time = cycles::rdtsc();
        // let queue_length = self.manager.ready.len();
        // #[cfg(feature = "queue_len")]
        // if (queue_length > 0 || self.timestamp.read().unwrap().len() > 0)
        //     && !self.terminate.load(Ordering::Relaxed)
        // {
        //     self.timestamp.write().unwrap().push(current_time);
        //     // #[cfg(feature = "queue_len")]
        //     self.raw_length.write().unwrap().push(queue_length);
        // }
        // // TODO: smooth this queue_length
        // self.run_extensions(queue_length as f64);
        self.send_response();
    }
    */
    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

// impl Drop for ComputeNodeWorker {
//     fn drop(&mut self) {
//         self.send_response();
//     }
// }
