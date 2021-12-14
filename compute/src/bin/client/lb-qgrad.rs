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

extern crate atomic_float;
extern crate bytes;
extern crate db;
extern crate order_stat;
extern crate rand;
extern crate sandstorm;
extern crate spin;
extern crate splinter;
extern crate time;
extern crate zipf;

// mod setup;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Display;
use std::mem;
use std::mem::transmute;
use std::sync::Arc;

use self::bytes::Bytes;
use db::config::{self, *};
use db::cycles;
use db::e2d2::allocators::*;
use db::e2d2::config::{NetbricksConfiguration, PortConfiguration};
use db::e2d2::interface::*;
use db::e2d2::scheduler::*;
use db::log::*;
use db::master::Master;
use db::rpc::*;
use db::wireformat::*;
// use db::e2d2::scheduler::NetBricksContext as NetbricksContext;

use rand::distributions::{Normal, Sample};
use rand::{Rng, SeedableRng, XorShiftRng};
use splinter::nativestate::PushbackState;
use splinter::proxy::KV;
use splinter::sched::TaskManager;
use splinter::*;
use zipf::ZipfDistribution;

use self::atomic_float::AtomicF64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use dispatch::LBDispatcher;
use std::fmt::{self, Write};
use std::fs::File;
use std::io::Write as writef;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::RwLock;
use xloop::*;

// Flag to indicate that the client has finished sending and receiving the packets.
static mut FINISHED: bool = false;

// PUSHBACK benchmark.
// The benchmark is created and parameterized with `new()`. Many threads
// share the same benchmark instance. Each thread can call `abc()` which
// runs the benchmark until another thread calls `stop()`. Each thread
// then returns their runtime and the number of gets and puts they have done.
// This benchmark doesn't care about how get/put are implemented; it takes
// function pointers to get/put on `new()` and just calls those as it runs.
//
// The tests below give an example of how to use it and how to aggregate the results.
pub struct Pushback {
    put_pct: usize,
    rng: Box<dyn Rng>,
    key_rng: Box<ZipfDistribution>,
    tenant_rng: Box<ZipfDistribution>,
    // order_rng: Box<Normal>,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
}

impl Pushback {
    // Create a new benchmark instance.
    //
    // # Arguments
    //  - key_len: Length of the keys to generate per get/put. Most bytes will be zero, since
    //             the benchmark poplates them from a random 32-bit value.
    //  - value_len: Length of the values to store per put. Always all zero bytes.
    //  - n_keys: Number of keys from which random keys are drawn.
    //  - put_pct: Number between 0 and 100 indicating percent of ops that are sets.
    //  - skew: Zipfian skew parameter. 0.99 is PUSHBACK default.
    //  - n_tenants: The number of tenants from which the tenant id is chosen.
    //  - tenant_skew: The skew in the Zipfian distribution from which tenant id's are drawn.
    // # Return
    //  A new instance of PUSHBACK that threads can call `abc()` on to run.
    fn new(
        key_len: usize,
        value_len: usize,
        n_keys: usize,
        put_pct: usize,
        skew: f64,
        n_tenants: u32,
        tenant_skew: f64,
    ) -> Pushback {
        let seed: [u32; 4] = rand::random::<[u32; 4]>();

        let mut key_buf: Vec<u8> = Vec::with_capacity(key_len);
        key_buf.resize(key_len, 0);
        let mut value_buf: Vec<u8> = Vec::with_capacity(value_len);
        value_buf.resize(value_len, 0);

        Pushback {
            put_pct: put_pct,
            rng: Box::new(XorShiftRng::from_seed(seed)),
            key_rng: Box::new(
                ZipfDistribution::new(n_keys, skew).expect("Couldn't create key RNG."),
            ),
            tenant_rng: Box::new(
                ZipfDistribution::new(n_tenants as usize, tenant_skew)
                    .expect("Couldn't create tenant RNG."),
            ),
            // order_rng: Box::new(Normal::new(ORDER, STD_DEV)),
            key_buf: key_buf,
            value_buf: value_buf,
        }
    }

    // Run PUSHBACK A, B, or C (depending on `new()` parameters).
    // The calling thread will not return until `done()` is called on this `Pushback` instance.
    //
    // # Arguments
    //  - get: A function that fetches the data stored under a bytestring key of `self.key_len` bytes.
    //  - set: A function that stores the data stored under a bytestring key of `self.key_len` bytes
    //         with a bytestring value of `self.value_len` bytes.
    // # Return
    //  A three tuple consisting of the duration that this thread ran the benchmark, the
    //  number of gets it performed, and the number of puts it performed.
    pub fn abc<G, P, R>(&mut self, mut get: G, mut put: P) -> R
    where
        G: FnMut(u32, &[u8]) -> R,
        P: FnMut(u32, &[u8], &[u8]) -> R,
    {
        let is_get = (self.rng.gen::<u32>() % 100) >= self.put_pct as u32;

        // Sample a tenant.
        let t = self.tenant_rng.sample(&mut self.rng) as u32;

        // Sample a key, and convert into a little endian byte array.
        let k = self.key_rng.sample(&mut self.rng) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };
        self.key_buf[0..mem::size_of::<u32>()].copy_from_slice(&k);

        // We dont change this, b/c we bypass this part
        // let o = self.order_rng.sample(&mut self.rng).abs() as u32;

        if is_get {
            get(t, self.key_buf.as_slice())
        } else {
            put(t, self.key_buf.as_slice(), self.value_buf.as_slice())
        }
    }
}

#[derive(Debug)]
struct MultiType {
    num_kv: u32,
    order: u32,
    // steps: u32,
}
impl MultiType {
    fn new(multi_kv: &Vec<u32>, multi_ord: &Vec<u32>) -> Vec<MultiType> {
        let mut multi_types = Vec::<MultiType>::new();
        for (kv, ord) in multi_kv.iter().zip(multi_ord.iter())
        // .zip(multi_steps.iter())
        {
            multi_types.push(MultiType {
                num_kv: *kv,
                order: *ord,
                // steps: *steps,
            });
        }
        multi_types
    }
}

struct Partition {
    x: Arc<AtomicF64>,
    upperbound: f64,
    lowerbound: f64,
}
impl XInterface for Partition {
    type X = f64;
    fn update(&self, delta_x: f64) {
        let x = self.x.load(Ordering::Relaxed);
        let mut new_x = x + delta_x;
        if new_x > self.upperbound || new_x < self.lowerbound {
            new_x = x - delta_x;
        }
        self.x.store(new_x, Ordering::Relaxed);
    }
    fn get(&self) -> f64 {
        self.x.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct Slot {
    counter: usize,
    type_id: usize,
}

/// Receives responses to PUSHBACK requests sent out by PushbackSend.
struct LoadBalancer {
    dispatcher: LBDispatcher,
    workload: RefCell<Pushback>,
    payload_pushback: RefCell<Vec<u8>>,
    payload_put: RefCell<Vec<u8>>,
    key_len: usize,
    record_len: usize,
    num_types: usize,
    multi_types: Vec<MultiType>,
    cum_prob: Vec<f32>,
    // worker stats
    id: usize,
    init_rdtsc: u64,
    start: u64,
    stop: u64,
    // finished: bool,
    requests: usize,
    global_recvd: Arc<AtomicUsize>,
    recvd: usize,
    latencies: Vec<Vec<u64>>,
    kth: Vec<Vec<Arc<AtomicUsize>>>,
    avg_lat: Vec<usize>,
    outstanding_reqs: HashMap<u64, usize>,
    slots: Vec<Slot>,
    // load balancing
    // storage_load: Arc<ServerLoad>,
    // compute_load: Arc<ServerLoad>,
    // rpc control
    max_out: u32,
    partition: Partition,
    // xloop
    learnable: bool,
    xloop: QueueGrad,
    // // bimodal
    // bimodal: bool,
    // bimodal_interval: u64,
    // bimodal_interval2: u64,
    // bimodal_ratio: Vec<Vec<u32>>,
    // bimodal_rpc: Vec<u32>,
    // modal_idx: usize,
    // output
    output_factor: u64,
    output_last_rdtsc: u64,
    output_last_recvd: usize,
    finished: Arc<AtomicBool>,
    tput: f64,
}

// Implementation of methods on LoadBalancer.
impl LoadBalancer {
    /// Constructs a PushbackRecv.
    ///
    /// # Arguments
    ///
    /// * `port` :  Network port on which responses will be polled for.
    /// * `resps`:  The number of responses to wait for before calculating statistics.
    /// * `master`: Boolean indicating if the receiver should make latency measurements.
    /// * `native`: If true, responses will be considered to correspond to native gets and puts.
    ///
    /// # Return
    ///
    /// A PUSHBACK response receiver that measures the median latency and throughput of a Sandstorm
    /// server.
    fn new(
        id: usize,
        config: &config::LBConfig,
        net_port: CacheAligned<PortQueue>,
        num_types: usize,
        partition: Arc<AtomicF64>,
        storage_load: Arc<ServerLoad>,
        compute_load: Arc<ServerLoad>,
        kth: Vec<Vec<Arc<AtomicUsize>>>,
        global_recvd: Arc<AtomicUsize>,
        init_rdtsc: u64,
        finished: Arc<AtomicBool>,
    ) -> LoadBalancer {
        // The payload on an invoke() based get request consists of the extensions name ("pushback"),
        // the table id to perform the lookup on, number of get(), number of CPU cycles and the key to lookup.
        let payload_len = "pushback".as_bytes().len()
            + mem::size_of::<u64>()
            + mem::size_of::<u32>()
            + mem::size_of::<u32>()
            + config.key_len;
        let mut payload_pushback = Vec::with_capacity(payload_len);
        payload_pushback.extend_from_slice("pushback".as_bytes());
        payload_pushback.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        // payload_pushback.extend_from_slice(&[unsafe { transmute::<u32, [u8; 4]>(number.to_le()) }]);
        payload_pushback.extend_from_slice(&[0u8; 4]);
        // payload_pushback.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(order.to_le()) });
        payload_pushback.extend_from_slice(&[0u8; 4]);
        payload_pushback.resize(payload_len, 0);

        // The payload on an invoke() based put request consists of the extensions name ("put"),
        // the table id to perform the lookup on, the length of the key to lookup, the key, and the
        // value to be inserted into the database.
        let payload_len = "pushback".as_bytes().len()
            + mem::size_of::<u64>()
            + mem::size_of::<u16>()
            + config.key_len
            + config.value_len;
        let mut payload_put = Vec::with_capacity(payload_len);
        payload_put.extend_from_slice("pushback".as_bytes());
        payload_put.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(1u64.to_le()) });
        payload_put.extend_from_slice(&unsafe {
            transmute::<u16, [u8; 2]>((config.key_len as u16).to_le())
        });
        payload_put.resize(payload_len, 0);
        // cumulative prob
        let mut cumsum: f32 = 0.0;
        let cum_prob = config
            .multi_ratio
            .iter()
            .map(|&x| {
                cumsum += x;
                cumsum * 100.0
            })
            .collect();

        let mut latencies: Vec<Vec<u64>> = Vec::with_capacity(num_types);
        for _ in 0..num_types {
            latencies.push(Vec::with_capacity(config.num_reqs));
        }
        // let mut bimodal_ratio = vec![];
        // for type1_ratio in &config.bimodal_ratio {
        //     bimodal_ratio.push(vec![type1_ratio * 100, 10000]);
        // }
        let mut slots = vec![];
        for _ in 0..config.max_out {
            slots.push(Slot::default());
        }

        LoadBalancer {
            tput: 0.0,
            finished: finished,
            // storage_load: storage_load,
            // compute_load: compute_load,
            dispatcher: LBDispatcher::new(config, net_port),
            workload: RefCell::new(Pushback::new(
                config.key_len,
                config.value_len,
                config.n_keys,
                config.put_pct,
                config.skew,
                config.num_tenants,
                config.tenant_skew,
            )),
            payload_pushback: RefCell::new(payload_pushback),
            payload_put: RefCell::new(payload_put),
            key_len: config.key_len,
            record_len: 1 + 8 + config.key_len + config.value_len,
            num_types: num_types,
            multi_types: MultiType::new(&config.multi_kv, &config.multi_ord),
            cum_prob: cum_prob,
            // worker stats
            id: id,
            init_rdtsc: init_rdtsc,
            start: 0,
            stop: 0,
            // finished: false,
            requests: config.num_reqs / 2,
            global_recvd: global_recvd,
            recvd: 0,
            latencies: latencies,
            kth: kth,
            avg_lat: vec![0; num_types],
            outstanding_reqs: HashMap::new(),
            slots: slots,
            // rloop_factor: usize,
            // rloop_last_recvd: u64,
            // rloop_last_rdtsc: u64,
            // rloop_last_out: f32,
            // rloop_last_kth: u64,
            // slo: u64,
            max_out: config.max_out,
            partition: Partition {
                x: partition,
                upperbound: 10000.0,
                lowerbound: 0.0,
            },
            learnable: config.learnable,
            xloop: QueueGrad::new(config, storage_load, compute_load),
            // not used
            // bimodal: config.bimodal,
            // bimodal_interval: config.bimodal_interval,
            // bimodal_interval2: config.bimodal_interval2,
            // bimodal_ratio: bimodal_ratio,
            // bimodal_rpc: config.bimodal_rpc.clone(),
            // modal_idx: 0,
            // output for bimodal
            output_factor: config.output_factor,
            output_last_rdtsc: init_rdtsc,
            output_last_recvd: 0,
        }
    }
    #[inline]
    fn gen_request(&mut self) -> (usize, bool) {
        let x = (self.workload.borrow_mut().rng.gen::<u32>() % 10000) as f32;
        let type_id = self.cum_prob.iter().position(|&p| p > x).unwrap();
        // let partition = self.partition.load(Ordering::Relaxed);
        let partition = self.partition.get();
        let native = x as f64 >= partition;
        (type_id, native)
    }
    /*
    // is it the same across all cores?
    fn get_type_id(&mut self) -> usize {
        let o = self.workload.borrow_mut().rng.gen::<u32>() % 10000;
        if self.bimodal {
            self.modal_idx = if (cycles::rdtsc() - self.init_rdtsc)
                % (self.bimodal_interval + self.bimodal_interval2)
                < self.bimodal_interval
            {
                0
            } else {
                1
            };
            self.bimodal_ratio[self.modal_idx]
                .iter()
                .position(|&x| x > o)
                .unwrap() as usize
        } else {
            self.cum_prob.iter().position(|&x| x > o).unwrap() as usize
        }
    }

    fn native_or_rpc(&mut self, type_id: usize) -> bool {
        // partition = -1 if not ours
        let o = self.workload.borrow_mut().rng.gen::<u32>() % 10000;
        if self.partition >= 0 {
            if (type_id as i32) < self.partition {
                false
            } else if self.partition < type_id as i32 {
                true
            } else {
                let ext_p = if self.bimodal {
                    self.bimodal_rpc[self.modal_idx]
                } else {
                    (self.ext_p[0].load(Ordering::Relaxed) * 100.0) as u32
                    // self.ext_p[0] as u32
                };
                o > ext_p
            }
        } else if self.ext_p.len() > 1 {
            o > (self.ext_p[type_id].load(Ordering::Relaxed) * 100.0) as u32
            // o > self.ext_p[type_id] as u32
        } else {
            o > (self.ext_p[0].load(Ordering::Relaxed) * 100.0) as u32
            // o > self.ext_p[0] as u32
        }
    }
    */

    fn send_once(&mut self, slot_id: usize) {
        let curr = cycles::rdtsc();
        let (type_id, native) = self.gen_request();
        let mut p_get = self.payload_pushback.borrow_mut();
        let mut p_put = self.payload_put.borrow_mut();
        let mut workload = self.workload.borrow_mut();
        if native {
            workload.abc(
                |tenant, key| {
                    unsafe {
                        p_get[16..20].copy_from_slice(&{
                            transmute::<u32, [u8; 4]>(self.multi_types[type_id].num_kv.to_le())
                        });
                        p_get[20..24].copy_from_slice(&{
                            transmute::<u32, [u8; 4]>(self.multi_types[type_id].order.to_le())
                        });
                    }
                    // First 24 bytes on the payload were already pre-populated with the
                    // extension name (8 bytes), the table id (8 bytes), number of get()
                    // (4 bytes), and number of CPU cycles compute(4 bytes). Just write
                    // in the first 4 bytes of the key.
                    p_get[24..28].copy_from_slice(&key[0..4]);
                    trace!("send type {} rpc {}", type_id, native);
                    self.dispatcher
                        .sender2compute
                        .send_invoke(tenant, 8, &p_get, curr, type_id)
                },
                |tenant, key, _val| {
                    // First 18 bytes on the payload were already pre-populated with the
                    // extension name (8 bytes), the table id (8 bytes), and the key length (2
                    // bytes). Just write in the first 4 bytes of the key. The value is anyway
                    // always zero.
                    p_put[18..22].copy_from_slice(&key[0..4]);
                    // self.dispatcher.sender2compute.send_invoke(tenant, 8, &p_put, curr, type_id)
                },
            );
            // self.outstanding += 1;
        } else {
            workload.abc(
                |tenant, key| {
                    unsafe {
                        p_get[16..20].copy_from_slice(&{
                            transmute::<u32, [u8; 4]>(self.multi_types[type_id].num_kv.to_le())
                        });
                        p_get[20..24].copy_from_slice(&{
                            transmute::<u32, [u8; 4]>(self.multi_types[type_id].order.to_le())
                        });
                    }
                    // First 24 bytes on the payload were already pre-populated with the
                    // extension name (8 bytes), the table id (8 bytes), number of get()
                    // (4 bytes), and number of CPU cycles compute(4 bytes). Just write
                    // in the first 4 bytes of the key.
                    p_get[24..28].copy_from_slice(&key[0..4]);
                    self.dispatcher
                        .sender2storage
                        .send_invoke(tenant, 8, &p_get, curr, type_id)
                },
                |tenant, key, _val| {
                    // First 18 bytes on the payload were already pre-populated with the
                    // extension name (8 bytes), the table id (8 bytes), and the key length (2
                    // bytes). Just write in the first 4 bytes of the key. The value is anyway
                    // always zero.
                    p_put[18..22].copy_from_slice(&key[0..4]);
                    // self.dispatcher.sender2storage.send_invoke(tenant, 8, &p_put, curr, type_id)
                },
            );
            // self.outstanding += 1;
        }
        self.slots[slot_id].counter += 1;
        self.slots[slot_id].type_id = type_id;
        self.outstanding_reqs.insert(curr, slot_id);
    }

    fn send_all(&mut self) {
        self.start = cycles::rdtsc();
        for i in 0..self.max_out as usize {
            self.send_once(i);
        }
    }

    fn update_load(
        &self,
        src_ip: u32,
        src_port: u16,
        curr_rdtsc: u64,
        queue_length: f64,
        task_duration_cv: f64,
    ) {
        // set to -1 after the first rpc resp packet in that round
        if queue_length < 0.0 || task_duration_cv == 0.0 {
            return;
        }
        if let Ok(_) = self.xloop.storage_load.update_load(
            src_ip,
            src_port,
            curr_rdtsc - self.start,
            queue_length,
            task_duration_cv,
        )
        // .update_load(src_ip, src_port, queue_length, task_duration_cv)
        {
            // #[cfg(feature = "server_stats")]
            // self.xloop.storage_load.update_trace(
            //     src_ip,
            //     src_port,
            //     curr_rdtsc,
            //     server_load,
            //     task_duration_cv,
            //     self.id,
            // );
        } else {
            self.xloop.compute_load.update_load(
                src_ip,
                src_port,
                curr_rdtsc - self.start,
                queue_length,
                task_duration_cv,
            );
            // .update_load(src_ip, src_port, queue_length, task_duration_cv);
            // #[cfg(feature = "server_stats")]
            // self.xloop.compute_load.update_trace(
            //     src_ip,
            //     src_port,
            //     curr_rdtsc,
            //     server_load,
            //     task_duration_cv,
            //     self.id,
            // );
        }
    }

    fn recv(&mut self) {
        // // Don't do anything after all responses have been received.
        // if self.finished.load(Ordering::Relaxed) == true && self.stop > 0 {
        //     return;
        // }

        let mut packet_recvd_signal = false;

        // Try to receive packets from the network port.
        // If there are packets, sample the latency of the server.
        // TODO: process server_load in hdr (from compute or storage?)
        // TODO: our algorithm
        if let Some(mut packets) = self.dispatcher.receiver.recv() {
            let curr_rdtsc = cycles::rdtsc();
            while let Some((packet, (src_ip, src_port))) = packets.pop() {
                match parse_rpc_opcode(&packet) {
                    // The response corresponds to an invoke() RPC.
                    OpCode::SandstormInvokeRpc => {
                        let p = packet.parse_header::<InvokeResponse>();
                        let hdr = p.get_header();
                        match hdr.common_header.status {
                            // If the status is StatusOk then add the stamp to the latencies and
                            // free the packet.
                            RpcStatus::StatusOk => {
                                let timestamp = hdr.common_header.stamp;
                                if let Some(&slot_id) = self.outstanding_reqs.get(&timestamp) {
                                    let type_id = self.slots[slot_id].type_id;
                                    trace!("req type {} finished", type_id);
                                    packet_recvd_signal = true;
                                    self.recvd += 1;
                                    self.global_recvd.fetch_add(1, Ordering::Relaxed);
                                    // TODO: reimpl update, server will do smoothing and return avg
                                    self.update_load(
                                        src_ip,
                                        src_port,
                                        curr_rdtsc,
                                        hdr.server_load,
                                        hdr.task_duration_cv,
                                        // #[cfg(feature = "server_stats")]
                                        // (curr_rdtsc - self.init_rdtsc),
                                    );
                                    self.latencies[type_id].push(curr_rdtsc - timestamp);
                                    self.outstanding_reqs.remove(&timestamp);
                                    self.send_once(slot_id);
                                } else {
                                    warn!("no outstanding request");
                                }
                            }

                            _ => {}
                        }
                        p.free_packet();
                    }

                    _ => packet.free_packet(),
                }

                // kth measurement here
                for (type_id, lat) in self.latencies.iter().enumerate() {
                    let len = lat.len();
                    if len > 100 && len % 10 == 0 {
                        let mut tmp = &lat[(len - 100)..len];
                        let mut tmpvec = tmp.to_vec();
                        self.kth[type_id][self.id].store(
                            *order_stat::kth(&mut tmpvec, 98) as usize,
                            Ordering::Relaxed,
                        );
                    }
                }
                if self.id == 0 {
                    let curr_rdtsc = cycles::rdtsc();
                    let global_recvd = self.global_recvd.load(Ordering::Relaxed);
                    if self.learnable
                        && self.xloop.ready(curr_rdtsc)
                        && packet_recvd_signal
                        && global_recvd - self.xloop.last_recvd > 8000
                    {
                        // let ql_storage = self.storage_load.avg_all();
                        // let ql_compute = self.compute_load.avg_all();
                        self.xloop
                            .update_x(&self.partition, curr_rdtsc, global_recvd);
                        // reset even if not updated
                        self.dispatcher.sender2storage.send_reset();
                        self.dispatcher.sender2compute.send_reset();
                        // let output_tput = 2.4e6 * (global_recvd - self.output_last_recvd) as f32
                        //     / (curr_rdtsc - self.output_last_rdtsc) as f32;
                        // println!("rdtsc {} tput {:.2}", curr_rdtsc / 2400000, output_tput);
                        // println!("xloop {}", self.xloop);
                        // self.output_last_rdtsc = curr_rdtsc;
                        // self.output_last_recvd = global_recvd;
                    }
                    // let xloop_rate = 2.4e6 * (global_recvd - self.xloop_last_recvd) as f32
                    //     / (xloop_rdtsc - self.xloop_last_rdtsc) as f32;
                    if self.output_factor != 0
                        && (curr_rdtsc - self.output_last_rdtsc > 2400000000 / self.output_factor)
                    {
                        // tput
                        let global_recvd = self.global_recvd.load(Ordering::Relaxed);
                        let output_tput = 2.4e6 * (global_recvd - self.output_last_recvd) as f32
                            / (curr_rdtsc - self.output_last_rdtsc) as f32;
                        // lat
                        for (type_id, kth) in self.kth.iter().enumerate() {
                            self.avg_lat[type_id] = 0;
                            for lat in kth {
                                self.avg_lat[type_id] += lat.load(Ordering::Relaxed);
                            }
                            self.avg_lat[type_id] /= kth.len();
                        }
                        // partition
                        // let partition = self.partition.load(Ordering::Relaxed) / 100.0;
                        // let partition = self.partition.get() / 100.0;
                        println!(
                            "rdtsc {} tail {:?} tput {}",
                            curr_rdtsc / 2400000,
                            self.avg_lat,
                            output_tput,
                            // self.xloop,
                        );
                        // xloop
                        println!("xloop {}", self.xloop);
                        // #[cfg(feature = "server_stats")]
                        // debug!("{}", self.xloop.storage_load);
                        // #[cfg(feature = "server_stats")]
                        // debug!("{}", self.xloop.compute_load);
                        self.output_last_rdtsc = curr_rdtsc;
                        self.output_last_recvd = global_recvd;
                    }
                }
            }
        }

        // The moment all response packets have been received, set the value of the
        // stop timestamp so that throughput can be estimated later.
        if self.requests <= self.recvd {
            self.stop = cycles::rdtsc();
            let already_finished = self.finished.swap(true, Ordering::Relaxed);
            if !already_finished {
                self.tput = self.global_recvd.load(Ordering::Relaxed) as f64
                    / cycles::to_seconds(self.stop - self.start);
                self.dispatcher.sender2storage.send_reset();
                self.dispatcher.sender2compute.send_reset();
            }
            // #[cfg(feature = "queue_len")]
            // self.dispatcher.sender2compute.send_terminate();
            // #[cfg(feature = "queue_len")]
            // self.dispatcher.sender2storage.send_terminate();
        }
    }
}

// Implementation of the `Drop` trait on PushbackRecv.
impl Drop for LoadBalancer {
    fn drop(&mut self) {
        if self.stop == 0 {
            // self.stop = cycles::rdtsc();
            info!(
                "The client thread {} received only {} packets",
                self.id, self.recvd
            );
        } else {
            let slots = self.slots.iter().map(|x| x.counter).collect::<Vec<_>>();
            let mean = slots.iter().sum::<usize>() as f32 / self.slots.len() as f32;
            let std_dev = (slots
                .iter()
                .map(|&x| (x as f32 - mean) * (x as f32 - mean))
                .sum::<f32>()
                / self.slots.len() as f32)
                .sqrt();
            info!(
                "client thread {} recvd {} slots {}x(mean:{:.2},std:{:.2})",
                self.id, self.recvd, self.max_out, mean, std_dev,
            );
        }

        // Calculate & print the throughput for all client threads.
        if self.tput > 0.0 {
            // Calculate & print median & tail latency only on the master thread.
            for (type_id, lat) in self.latencies.iter_mut().enumerate() {
                lat.sort();
                let m;
                let t = lat[(lat.len() * 99) / 100];
                match lat.len() % 2 {
                    0 => {
                        let n = lat.len();
                        m = (lat[n / 2] + lat[(n / 2) + 1]) / 2;
                    }

                    _ => m = lat[lat.len() / 2],
                }
                println!(
                    "type {} >>> lat50:{:.2} lat99:{:.2}",
                    type_id,
                    cycles::to_seconds(m) * 1e9,
                    cycles::to_seconds(t) * 1e9
                );
            }
            println!("PUSHBACK Throughput {:.2}", self.tput);
        }
        if self.id == 0 && cfg!(feature = "xtrace") {
            let mut f = File::create(format!("convergence{}.log", self.max_out)).unwrap();
            for s in &self.xloop.xtrace {
                writeln!(f, "{}", s);
            }
        }
    }
}

// Executable trait allowing PushbackRecv to be scheduled by Netbricks.
impl Executable for LoadBalancer {
    // Called internally by Netbricks.
    fn execute(&mut self) {
        if self.requests <= self.recvd {
            return;
        }
        if self.start == 0 {
            self.send_all();
        }
        // trace!("{:?}", self.outstanding_reqs.keys());
        self.recv();
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

fn setup_lb(
    config: &config::LBConfig,
    scheduler: &mut StandaloneScheduler,
    ports: Vec<CacheAligned<PortQueue>>,
    core_id: usize,
    num_types: usize,
    partition: Arc<AtomicF64>,
    storage_load: Arc<ServerLoad>,
    compute_load: Arc<ServerLoad>,
    kth: Vec<Vec<Arc<AtomicUsize>>>,
    global_recvd: Arc<AtomicUsize>,
    init_rdtsc: u64,
    finished: Arc<AtomicBool>,
) {
    if ports.len() != 1 {
        error!("LB should be configured with exactly 1 port!");
        std::process::exit(1);
    }
    match scheduler.add_task(LoadBalancer::new(
        core_id,
        config,
        ports[0].clone(),
        num_types,
        partition,
        storage_load,
        compute_load,
        kth,
        global_recvd,
        init_rdtsc,
        finished,
    )) {
        Ok(_) => {
            info!(
                "Successfully added LB with rx-tx queue {:?}.",
                (ports[0].rxq(), ports[0].txq()),
            )
        }

        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

fn main() {
    db::env_logger::init().expect("ERROR: failed to initialize logger!");

    let config: config::LBConfig = config::load("lb.toml");
    warn!("Starting up Sandstorm client with config {:?}", config);

    // Setup Netbricks.
    let mut net_context = config_and_init_netbricks(&config.lb);
    net_context.start_schedulers();
    // setup shared data
    let partition = Arc::new(AtomicF64::new(config.partition * 100.0));
    let storage_servers: Vec<_> = config
        .storage
        .iter()
        .map(|x| (&x.ip_addr, x.rx_queues))
        .collect();
    let compute_servers: Vec<_> = config
        .compute
        .iter()
        .map(|x| (&x.ip_addr, x.rx_queues))
        .collect();
    let storage_load = Arc::new(ServerLoad::new(
        "storage",
        storage_servers,
        config.moving_exp,
    ));
    let compute_load = Arc::new(ServerLoad::new(
        "compute",
        compute_servers,
        config.moving_exp,
    ));
    let num_types = config.multi_kv.len();
    let mut kth = vec![];
    for _ in 0..num_types {
        let mut kth_type = vec![];
        for _ in 0..config.lb.num_cores {
            kth_type.push(Arc::new(AtomicUsize::new(0)));
        }
        kth.push(kth_type);
    }
    let recvd = Arc::new(AtomicUsize::new(0));
    let init_rdtsc = cycles::rdtsc();
    let finished = Arc::new(AtomicBool::new(false));
    // setup lb
    for (core_id, &core) in net_context.active_cores.clone().iter().enumerate() {
        let cfg = config.clone();
        let kth_copy = kth.clone();
        let partition_copy = partition.clone();
        let recvd_copy = recvd.clone();
        let storage_load_copy = storage_load.clone();
        let compute_load_copy = compute_load.clone();
        let finished_copy = finished.clone();
        net_context.add_pipeline_to_core(
            core,
            Arc::new(
                move |ports, _sib_port, scheduler: &mut StandaloneScheduler| {
                    setup_lb(
                        &cfg.clone(),
                        scheduler,
                        ports,
                        core_id,
                        num_types,
                        partition_copy.clone(),
                        storage_load_copy.clone(),
                        compute_load_copy.clone(),
                        kth_copy.clone(),
                        recvd_copy.clone(),
                        init_rdtsc,
                        finished_copy.clone(),
                    )
                },
            ),
        );
    }

    // Allow the system to bootup fully.
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Run the client.
    net_context.execute();

    // Sleep for an amount of time approximately equal to the estimated execution time, and then
    // shutdown the client.
    // unsafe {
    //     while !FINISHED {
    //         std::thread::sleep(std::time::Duration::from_secs(2));
    //     }
    // }
    while !finished.load(Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    std::thread::sleep(std::time::Duration::from_secs(2));
    // Stop the client.
    net_context.stop();

    if cfg!(feature = "summary") {
        // let (ql_storage_moving, cv_storage) = storage_load.avg_all();
        let (ql_storage_mean, cv_storage) = storage_load.mean_all();
        // let (ql_compute_moving, cv_compute) = compute_load.avg_all();
        let (ql_compute_mean, cv_compute) = compute_load.mean_all();
        print!("{}", storage_load);
        // println!(
        //     "storage summary ql {:.2} mean {:.2} cv {:.2}",
        //     ql_storage_moving, ql_storage_mean, cv_storage
        // );
        println!(
            "storage summary ql {:.2} cv {:.2}",
            ql_storage_mean, cv_storage
        );
        print!("{}", compute_load);
        println!(
            "compute summary ql {:.2} cv {:.2}",
            ql_compute_mean, cv_compute
        );
    }
    // // #[cfg(feature = "server_stats")]
    // // storage_load.print_trace();
    // // // storage_load.write_trace();
    // // #[cfg(feature = "server_stats")]
    // // compute_load.print_trace();
    // // // compute_load.write_trace();
}

// #[cfg(test)]
// mod test {
//     use std;
//     use std::collections::HashMap;
//     use std::sync::atomic::{AtomicBool, Ordering};
//     use std::sync::{Arc, Mutex};
//     use std::thread;
//     use std::time::{Duration, Instant};

//     #[test]
//     fn pushback_abc_basic() {
//         let n_threads = 1;
//         let mut threads = Vec::with_capacity(n_threads);
//         let done = Arc::new(AtomicBool::new(false));

//         for _ in 0..n_threads {
//             let done = done.clone();
//             threads.push(thread::spawn(move || {
//                 let mut b = super::Pushback::new(10, 100, 1000000, 5, 0.99, 1024, 0.1);
//                 let mut n_gets = 0u64;
//                 let mut n_puts = 0u64;
//                 let start = Instant::now();
//                 while !done.load(Ordering::Relaxed) {
//                     b.abc(
//                         |_t, _key, _ord| n_gets += 1,
//                         |_t, _key, _value, _ord| n_puts += 1,
//                     );
//                 }
//                 (start.elapsed(), n_gets, n_puts)
//             }));
//         }

//         thread::sleep(Duration::from_secs(2));
//         done.store(true, Ordering::Relaxed);

//         // Iterate across all threads. Return a tupule whose first member consists
//         // of the highest execution time across all threads, and whose second member
//         // is the sum of the number of iterations run on each benchmark thread.
//         // Dividing the second member by the first, will yeild the throughput.
//         let (duration, n_gets, n_puts) = threads
//             .into_iter()
//             .map(|t| t.join().expect("ERROR: Thread join failed."))
//             .fold(
//                 (Duration::new(0, 0), 0, 0),
//                 |(ldur, lgets, lputs), (rdur, rgets, rputs)| {
//                     (std::cmp::max(ldur, rdur), lgets + rgets, lputs + rputs)
//                 },
//             );

//         let secs = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1e9);
//         println!(
//             "{} threads: {:.0} gets/s {:.0} puts/s {:.0} ops/s",
//             n_threads,
//             n_gets as f64 / secs,
//             n_puts as f64 / secs,
//             (n_gets + n_puts) as f64 / secs
//         );
//     }

//     // Convert a key to u32 assuming little endian.
//     fn convert_key(key: &[u8]) -> u32 {
//         assert_eq!(4, key.len());
//         let k: u32 = 0
//             | key[0] as u32
//             | (key[1] as u32) << 8
//             | (key[2] as u32) << 16
//             | (key[3] as u32) << 24;
//         k
//     }

//     #[test]
//     fn pushback_abc_histogram() {
//         let hist = Arc::new(Mutex::new(HashMap::new()));

//         let n_keys = 20;
//         let n_threads = 1;

//         let mut threads = Vec::with_capacity(n_threads);
//         let done = Arc::new(AtomicBool::new(false));
//         for _ in 0..n_threads {
//             let hist = hist.clone();
//             let done = done.clone();
//             threads.push(thread::spawn(move || {
//                 let mut b = super::Pushback::new(4, 100, n_keys, 5, 0.99, 1024, 0.1);
//                 let mut n_gets = 0u64;
//                 let mut n_puts = 0u64;
//                 let start = Instant::now();
//                 while !done.load(Ordering::Relaxed) {
//                     b.abc(
//                         |_t, key, _ord| {
//                             // get
//                             let k = convert_key(key);
//                             let mut ht = hist.lock().unwrap();
//                             ht.entry(k).or_insert((0, 0)).0 += 1;
//                             n_gets += 1
//                         },
//                         |_t, key, _value, _ord| {
//                             // put
//                             let k = convert_key(key);
//                             let mut ht = hist.lock().unwrap();
//                             ht.entry(k).or_insert((0, 0)).1 += 1;
//                             n_puts += 1
//                         },
//                     );
//                 }
//                 (start.elapsed(), n_gets, n_puts)
//             }));
//         }

//         thread::sleep(Duration::from_secs(2));
//         done.store(true, Ordering::Relaxed);

//         // Iterate across all threads. Return a tupule whose first member consists
//         // of the highest execution time across all threads, and whose second member
//         // is the sum of the number of iterations run on each benchmark thread.
//         // Dividing the second member by the first, will yeild the throughput.
//         let (duration, n_gets, n_puts) = threads
//             .into_iter()
//             .map(|t| t.join().expect("ERROR: Thread join failed."))
//             .fold(
//                 (Duration::new(0, 0), 0, 0),
//                 |(ldur, lgets, lputs), (rdur, rgets, rputs)| {
//                     (std::cmp::max(ldur, rdur), lgets + rgets, lputs + rputs)
//                 },
//             );

//         let secs = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1e9);
//         println!(
//             "{} threads: {:.0} gets/s {:.0} puts/s {:.0} ops/s",
//             n_threads,
//             n_gets as f64 / secs,
//             n_puts as f64 / secs,
//             (n_gets + n_puts) as f64 / secs
//         );

//         let ht = hist.lock().unwrap();
//         let mut kvs: Vec<_> = ht.iter().collect();
//         kvs.sort();
//         let v: Vec<_> = kvs
//             .iter()
//             .map(|&(k, v)| println!("Key {:?}: {:?} gets/puts", k, v))
//             .collect();
//         println!("Unique key count: {}", v.len());
//         assert_eq!(n_keys, v.len());

//         let total: i64 = kvs.iter().map(|&(_, &(g, s))| (g + s) as i64).sum();

//         let mut sum = 0;
//         for &(k, v) in kvs.iter() {
//             let &(g, s) = v;
//             sum += g + s;
//             let percentile = sum as f64 / total as f64;
//             println!("Key {:?}: {:?} percentile", k, percentile);
//         }
//         // For 20 keys median key should be near 4th key, so this checks out.
//     }
// }
