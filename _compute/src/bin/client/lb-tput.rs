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

mod setup;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Display;
use std::mem;
use std::mem::transmute;
use std::sync::Arc;

use self::bytes::Bytes;
use db::config;
use db::cycles;
use db::e2d2::allocators::*;
use db::e2d2::interface::*;
use db::e2d2::scheduler::*;
use db::log::*;
use db::master::Master;
use db::rpc::*;
use db::wireformat::*;

use rand::distributions::{Normal, Sample};
use rand::{Rng, SeedableRng, XorShiftRng};
use splinter::nativestate::PushbackState;
use splinter::proxy::KV;
use splinter::sched::TaskManager;
use splinter::*;
use zipf::ZipfDistribution;

use self::atomic_float::AtomicF64;
use std::sync::atomic::{AtomicUsize, Ordering};

use dispatch::LBDispatcher;
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
    finished: bool,
    requests: usize,
    global_recvd: Arc<AtomicUsize>,
    recvd: usize,
    latencies: Vec<Vec<u64>>,
    kth: Vec<Vec<Arc<AtomicUsize>>>,
    avg_lat: Vec<usize>,
    outstanding_reqs: HashMap<u64, usize>,
    // load balancing
    // storage_load: Arc<ServerLoad>,
    // compute_load: Arc<ServerLoad>,
    // rpc control
    max_out: u32,
    partition: Partition,
    // xloop
    learnable: bool,
    xloop: TputGrad,
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
    // output_last_recvd: usize,
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
        kth: Vec<Vec<Arc<AtomicUsize>>>,
        global_recvd: Arc<AtomicUsize>,
        init_rdtsc: u64,
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

        LoadBalancer {
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
            finished: false,
            requests: config.num_reqs / 2,
            global_recvd: global_recvd,
            recvd: 0,
            latencies: latencies,
            kth: kth,
            avg_lat: vec![0; num_types],
            outstanding_reqs: HashMap::new(),
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
            xloop: TputGrad::new(
                config.xloop_factor,
                config.lr,
                config.min_step,
                config.max_step,
            ),
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
            // output_last_recvd: 0,
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

    fn send_once(&mut self) {
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
        self.outstanding_reqs.insert(curr, type_id);
    }

    fn send_all(&mut self) {
        self.start = cycles::rdtsc();
        for _ in 0..self.max_out {
            self.send_once();
        }
    }

    // fn update_load(&self, src_ip: u32, src_port: u16, core_load: u64) {
    //     if core_load == 0 {
    //         return;
    //     }
    //     if let Ok(_) = self.storage_load.update(src_ip, src_port, core_load as f64) {
    //     } else {
    //         self.compute_load.update(src_ip, src_port, core_load as f64);
    //     }
    // }

    fn recv(&mut self) {
        // Don't do anything after all responses have been received.
        if self.finished == true && self.stop > 0 {
            return;
        }

        let mut packet_recvd_signal = false;

        // Try to receive packets from the network port.
        // If there are packets, sample the latency of the server.
        // TODO: process server_load in hdr (from compute or storage?)
        // TODO: our algorithm
        if let Some(mut packets) = self.dispatcher.receiver.recv() {
            let curr = cycles::rdtsc();
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
                                if let Some(type_id) = self.outstanding_reqs.get(&timestamp) {
                                    trace!("req finished");
                                    packet_recvd_signal = true;
                                    self.recvd += 1;
                                    self.global_recvd.fetch_add(1, Ordering::Relaxed);
                                    // self.update_load(src_ip, src_port, hdr.core_load);
                                    self.latencies[*type_id].push(curr - timestamp);
                                    self.outstanding_reqs.remove(&timestamp);
                                    self.send_once();
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
                    if self.learnable {
                        self.xloop.xloop(&self.partition, curr_rdtsc, global_recvd);
                    }
                    // let xloop_rate = 2.4e6 * (global_recvd - self.xloop_last_recvd) as f32
                    //     / (xloop_rdtsc - self.xloop_last_rdtsc) as f32;
                    if self.output_factor != 0
                        && (curr_rdtsc - self.output_last_rdtsc > 2400000000 / self.output_factor)
                    {
                        // tput
                        // let output_rate = 2.4e6 * (global_recvd - self.output_last_recvd) as f32
                        //     / (curr_rdtsc - self.output_last_rdtsc) as f32;
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
                        let partition = self.partition.get() / 100.0;
                        println!(
                            "rdtsc {} tail {:?} {}",
                            curr_rdtsc, self.avg_lat, self.xloop,
                        );
                        self.output_last_rdtsc = curr_rdtsc;
                        // self.output_last_recvd = global_recvd;
                    }
                    // // X-loop with tput
                    // if self.learnable
                    //     && packet_recvd_signal
                    //     && (curr_rdtsc - self.xloop_last_rdtsc
                    //         > 2400000000 / self.xloop_factor as u64)
                    //     && global_recvd > 100
                    //     && global_recvd % self.xloop_factor as u64 == 0
                    // {
                    //     let xloop_rate = 2.4e6 * (global_recvd - self.xloop_last_recvd) as f32
                    //         / (curr_rdtsc - self.xloop_last_rdtsc) as f32;
                    //     self.xloop_last_rdtsc = curr_rdtsc;
                    //     self.xloop_last_recvd = global_recvd;
                    //     // ours loop: grad tput/x
                    //     // delta tput
                    //     let delta_rate = xloop_rate - self.xloop_last_rate;
                    //     self.xloop_last_rate = xloop_rate;
                    //     // delta x
                    //     let x = self.partition.load(Ordering::Relaxed);
                    //     let delta_x = x - self.xloop_last_x;
                    //     self.xloop_last_x = x;
                    //     // TODO: add grad factor as config param
                    //     let grad = 2.0 * delta_rate / delta_x;
                    //     let mut bounded_offset_x: f32;
                    //     if grad > 0.0 {
                    //         if grad < 1.0 {
                    //             bounded_offset_x = 1.0;
                    //         } else if grad > 20.0 {
                    //             bounded_offset_x = 5.0;
                    //         } else {
                    //             bounded_offset_x = grad;
                    //         }
                    //     } else {
                    //         if grad > -1.0 {
                    //             bounded_offset_x = -1.0;
                    //         } else if grad < -20.0 {
                    //             bounded_offset_x = -5.0;
                    //         } else {
                    //             bounded_offset_x = grad;
                    //         }
                    //     }
                    //     let mut new_x = x + bounded_offset_x;
                    //     if new_x > 100.0 || new_x < 0.0 {
                    //         new_x = x - bounded_offset_x; // bounce back
                    //     }
                    //     self.partition.store(new_x, Ordering::Relaxed);
                    //     trace!(
                    //         "rdtsc {} rate {} partition {} d_rate {} d_x {} step {} ",
                    //         curr_rdtsc,
                    //         xloop_rate,
                    //         x,
                    //         delta_rate,
                    //         delta_x,
                    //         bounded_offset_x,
                    //     );
                    // }
                }
            }
        }

        // The moment all response packets have been received, set the value of the
        // stop timestamp so that throughput can be estimated later.
        if self.requests <= self.recvd {
            self.stop = cycles::rdtsc();
            self.finished = true;
        }
    }
}

// Implementation of the `Drop` trait on PushbackRecv.
impl Drop for LoadBalancer {
    fn drop(&mut self) {
        if self.stop == 0 {
            self.stop = cycles::rdtsc();
            info!(
                "The client thread {} received only {} packets",
                self.id, self.recvd
            );
        } else {
            info!("client thread {} recvd {}", self.id, self.recvd);
        }

        // Calculate & print the throughput for all client threads.
        if self.id == 0 {
            println!(
                "PUSHBACK Throughput {}",
                self.global_recvd.load(Ordering::Relaxed) as f64
                    / cycles::to_seconds(self.stop - self.start) // self.tput_meter.avg()
            );
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
                    "type {} >>> lat50:{} lat99:{}",
                    type_id,
                    cycles::to_seconds(m) * 1e9,
                    cycles::to_seconds(t) * 1e9
                );
            }
        }
    }
}

// Executable trait allowing PushbackRecv to be scheduled by Netbricks.
impl Executable for LoadBalancer {
    // Called internally by Netbricks.
    fn execute(&mut self) {
        if self.start == 0 {
            self.send_all();
        }
        // trace!("{:?}", self.outstanding_reqs.keys());
        self.recv();
        if self.finished == true {
            unsafe { FINISHED = true }
            return;
        }
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

fn setup_lb(
    config: &config::LBConfig,
    ports: Vec<CacheAligned<PortQueue>>,
    scheduler: &mut StandaloneScheduler,
    core_id: i32,
    num_types: usize,
    partition: Arc<AtomicF64>,
    kth: Vec<Vec<Arc<AtomicUsize>>>,
    global_recvd: Arc<AtomicUsize>,
    init_rdtsc: u64,
) {
    if ports.len() != 1 {
        error!("LB should be configured with exactly 1 port!");
        std::process::exit(1);
    }
    match scheduler.add_task(LoadBalancer::new(
        core_id as usize,
        config,
        ports[0].clone(),
        num_types,
        partition,
        kth,
        global_recvd,
        init_rdtsc,
    )) {
        Ok(_) => {
            info!("Successfully added LB with rx-tx queue {}.", ports[0].rxq());
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
    let mut net_context =
        setup::config_and_init_netbricks(config.nic_pci.clone(), config.src.num_ports);
    net_context.start_schedulers();
    // setup shared data
    let partition = Arc::new(AtomicF64::new(config.partition * 100.0));
    let num_types = config.multi_kv.len();
    let mut kth = vec![];
    for _ in 0..num_types {
        let mut kth_type = vec![];
        for _ in 0..config.src.num_ports {
            kth_type.push(Arc::new(AtomicUsize::new(0)));
        }
        kth.push(kth_type);
    }
    let recvd = Arc::new(AtomicUsize::new(0));
    let init_rdtsc = cycles::rdtsc();
    // Setup the client pipeline.
    for core_id in 0..config.src.num_ports {
        let kth_copy = kth.clone();
        let partition_copy = partition.clone();
        let recvd_copy = recvd.clone();
        net_context.add_pipeline_to_core(
            core_id as i32,
            Arc::new(
                move |ports, sched: &mut StandaloneScheduler, core: i32, _sibling| {
                    setup_lb(
                        &config::load::<config::LBConfig>("lb.toml"),
                        ports,
                        sched,
                        core,
                        num_types,
                        partition_copy.clone(),
                        kth_copy.clone(),
                        recvd_copy.clone(),
                        init_rdtsc,
                    )
                },
            ),
        );
    }

    // Allow the system to bootup fully.
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Run the client.
    net_context.execute();

    // Sleep for an amount of time approximately equal to the estimated execution time, and then
    // shutdown the client.
    unsafe {
        while !FINISHED {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Stop the client.
    net_context.stop();
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
