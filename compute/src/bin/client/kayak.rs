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
use std::collections::{HashMap, HashSet};
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

use dispatch::KayakDispatcher;
use std::fmt::{self, Write};
use std::fs::File;
use std::io::Write as writef;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::RwLock;
use workload::*;
use xloop::*;

#[derive(Default)]
struct Slot {
    counter: usize,
    type_id: usize,
}
/*
struct Workload {
    // rng: Box<dyn Rng>,
    key_rng: Box<ZipfDistribution>,
    // tenant_rng: Box<ZipfDistribution>,
    payload: Vec<u8>,
    // name of extension, e.g. pushback
    name_len: u32,
    // key_len: usize,
    key_offset: usize,
}

impl Workload {
    fn set_query_payload(config: &WorkloadConfig, table: &TableConfig, payload: &mut Vec<u8>) {
        let mut value_len = if config.opcode == 2 {
            // for multiget
            table.record_len as usize
        } else {
            table.value_len
        };
        payload.extend_from_slice(value_len.to_le_bytes());
        payload.extend_from_slice(&table.record_len.to_le_bytes());
        payload.extend_from_slice(&config.opcode.to_le_bytes());
    }
    fn new(config: &WorkloadConfig, table: &TableConfig) -> Workload {
        let extension = config.extension.as_bytes();
        // let key_offset =
        //     extension.len() + mem::size_of::<u64>() + mem::size_of::<u32>() + mem::size_of::<u32>();
        // let payload_len = key_offset + table.key_len;
        // let mut payload = Vec::with_capacity(payload_len);
        // payload.extend_from_slice(extension);
        // // TODO: introduce variation in kv and order, these fields will not be static
        // payload.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(config.table_id.to_le()) });
        // payload.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(config.kv.to_le()) });
        // payload.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(config.order.to_le()) });
        // payload.resize(payload_len, 0);
        let mut payload = vec![];
        payload.extend_from_slice(extension);
        // payload.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(config.table_id.to_le()) });
        payload.extend_from_slice(&config.table_id.to_le_bytes());
        if config.opcode == 0 {
            // TODO: introduce variation in kv and order, these fields will not be static
            payload.extend_from_slice(&table.value_len.to_le_bytes());
            payload.extend_from_slice(&config.kv.to_le_bytes());
            payload.extend_from_slice(&config.order.to_le_bytes());
        } else {
            // for real workload
            Self::set_query_payload(config, table, payload);
        }
        let key_offset = payload.len();
        let payload_len = key_offset + table.key_len;
        payload.resize(payload_len, 0);
        Workload {
            // rng: {
            //     let seed: [u32; 4] = rand::random::<[u32; 4]>();
            //     Box::new(XorShiftRng::from_seed(seed))
            // },
            key_rng: Box::new(
                ZipfDistribution::new(table.num_records as usize, config.skew)
                    .expect("Couldn't create key RNG."),
            ),
            payload: payload,
            name_len: extension.len() as u32,
            // key_len: config.key_len,
            key_offset: key_offset,
        }
    }
    fn sample_key(&mut self, rng: &mut impl Rng) -> &[u8] {
        let key = self.key_rng.sample(rng) as u32;
        let key: [u8; 4] = unsafe { transmute(key.to_le()) };
        self.payload[self.key_offset..self.key_offset + key.len()].copy_from_slice(&key);
        &self.payload
    }
}
*/

struct LoadGenerator {
    rng: Box<XorShiftRng>,
    tenant_rng: Box<ZipfDistribution>,
    workloads: Vec<Box<dyn Workload>>,
    loop_interval: u64,
    junctures: Vec<u64>,
    cum_ratios: Vec<Vec<f32>>, // 0-10000
}

impl LoadGenerator {
    fn new(config: &KayakConfig) -> LoadGenerator {
        // workloads
        let mut workloads = vec![];
        for workload in &config.workloads {
            let table_id = workload.table_id as usize;
            let table = &config.tables[table_id - 1];
            // workloads.push(Workload::new(workload, table));
            workloads.push(create_workload(workload, table));
        }
        // phases
        let mut cum_time = 0u64;
        let mut junctures = vec![];
        let mut cum_ratios = vec![];
        for phase in &config.phases {
            cum_time += phase.duration;
            junctures.push(cum_time);
            let mut cum_sum = 0f32;
            let cum_ratio = phase
                .ratios
                .iter()
                .map(|&x| {
                    cum_sum += x;
                    cum_sum * 100.0
                })
                .collect();
            cum_ratios.push(cum_ratio);
        }
        LoadGenerator {
            rng: {
                let seed: [u32; 4] = rand::random::<[u32; 4]>();
                Box::new(XorShiftRng::from_seed(seed))
            },
            tenant_rng: Box::new(
                ZipfDistribution::new(config.num_tenants, config.tenant_skew)
                    .expect("Couldn't create key RNG."),
            ),
            workloads: workloads,
            loop_interval: cum_time,
            junctures: junctures,
            cum_ratios: cum_ratios,
        }
    }
    fn gen_request(
        &mut self,
        mut curr_rdtsc: u64,
        partition: f64,
    ) -> (bool, usize, u32, u32, &[u8]) {
        let phase_id = if self.loop_interval > 0 {
            curr_rdtsc %= self.loop_interval;
            self.junctures.iter().position(|&t| t > curr_rdtsc).unwrap()
        } else {
            0
        };
        let cum_ratio = &self.cum_ratios[phase_id];
        let rand_ratio = (self.rng.gen::<u32>() % 10000) as f32;
        let workload_id = cum_ratio.iter().position(|&p| p > rand_ratio).unwrap();
        let rand_prob = (self.rng.gen::<u32>() % 10000) as f64;
        (
            partition > rand_prob,
            workload_id,
            self.tenant_rng.sample(&mut self.rng) as u32,
            // self.workloads[workload_id].name_len,
            self.workloads[workload_id].name_len(),
            // self.workloads[workload_id].sample_key(&mut self.rng),
            self.workloads[workload_id].gen(&mut self.rng),
        )
    }
}

#[derive(Clone)]
struct TypeStats {
    cost_storage: Avg,
    cost_compute: Avg,
    // NOTE: we need two fields for storage, in case parition is in the middle of some type
    overhead_storage: Avg,
    // TODO: avoid using out-of-date response, set timestamp after xloop
    // timestamp: u64,
}
impl TypeStats {
    fn new() -> TypeStats {
        TypeStats {
            cost_storage: Avg::new(),
            cost_compute: Avg::new(),
            overhead_storage: Avg::new(),
        }
    }
    fn update(&mut self, c_s: f64, c_c: f64, o_s: f64) {
        if c_s > 0f64 {
            // from storage
            self.cost_storage.update(c_s);
        } else {
            // from compute
            self.cost_compute.update(c_c);
            self.overhead_storage.update(o_s);
        }
    }
    fn get_counter(&self) -> f64 {
        self.cost_storage.counter + self.cost_compute.counter
    }
    fn get_cost(&self) -> f64 {
        let c = self.cost_compute.avg();
        let s = self.cost_storage.avg();
        // c.max(s)
        c.max(s).max(12000 as f64)
    }
    fn merge(&mut self, other: &TypeStats) {
        self.cost_storage.merge(&other.cost_storage);
        self.cost_compute.merge(&other.cost_compute);
        self.overhead_storage.merge(&other.overhead_storage);
    }
    // TODO:
    // 1. return counter in get_key(counter c_s + c_c); delete type counter, use returned counter to compare with thershold
    // 2. expose one for concurrent update, another for local sort
    // 4. periodically snapshot stats in time window; compare with local stats(impl partial_eq); batch update local
    // 5. sort and write to global type_order
}

#[derive(Clone)]
struct Sampler {
    type_stats: Vec<Arc<RwLock<TypeStats>>>,
    type_cost: Vec<Arc<AtomicF64>>,
    interval: u64,
    last_rdtsc: u64,
}

impl Sampler {
    fn new(config: &KayakConfig) -> Sampler {
        let mut type_stats = vec![];
        let mut type_cost = vec![];
        for _ in 0..config.workloads.len() {
            type_stats.push(Arc::new(RwLock::new(TypeStats::new())));
            type_cost.push(Arc::new(AtomicF64::new(0.0)));
        }
        Sampler {
            type_stats: type_stats,
            type_cost: type_cost,
            interval: CPU_FREQUENCY / config.sample_factor,
            last_rdtsc: 0,
        }
    }
    fn sample(&self) {
        for i in 0..self.type_cost.len() {
            let stats = self.type_stats[i].read().unwrap();
            self.type_cost[i].store(stats.get_cost(), Ordering::Relaxed);
        }
    }
    fn get_cost(&self, type_id: usize) -> f64 {
        self.type_cost[type_id].load(Ordering::Relaxed)
        // self.type_stats[type_id].read().unwrap().get_cost()
    }
    fn update_stats(&self, type_id: usize, c_s: f64, c_c: f64, o_s: f64) {
        self.type_stats[type_id]
            .write()
            .unwrap()
            .update(c_s, c_c, o_s);
    }
    fn ready(&mut self, curr_rdtsc: u64 /*, recvd: usize*/) -> bool {
        if self.last_rdtsc == 0 {
            self.last_rdtsc = curr_rdtsc;
            false
        } else if curr_rdtsc - self.last_rdtsc > self.interval {
            self.last_rdtsc = curr_rdtsc;
            true
        } else {
            false
        }
    }
}

/// Receives responses to PUSHBACK requests sent out by PushbackSend.
struct LoadBalancer {
    dispatcher: KayakDispatcher,
    generator: LoadGenerator,
    // worker stats
    id: usize,
    init_rdtsc: u64,
    start: u64,
    stop: u64,
    duration: u64, // stop when curr - start > duration
    global_recvd: Arc<AtomicUsize>,
    recvd: usize,
    // latencies: Vec<u64>,
    latencies: Vec<Vec<u64>>,
    outstanding_reqs: HashMap<u64, usize>,
    // slots: Vec<Slot>,
    // rpc control
    max_out: u32,
    learnable: bool,
    partition: Arc<AtomicF64>,
    // TODO: add kayak's params, e.g. xloop_factor
    xloop_last_rdtsc: u64,
    xloop_last_recvd: u64,
    xloop_last_tput: f64,
    xloop_last_X: f64,
    xloop_factor: u64,
    xloop_learning_rate: f64,
    xloop_interval: Vec<f64>,
    tput_vec: Vec<f64>,
    rpc_vec: Vec<f64>,

    output: bool,
    output_factor: u64,
    output_last_rdtsc: u64,
    output_last_recvd: usize,
    finished: Arc<AtomicBool>,
    tput: f64,
    cfg: config::KayakConfig,
    sampler: Sampler,
    rloop: Rloop,
    log: Vec<(f64, f64)>,
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
        config: &config::KayakConfig,
        net_port: CacheAligned<PortQueue>,
        partition: Arc<AtomicF64>,
        global_recvd: Arc<AtomicUsize>,
        init_rdtsc: u64,
        finished: Arc<AtomicBool>,
        sampler: Sampler,
        rloop: Rloop,
    ) -> LoadBalancer {
        // let mut slots = vec![];
        // for _ in 0..config.max_out {
        //     slots.push(Slot::default());
        // }
        let mut latencies = vec![];
        for _ in 0..config.workloads.len() {
            latencies.push(Vec::with_capacity(4000000));
        }

        LoadBalancer {
            duration: config.duration * CPU_FREQUENCY,
            tput: 0.0,
            finished: finished,
            dispatcher: KayakDispatcher::new(config, net_port),
            generator: LoadGenerator::new(config),
            // worker stats
            id: id,
            init_rdtsc: init_rdtsc,
            start: 0,
            stop: 0,
            global_recvd: global_recvd,
            recvd: 0,
            // latencies: Vec::with_capacity(64000000), // latencies: latencies,
            latencies: latencies,
            outstanding_reqs: HashMap::new(),
            // slots: slots,
            max_out: config.max_out,
            partition: partition,
            learnable: config.learnable,
            xloop_last_rdtsc: 0,
            xloop_last_recvd: 0,
            xloop_last_tput: 0.0,
            xloop_last_X: 45.0,
            xloop_factor: config.xloop_factor,
            xloop_learning_rate: config.xloop_learning_rate,
            xloop_interval: Vec::new(),
            tput_vec: Vec::new(),
            rpc_vec: Vec::new(),
            output: config.output,
            output_factor: config.output_factor,
            output_last_rdtsc: 0,
            output_last_recvd: 0,
            cfg: config.clone(),
            sampler: sampler,
            rloop: rloop,
            log: Vec::with_capacity(40000),
        }
    }

    fn send_once(&mut self /*, slot_id: usize*/) {
        let curr = cycles::rdtsc() - self.start;
        let partition = self.partition.load(Ordering::Relaxed);
        let (to_storage, type_id, tenant, name_len, request_payload) =
            self.generator.gen_request(curr, partition);
        if to_storage {
            let (ip, port) =
                self.dispatcher
                    .sender2storage
                    .send_invoke(tenant, name_len, request_payload, curr);
        } else {
            let (ip, port) =
                self.dispatcher
                    .sender2compute
                    .send_invoke(tenant, name_len, request_payload, curr);
        }
        // self.slots[slot_id].counter += 1;
        // self.slots[slot_id].type_id = workload_id;
        // self.outstanding_reqs.insert(curr, slot_id);
        self.outstanding_reqs.insert(curr, type_id);
    }

    fn send_all(&mut self) {
        if self.start == 0 {
            self.start = cycles::rdtsc();
        }
        for i in 0..self.max_out as usize {
            self.send_once();
        }
    }

    fn recv(&mut self) {
        let mut packet_recvd_signal = false;

        // Try to receive packets from the network port.
        // If there are packets, sample the latency of the server.
        if let Some(mut packets) = self.dispatcher.receiver.recv(|pkt| Some(pkt)) {
            let curr_rdtsc = cycles::rdtsc() - self.start;
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
                                if let Some(&type_id) = self.outstanding_reqs.get(&timestamp) {
                                    // let type_id = self.slots[slot_id].type_id;
                                    trace!("req type {} finished", type_id);
                                    packet_recvd_signal = true;
                                    self.recvd += 1;
                                    self.global_recvd.fetch_add(1, Ordering::Relaxed);
                                    // self.latencies[type_id].push(curr_rdtsc - timestamp);
                                    let order = self.sampler.get_cost(type_id);
                                    if order > 0.0 {
                                        self.latencies[type_id].push(curr_rdtsc - timestamp);
                                        self.rloop.record_latency(curr_rdtsc - timestamp, order);
                                    }
                                    if hdr.overhead > 0 {
                                        self.sampler.update_stats(
                                            type_id,
                                            0.0,
                                            hdr.common_header.duration as f64,
                                            hdr.overhead as f64,
                                        );
                                    } else {
                                        self.sampler.update_stats(
                                            type_id,
                                            hdr.common_header.duration as f64,
                                            0.0,
                                            0.0,
                                        );
                                    }
                                    self.outstanding_reqs.remove(&timestamp);
                                    self.send_once(/*slot_id*/);
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
                let curr_rdtsc = cycles::rdtsc() - self.start;
                if packet_recvd_signal && self.rloop.ready(curr_rdtsc) {
                    self.rloop.rate_control(self.max_out as usize);
                }
                if self.id == 0 {
                    let global_recvd = self.global_recvd.load(Ordering::Relaxed);
                    // TODO: impl kayak xloop here
                    // if self.learnable && xxxx
                    // NOTE: self.partition in [0,10000], see fn main
                    if self.output_factor != 0
                        && (curr_rdtsc - self.output_last_rdtsc
                            > CPU_FREQUENCY / self.output_factor)
                    {
                        let output_tput = (global_recvd - self.output_last_recvd) as f64
                            * (CPU_FREQUENCY / 1000) as f64
                            / (curr_rdtsc - self.output_last_rdtsc) as f64;
                        self.output_last_recvd = global_recvd;
                        self.output_last_rdtsc = curr_rdtsc;
                        let tail = self.rloop.tail.load(Ordering::Relaxed);
                        if self.output {
                            println!(
                                "rdtsc {} tput {:.2} tail {:.2} rpc {:.2}",
                                curr_rdtsc / (CPU_FREQUENCY / 1000),
                                output_tput,
                                tail,
                                // self.rloop.std,
                                self.partition.load(Ordering::Relaxed),
                            );
                        }
                        self.log.push((output_tput, tail));
                        // self.tput_vec.push(output_tput);
                        // self.rpc_vec
                        //     .push((self.partition.load(Ordering::Relaxed) as f64) / 100.0);
                    }

                    if self.xloop_factor != 0
                        && packet_recvd_signal
                        && (curr_rdtsc - self.xloop_last_rdtsc > CPU_FREQUENCY / self.xloop_factor)
                        && global_recvd > 100
                        && global_recvd as u64 % self.xloop_factor == 0
                    {
                        self.xloop_interval.push(
                            (curr_rdtsc - self.xloop_last_rdtsc) as f64
                                / (CPU_FREQUENCY / 1000) as f64,
                        );
                        let xloop_tput = (global_recvd as u64 - self.xloop_last_recvd) as f64
                            * (CPU_FREQUENCY / 1000) as f64
                            / (curr_rdtsc - self.xloop_last_rdtsc) as f64;
                        self.xloop_last_rdtsc = curr_rdtsc;
                        self.xloop_last_recvd = global_recvd as u64;

                        if self.learnable {
                            let delta_tput = xloop_tput - self.xloop_last_tput;
                            self.xloop_last_tput = xloop_tput;
                            let x = (self.partition.load(Ordering::Relaxed) as f64) / 100.0;
                            let delta_X = x - self.xloop_last_X;
                            self.xloop_last_X = x;
                            let grad = self.xloop_learning_rate * delta_tput / delta_X;
                            let mut bounded_offset_X: f64 = -1.0;
                            if grad > 0.0 {
                                if grad < 1.0 {
                                    bounded_offset_X = 1.0;
                                } else if grad > 10.0 {
                                    bounded_offset_X = 5.0;
                                } else {
                                    bounded_offset_X = grad;
                                }
                            } else {
                                if grad > -1.0 {
                                    bounded_offset_X = -1.0;
                                } else if grad < -10.0 {
                                    bounded_offset_X = -5.0;
                                } else {
                                    bounded_offset_X = grad;
                                }
                            }
                            debug!("delta_X {:.2} delta_tput/delta_X {:.2}, grad {:.2}, bounded_offset_X {:.2}",
                                    delta_X, delta_tput / delta_X, grad, bounded_offset_X);
                            let mut new_X = bounded_offset_X + x;
                            if new_X > 100.0 || new_X < 0.0 {
                                new_X = x - bounded_offset_X;
                            }
                            self.partition.store(new_X * 100.0, Ordering::Relaxed);
                        }
                        if self.sampler.ready(curr_rdtsc) && packet_recvd_signal {
                            self.sampler.sample();
                        }
                    }
                }
            }
        }
        let curr_rdtsc = cycles::rdtsc() - self.start;
        if curr_rdtsc > self.duration {
            self.stop = curr_rdtsc;
            if !self.finished.swap(true, Ordering::Relaxed) {
                self.tput = (self.global_recvd.load(Ordering::Relaxed) as f64)
                    * CPU_FREQUENCY as f64
                    / self.stop as f64;
                // self.dispatcher.sender2storage.send_reset();
                // self.dispatcher.sender2compute.send_reset();
            }
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
            // let slots = self.slots.iter().map(|x| x.counter).collect::<Vec<_>>();
            // let mean = slots.iter().sum::<usize>() as f32 / self.slots.len() as f32;
            // let std_dev = (slots
            //     .iter()
            //     .map(|&x| (x as f32 - mean) * (x as f32 - mean))
            //     .sum::<f32>()
            //     / self.slots.len() as f32)
            //     .sqrt();
            // info!(
            //     "client thread {} recvd {} slots {}x(mean:{:.2},std:{:.2})",
            //     self.id, self.recvd, self.max_out, mean, std_dev,
            // );
            info!("client thread {} recvd {}", self.id, self.recvd);
        }
        if self.tput > 0.0 {
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            let mut metric = 0.0;
            for (i, lat) in self.latencies.iter_mut().enumerate() {
                let len = lat.len();
                let tail = *order_stat::kth(&mut lat[..], (len * 99 / 100) - 1);
                let meantime = self.sampler.get_cost(i);
                println!(
                    ">>> {} meantime {:.0} lat99:{}({:.2})",
                    i,
                    meantime,
                    tail,
                    tail as f64 / meantime,
                );
                if tail as f64 / meantime > metric {
                    metric = tail as f64 / meantime;
                }
            }
            // println!("total SLO metric {:.2}", metric);
            // let mut lat = &mut self.latencies;
            // lat.sort();
            // let m;
            // let t = lat[(lat.len() * 99) / 100];
            // match lat.len() % 2 {
            //     0 => {
            //         let n = lat.len();
            //         m = (lat[n / 2] + lat[(n / 2) + 1]) / 2;
            //     }

            //     _ => m = lat[lat.len() / 2],
            // }
            // println!(
            //     ">>> lat50:{:.2} lat99:{:.2}",
            //     cycles::to_seconds(m) * 1e9,
            //     cycles::to_seconds(t) * 1e9
            // );
            // println!("PUSHBACK Throughput {:.2}", self.tput.moving());
            // println!("PUSHBACK Throughput {:.2}", self.tput);
        }
        if self.id == 0 {
            std::thread::sleep(std::time::Duration::from_secs(2));
            // let total_slo = self
            //     .rloop
            //     .log
            //     .iter()
            //     .map(|&(t, slo)| slo as f64)
            //     .sum::<f64>()
            //     / self.rloop.log.len() as f64;
            let len = self.log.len();
            let tput = self.log.iter().map(|&(tput, slo)| tput).sum::<f64>() / len as f64;
            let slo = self.log.iter().map(|&(tput, slo)| slo).sum::<f64>() / len as f64;
            println!("PUSHBACK Throughput {:.2}", tput);
            println!("total SLO metric {:.2}", slo);
            let mut avg_x_interval: f64 = 0.0;
            let mut std_x_interval: f64 = 0.0;
            // println!("xloop learning rate: {}", self.xloop_learning_rate);
            println!("number of xloop tunes: {}", self.xloop_interval.len());
            for i in &self.xloop_interval[1..] {
                avg_x_interval += i;
            }
            avg_x_interval /= self.xloop_interval.len() as f64;
            println!("xloop interval avg: {}", avg_x_interval);
            for i in &self.xloop_interval[1..] {
                std_x_interval += (i - avg_x_interval) * (i - avg_x_interval);
            }
            std_x_interval /= (self.xloop_interval.len() - 1) as f64;
            std_x_interval = std_x_interval.sqrt();
            println!("xloop interval std: {}", std_x_interval);
            // for t in 0..self.tput_vec.len() {
            //     println!(
            //         "tput {:.2} rpc {:.2}",
            //         &self.tput_vec[t as usize], &self.rpc_vec[t as usize]
            //     );
            // }
            for (i, history) in self.sampler.type_stats.iter().enumerate() {
                let history = history.read().unwrap();
                println!(
                    "type {} cnt {:.0} s {:.2}({:.0}) c {:.2}({:.0}) s' {:.2}",
                    i,
                    history.get_counter(),
                    history.cost_storage.avg(),
                    history.cost_storage.counter,
                    history.cost_compute.avg(),
                    history.cost_compute.counter,
                    history.overhead_storage.avg(),
                );
            }
        }
    }
}

// Executable trait allowing PushbackRecv to be scheduled by Netbricks.
impl Executable for LoadBalancer {
    // Called internally by Netbricks.
    fn execute(&mut self) {
        if self.stop > 0 {
            return;
        }
        if self.start == 0 {
            let storage_cores = self.cfg.provision.storage;
            let compute_cores = self.cfg.provision.compute;
            if self.id == 0 {
                // this message is sent to all compute nodes, regardless of compute provision
                self.dispatcher.sender2compute.send_scaling(storage_cores);
            }
            self.dispatcher
                .sender2storage
                .set_endpoints(storage_cores as usize);
            // compute
            self.dispatcher
                .sender2compute
                .set_endpoints(compute_cores as usize);
            self.send_all();
        }
        self.recv();
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

fn setup_lb(
    config: &config::KayakConfig,
    scheduler: &mut StandaloneScheduler,
    ports: Vec<CacheAligned<PortQueue>>,
    core_id: usize,
    partition: Arc<AtomicF64>,
    global_recvd: Arc<AtomicUsize>,
    init_rdtsc: u64,
    finished: Arc<AtomicBool>,
    sampler: Sampler,
    rloop: Rloop,
) {
    if ports.len() != 1 {
        error!("LB should be configured with exactly 1 port!");
        std::process::exit(1);
    }
    match scheduler.add_task(LoadBalancer::new(
        core_id,
        config,
        ports[0].clone(),
        partition,
        global_recvd,
        init_rdtsc,
        finished,
        sampler,
        rloop,
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

    let mut config: config::KayakConfig = config::load("kayak.toml");
    config
        .compute
        .sort_by_key(|server| u32::from(Ipv4Addr::from_str(&server.ip_addr).unwrap()));
    config
        .storage
        .sort_by_key(|server| u32::from(Ipv4Addr::from_str(&server.ip_addr).unwrap()));
    warn!("Starting up Sandstorm client with config {:?}", config);

    // Setup Netbricks.
    let mut net_context = config_and_init_netbricks(&config.lb);
    net_context.start_schedulers();
    // setup shared data
    let partition = Arc::new(AtomicF64::new(config.partition * 100.0));
    let recvd = Arc::new(AtomicUsize::new(0));
    let init_rdtsc = cycles::rdtsc();
    let finished = Arc::new(AtomicBool::new(false));
    let sampler = Sampler::new(&config);
    let rloop = Rloop::new(&config.rloop);
    // setup lb
    for (core_id, &core) in net_context.active_cores.clone().iter().enumerate() {
        let cfg = config.clone();
        let partition_copy = partition.clone();
        let recvd_copy = recvd.clone();
        let finished_copy = finished.clone();
        let sampler_copy = sampler.clone();
        let rloop_copy = rloop.clone();
        net_context.add_pipeline_to_core(
            core,
            Arc::new(
                move |ports, _sib_port, scheduler: &mut StandaloneScheduler| {
                    setup_lb(
                        &cfg.clone(),
                        scheduler,
                        ports,
                        core_id,
                        partition_copy.clone(),
                        recvd_copy.clone(),
                        init_rdtsc,
                        finished_copy.clone(),
                        sampler_copy.clone(),
                        rloop_copy.clone(),
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
