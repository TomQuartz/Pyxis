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
use xloop::*;

#[derive(Default)]
struct Slot {
    counter: usize,
    type_id: usize,
}

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
    fn new(config: &WorkloadConfig, table: &TableConfig) -> Workload {
        let extension = config.extension.as_bytes();
        let key_offset =
            extension.len() + mem::size_of::<u64>() + mem::size_of::<u32>() + mem::size_of::<u32>();
        let payload_len = key_offset + table.key_len;
        let mut payload = Vec::with_capacity(payload_len);
        payload.extend_from_slice(extension);
        payload.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(config.table_id.to_le()) });
        payload.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(config.kv.to_le()) });
        payload.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(config.order.to_le()) });
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

struct LoadGenerator {
    rng: Box<dyn Rng>,
    tenant_rng: Box<ZipfDistribution>,
    workloads: Vec<Workload>,
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
            workloads.push(Workload::new(workload, table));
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
            self.workloads[workload_id].name_len,
            self.workloads[workload_id].sample_key(&mut self.rng),
        )
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
    latencies: Vec<u64>,
    outstanding_reqs: HashMap<u64, usize>,
    slots: Vec<Slot>,
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

    output_factor: u64,
    output_last_rdtsc: u64,
    output_last_recvd: usize,
    finished: Arc<AtomicBool>,
    tput: f64,
    // cfg: config::KayakConfig,
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
    ) -> LoadBalancer {
        let mut slots = vec![];
        for _ in 0..config.max_out {
            slots.push(Slot::default());
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
            latencies: Vec::with_capacity(64000000), // latencies: latencies,
            outstanding_reqs: HashMap::new(),
            slots: slots,
            max_out: config.max_out,
            partition: partition,
            learnable: config.learnable,
            xloop_last_rdtsc: cycles::rdtsc(),
            xloop_last_recvd: 0,
            xloop_last_tput: 0.0,
            xloop_last_X: 45.0,
            xloop_factor: config.xloop_factor,
            xloop_learning_rate: config.xloop_learning_rate,
            xloop_interval: Vec::new(),
            tput_vec: Vec::new(),
            rpc_vec: Vec::new(),
            output_factor: config.output_factor,
            output_last_rdtsc: init_rdtsc,
            output_last_recvd: 0,
        }
    }

    fn send_once(&mut self, slot_id: usize) {
        let curr = cycles::rdtsc();
        let partition = self.partition.load(Ordering::Relaxed);
        let (to_storage, workload_id, tenant, name_len, request_payload) =
            self.generator.gen_request(curr, partition);
        if to_storage {
            let (ip, port) = self.dispatcher.sender2storage.send_invoke(
                tenant,
                name_len,
                request_payload,
                curr,
                0, // not used
            );
        } else {
            let (ip, port) = self.dispatcher.sender2compute.send_invoke(
                tenant,
                name_len,
                request_payload,
                curr,
                0, // not used
            );
        }
        self.slots[slot_id].counter += 1;
        self.slots[slot_id].type_id = workload_id;
        self.outstanding_reqs.insert(curr, slot_id);
    }

    fn send_all(&mut self) {
        self.start = cycles::rdtsc();
        for i in 0..self.max_out as usize {
            self.send_once(i);
        }
    }

    fn recv(&mut self) {
        let mut packet_recvd_signal = false;

        // Try to receive packets from the network port.
        // If there are packets, sample the latency of the server.
        if let Some(mut packets) = self.dispatcher.receiver.recv(|pkt| Some(pkt)) {
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
                                    self.latencies.push(curr_rdtsc - timestamp);
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
                if self.id == 0 {
                    let curr_rdtsc = cycles::rdtsc();
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
                        // println!(
                        //     "rdtsc {} tput {:.2}",
                        //     curr_rdtsc / (CPU_FREQUENCY / 1000),
                        //     output_tput
                        // )
                        self.tput_vec.push(output_tput);
                        self.rpc_vec.push((self.partition.load(Ordering::Relaxed) as f64) / 100.0);
                    }

                    if self.xloop_factor != 0
                        && packet_recvd_signal
                        && (curr_rdtsc - self.xloop_last_rdtsc
                            > CPU_FREQUENCY / self.xloop_factor)
                        && global_recvd > 100
                        && global_recvd as u64 % self.xloop_factor == 0
                    {
                        self.xloop_interval.push((curr_rdtsc - self.xloop_last_rdtsc) as f64 / (CPU_FREQUENCY / 1000) as f64);
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
                    }
                }
            }
        }
        let curr_rdtsc = cycles::rdtsc();
        if curr_rdtsc - self.start > self.duration {
            self.stop = curr_rdtsc;
            if !self.finished.swap(true, Ordering::Relaxed) {
                self.tput = (self.global_recvd.load(Ordering::Relaxed) as f64)
                    * CPU_FREQUENCY as f64
                    / (self.stop - self.start) as f64;
                self.dispatcher.sender2storage.send_reset();
                self.dispatcher.sender2compute.send_reset();
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
        if self.tput > 0.0 {
            // for (type_id, lat) in self.latencies.iter_mut().enumerate() {
            let mut lat = &mut self.latencies;
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
                ">>> lat50:{:.2} lat99:{:.2}",
                cycles::to_seconds(m) * 1e9,
                cycles::to_seconds(t) * 1e9
            );
            // println!("PUSHBACK Throughput {:.2}", self.tput.moving());
            println!("PUSHBACK Throughput {:.2}", self.tput);
        }
        if self.id == 0 {
            let mut avg_x_interval:f64 = 0.0;
            let mut std_x_interval:f64 = 0.0;
            // println!("xloop learning rate: {}", self.xloop_learning_rate);
            println!("number of xloop tunes: {}", self.xloop_interval.len());
            for i in &self.xloop_interval {
                avg_x_interval += i;
            }
            avg_x_interval /= self.xloop_interval.len() as f64;
            println!("xloop interval avg: {}", avg_x_interval);
            for i in &self.xloop_interval {
                std_x_interval += (i - avg_x_interval) * (i - avg_x_interval);
            }
            std_x_interval /= (self.xloop_interval.len() - 1) as f64;
            std_x_interval = std_x_interval.sqrt();
            println!("xloop interval std: {}", std_x_interval);
            for t in 0..self.tput_vec.len() {
                println!("tput {:.2} rpc {:.2}", 
                         &self.tput_vec[t as usize], 
                         &self.rpc_vec[t as usize]);
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

    let config: config::KayakConfig = config::load("kayak.toml");
    warn!("Starting up Sandstorm client with config {:?}", config);

    // Setup Netbricks.
    let mut net_context = config_and_init_netbricks(&config.lb);
    net_context.start_schedulers();
    // setup shared data
    let partition = Arc::new(AtomicF64::new(config.partition * 100.0));
    let recvd = Arc::new(AtomicUsize::new(0));
    let init_rdtsc = cycles::rdtsc();
    let finished = Arc::new(AtomicBool::new(false));
    // setup lb
    for (core_id, &core) in net_context.active_cores.clone().iter().enumerate() {
        let cfg = config.clone();
        let partition_copy = partition.clone();
        let recvd_copy = recvd.clone();
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
                        partition_copy.clone(),
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
