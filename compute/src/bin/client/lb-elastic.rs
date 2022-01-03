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

use dispatch::LBDispatcher;
use std::fmt::{self, Write};
use std::fs::File;
use std::io::Write as writef;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::RwLock;
use xloop::*;

struct MovingAvg {
    moving: f64,
    exp_decay: f64,
    norm: f64,
}
impl MovingAvg {
    fn new(exp_decay: f64) -> MovingAvg {
        MovingAvg {
            moving: 0.0,
            exp_decay: exp_decay,
            norm: 0.0,
        }
    }
    fn update(&mut self, delta: f64) {
        self.norm = self.norm * self.exp_decay + (1.0 - self.exp_decay);
        self.moving = self.moving * self.exp_decay + delta * (1.0 - self.exp_decay);
    }
    fn moving(&self) -> f64 {
        self.moving / self.norm
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
        if new_x > self.upperbound {
            new_x = self.upperbound - (new_x - self.upperbound);
        } else if new_x < self.lowerbound {
            // bounce back
            new_x = self.lowerbound + (self.lowerbound - new_x);
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

// this is a blackbox to lb, only used to generate requests
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

// this is a blackbox to lb, only used to generate requests
struct LoadGenerator {
    rng: Box<dyn Rng>,
    tenant_rng: Box<ZipfDistribution>,
    workloads: Vec<Workload>,
    loop_interval: u64,
    junctures: Vec<u64>,
    cum_ratios: Vec<Vec<f32>>, // 0-10000
}

impl LoadGenerator {
    fn new(config: &LBConfig) -> LoadGenerator {
        // workloads
        let mut workloads = vec![];
        for workload in &config.workloads {
            let table_id = workload.table_id as usize;
            let table = &config.tables[table_id - 1];
            workloads.push(Workload::new(workload, table));
        }
        // phases
        let mut sum_time = 0u64;
        let mut junctures = vec![];
        let mut cum_ratios = vec![];
        for phase in &config.phases {
            sum_time += phase.duration * CPU_FREQUENCY;
            junctures.push(sum_time);
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
            loop_interval: sum_time,
            junctures: junctures,
            cum_ratios: cum_ratios,
        }
    }
    fn gen_request(&mut self, mut curr_rdtsc: u64) -> usize {
        let phase_id = if self.loop_interval > 0 {
            curr_rdtsc %= self.loop_interval;
            // self.junctures.iter().position(|&t| t > curr_rdtsc).unwrap()
            self.junctures.partition_point(|&t| t <= curr_rdtsc)
        } else {
            0
        };
        let cum_ratio = &self.cum_ratios[phase_id];
        let rand_ratio = (self.rng.gen::<u32>() % 10000) as f32;
        // let type_id = cum_ratio.iter().position(|&p| p > rand_ratio).unwrap();
        let type_id = cum_ratio.partition_point(|&p| p <= rand_ratio);
        type_id
    }
    fn gen_payload(&mut self, type_id: usize) -> (u32, u32, &[u8]) {
        (
            // partition > rand_ratio as f64,
            self.tenant_rng.sample(&mut self.rng) as u32,
            self.workloads[type_id].name_len,
            self.workloads[type_id].sample_key(&mut self.rng),
        )
    }
}

// keep track of workload stats
// TODO: considered stale if diff(last,now)>thresh and both counter!=0(which means scheduling plan has changed)
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
    fn get_key(&self) -> f64 {
        (self.cost_storage.avg() - self.overhead_storage.avg()) / (self.cost_compute.avg() + 1e-9)
    }
    // fn reset(&mut self) {}
}
#[derive(Clone)]
struct Sampler {
    type_stats: Vec<Arc<RwLock<TypeStats>>>,
    type_order: Vec<usize>,
    // TODO: detects out-of-date type
    // type_history: Vec<TypeStats>,
    type_counter: Vec<Arc<AtomicUsize>>,
    // sample factor
    last_rdtsc: u64,
    last_recvd: usize,
    interval: u64,
    // last_recvd: usize,
    cum_ratio: Vec<Arc<(AtomicF64, AtomicF64)>>,
    undetermined: Arc<AtomicUsize>,
    msg: String,
}
impl Sampler {
    // TODO: reset, store cum_ratio
    fn new(num_types: usize, sample_factor: u64) -> Sampler {
        let mut type_stats = vec![];
        let mut type_counter = vec![];
        let mut cum_ratio = vec![];
        for _ in 0..num_types {
            type_stats.push(Arc::new(RwLock::new(TypeStats::new())));
            type_counter.push(Arc::new(AtomicUsize::new(0)));
            cum_ratio.push(Arc::new((AtomicF64::new(0.0), AtomicF64::new(0.0))));
        }
        Sampler {
            type_stats: type_stats,
            type_order: (0..num_types).collect(),
            type_counter: type_counter,
            cum_ratio: cum_ratio,
            undetermined: Arc::new(AtomicUsize::new(0)),
            last_rdtsc: 0,
            last_recvd: 0,
            interval: CPU_FREQUENCY / sample_factor,
            msg: String::new(),
        }
    }
    fn get_range(&self, type_id: usize) -> (f64, f64) {
        (
            self.cum_ratio[type_id].0.load(Ordering::Acquire),
            self.cum_ratio[type_id].1.load(Ordering::Acquire),
        )
    }
    fn ready(&mut self, curr_rdtsc: u64 /*, recvd: usize*/) -> bool {
        if self.last_rdtsc == 0 {
            self.last_rdtsc = curr_rdtsc;
            false
        } else if curr_rdtsc - self.last_rdtsc > self.interval
        /* && recvd - self.last_recvd > 10000 */
        {
            self.last_rdtsc = curr_rdtsc;
            // self.last_recvd = recvd;
            true
        } else {
            false
        }
    }
    fn undetermined_requests(&self) -> usize {
        self.undetermined.load(Ordering::Relaxed)
    }
    fn sample_ratio(&mut self, curr_rdtsc: u64) {
        self.last_rdtsc = curr_rdtsc;
        let mut sum = 0usize;
        let mut cum_sum = vec![];
        for &idx in &self.type_order {
            let cnt = self.type_counter[idx].load(Ordering::Relaxed);
            sum += cnt;
            cum_sum.push(sum as f64);
        }
        let mut prev = 0f64;
        self.msg.clear();
        for (&idx, &cnt) in self.type_order.iter().zip(cum_sum.iter()) {
            let count = if cnt == prev { 0f64 } else { cnt };
            self.cum_ratio[idx]
                .0
                .store(10000.0 * prev / sum as f64, Ordering::Release);
            self.cum_ratio[idx]
                .1
                .store(10000.0 * count / sum as f64, Ordering::Release);
            write!(
                self.msg,
                "{}({:.2},{}) ",
                idx,
                10000.0 * count / sum as f64,
                cnt,
            );
            prev = cnt;
        }
        // debug!("{}", self.msg);
        self.reset_counter();
    }
    fn inc_counter(&self, type_id: usize) {
        self.type_counter[type_id].fetch_add(1, Ordering::Relaxed);
    }
    fn reset_counter(&self) {
        self.undetermined.store(0, Ordering::Relaxed);
        for counter in &self.type_counter {
            counter.store(0, Ordering::Relaxed);
        }
    }
    fn sort_type(&mut self) {
        let type_stats: Vec<f64> = self
            .type_stats
            .iter()
            .map(|s| s.read().unwrap().get_key())
            .collect();
        self.type_order.sort_by(|&idx1, &idx2| {
            let key1 = type_stats[idx1];
            let key2 = type_stats[idx2];
            key1.partial_cmp(&key2).unwrap()
        });
        debug!("order {:?} stats {:?}", self.type_order, type_stats);
    }
    fn update_stats(&self, type_id: usize, c_s: f64, c_c: f64, o_s: f64) {
        self.type_stats[type_id]
            .write()
            .unwrap()
            .update(c_s, c_c, o_s);
    }
    // fn reset_stats(&self) {
    //     // set history
    //     // set timestamp
    // }
}
impl fmt::Display for Sampler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

// storage provision
struct Provision {
    // max_quota: u32,
    provisions: Vec<u32>,
    loop_interval: u64,
    junctures: Vec<u64>,
    current: u32,
}
impl Provision {
    fn new(config: &LBConfig) -> Provision {
        // phases
        let mut sum_time = 0u64;
        let mut junctures = vec![];
        let mut provisions = vec![];
        for provision in &config.provisions {
            sum_time += provision.duration * CPU_FREQUENCY;
            junctures.push(sum_time);
            provisions.push(provision.storage);
        }
        Provision {
            // max_quota: config.storage.iter().map(|cfg| cfg.rx_queues).sum::<i32>() as u32,
            provisions: provisions,
            loop_interval: sum_time,
            junctures: junctures,
            current: 0,
        }
    }
    fn adjust(&mut self, mut curr_rdtsc: u64) -> u32 {
        let phase_id = if self.loop_interval > 0 {
            curr_rdtsc %= self.loop_interval;
            self.junctures.partition_point(|&t| t <= curr_rdtsc)
        } else {
            0
        };
        self.current = self.provisions[phase_id];
        self.current
    }
}

/// Receives responses to PUSHBACK requests sent out by PushbackSend.
struct LoadBalancer {
    dispatcher: LBDispatcher,
    // workload: RefCell<Pushback>,
    // payload_pushback: RefCell<Vec<u8>>,
    // payload_put: RefCell<Vec<u8>>,
    // key_len: usize,
    // record_len: usize,
    // num_types: usize,
    // multi_types: Vec<MultiType>,
    storage_provision: Provision,
    generator: LoadGenerator,
    sampler: Sampler,
    // worker stats
    id: usize,
    init_rdtsc: u64,
    start: u64,
    stop: u64,
    duration: u64, // stop when curr - start > duration
    // requests: usize,
    global_recvd: Arc<AtomicUsize>,
    recvd: usize,
    // latencies: Vec<Vec<u64>>,
    latencies: Vec<u64>,
    // kth: Vec<Vec<Arc<AtomicUsize>>>,
    // avg_lat: Vec<usize>,
    // outstanding_reqs: HashSet<u64>,
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
    xloop: TputGrad,
    elastic: ElasticScaling,
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
    // tput: MovingAvg,
    tput: f64,
    // outs_storage: usize,
    // outs_compute: usize,
    cfg: config::LBConfig,
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
        // num_types: usize,
        partition: Arc<AtomicF64>,
        sampler: Sampler,
        elastic: ElasticScaling,
        // storage_load: Arc<ServerLoad>,
        // compute_load: Arc<ServerLoad>,
        // kth: Vec<Vec<Arc<AtomicUsize>>>,
        global_recvd: Arc<AtomicUsize>,
        init_rdtsc: u64,
        finished: Arc<AtomicBool>,
    ) -> LoadBalancer {
        // let mut latencies: Vec<Vec<u64>> = Vec::with_capacity(num_types);
        // for _ in 0..num_types {
        //     latencies.push(Vec::with_capacity(config.num_reqs));
        // }
        let mut slots = vec![];
        for _ in 0..config.max_out {
            slots.push(Slot::default());
        }

        LoadBalancer {
            cfg: config.clone(),
            duration: config.duration * CPU_FREQUENCY,
            // tput: MovingAvg::new(config.moving_exp),
            tput: 0.0,
            finished: finished,
            // storage_load: storage_load,
            // compute_load: compute_load,
            dispatcher: LBDispatcher::new(config, net_port),
            storage_provision: Provision::new(config),
            generator: LoadGenerator::new(config),
            sampler: sampler,
            elastic: elastic,
            // num_types: num_types,
            // multi_types: MultiType::new(&config.multi_kv, &config.multi_ord),
            // cum_prob: cum_prob,
            // worker stats
            id: id,
            init_rdtsc: init_rdtsc,
            start: 0,
            stop: 0,
            // finished: false,
            // requests: config.num_reqs / 2,
            global_recvd: global_recvd,
            recvd: 0,
            latencies: Vec::with_capacity(64000000), // latencies: latencies,
            // kth: kth,
            // avg_lat: vec![0; num_types],
            // outstanding_reqs: HashSet::new(),
            outstanding_reqs: HashMap::new(),
            slots: slots,
            max_out: config.max_out,
            partition: Partition {
                x: partition,
                upperbound: 10000.0,
                lowerbound: 0.0,
            },
            learnable: config.learnable,
            xloop: TputGrad::new(
                &config.xloop,
                config.partition, /*, storage_load, compute_load*/
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
            output_last_recvd: 0,
        }
    }
    fn adjust_provision(&mut self, curr_rdtsc: u64) {
        // storage
        let prev_storage = self.storage_provision.current;
        let current_storage = self.storage_provision.adjust(curr_rdtsc);
        if self.id == 0 && prev_storage != current_storage {
            // this message is sent to all compute nodes, regardless of compute provision
            self.dispatcher.sender2compute.send_scaling(current_storage);
        }
        self.dispatcher
            .sender2storage
            .set_endpoints(current_storage as usize);
        self.elastic.storage_cores = current_storage;
        // compute
        let current_compute = self.elastic.compute_cores.load(Ordering::Relaxed);
        self.dispatcher
            .sender2compute
            .set_endpoints(current_compute as usize);
    }
    fn storage_or_compute(&mut self, type_id: usize) -> bool {
        let partition = self.partition.get();
        let (lowerbound, upperbound) = self.sampler.get_range(type_id);
        if upperbound == 0f64 {
            self.sampler.undetermined.fetch_add(1, Ordering::Relaxed);
            self.generator.rng.gen::<u32>() % 10000 < 5000
        } else if upperbound < partition {
            true
        } else if lowerbound < partition {
            let ratio = 10000.0 * (partition - lowerbound) / (upperbound - lowerbound);
            self.generator.rng.gen::<u32>() % 10000 < ratio as u32
        } else {
            false
        }
    }
    fn send_once(&mut self, slot_id: usize) {
        let curr = cycles::rdtsc();
        // change storage cores
        self.adjust_provision(curr);
        // if self.storage_provision.current == 0 {
        //     return;
        // }
        // sample ratio
        // let recvd = self.global_recvd.load(Ordering::Relaxed);
        if self.id == 0 && self.sampler.ready(curr /*, recvd*/) {
            // self.sampler.sort_type();
            self.sampler.sample_ratio(curr);
        }
        let type_id = self.generator.gen_request(curr);
        self.sampler.inc_counter(type_id);
        let to_storage = self.storage_or_compute(type_id);
        let (tenant, name_len, request_payload) = self.generator.gen_payload(type_id);
        if to_storage {
            let (ip, port) = self.dispatcher.sender2storage.send_invoke(
                tenant,
                name_len,
                request_payload,
                curr,
                0, // not used
            );
            self.elastic.storage_load.inc_outstanding(/*ip, port*/);
        } else {
            let (ip, port) = self.dispatcher.sender2compute.send_invoke(
                tenant,
                name_len,
                request_payload,
                curr,
                0, // not used
            );
            self.elastic.compute_load.inc_outstanding(/*ip, port*/);
        }
        self.slots[slot_id].counter += 1;
        self.slots[slot_id].type_id = type_id;
        self.outstanding_reqs.insert(curr, slot_id);
        // self.outstanding_reqs.insert(curr, type_id);
    }

    fn send_all(&mut self) {
        self.start = cycles::rdtsc();
        for i in 0..self.max_out as usize {
            self.send_once(i);
        }
    }
    /*
    fn update_load(
        &self,
        src_ip: u32,
        src_port: u16,
        // curr_rdtsc: u64,
        queue_length: f64,
        task_duration_cv: f64,
    ) {
        // set to -1 after the first rpc resp packet in that round
        if queue_length < 0.0 || task_duration_cv == 0.0 {
            return;
        }
        if self.xloop.storage_load.ip2load.contains_key(&src_ip) {
            self.xloop.storage_load.update_load(
                src_ip,
                src_port,
                // curr_rdtsc - self.start,
                queue_length,
                task_duration_cv,
            );
        } else {
            self.xloop.compute_load.update_load(
                src_ip,
                src_port,
                // curr_rdtsc - self.start,
                queue_length,
                task_duration_cv,
            );
        }
    }
    */

    fn recv(&mut self) {
        // // Don't do anything after all responses have been received.
        // if self.finished.load(Ordering::Relaxed) == true && self.stop > 0 {
        //     return;
        // }

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
                                    // // TODO: reimpl update, server will do smoothing and return avg
                                    // self.update_load(
                                    //     src_ip,
                                    //     src_port,
                                    //     // curr_rdtsc,
                                    //     hdr.server_load,
                                    //     hdr.task_duration_cv,
                                    //     // #[cfg(feature = "server_stats")]
                                    //     // (curr_rdtsc - self.init_rdtsc),
                                    // );
                                    self.elastic.update_load(
                                        src_ip,
                                        src_port,
                                        hdr.server_load,
                                        hdr.task_duration_cv,
                                    );
                                    // self.latencies[type_id].push(curr_rdtsc - timestamp);
                                    self.latencies.push(curr_rdtsc - timestamp);
                                    self.outstanding_reqs.remove(&timestamp);
                                    if self.elastic.storage_load.ip2load.contains_key(&src_ip) {
                                        self.elastic.storage_load.dec_outstanding(/*src_ip, src_port*/);
                                    } else {
                                        self.elastic.compute_load.dec_outstanding(/*src_ip, src_port*/);
                                    }
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
                // // kth measurement here
                // for (type_id, lat) in self.latencies.iter().enumerate() {
                //     let len = lat.len();
                //     if len > 100 && len % 10 == 0 {
                //         let mut tmp = &lat[(len - 100)..len];
                //         let mut tmpvec = tmp.to_vec();
                //         self.kth[type_id][self.id].store(
                //             *order_stat::kth(&mut tmpvec, 98) as usize,
                //             Ordering::Relaxed,
                //         );
                //     }
                // }
                if self.id == 0 {
                    let curr_rdtsc = cycles::rdtsc();
                    let global_recvd = self.global_recvd.load(Ordering::Relaxed);
                    if self.learnable
                        && self.xloop.ready(curr_rdtsc)
                        && packet_recvd_signal
                        && global_recvd - self.xloop.last_recvd > 4000
                    // && global_recvd > 10000
                    {
                        if self.sampler.undetermined_requests() == 0 {
                            self.xloop
                                .update_x(&self.partition, curr_rdtsc, global_recvd);
                            // short circuit scaling if anomalies > 0, which is always true if not converged
                            if self.xloop.anomalies > 0 || self.elastic.scaling() {
                                self.elastic.reset();
                                // self.elastic.compute_load.reset();
                                // self.elastic.compute_outs.reset();
                                self.dispatcher.sender2compute.send_reset();
                                // self.elastic.storage_load.reset();
                                // self.elastic.storage_outs.reset();
                                self.dispatcher.sender2storage.send_reset();
                            }
                            // self.elastic.reset();
                            // self.dispatcher.sender2compute.send_reset();
                            // self.dispatcher.sender2storage.send_reset();
                            // skip reset if anomalies==0(implies convergence) and scaling has no update
                        } else {
                            self.xloop.sync(curr_rdtsc, global_recvd);
                        }
                        debug!("{} ratio {} {}", self.xloop, self.sampler, self.elastic);
                        // self.xloop.msg.clear();
                        self.elastic.msg.clear();
                    }
                    if self.output_factor != 0
                        && (curr_rdtsc - self.output_last_rdtsc
                            > CPU_FREQUENCY / self.output_factor)
                    {
                        let output_tput = (global_recvd - self.output_last_recvd) as f64
                            * (CPU_FREQUENCY / 1000) as f64
                            / (curr_rdtsc - self.output_last_rdtsc) as f64;
                        self.output_last_recvd = global_recvd;
                        self.output_last_rdtsc = curr_rdtsc;
                        println!(
                            "rdtsc {} tput {:.2} x {:.2} cores {},{} ratio {}",
                            curr_rdtsc,
                            output_tput,
                            self.partition.get(),
                            self.storage_provision.current,
                            self.elastic.compute_cores.load(Ordering::Relaxed),
                            self.sampler,
                        )
                        // self.tput.update(output_tput);
                        // if self.output {
                        //     println!(
                        //         "rdtsc {} tput {:.2}",
                        //         (1000 * curr_rdtsc) / CPU_FREQUENCY,
                        //         output_tput
                        //     )
                        // }
                    }
                }
            }
        }
        if self.id == 0 {
            self.elastic.snapshot();
        }
        let curr_rdtsc = cycles::rdtsc();
        if curr_rdtsc - self.start > self.duration {
            self.stop = curr_rdtsc;
            if !self.finished.swap(true, Ordering::Relaxed) {
                self.tput = (self.global_recvd.load(Ordering::Relaxed) as f64)
                    * CPU_FREQUENCY as f64
                    / (self.stop - self.start) as f64;
                // // self.dispatcher.sender2storage.send_reset();
                // self.dispatcher.sender2compute.send_reset();
                // // reset storage cores available to compute to max_quota
                // self.dispatcher
                //     .sender2compute
                //     .send_scaling(self.storage_provision.max_quota);
            }
        }
        // // The moment all response packets have been received, set the value of the
        // // stop timestamp so that throughput can be estimated later.
        // if self.requests <= self.recvd {
        //     self.stop = cycles::rdtsc();
        //     let already_finished = self.finished.swap(true, Ordering::Relaxed);
        //     if !already_finished {
        //         self.tput = self.global_recvd.load(Ordering::Relaxed) as f64
        //             / cycles::to_seconds(self.stop - self.start);
        //         self.dispatcher.sender2storage.send_reset();
        //         self.dispatcher.sender2compute.send_reset();
        //     }
        //     // #[cfg(feature = "queue_len")]
        //     // self.dispatcher.sender2compute.send_terminate();
        //     // #[cfg(feature = "queue_len")]
        //     // self.dispatcher.sender2storage.send_terminate();
        // }
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
        if self.id == 0 && cfg!(feature = "xtrace") {
            let mut f = File::create("xtrace.log").unwrap();
            // writeln!(f, "{:?}\n", self.cfg);
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
    config: &config::LBConfig,
    scheduler: &mut StandaloneScheduler,
    ports: Vec<CacheAligned<PortQueue>>,
    core_id: usize,
    // num_types: usize,
    partition: Arc<AtomicF64>,
    sampler: Sampler,
    elastic: ElasticScaling,
    // storage_load: Arc<ServerLoad>,
    // compute_load: Arc<ServerLoad>,
    // kth: Vec<Vec<Arc<AtomicUsize>>>,
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
        // num_types,
        partition,
        sampler,
        elastic,
        // storage_load,
        // compute_load,
        // kth,
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

    let mut config: config::LBConfig = config::load("lb.toml");
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
    let partition = if config.learnable {
        5000.0
    } else {
        config.partition * 100.0
    };
    let partition = Arc::new(AtomicF64::new(partition));
    let sampler = Sampler::new(config.workloads.len(), config.sample_factor);
    /*
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
        // config.moving_exp,
    ));
    let compute_load = Arc::new(ServerLoad::new(
        "compute",
        compute_servers,
        // config.moving_exp,
    ));
    */
    let elastic = ElasticScaling::new(&config);
    // let num_types = config.multi_kv.len();
    // let mut kth = vec![];
    // for _ in 0..num_types {
    //     let mut kth_type = vec![];
    //     for _ in 0..config.lb.num_cores {
    //         kth_type.push(Arc::new(AtomicUsize::new(0)));
    //     }
    //     kth.push(kth_type);
    // }
    let recvd = Arc::new(AtomicUsize::new(0));
    let init_rdtsc = cycles::rdtsc();
    let finished = Arc::new(AtomicBool::new(false));
    // setup lb
    for (core_id, &core) in net_context.active_cores.clone().iter().enumerate() {
        let cfg = config.clone();
        // let kth_copy = kth.clone();
        let partition_copy = partition.clone();
        let sampler_copy = sampler.clone();
        let elastic_copy = elastic.clone();
        let recvd_copy = recvd.clone();
        // let storage_load_copy = storage_load.clone();
        // let compute_load_copy = compute_load.clone();
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
                        // num_types,
                        partition_copy.clone(),
                        sampler_copy.clone(),
                        elastic_copy.clone(),
                        // storage_load_copy.clone(),
                        // compute_load_copy.clone(),
                        // kth_copy.clone(),
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
    /*
    if cfg!(feature = "summary") {
        // let (ql_storage_mean, cv_storage) = storage_load.mean_all();
        let (out_storage, w_storage, cv_storage, ncores_storage) = storage_load.aggr_all();
        let out_storage = storage_load.outs_trace.borrow().avg();
        let out_storage_std = storage_load.outs_trace.borrow().std();
        // let (ql_compute_mean, cv_compute) = compute_load.mean_all();
        let (out_compute, w_compute, cv_compute, ncores_compute) = compute_load.aggr_all();
        let out_compute = compute_load.outs_trace.borrow().avg();
        let out_compute_std = compute_load.outs_trace.borrow().std();
        // out_storage + w_compute=ql_storage
        let ql_storage_raw = (out_storage + w_compute) / ncores_storage;
        let ql_compute_raw = (out_compute - w_compute) / ncores_compute;
        let ql_storage = ql_storage_raw / (1.0 + cv_storage / ncores_storage);
        let ql_compute = ql_compute_raw / (1.0 + cv_compute / ncores_compute);
        print!("{}", storage_load);
        // println!(
        //     "storage summary ql {:.2} cv {:.2}",
        //     ql_storage_mean, cv_storage
        // );
        println!(
            "storage summary ql {:.2} raw {:.2} outs {:.2}({:.2}) waiting {:.2} cv {:.2}",
            ql_storage,
            ql_storage_raw,
            out_storage / ncores_storage,
            out_storage_std / ncores_storage,
            w_storage / ncores_storage,
            cv_storage / ncores_storage
        );
        print!("{}", compute_load);
        // println!(
        //     "compute summary ql {:.2} cv {:.2}",
        //     ql_compute_mean, cv_compute
        // );
        println!(
            "compute summary ql {:.2} raw {:.2} outs {:.2}({:.2}) waiting {:.2} cv {:.2}",
            ql_compute,
            ql_compute_raw,
            out_compute / ncores_compute,
            out_compute_std / ncores_compute,
            w_compute / ncores_compute,
            cv_compute / ncores_compute
        );
    }
    */
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
