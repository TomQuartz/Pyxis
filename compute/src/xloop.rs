use db::config::*;
use db::cycles::rdtsc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::fs::File;
use std::io::Write as writef;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

pub const CPU_FREQUENCY: u64 = 2400000000;

pub trait XInterface {
    type X;
    // &mut self?
    // bounce back here
    fn update(&self, delta_x: Self::X);
    fn get(&self) -> Self::X;
}
struct MovingTimeAvg {
    moving_avg: f64,
    elapsed: f64,
    latest: f64,
    last_time: u64,
    moving_exp: f64,
    counter: f64,
    E_x: f64,
    E_x2: f64,
    // #[cfg(feature = "queue_len")]
    // avg: Avg,
}
impl MovingTimeAvg {
    fn new(/*moving_exp: f64*/) -> MovingTimeAvg {
        MovingTimeAvg {
            moving_avg: 0.0,
            elapsed: 0.0,
            latest: 0.0,
            last_time: 0,
            moving_exp: 1.0,
            counter: 0.0,
            E_x: 0.0,
            E_x2: 0.0,
            // #[cfg(feature = "queue_len")]
            // avg: Avg::new(),
        }
    }
    // reset after xloop changes parition
    fn reset(&mut self) {
        self.moving_avg = 0.0;
        self.elapsed = 0.0;
        self.counter = 0.0;
        self.E_x = 0.0;
        self.E_x2 = 0.0;
    }
    fn update(&mut self, current_time: u64, new: f64) {
        if self.elapsed > 0.0 || new > 0.0 {
            let elapsed = (current_time - self.last_time) as f64;
            self.elapsed = self.elapsed * self.moving_exp + elapsed;
            let update_ratio = elapsed / self.elapsed;
            let interval_avg = (self.latest + new) / 2.0;
            self.moving_avg = self.moving_avg * (1.0 - update_ratio) + interval_avg * update_ratio;
            // mean
            self.counter += 1.0;
            self.E_x =
                self.E_x * ((self.counter - 1.0) / self.counter) + interval_avg / self.counter;
            self.E_x2 = self.E_x2 * ((self.counter - 1.0) / self.counter)
                + interval_avg * interval_avg / self.counter;
            self.latest = new;
        }
        // sync
        self.last_time = current_time;
        // #[cfg(feature = "queue_len")]
        // self.avg.update(delta_avg);
    }
    fn avg(&self) -> f64 {
        self.moving_avg
    }
    fn mean(&self) -> f64 {
        self.E_x
    }
    fn std(&self) -> f64 {
        (self.E_x2 - self.E_x * self.E_x).sqrt()
    }
}
impl fmt::Display for MovingTimeAvg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "moving {:.2} mean {:.2} std {:.2} counter {} ",
            self.moving_avg,
            // self.elapsed,
            self.E_x,
            self.std(),
            self.counter as usize,
        )
    }
}
/*
struct TimeAvg {
    time_avg: f64,
    elapsed: f64,
    latest: f64,
    last_time: u64,
    // #[cfg(feature = "queue_len")]
    // avg: Avg,
}

impl TimeAvg {
    fn new() -> TimeAvg {
        TimeAvg {
            time_avg: 0.0,
            elapsed: 0.0,
            latest: 0.0,
            last_time: 0,
            // #[cfg(feature = "queue_len")]
            // avg: Avg::new(),
        }
    }
    fn reset(&mut self){
        self.time_avg = 0.0;
        self.elapsed = 0.0;
    }
    fn update(&mut self, current_time: u64, new: f64) {
        let elapsed = (current_time - self.last_time) as f64;
        self.elapsed += elapsed;
        let update_ratio = elapsed / self.elapsed;
        let interval_avg = (self.latest + new) / 2.0;
        self.time_avg = self.time_avg * (1.0 - update_ratio) + interval_avg * update_ratio;
        // sync
        self.last_time = current_time;
        self.latest = new;
        // #[cfg(feature = "queue_len")]
        // self.avg.update(delta_avg);
    }
    fn avg(&self) -> f64 {
        self.time_avg
    }
}
*/
#[derive(Clone)]
pub struct Avg {
    pub counter: f64,
    lastest: f64,
    E_x: f64,
    E_x2: f64,
}
impl Avg {
    pub fn new() -> Avg {
        Avg {
            counter: 0.0,
            lastest: 0.0,
            E_x: 0.0,
            E_x2: 0.0,
        }
    }
    pub fn reset(&mut self) {
        self.counter = 0.0;
        self.E_x = 0.0;
        self.E_x2 = 0.0;
    }
    pub fn update(&mut self, delta: f64) {
        self.counter += 1.0;
        self.lastest = delta;
        self.E_x = self.E_x * ((self.counter - 1.0) / self.counter) + delta / self.counter;
        self.E_x2 =
            self.E_x2 * ((self.counter - 1.0) / self.counter) + delta * delta / self.counter;
    }
    pub fn diff(&mut self, other: &Avg, max_err: f64) -> bool {
        self.counter > 0.0
            && other.counter > 0.0
            && relative_err(self.avg(), other.avg()) >= max_err
    }
    pub fn merge(&mut self, other: &Avg) {
        if other.counter > 0.0 {
            self.batch_update(other.avg(), other.counter);
        }
    }
    fn batch_update(&mut self, delta: f64, cnt: f64) {
        self.counter += cnt;
        self.E_x = self.E_x * ((self.counter - cnt) / self.counter) + delta * (cnt / self.counter);
        self.E_x2 = self.E_x2 * ((self.counter - cnt) / self.counter)
            + delta * delta * (cnt / self.counter);
    }
    pub fn avg(&self) -> f64 {
        self.E_x
    }
    pub fn std(&self) -> f64 {
        (self.E_x2 - self.E_x * self.E_x).sqrt()
    }
}
impl fmt::Display for Avg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "mean {:.2} std {:.2} latest {:.2} counter {}",
            self.E_x,
            self.std(),
            self.lastest,
            self.counter as usize,
        )
    }
}

pub struct CoreLoad {
    // queue_length: MovingTimeAvg,
    // outstanding: Avg,
    waiting: Avg,
    task_duration_cv: Avg,
}
impl CoreLoad {
    fn new() -> CoreLoad {
        CoreLoad {
            // queue_length: MovingTimeAvg::new(),
            // outstanding: Avg::new(),
            waiting: Avg::new(),
            task_duration_cv: Avg::new(),
        }
    }
    // fn update_outstanding(&mut self, outstanding: f64){
    //     self.outstanding.update(outstanding);
    // }
    fn update_load(
        &mut self,
        /*current_time: u64, */ queue_length: f64,
        task_duration_cv: f64,
    ) {
        // self.queue_length.update(current_time, queue_length);
        self.waiting.update(queue_length);
        self.task_duration_cv.update(task_duration_cv);
    }
    fn avg(&self) -> (f64, f64) {
        // (self.queue_length.avg(), self.task_duration_cv.avg())
        (self.waiting.avg(), self.task_duration_cv.avg())
    }
    fn reset(&mut self) {
        // self.queue_length.reset();
        // self.outstanding.reset();
        self.waiting.reset();
        self.task_duration_cv.reset();
    }
    // fn mean(&self) -> (f64, f64) {
    //     (self.queue_length.mean(), self.task_duration_cv.avg())
    // }
}
impl fmt::Display for CoreLoad {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // write!(f, "ql ({}) ", self.queue_length)?;
        // write!(f, "outstanding ({}) ", self.outstanding)?;
        write!(f, "waiting ({}) ", self.waiting)?;
        write!(f, "cv ({}) ", self.task_duration_cv)
        // write!(
        //     f,
        //     "ql (mean {:.2} std {:.2} latest {:.2} counter {}) ",
        //     self.queue_length.E_x,
        //     self.queue_length.std(),
        //     self.queue_length.lastest,
        //     self.queue_length.counter as usize,
        // )?;
        // write!(
        //     f,
        //     "cv (mean {:.2} std {:.2} latest {:.2} counter {}) ",
        //     self.task_duration_cv.E_x,
        //     self.task_duration_cv.std(),
        //     self.task_duration_cv.lastest,
        //     self.task_duration_cv.counter as usize,
        // )
    }
}

pub struct ServerLoad {
    cluster_name: String,
    // NOTE: outs is not exact since there's work stealing
    // pub ip2outs: HashMap<u32,Vec<AtomicIsize>>,
    outstanding: AtomicUsize,
    // pub outs_trace: RefCell<Avg>,
    pub ip2load: HashMap<u32, Vec<RwLock<CoreLoad>>>,
    pub ip_addrs: Vec<u32>,
    pub cumsum_ports: Vec<usize>,
    // num_queues: usize,
    // #[cfg(feature = "server_stats")]
    // ip2trace: HashMap<u32, RwLock<Vec<(usize, (u64, (f64, f64)))>>>,
}

impl ServerLoad {
    pub fn new(
        cluster_name: &str,
        mut ip_and_ports: Vec<(&String, i32)>,
        // moving_exp: f64,
    ) -> ServerLoad {
        // NOTE: assume that ip_and_ports is sorted by ip
        // ip_and_ports.sort_by_key(|&(ip,ports)|u32::from(Ipv4Addr::from_str(ip).unwrap()));
        let mut ip2load = HashMap::new();
        // let mut ip2outs = HashMap::new();
        // let mut num_queues = 0usize;
        let mut ip_addrs = vec![];
        let mut cumsum_ports = vec![];
        let mut sum = 0usize;
        for &(ip, num_ports) in &ip_and_ports {
            let mut server_load = vec![];
            // let mut outs = vec![];
            for _ in 0..num_ports {
                server_load.push(RwLock::new(CoreLoad::new()));
                // outs.push(AtomicIsize::new(0));
            }
            let ip = u32::from(Ipv4Addr::from_str(ip).unwrap());
            // ip2load.insert(ip, RwLock::new(MovingTimeAvg::new(moving_exp)));
            ip2load.insert(ip, server_load);
            ip_addrs.push(ip);
            cumsum_ports.push(sum);
            sum += num_ports as usize;
            // ip2outs.insert(ip,outs);
        }
        // ip_addrs.sort();
        // #[cfg(feature = "server_stats")]
        // let mut ip2trace = HashMap::new();
        // #[cfg(feature = "server_stats")]
        // for &(ip, num_ports) in &ip_and_ports {
        //     // let mut server_trace = vec![];
        //     // for _ in 0..num_ports {
        //     //     server_trace.push(RwLock::new(vec![]));
        //     // }
        //     let ip = u32::from(Ipv4Addr::from_str(ip).unwrap());
        //     ip2trace.insert(ip, RwLock::new(Vec::with_capacity(128000)));
        // }
        ServerLoad {
            cluster_name: cluster_name.to_string(),
            ip2load: ip2load,
            ip_addrs: ip_addrs,
            cumsum_ports: cumsum_ports,
            // ip2outs:ip2outs,
            outstanding: AtomicUsize::new(0),
            // outs_trace: RefCell::new(Avg::new()),
            // num_queues: num_queues,
            // #[cfg(feature = "server_stats")]
            // ip2trace: ip2trace,
        }
    }
    pub fn inc_outstanding(&self /*, src_ip: u32, src_port: u16*/) {
        // let outs = self.ip2outs.get(&src_ip).unwrap();
        // outs[src_port as usize].fetch_add(1,Ordering::SeqCst)
        self.outstanding.fetch_add(1, Ordering::SeqCst);
    }
    pub fn dec_outstanding(&self /*, src_ip: u32, src_port: u16*/) {
        // let outs = self.ip2outs.get(&src_ip).unwrap();
        // outs[src_port as usize].fetch_sub(1,Ordering::SeqCst)
        self.outstanding.fetch_sub(1, Ordering::SeqCst);
    }
    pub fn update_load(
        &self,
        src_ip: u32,
        src_port: u16,
        // curr_rdtsc: u64,
        queue_length: f64,
        task_duration_cv: f64,
    ) {
        if let Some(server_load) = self.ip2load.get(&src_ip) {
            server_load[src_port as usize].write().unwrap().update_load(
                // curr_rdtsc,
                queue_length,
                task_duration_cv,
            );
            // server_load[src_port as usize]
            //     .write()
            //     .unwrap()
            //     .update_load(queue_length, task_duration_cv);
        }
    }
    // #[cfg(feature = "server_stats")]
    // pub fn update_trace(
    //     &self,
    //     src_ip: u32,
    //     src_port: u16,
    //     curr_rdtsc: u64,
    //     server_ql: f64,
    //     task_duration_cv: f64,
    //     id: usize,
    // ) {
    //     if let Some(server_trace) = self.ip2trace.get(&src_ip) {
    //         server_trace
    //             .write()
    //             .unwrap()
    //             .push((id, (curr_rdtsc, (server_ql, task_duration_cv))));
    //     }
    // }
    pub fn aggr_server(&self, ip: u32) -> (f64, f64, f64) {
        let server_load = self.ip2load.get(&ip).unwrap();
        // let num_cores = server_load.len() as f64;
        // let mut total_outs = 0f64;
        let mut total_w = 0f64;
        let mut total_cv = 0f64;
        let mut total_cores = 0f64;
        for core_load in server_load.iter() {
            let core_load = core_load.read().unwrap();
            if core_load.waiting.counter > 10f64 {
                let (w, cv) = core_load.avg();
                // total_ql += ql;
                total_w += w;
                total_cv += cv;
                total_cores += 1.0;
            }
        }
        // let outs = self.ip2outs.get(&ip).unwrap();
        // for out in outs.iter(){
        //     total_outs+=out.load(Ordering::Acquire) as f64;
        // }
        // (total_ql / num_cores, total_cv / num_cores)
        // (total_w, total_cv, num_cores)
        (total_w, total_cv, total_cores)
        // server_load.read().unwrap().avg()
    }
    pub fn aggr_all(&self) -> (f64, f64, f64) {
        // let mut total_ql = 0f64;
        // let mut total_outs = 0f64;
        let mut total_w = 0f64;
        let mut total_cv = 0f64;
        // let num_servers = self.ip2load.len() as f64;
        let mut num_cores = 0f64;
        for &ip in self.ip2load.keys() {
            // let (ql, cv) = self.avg_server(ip);
            let (w, cv, ncores) = self.aggr_server(ip);
            // total_ql += ql;
            // total_outs+=outs;
            total_w += w;
            total_cv += cv;
            num_cores += ncores;
        }
        // (total_ql / num_servers, total_cv / num_servers)
        // let total_outs = self.outstanding.load(Ordering::Acquire) as f64;
        (/*total_outs, */ total_w, total_cv, num_cores)
    }
    // pub fn mean_server(&self, ip: u32) -> (f64,f64) {
    //     let server_load = self.ip2load.get(&ip).unwrap();
    //     let num_cores = server_load.len() as f64;
    //     let mut total_ql = 0f64;
    //     let mut total_cv = 0f64;
    //     for core_load in server_load.iter() {
    //         let (ql, cv) = core_load.read().unwrap().mean();
    //         total_ql += ql;
    //         total_cv += cv;
    //     }
    //     // total_ql / num_cores
    //     (total_ql / num_cores, total_cv / num_cores)
    //     // server_load.read().unwrap().avg()
    // }
    // pub fn mean_all(&self) -> (f64,f64) {
    //     let mut total_ql = 0f64;
    //     let mut total_cv = 0f64;
    //     let num_servers = self.ip2load.len() as f64;
    //     for &ip in self.ip2load.keys() {
    //         // let (ql, cv) = self.avg_server(ip);
    //         let (ql,cv) = self.mean_server(ip);
    //         total_ql += ql;
    //         total_cv += cv;
    //     }
    //     // total_ql / num_servers
    //     (total_ql / num_servers, total_cv / num_servers)
    // }
    pub fn reset(&self) {
        for &ip in self.ip2load.keys() {
            let server_load = self.ip2load.get(&ip).unwrap();
            for core_load in server_load.iter() {
                core_load.write().unwrap().reset();
            }
            // NOTE: there's no need to reset outstanding
            // server_load.write().unwrap().reset();
        }
    }
    // #[cfg(feature = "server_stats")]
    // pub fn print_trace(&self) {
    //     let mut ql_total: f64 = 0.0;
    //     let mut ql_total_std: f64 = 0.0;
    //     let mut task_cv_total: f64 = 0.0;
    //     let mut task_cv_total_std: f64 = 0.0;
    //     let mut task_cv_total_median: f64 = 0.0;
    //     for &ip in self.ip2trace.keys() {
    //         let mut server_trace = self.ip2trace.get(&ip).unwrap().write().unwrap();
    //         let num_records = server_trace.len();
    //         let mut duration: u64 = 0;
    //         let mut ql_mean: f64 = 0.0;
    //         let mut ql_std: f64 = 0.0;
    //         let mut task_cv_mean: f64 = 0.0;
    //         let mut task_cv_std: f64 = 0.0;
    //         let mut task_cv_median: f64 = 0.0;
    //         if num_records > 0 {
    //             let start: u64 = server_trace[0].1 .0;
    //             let end: u64 = server_trace[num_records - 1].1 .0;
    //             duration = end - start;
    //             ql_mean = server_trace.iter().map(|x| x.1 .1 .0).sum::<f64>() / num_records as f64;
    //             ql_std = (server_trace
    //                 .iter()
    //                 .map(|x| (x.1 .1 .0 - ql_mean) * (x.1 .1 .0 - ql_mean))
    //                 .sum::<f64>()
    //                 / num_records as f64)
    //                 .sqrt();
    //             server_trace.sort_by(|a, b| a.1 .1 .1.partial_cmp(&b.1 .1 .1).unwrap());
    //             task_cv_median = server_trace[num_records / 2].1 .1 .1;
    //             task_cv_mean =
    //                 server_trace.iter().map(|x| x.1 .1 .1).sum::<f64>() / num_records as f64;
    //             task_cv_std = (server_trace
    //                 .iter()
    //                 .map(|x| (x.1 .1 .1 - task_cv_mean) * (x.1 .1 .1 - task_cv_mean))
    //                 .sum::<f64>()
    //                 / num_records as f64)
    //                 .sqrt();
    //         }
    //         ql_total += ql_mean;
    //         ql_total_std += ql_std;
    //         task_cv_total += task_cv_mean;
    //         task_cv_total_std += task_cv_std;
    //         task_cv_total_median += task_cv_median;
    //         println!(
    //             "cluster {} ip {} ql_mean {:.2} ql_std {:.2} task_cv_mean {:.2} task_cv_std {:.2} task_cv_median {:.2} duration {} total {} records",
    //             self.cluster_name,
    //             ip,
    //             ql_mean,
    //             ql_std,
    //             task_cv_mean,
    //             task_cv_std,
    //             task_cv_median,
    //             duration / 2400,
    //             num_records,
    //         )
    //     }
    //     let num_servers = self.ip2trace.keys().len() as f64;
    //     let ql_mean = ql_total / num_servers;
    //     let ql_std = ql_total_std / num_servers;
    //     let task_cv_mean = task_cv_total / num_servers;
    //     let task_cv_std = task_cv_total_std / num_servers;
    //     let task_cv_median = task_cv_total_median / num_servers;
    //     println!(
    //         "{} summary ql_mean {:.2} ql_std {:.2} task_cv_mean {:.2} task_cv_std {:.2} task_cv_median {:.2}",
    //         self.cluster_name, ql_mean, ql_std, task_cv_mean, task_cv_std, task_cv_median,
    //     );
    // }
    // #[cfg(feature = "server_stats")]
    // pub fn write_trace(&self) {
    //     for &ip in self.ip2trace.keys() {
    //         let server_trace = self.ip2trace.get(&ip).unwrap().read().unwrap();
    //         let mut f = File::create(format!("{}_ip{}.log", self.cluster_name, ip)).unwrap();
    //         let mut start: u64 = 0;
    //         let mut duration: f32 = 0.0;
    //         for &(id, (timestamp, queue_len)) in server_trace.iter() {
    //             if start == 0 {
    //                 start = timestamp;
    //             }
    //             duration = (timestamp - start) as f32 / 2400.0;
    //             writeln!(f, "{} {:.2} {}", id, duration, queue_len);
    //         }
    //         println!(
    //             "cluster {} ip {} duration {:.2} total {} records",
    //             self.cluster_name,
    //             ip,
    //             duration,
    //             server_trace.len(),
    //         )
    //     }
    // }
}
// #[cfg(feature = "server_stats")]
impl fmt::Display for ServerLoad {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "#######{}#######", self.cluster_name)?;
        for &ip in self.ip2load.keys() {
            // let server_load = self.ip2load.get(&ip).unwrap().read().unwrap();
            // writeln!(f, "ip {} load {}", ip, server_load)?;
            writeln!(f, "ip {}", ip)?;
            let server_load = self.ip2load.get(&ip).unwrap();
            // let mut server_total = 0f64;
            // let mut server_total_moving = 0f64;
            // let num_cores = server_load.len() as f64;
            for (i, core_load) in server_load.iter().enumerate() {
                let core_load = core_load.read().unwrap();
                // server_total += core_load.avg.E_x;
                // server_total_moving += core_load.avg();
                writeln!(f, "    core {} {}", i, *core_load)?;
            }
            // writeln!(f,"    ip {} avg {} moving {}", ip,server_total/num_cores,server_total_moving/num_cores)?;
        }
        Ok(())
    }
}
unsafe impl Send for ServerLoad {}
unsafe impl Sync for ServerLoad {}

fn clamp(raw_step: f64, min_step: f64, max_step: f64) -> f64 {
    let mut bounded_step = raw_step.abs().max(min_step);
    if max_step < min_step {
        bounded_step = min_step;
    } else {
        bounded_step = bounded_step.min(max_step);
    }
    bounded_step * raw_step.signum()
}

fn relative_err(x: f64, y: f64) -> f64 {
    (x - y).abs() * 2.0 / (x + y)
}

pub struct TputGrad {
    // pub storage_load: Arc<ServerLoad>,
    // pub compute_load: Arc<ServerLoad>,
    interval: u64,
    last_rdtsc: u64,
    pub last_recvd: usize,
    // last_tput: f64,
    last_inv_tput: f64,
    // last_ql_storage: f64,
    // last_ql_compute: f64,
    last_x: f64,
    // // thresh
    // max_ql_diff: f64,
    // min_ql_diff: f64,
    // ql_lowerbound: f64,
    // exp_lowerbound: f64,
    max_step_rel: f64,
    max_step_abs: f64,
    min_step_rel: f64,
    min_step_abs: f64,
    lr: f64,
    // lr_decay: f64,
    // default_lr: f64,
    // min_lr: f64,
    upperbound: f64,
    lowerbound: f64,
    min_interval: f64,
    min_delta_rel: f64,
    min_delta_abs: f64,
    max_err: f64,
    tolerance: u32,
    pub anomalies: u32,
    // not useful
    min_err: f64,
    pub converged: bool,
    // best_y: f64,
    // best_x: f64,
    msg: String,
    start: u64,
    elapsed: u64,
    // trace
    pub xtrace: Vec<String>,
}

impl TputGrad {
    pub fn new(
        config: &XloopConfig,
        initial_partition: f64,
        // storage_load: Arc<ServerLoad>,
        // compute_load: Arc<ServerLoad>,
    ) -> TputGrad {
        TputGrad {
            // storage_load: storage_load,
            // compute_load: compute_load,
            interval: CPU_FREQUENCY / config.factor,
            last_rdtsc: 0,
            last_recvd: 0,
            // last_tput: 0.0,
            last_inv_tput: 0.0,
            // last_ql_storage: 0.0,
            // last_ql_compute: 0.0,
            // last_x: config.partition * 100.0,
            last_x: initial_partition * 100.0,
            // last_x: 0.0,
            // max_ql_diff:config.max_ql_diff,
            // min_ql_diff:config.min_ql_diff,
            // ql_lowerbound: config.ql_lowerbound,
            // exp_lowerbound: config.exp_lowerbound,
            lr: config.lr,
            max_step_rel: config.max_step_rel,
            max_step_abs: config.max_step_abs,
            min_step_rel: config.min_step_rel,
            min_step_abs: config.min_step_abs,
            min_interval: config.min_interval,
            min_delta_rel: config.min_delta_rel,
            min_delta_abs: config.min_delta_abs,
            max_err: config.max_err,
            tolerance: config.tolerance,
            anomalies: 0,
            // not useful
            min_err: config.min_err,
            converged: true,
            upperbound: 10000.0,
            lowerbound: 0.0,
            msg: String::new(),
            start: 0,
            elapsed: 0,
            xtrace: Vec::with_capacity(64000000),
        }
    }
    // pub fn snapshot(&mut self,curr_rdtsc:u64){
    //     let (out_storage,_,cv_storage,ncores_storage)=self.storage_load.aggr_all();
    //     let (out_compute,w_compute,cv_compute,ncores_compute)=self.compute_load.aggr_all();
    //     self.storage_load.outs_trace.borrow_mut().update(out_storage);
    //     self.compute_load.outs_trace.borrow_mut().update(out_compute);
    // }
    pub fn ready(&mut self, curr_rdtsc: u64) -> bool {
        if self.last_rdtsc == 0 {
            self.last_rdtsc = curr_rdtsc;
            false
        } else {
            curr_rdtsc - self.last_rdtsc > self.interval
        }
    }
    pub fn sync(&mut self, curr_rdtsc: u64, recvd: usize) {
        let delta_recvd = recvd - self.last_recvd;
        let delta_t = curr_rdtsc - self.last_rdtsc;
        // let tput = 2.4e6 * (recvd - self.last_recvd) as f64 / (curr_rdtsc - self.last_rdtsc) as f64;
        let inv_tput = delta_t as f64 / delta_recvd as f64;
        let delta_inv_tput = inv_tput - self.last_inv_tput;
        self.last_rdtsc = curr_rdtsc;
        self.last_recvd = recvd;
        self.msg.clear();
        write!(
            self.msg,
            "rdtsc {} inv_tput {:.2}({:.2}) tput {:.2}({}/{})",
            curr_rdtsc,
            inv_tput,
            delta_inv_tput,
            CPU_FREQUENCY as f64 / inv_tput / 1000.0,
            delta_recvd,
            delta_t,
        );
    }
    /// update
    pub fn update_x(
        &mut self,
        xinterface: &impl XInterface<X = f64>,
        curr_rdtsc: u64,
        recvd: usize,
    ) {
        let x = xinterface.get();
        let delta_x = x - self.last_x;
        // tput
        let delta_recvd = recvd - self.last_recvd;
        let delta_t = curr_rdtsc - self.last_rdtsc;
        // let tput = 2.4e6 * (recvd - self.last_recvd) as f64 / (curr_rdtsc - self.last_rdtsc) as f64;
        let inv_tput = delta_t as f64 / delta_recvd as f64;
        // let delta_tput = tput - self.last_tput;
        let delta_inv_tput = inv_tput - self.last_inv_tput;
        let rel_err = relative_err(inv_tput, self.last_inv_tput);
        self.msg.clear();
        write!(
            self.msg,
            "rdtsc {} x {:.2}({:.2}) tput {:.2}({}/{}) inv_tput {:.2}({:.2}~{:.2}%) ",
            curr_rdtsc,
            x,
            delta_x,
            CPU_FREQUENCY as f64 / inv_tput / 1000.0,
            delta_recvd,
            delta_t,
            inv_tput,
            // tput,
            delta_inv_tput,
            // delta_tput,
            rel_err * 100.0,
        );
        // sync
        self.last_rdtsc = curr_rdtsc;
        self.last_recvd = recvd;
        // NOTE: we do not always update last_inv_tput after introducing tolerance, so copy before update
        let last_inv_tput = self.last_inv_tput;
        self.last_inv_tput = inv_tput;
        // convergence
        if self.converged {
            // avoid updating if this is within #tolerance steps after jumping out of convergence
            if rel_err > self.max_err {
                self.anomalies += 1;
                if self.start == 0 {
                    self.start = curr_rdtsc;
                }
                if self.anomalies == self.tolerance || last_inv_tput == 0f64 {
                    self.converged = false;
                    // anomalies still equals tolerance, will be reset to 0 after convergence
                    // compute gradient next time
                } else {
                    // fall back to previous history
                    self.last_inv_tput = last_inv_tput;
                }
            } else {
                // only consecutive anomalies are taken into account, so reset
                self.anomalies = 0;
                self.start = 0;
            }
            write!(
                self.msg,
                "elapsed {}ms anomalies {}/{}",
                self.elapsed / (CPU_FREQUENCY / 1000),
                self.anomalies,
                self.tolerance
            );
        } else {
            // thresh for bounding interval
            let min_delta_rel = self.min_delta_rel * delta_x.abs();
            let min_delta = self.min_delta_abs.max(min_delta_rel);
            // grad
            let mut step = 0f64;
            let mut step_raw = 0f64;
            // interval
            // last_x used here, so update later
            if delta_x > 0.0 {
                if delta_inv_tput > min_delta {
                    self.upperbound = x;
                } else if delta_inv_tput < -min_delta {
                    self.lowerbound = self.last_x;
                }
            } else if delta_x < 0.0 {
                // exclude delta_x = 0, for the first call to xloop or after reset
                if delta_inv_tput < -min_delta {
                    self.upperbound = self.last_x;
                } else if delta_inv_tput > min_delta {
                    self.lowerbound = x;
                }
            }
            self.last_x = x;
            let interval = self.upperbound - self.lowerbound;
            // convergence criterion
            if interval <= self.min_interval
            /* && rel_err < self.min_err */
            {
                self.converged = true;
                self.anomalies = 0;
                self.upperbound = 10000.0;
                self.lowerbound = 0.0;
                self.elapsed = curr_rdtsc - self.start;
                self.start = 0;
            } else {
                // step_raw = -self.lr * interval * delta_inv_tput / (delta_x + 1e-9);
                // let max_step = self.max_step_abs.min(self.max_step_rel * interval);
                // let min_step = self.min_step_abs.max(self.min_step_rel * interval);
                step_raw = -self.lr * interval * (delta_inv_tput / (delta_x + 1e-9)).signum();
                let max_step = self.max_step_abs;
                let min_step = self.min_step_abs;
                step = clamp(step_raw, min_step, max_step);
                // update
                xinterface.update(step);
            }
            write!(
                self.msg,
                "min_delta {:.2}({:.2}) step {:.2}({:.2}) range {:.2}({:.2},{:.2}) ",
                min_delta,
                min_delta_rel,
                step,
                step_raw,
                interval,
                self.lowerbound,
                self.upperbound,
            );
        }
        /*
        // elastic scaling
        if converged{
            let (out_storage,w_storage,cv_storage,ncores_storage)=self.storage_load.aggr_all();
            let (out_compute,w_compute,cv_compute,ncores_compute)=self.compute_load.aggr_all();
            let out_storage = out_storage/ncores_storage;
            let out_compute = out_compute/ncores_compute;
            let w_compute = w_compute/ncores_compute;
            // w_storage == 0
            let ql_storage = out_storage+w_compute;
            let ql_compute = out_compute-w_compute;
            // ub and lb of ql_compute
            let (upperbound_compute,lowerbound_compute) = if ql_storage-self.min_ql_diff>self.ql_lowerbound{ // min 5, max 10, lb 1
                (ql_storage-self.min_ql_diff,(ql_storage-self.max_ql_diff).max(self.ql_lowerbound))
            }else{
                (ql_storage,ql_storage*self.exp_lowerbound)
            };
            balanced = if ql_compute > upperbound_compute{
                Some(1)  // compute overload
            }else if ql_compute > lowerbound_compute{
                Some(0)   // balanced
            }else{
                Some(-1)   // storage overload
            };
            // NOTE: if converged, ql_storage is always larger than ql_compute
            // // ub and lb of ql_compute
            // let (upperbound_compute,lowerbound_compute) = if ql_storage-self.min_ql_diff>self.ql_lowerbound{ // min 5, max 10, lb 1
            //     (ql_storage-self.min_ql_diff,(ql_storage-self.max_ql_diff).max(self.ql_lowerbound))
            // }else{
            //     (ql_storage,ql_storage*self.exp_lowerbound)
            // };
            // balanced = if ql_compute > upperbound_compute{
            //     Some(1)  // compute overload
            // }else if ql_compute > lowerbound_compute{
            //     Some(0)   // balanced
            // }else{
            //     Some(-1)   // storage overload
            // };
            write!(self.msg,"storage {:.2}({:.2}+{:.2}) compute {:.2}({:.2}-{:.2}) range ({:.2},{:.2})",ql_storage,out_storage,w_compute,ql_compute,out_compute,w_compute,upperbound_compute,lowerbound_compute);
        }
        */
        // // reset avg of waiting, locally on lb
        // self.compute_load.reset();
        // self.storage_load.reset();
        if cfg!(feature = "xtrace") {
            self.xtrace.push(self.msg.clone());
        }
    }
}

impl fmt::Display for TputGrad {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

// TODO: use compute cores, add compute_provision in provision
// TODO: snapshot outstanding when updating load
#[derive(Clone)]
pub struct ElasticScaling {
    pub compute_load: Arc<ServerLoad>,
    pub compute_outs: Avg, // snapshot
    pub storage_load: Arc<ServerLoad>,
    pub storage_outs: Avg, // snapshot
    // if elastic then use this, else use config.provision
    pub compute_cores: Arc<AtomicI32>,
    pub storage_cores: u32,
    max_quota: i32,
    min_quota: i32,
    // max_load_abs: f64,
    // max_load_rel: f64,
    // min_load_abs: f64,
    // min_load_rel: f64,
    max_load: f64,
    min_load: f64,
    max_step: i32,
    // confidence: u32,
    // convergence: u32,
    // history
    // last_inv_tput: f64,
    // last_provision: i32,
    upperbound: i32,
    lowerbound: i32,
    pub msg: String,
    pub elastic: bool,
    pub load_summary: (f64, f64),
}

impl ElasticScaling {
    pub fn new(config: &LBConfig) -> ElasticScaling {
        let compute_servers: Vec<_> = config
            .compute
            .iter()
            .map(|x| (&x.ip_addr, x.rx_queues))
            .collect();
        let compute_load = ServerLoad::new("compute", compute_servers);
        let storage_servers: Vec<_> = config
            .storage
            .iter()
            .map(|x| (&x.ip_addr, x.rx_queues))
            .collect();
        let storage_load = ServerLoad::new("storage", storage_servers);
        // TODO: if elastic then set initial provision to 32 cores
        // let initial_provision = config.provisions[0].compute as i32;
        let max_quota = config.compute.iter().map(|cfg| cfg.rx_queues).sum::<i32>();
        ElasticScaling {
            compute_load: Arc::new(compute_load),
            compute_outs: Avg::new(),
            storage_load: Arc::new(storage_load),
            storage_outs: Avg::new(),
            compute_cores: Arc::new(AtomicI32::new(config.provisions[0].compute as i32)),
            storage_cores: config.provisions[0].storage,
            // last_provision: initial_provision,
            upperbound: 0,
            lowerbound: max_quota,
            max_quota: max_quota,
            min_quota: 8,
            // max_load_abs: config.max_load_abs,
            // max_load_rel: config.max_load_rel,
            // min_load_abs: config.min_load_abs,
            // min_load_rel: config.min_load_rel,
            max_load: config.elastic.max_load,
            min_load: config.elastic.min_load,
            max_step: config.elastic.max_step,
            // confidence: 0,
            // convergence: config.elastic.convergence,
            msg: String::new(),
            elastic: config.elastic.elastic,
            load_summary: (0.0, 0.0),
        }
    }
    pub fn reset(&mut self) {
        self.compute_load.reset();
        self.compute_outs.reset();
        self.storage_load.reset();
        self.storage_outs.reset();
    }
    pub fn snapshot(&mut self) {
        let compute_outs = self.compute_load.outstanding.load(Ordering::Relaxed);
        self.compute_outs.update(compute_outs as f64);
        let storage_outs = self.storage_load.outstanding.load(Ordering::Relaxed);
        self.storage_outs.update(storage_outs as f64);
        // println!(
        //     "snapshot {:.2}({:.2}) {:.2}({:.2})",
        //     self.compute_outs.avg(),
        //     self.compute_outs.lastest,
        //     self.storage_outs.avg(),
        //     self.storage_outs.lastest,
        // );
    }
    // a wrapper around the actual update_load function
    // perform ip check, ignore response packet in case provision has changed
    pub fn update_load(
        &self,
        src_ip: u32,
        src_port: u16,
        queue_length: f64,
        task_duration_cv: f64,
    ) {
        // set to -1 after the first rpc resp packet in that round
        if queue_length < 0.0 || task_duration_cv == 0.0 {
            return;
        }
        // might be from storage or out-of-date compute node
        if let Ok(server_idx) = self.compute_load.ip_addrs.binary_search(&src_ip) {
            trace!(
                "ip {} port {} ql {} cv {}",
                src_ip,
                src_port,
                queue_length,
                task_duration_cv
            );
            let core_idx = self.compute_load.cumsum_ports[server_idx] + src_port as usize;
            if core_idx < self.compute_cores.load(Ordering::Relaxed) as usize {
                self.compute_load
                    .update_load(src_ip, src_port, queue_length, task_duration_cv);
            }
        } else if let Ok(server_idx) = self.storage_load.ip_addrs.binary_search(&src_ip) {
            let core_idx = self.storage_load.cumsum_ports[server_idx] + src_port as usize;
            if core_idx < self.storage_cores as usize {
                self.storage_load
                    .update_load(src_ip, src_port, queue_length, task_duration_cv);
            }
        }
    }
    fn ql2load(ql_raw: f64, cv: f64) -> f64 {
        let load = 2.0 * ql_raw / (1.0 + cv);
        ((load * load + 4.0 * load).sqrt() - load) / 2.0
    }
    pub fn calc_load(&self) -> (f64, f64) {
        let (w_compute, cv_compute, ncores_compute) = self.compute_load.aggr_all();
        let (_, cv_storage, ncores_storage) = self.storage_load.aggr_all();
        let out_compute = self.compute_outs.avg();
        let out_storage = self.storage_outs.avg();
        let ql_compute = (out_compute - w_compute).max(0f64) / (ncores_compute + 1e-9);
        let ql_storage = (out_storage + w_compute) / (ncores_storage + 1e-9);
        let cv_compute = cv_compute / (ncores_compute + 1e-9);
        let cv_storage = cv_storage / (ncores_storage + 1e-9);
        let load_compute = Self::ql2load(ql_compute, cv_compute);
        let load_storage = Self::ql2load(ql_storage, cv_storage);
        debug!(
            "outs {:.2} waiting {:.2} cores {:.0} ql {:.2} cv {:.2}",
            out_compute / (ncores_compute + 1e-9),
            w_compute / (ncores_compute + 1e-9),
            ncores_compute,
            ql_compute,
            cv_compute
        );
        (load_compute, load_storage)
    }
    fn bounded_step(&self) -> i32 {
        self.max_step.min((self.upperbound - self.lowerbound) / 2)
    }
    pub fn scaling(&mut self) -> bool {
        let (load_compute, load_storage) = self.calc_load();
        self.load_summary = (load_compute, load_storage);
        // provision
        let provision = self.compute_cores.load(Ordering::Relaxed);
        // let delta_provision = provision - self.last_provision;
        // // sync
        // self.last_provision = provision;
        // update
        let mut step = 0i32;
        if self.elastic {
            if load_compute < self.min_load {
                // self.confidence = 0;
                self.upperbound = provision;
                if self.upperbound > self.lowerbound {
                    step = -self.max_step.min((self.upperbound - self.lowerbound) / 2);
                } else if provision - self.max_step < self.min_quota {
                    step = -(provision - self.min_quota);
                } else {
                    step = -self.max_step;
                }
                // step = -self.bounded_step();
            } else if load_compute > self.max_load {
                // self.confidence = 0;
                self.lowerbound = provision;
                if self.upperbound > self.lowerbound {
                    step = self.max_step.min((self.upperbound - self.lowerbound) / 2);
                } else if provision + self.max_step > self.max_quota {
                    step = self.max_quota - provision;
                } else {
                    step = self.max_step;
                }
                // step = self.bounded_step();
            }
            /* else we have a resonable load on compute */
            // convergence: step=0, either because max_quota is reached or load is in accepted range
            if step != 0 {
                self.compute_cores
                    .store(provision + step, Ordering::Relaxed);
            } else {
                // converged, reset
                // self.lowerbound = 0;
                // self.upperbound = self.max_quota;
                self.lowerbound = self.max_quota;
                self.upperbound = 0;
            }
        }
        // NOTE: stats is reset by lb
        // NOTE: self.msg is cleared after printing
        write!(
            self.msg,
            "cores {}({}) range ({},{}) load {:.3},{:.3}",
            provision,
            // delta_provision,
            step,
            // self.confidence,
            // self.convergence,
            self.lowerbound,
            self.upperbound,
            load_compute,
            load_storage,
            // min_load,
            // min_load_rel,
            // max_load,
            // max_load_rel,
        );
        step != 0
    }
    /*
    pub fn scaling(&mut self) -> bool {
        if !self.elastic {
            return false;
        }
        let (load_compute, load_storage) = self.calc_load();
        let min_load_rel = self.min_load_rel * load_storage;
        let max_load_rel = self.max_load_rel * load_storage;
        let min_load = self.min_load_abs.min(min_load_rel);
        let max_load = self.max_load_abs.min(max_load_rel);
        // provision
        let provision = self.compute_cores.load(Ordering::Relaxed);
        let delta_provision = provision - self.last_provision;
        // sync
        self.last_provision = provision;
        // update
        let mut step = 0i32;
        if load_compute == 0f64 {
            step = 0 - provision;
        }
        if load_compute < min_load {
            if delta_provision > 0 {
                // inc too much
                step = -delta_provision / 2;
            } else {
                // still to dec
                if provision > self.max_step {
                    step = -self.max_step;
                } else {
                    step = -provision / 2;
                }
            }
        } else if load_compute > max_load {
            if delta_provision < 0 {
                // dec too much
                step = -delta_provision / 2;
            } else {
                // still to inc
                if provision + self.max_step > self.max_quota {
                    step = self.max_quota - provision;
                } else {
                    step = self.max_step;
                }
            }
        } /* else we have a resonable load on compute */
        // convergence: step=0
        // either because max_quota is reached or load is in accepted range
        if step != 0 {
            self.compute_cores
                .store(provision + step, Ordering::Relaxed);
        }
        // moved to fn recv in lb
        // self.compute_load.reset();
        // self.msg.clear();
        write!(
            self.msg,
            "cores {}({},{}) load {:.2},{:.2} min {:.2}({:.2}) max {:.2}({:.2})",
            provision,
            delta_provision,
            step,
            load_compute,
            load_storage,
            min_load,
            min_load_rel,
            max_load,
            max_load_rel,
        );
        step != 0
    }
    */
}
impl fmt::Display for ElasticScaling {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

/*
// inc by fixed step, dec by binary search
pub struct ElasticScaling {
    // thresh
    max_ql_diff: f64,
    min_ql_diff: f64,
    ql_lowerbound: f64,
    exp_lowerbound: f64,
    // update
    max_alloc: u32,
    // history
    cores: u32,
    last_state: i32,
    last_step: u32,
}
*/

/*
pub struct QueueGrad {
    pub storage_load: Arc<ServerLoad>,
    pub compute_load: Arc<ServerLoad>,
    interval: u64,
    last_rdtsc: u64,
    pub last_recvd: usize,
    last_tput: f64,
    // last_inv_tput: f64,
    last_ql_storage: f64,
    last_ql_compute: f64,
    last_x: f64,
    // thresh
    thresh_ql: f64,
    // thresh_tput: f64,
    // monotone_thresh: f64,
    // min_range: f64,
    // lowerbound: f64,
    // upperbound: f64,
    min_step: f64,
    max_step: f64,
    lr: f64,
    lr_default: f64,
    lr_decay: f64,
    msg: String,
    // trace
    pub xtrace: Vec<String>,
}

impl QueueGrad {
    pub fn new(
        config: &LBConfig,
        storage_load: Arc<ServerLoad>,
        compute_load: Arc<ServerLoad>,
    ) -> QueueGrad {
        QueueGrad {
            storage_load: storage_load,
            compute_load: compute_load,
            interval: CPU_FREQUENCY / config.xloop_factor,
            last_rdtsc: 0,
            last_recvd: 0,
            last_tput: 0.0,
            // last_inv_tput: 0.0,
            last_ql_storage: 0.0,
            last_ql_compute: 0.0,
            last_x: config.partition*100.0+1.0,
            thresh_ql: config.thresh_ql,
            // thresh_tput: config.thresh_tput,
            lr: config.lr, // 200?
            lr_default: config.lr,
            lr_decay: config.lr_decay,
            min_step: config.min_step,
            max_step: config.max_step,
            // exp: exp, // 0.5?
            msg: String::new(),
            xtrace: Vec::with_capacity(64000000),
        }
    }
    pub fn snapshot(&mut self,curr_rdtsc:u64/*,global_recvd:usize*/){
        let (out_storage,_,cv_storage,ncores_storage)=self.storage_load.aggr_all();
        let (out_compute,w_compute,cv_compute,ncores_compute)=self.compute_load.aggr_all();
        self.storage_load.outs_trace.borrow_mut().update(out_storage);
        self.compute_load.outs_trace.borrow_mut().update(out_compute);
        // let tput = 2.4e6 * (global_recvd - self.last_recvd) as f32
        //     / (curr_rdtsc - self.last_rdtsc) as f32;
        // self.last_rdtsc=curr_rdtsc;
        // self.last_recvd=global_recvd;
        // println!("rdtsc {} tput {:.2}",curr_rdtsc/2400000,tput);
    }
    pub fn ready(&mut self, curr_rdtsc: u64) -> bool {
        if self.last_rdtsc==0{
            self.last_rdtsc=curr_rdtsc;
            false
        }else{
            curr_rdtsc - self.last_rdtsc > self.interval
        }
    }
    fn clamp(&self, raw_step: f64) -> f64 {
        let bounded_step = raw_step.abs().min(self.max_step);
        bounded_step * raw_step.signum()
    }
    /// update
    pub fn update_x(
        &mut self,
        xinterface: &impl XInterface<X = f64>,
        curr_rdtsc: u64,
        recvd: usize,
    ) {
        let x = xinterface.get();
        let delta_x = x - self.last_x;
        // tput
        let tput = 2.4e6 * (recvd - self.last_recvd) as f64 / (curr_rdtsc - self.last_rdtsc) as f64;
        // let inv_tput = (curr_rdtsc - self.last_rdtsc) as f64 / (recvd - self.last_recvd) as f64;
        let delta_tput = tput - self.last_tput;
        // let delta_inv_tput = inv_tput - self.last_inv_tput;
        // // ql
        // let (ql_storage_raw, cv_storage) = self.storage_load.mean_all();
        // let (ql_compute_raw, cv_compute) = self.compute_load.mean_all();
        let (out_storage,_,cv_storage,ncores_storage)=self.storage_load.aggr_all();
        let (out_compute,w_compute,cv_compute,ncores_compute)=self.compute_load.aggr_all();
        // out_storage + w_compute=ql_storage
        let ql_storage_raw = (out_storage+w_compute)/ncores_storage;
        let ql_compute_raw = (out_compute-w_compute)/ncores_compute;
        let ql_storage = ql_storage_raw / (1.0 + cv_storage/ncores_storage);
        let ql_compute = ql_compute_raw / (1.0 + cv_compute/ncores_compute);
        // diff
        let last_ql_diff = self.last_ql_storage - self.last_ql_compute;
        let ql_diff = ql_storage - ql_compute;
        let delta_ql_storage = ql_storage - self.last_ql_storage;
        let delta_ql_compute = ql_compute - self.last_ql_compute;
        let delta_ql_diff = ql_diff - last_ql_diff;
        // sync
        self.last_rdtsc = curr_rdtsc;
        self.last_recvd = recvd;
        // NOTE: always update tput history because thresh_tput is based on delta
        self.last_tput = tput;
        // self.last_inv_tput = inv_tput;
        // sync on every update
        self.last_x = x;
        self.last_ql_storage = ql_storage;
        self.last_ql_compute = ql_compute;
        // update
        let mut step: f64 = 0.0;
        let mut step_raw: f64 = 0.0;
        // NOTE: suppress variation with two thresh
        if ql_diff.abs() > self.thresh_ql
        /*&& delta_inv_tput.abs() > self.thresh_tput*/
        {
            if delta_ql_diff * ql_diff < 0.0 {
                // Newton's method
                step_raw = -ql_diff * delta_x / delta_ql_diff;
                step = self.lr * self.clamp(step_raw);
            } else {
                // step = -self.min_step * delta_x.signum() * delta_inv_tput.signum();
                step = self.lr*self.min_step * delta_x.signum() * delta_tput.signum();
            }
            // decrease lr if direction has changed
            // in case of jump out, delta_x=0, so lr is unchaged
            if delta_x * step < 0.0 {
                self.lr *= self.lr_decay;
            }
            // // sync
            // self.last_x = x;
            // self.last_ql_storage = ql_storage;
            // self.last_ql_compute = ql_compute;
        } else {
            // reset lr, this is jump out
            self.lr = self.lr_default;
            // // do nothing for now, just reset history, prepare for jump out
            // self.last_x = 10000.0;
            // self.last_ql_storage = 10000.0;
            // self.last_ql_compute = 0.0;
        }
        // update
        if step != 0.0 {
            xinterface.update(step);
        }
        // reset after each update, in case workload has changed within loop interval
        self.compute_load.reset();
        self.storage_load.reset();
        self.msg.clear();
        write!(
            self.msg,
            // "rdtsc {} x {:.2} dx {:.2} ql_diff {:.2}({:.2}) inv_tput {:.2}({:.2}) step {:.2} step_raw {:.2} lr {:.2} storage {:.2}({:.2}) compute {:.2}({:.2}) ql_raw ({:.2},{:.2}) cv ({:.2},{:.2})",
            "rdtsc {} x {:.2} dx {:.2} ql_diff {:.2}({:.2}) tput {:.2}({:.2}) step {:.2} step_raw {:.2} lr {:.2} storage {:.2}({:.2}) compute {:.2}({:.2}) ql_raw ({:.2},{:.2}) cv ({:.2},{:.2})",
            curr_rdtsc,
            x,
            delta_x,
            ql_diff,
            delta_ql_diff,
            // inv_tput,
            tput,
            // delta_inv_tput,
            delta_tput,
            step,
            step_raw,
            self.lr,
            ql_storage,
            delta_ql_storage,
            ql_compute,
            delta_ql_compute,
            ql_storage_raw,
            ql_compute_raw,
            cv_storage,
            cv_compute,
        );
        if cfg!(feature = "xtrace"){
            self.xtrace.push(self.msg.clone());
        }
    }

    // /// xloop
    // pub fn xloop(
    //     &mut self,
    //     xinterface: &impl XInterface<X = f64>,
    //     curr_rdtsc: u64,
    //     recvd: usize,
    //     ql_storage: f64,
    //     ql_compute: f64,
    // ) {
    //     // recvd % self.factor?
    //     if curr_rdtsc - self.last_rdtsc > self.interval {
    //         self.update_x(xinterface, curr_rdtsc, ql_storage, ql_compute);
    //     }
    // }
    // /// update
    // pub fn update_x(
    //     &mut self,
    //     xinterface: &impl XInterface<X = f64>,
    //     curr_rdtsc: u64,
    //     recvd: usize,
    //     ql_storage: f64,
    //     ql_compute: f64,
    // ) {
    // let tput = 2.4e6 * (recvd - self.last_recvd) as f64 / (curr_rdtsc - self.last_rdtsc) as f64;
    // let delta_tput = tput - self.last_tput;
    //     // let last_ql_diff = self.last_ql_storage - self.last_ql_compute;
    //     let ql_diff = ql_storage - ql_compute;
    //     let delta_ql_storage = ql_storage - self.last_ql_storage;
    //     let delta_ql_compute = ql_compute - self.last_ql_compute;
    //     // let delta_ql_diff = ql_diff - last_ql_diff;
    //     let x = xinterface.get();
    //     let delta_x = x - self.last_x;
    //     // sync
    //     self.last_rdtsc = curr_rdtsc;
    //     self.last_ql_storage = ql_storage;
    //     self.last_ql_compute = ql_compute;
    //     self.last_x = x;
    //     // grad
    //     let mut step: f64 = 0.0;
    //     let mut raw_step: f64 = 0.0;
    //     if ql_diff > 0.0 {
    //         if delta_ql_storage < 0.0 {
    //             // proceed with grad
    //             raw_step = self.lr * delta_ql_storage / delta_x;
    //             let bounded_step = raw_step.abs().max(self.min_step).min(self.max_step);
    //             step = if raw_step > 0.0 {
    //                 bounded_step
    //             } else {
    //                 -bounded_step
    //             };
    //         } else {
    //             // bounce back
    //             step = -delta_x * (1.0 - self.exp);
    //         }
    //     } else {
    //         // if delta_ql_compute < 0.0 {
    //         //     let raw_step = self.lr * delta_ql_compute / delta_x;
    //         //     let bounded_step = raw_step.abs().max(self.min_step).min(self.max_step);
    //         //     step = if raw_step > 0.0 {
    //         //         bounded_step
    //         //     } else {
    //         //         -bounded_step
    //         //     };
    //         // } else {
    //         //     // bounce back
    //         //     step = -delta_x * (1.0 - self.exp);
    //         // }
    //     }
    //     xinterface.update(step);
    //     self.msg.clear();
    //     write!(
    //         self.msg,
    //         "ql {:?} x {} d_ql {:?} d_x {} step {} raw_step {}",
    //         (ql_storage, ql_compute),
    //         x,
    //         (delta_ql_storage, delta_ql_compute),
    //         delta_x,
    //         step,
    //         raw_step,
    //     );
    // }
}

impl fmt::Display for QueueGrad {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

pub struct QueueSweep {
    pub storage_load: Arc<ServerLoad>,
    pub compute_load: Arc<ServerLoad>,
    interval: u64,
    last_rdtsc: u64,
    last_ql_storage: f64,
    last_ql_compute: f64,
    last_x: f64,
    ql_diff_thresh: f64,
    monotone_thresh: f64,
    min_range: f64,
    lowerbound: f64,
    upperbound: f64,
    // min_step: f64,
    max_step: f64,
    lr: f64,
    lr_decay: f64,
    msg: String,
}

impl QueueSweep {
    pub fn new(
        xloop_factor: u64,
        min_range: f64,
        ql_diff_thresh: f64,
        monotone_thresh: f64,
        max_step: f64,
        lr_decay: f64,
        storage_load: Arc<ServerLoad>,
        compute_load: Arc<ServerLoad>,
    ) -> QueueSweep {
        QueueSweep {
            storage_load: storage_load,
            compute_load: compute_load,
            interval: CPU_FREQUENCY / xloop_factor,
            last_rdtsc: 0,
            last_ql_storage: 10000.0, // a large number
            last_ql_compute: 0.0,
            last_x: 10000.0,
            min_range: min_range,
            ql_diff_thresh: ql_diff_thresh,
            monotone_thresh: monotone_thresh,
            lowerbound: 0.0,
            upperbound: 10000.0,
            max_step: max_step,
            lr: 1.0,
            lr_decay: lr_decay,
            msg: String::new(),
        }
    }
    pub fn ready(&mut self, curr_rdtsc: u64) -> bool {
        curr_rdtsc - self.last_rdtsc > self.interval
    }
    /*
    /// update
    pub fn update_x(&mut self, xinterface: &impl XInterface<X = f64>, curr_rdtsc: u64) {
        let (ql_storage_raw, cv_storage) = self.storage_load.avg_all();
        let (ql_compute_raw, cv_compute) = self.compute_load.avg_all();
        let ql_storage = ql_storage_raw / (1.0 + cv_storage);
        let ql_compute = ql_compute_raw / (1.0 + cv_compute);
        // let last_ql_diff = self.last_ql_storage - self.last_ql_compute;
        let ql_diff = ql_storage - ql_compute;
        let delta_ql_storage = ql_storage - self.last_ql_storage;
        let delta_ql_compute = ql_compute - self.last_ql_compute;
        // let delta_ql_diff = ql_diff - last_ql_diff;
        let x = xinterface.get();
        let delta_x = x - self.last_x;
        // sync timer
        self.last_rdtsc = curr_rdtsc;
        // sweep
        let mut step: f64 = 0.0;
        if ql_diff > self.ql_diff_thresh {
            if self.upperbound - self.lowerbound > self.min_range {
                // TODO: dynamic thresh? like lr
                if delta_ql_storage < -self.monotone_thresh {
                    if delta_x > 0.0 {
                        self.lowerbound = self.last_x;
                        step = self.max_step * self.lr;
                    } else {
                        self.upperbound = self.last_x;
                        step = -self.max_step * self.lr;
                    }
                } else {
                    // ql_storage stop descending
                    // decrease step size and sweep [lb,ub] in the opposite direction
                    // TODO: other stop criterion
                    self.lr *= self.lr_decay;
                    if delta_x > 0.0 {
                        self.upperbound = x;
                        step = -self.max_step * self.lr;
                    } else {
                        self.lowerbound = x;
                        step = self.max_step * self.lr;
                    }
                }
            } else {
                // jump out, sweep again
                self.upperbound = 10000.0;
                self.lowerbound = 0.0;
                self.lr = 1.0;
                if x <= 5000.0 {
                    step = self.max_step * self.lr;
                } else {
                    step = -self.max_step * self.lr;
                }
            }
            // sync
            self.last_ql_storage = ql_storage;
            self.last_x = x;
        } else {
            // do nothing for now, just reset history
            // if xloop escape this branch, then the workload must have been changed
            // prepare to sweep again
            self.upperbound = 10000.0;
            self.lowerbound = 0.0;
            self.lr = 1.0;
            self.last_ql_storage = 10000.0;
            self.last_x = 10000.0;
        }
        // update
        if step != 0.0 {
            xinterface.update(step);
        }
        // reset after each update
        self.compute_load.reset();
        self.storage_load.reset();
        self.msg.clear();
        write!(
            self.msg,
            "x {} ql ({:.2},{:.2}) d_ql ({:.2},{:.2}) d_x {} step {} range ({:.2},{:.2}) lr {} ql_raw ({:.2},{:.2}) cv ({:.2},{:.2})",
            x,
            ql_storage,
            ql_compute,
            delta_ql_storage,
            delta_ql_compute,
            delta_x,
            step,
            self.lowerbound,
            self.upperbound,
            self.lr,
            ql_storage_raw,
            ql_compute_raw,
            cv_storage,
            cv_compute,
        );
    }
    */
}

impl fmt::Display for QueueSweep {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}
*/
/*
pub struct QueuePivot {
    interval: u64,
    last_rdtsc: u64,
    last_ql_storage: f64,
    last_ql_compute: f64,
    last_x: f64,
    ql_diff_thresh: f64,
    pivot: f64,
    range: f64,
    step_large: f64,
    step_small: f64,
    msg: String,
}

impl QueuePivot {
    pub fn new(
        xloop_factor: u64,
        pivot: f64,
        range: f64,
        ql_diff_thresh: f64,
        step_large: f64,
        step_small: f64,
    ) -> QueuePivot {
        QueuePivot {
            interval: CPU_FREQUENCY / xloop_factor,
            last_rdtsc: 0,
            last_ql_storage: 10000.0, // a large number
            last_ql_compute: 0.0,
            last_x: 10000.0,
            ql_diff_thresh: ql_diff_thresh,
            pivot: pivot,
            range: range,           // 100
            step_large: step_large, // 1000
            step_small: step_small, // 25
            msg: String::new(),
        }
    }
    pub fn ready(&mut self, curr_rdtsc: u64) -> bool {
        curr_rdtsc - self.last_rdtsc > self.interval
    }

    /// update
    pub fn update_x(
        &mut self,
        xinterface: &impl XInterface<X = f64>,
        curr_rdtsc: u64,
        ql_storage: f64,
        ql_compute: f64,
    ) {
        // let last_ql_diff = self.last_ql_storage - self.last_ql_compute;
        let ql_diff = ql_storage - ql_compute;
        let delta_ql_storage = ql_storage - self.last_ql_storage;
        let delta_ql_compute = ql_compute - self.last_ql_compute;
        // let delta_ql_diff = ql_diff - last_ql_diff;
        let x = xinterface.get();
        let delta_x = x - self.last_x;
        // sweep
        let dist = x - self.pivot;
        let mut step: f64 = 0.0;
        if ql_diff > self.ql_diff_thresh {
            if dist.abs() > self.range {
                let bounded_step = self.step_large.min(dist.abs() - self.range);
                if dist > 0.0 {
                    step = -bounded_step;
                } else {
                    step = bounded_step;
                }
            } else {
                if delta_x > 0.0 {
                    step = self.step_small;
                } else {
                    step = -self.step_small;
                }
            }
        } else {
            // do nothing for now
        }
        // sync
        self.last_rdtsc = curr_rdtsc;
        self.last_ql_storage = ql_storage;
        self.last_x = x;
        // update
        xinterface.update(step);
        self.msg.clear();
        write!(
            self.msg,
            "x {} ql {:?} d_ql {:?} d_x {} step {}",
            x,
            (ql_storage, ql_compute),
            (delta_ql_storage, delta_ql_compute),
            delta_x,
            step,
            // (self.lowerbound, self.upperbound),
            // self.lr,
        );
    }
}

impl fmt::Display for QueuePivot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

pub struct TputGrad {
    // pub timer: Timer,
    interval: u64,
    // factor: usize,
    last_rdtsc: u64,
    last_recvd: usize,
    last_tput: f64,
    last_x: f64,
    lr: f64,
    min_step: f64,
    max_step: f64,
    msg: String,
}

impl TputGrad {
    pub fn new(xloop_factor: u64, lr: f64, min_step: f64, max_step: f64) -> TputGrad {
        // let timer = Timer::new(factor as u64);
        TputGrad {
            interval: CPU_FREQUENCY / xloop_factor,
            last_rdtsc: 0,
            last_recvd: 0,
            last_tput: 0.0,
            last_x: 0.0,
            lr: lr, // 200?
            min_step: min_step,
            max_step: max_step,
            msg: String::new(),
        }
    }
    pub fn xloop(&mut self, xinterface: &impl XInterface<X = f64>, curr_rdtsc: u64, recvd: usize) {
        // recvd % self.factor?
        if curr_rdtsc - self.last_rdtsc > self.interval && recvd > 100 {
            self.update_x(xinterface, curr_rdtsc, recvd);
        }
    }
    pub fn update_x(
        &mut self,
        xinterface: &impl XInterface<X = f64>,
        curr_rdtsc: u64,
        recvd: usize,
    ) {
        let tput = 2.4e6 * (recvd - self.last_recvd) as f64 / (curr_rdtsc - self.last_rdtsc) as f64;
        let delta_tput = tput - self.last_tput;
        let x = xinterface.get();
        let delta_x = x - self.last_x;
        // sync
        self.last_rdtsc = curr_rdtsc;
        self.last_recvd = recvd;
        self.last_tput = tput;
        self.last_x = x;
        // grad
        let raw_step = self.lr * delta_tput / delta_x;
        let bounded_step = raw_step.abs().max(self.min_step).min(self.max_step);
        let step = if raw_step > 0.0 {
            bounded_step
        } else {
            -bounded_step
        };
        xinterface.update(step);
        self.msg.clear();
        write!(
            self.msg,
            "tput {} x {} d_tput {} d_x {} raw_step {} step {}",
            tput, x, delta_tput, delta_x, raw_step, step,
        );
    }
}

impl fmt::Display for TputGrad {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

pub struct TputPivot {
    interval: u64,
    last_rdtsc: u64,
    last_recvd: usize,
    last_tput: f64,
    last_x: f64,
    lr: f64,
    min_step: f64,
    max_step: f64,
    pivot: f64,
    range: f64,
    exp: f64,
    msg: String,
}

impl TputPivot {
    pub fn new(
        xloop_factor: u64,
        lr: f64,
        min_step: f64,
        max_step: f64,
        pivot: f64,
        range: f64,
        exp: f64,
    ) -> TputPivot {
        TputPivot {
            interval: CPU_FREQUENCY / xloop_factor,
            last_rdtsc: 0,
            last_recvd: 0,
            last_tput: 0.0,
            last_x: 0.0,
            lr: lr, // 200? should be smaller
            min_step: min_step,
            max_step: max_step,
            pivot: pivot,
            range: range,
            exp: exp,
            msg: String::new(),
        }
    }
    /// xloop
    pub fn xloop(&mut self, xinterface: &impl XInterface<X = f64>, curr_rdtsc: u64, recvd: usize) {
        // recvd % self.factor?
        if curr_rdtsc - self.last_rdtsc > self.interval && recvd > 100 {
            self.update_x(xinterface, curr_rdtsc, recvd);
        }
    }
    /// update x
    pub fn update_x(
        &mut self,
        xinterface: &impl XInterface<X = f64>,
        curr_rdtsc: u64,
        recvd: usize,
    ) {
        let tput = 2.4e6 * (recvd - self.last_recvd) as f64 / (curr_rdtsc - self.last_rdtsc) as f64;
        let delta_tput = tput - self.last_tput;
        let x = xinterface.get();
        let delta_x = x - self.last_x;
        // sync
        self.last_rdtsc = curr_rdtsc;
        self.last_recvd = recvd;
        self.last_tput = tput;
        self.last_x = x;
        // grad
        let raw_step = self.lr * delta_tput / delta_x;
        let mut bounded_step = raw_step.abs().max(self.min_step).min(self.max_step);
        // pivot
        let dist_x = x - self.pivot;
        let mut bounce: f64 = 0.0;
        // approaching pivot with positive gain, accelerate
        // NOTE: if the condition holds, then step has the same sign as bounced step
        if dist_x.abs() > self.range && dist_x * delta_x < 0.0 && delta_tput > 0.0 {
            bounce = dist_x.abs() * (1.0 - self.exp);
            bounded_step = bounded_step.max(bounce);
        }
        let step = if raw_step > 0.0 {
            bounded_step
        } else {
            -bounded_step
        };
        xinterface.update(step);
        self.msg.clear();
        write!(
            self.msg,
            "tput {} x {} d_tput {} d_x {} raw_step {} step {} dist {} bounded_step {} bounce {}",
            tput, x, delta_tput, delta_x, raw_step, step, dist_x, bounded_step, bounce
        );
    }
}

impl fmt::Display for TputPivot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

pub struct QueueGrad {
    interval: u64,
    last_rdtsc: u64,
    last_ql_storage: f64,
    last_ql_compute: f64,
    last_x: f64,
    lr: f64,
    min_step: f64,
    max_step: f64,
    exp: f64,
    msg: String,
}

impl QueueGrad {
    pub fn new(xloop_factor: u64, lr: f64, min_step: f64, max_step: f64, exp: f64) -> QueueGrad {
        QueueGrad {
            interval: CPU_FREQUENCY / xloop_factor,
            last_rdtsc: 0,
            last_ql_storage: 0.0,
            last_ql_compute: 0.0,
            last_x: 0.0,
            lr: lr, // 200?
            min_step: min_step,
            max_step: max_step,
            exp: exp, // 0.5?
            msg: String::new(),
        }
    }
    pub fn ready(&mut self, curr_rdtsc: u64) -> bool {
        curr_rdtsc - self.last_rdtsc > self.interval
    }

    /// xloop
    pub fn xloop(
        &mut self,
        xinterface: &impl XInterface<X = f64>,
        curr_rdtsc: u64,
        ql_storage: f64,
        ql_compute: f64,
    ) {
        // recvd % self.factor?
        if curr_rdtsc - self.last_rdtsc > self.interval {
            self.update_x(xinterface, curr_rdtsc, ql_storage, ql_compute);
        }
    }
    /// update
    pub fn update_x(
        &mut self,
        xinterface: &impl XInterface<X = f64>,
        curr_rdtsc: u64,
        ql_storage: f64,
        ql_compute: f64,
    ) {
        // let last_ql_diff = self.last_ql_storage - self.last_ql_compute;
        let ql_diff = ql_storage - ql_compute;
        let delta_ql_storage = ql_storage - self.last_ql_storage;
        let delta_ql_compute = ql_compute - self.last_ql_compute;
        // let delta_ql_diff = ql_diff - last_ql_diff;
        let x = xinterface.get();
        let delta_x = x - self.last_x;
        // sync
        self.last_rdtsc = curr_rdtsc;
        self.last_ql_storage = ql_storage;
        self.last_ql_compute = ql_compute;
        self.last_x = x;
        // grad
        let mut step: f64 = 0.0;
        let mut raw_step: f64 = 0.0;
        if ql_diff > 0.0 {
            if delta_ql_storage < 0.0 {
                // proceed with grad
                raw_step = self.lr * delta_ql_storage / delta_x;
                let bounded_step = raw_step.abs().max(self.min_step).min(self.max_step);
                step = if raw_step > 0.0 {
                    bounded_step
                } else {
                    -bounded_step
                };
            } else {
                // bounce back
                step = -delta_x * (1.0 - self.exp);
            }
        } else {
            // if delta_ql_compute < 0.0 {
            //     let raw_step = self.lr * delta_ql_compute / delta_x;
            //     let bounded_step = raw_step.abs().max(self.min_step).min(self.max_step);
            //     step = if raw_step > 0.0 {
            //         bounded_step
            //     } else {
            //         -bounded_step
            //     };
            // } else {
            //     // bounce back
            //     step = -delta_x * (1.0 - self.exp);
            // }
        }
        xinterface.update(step);
        self.msg.clear();
        write!(
            self.msg,
            "ql {:?} x {} d_ql {:?} d_x {} step {} raw_step {}",
            (ql_storage, ql_compute),
            x,
            (delta_ql_storage, delta_ql_compute),
            delta_x,
            step,
            raw_step,
        );
    }
}

impl fmt::Display for QueueGrad {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

pub struct Timer {
    pub last: u64,
    pub current: u64,
    interval: u64,
}

impl Timer {
    pub fn new(factor: u64) -> Timer {
        Timer {
            last: rdtsc(),
            current: 0,
            interval: 2400000000 / factor,
        }
    }
    pub fn check_time(&mut self) -> bool {
        self.current = rdtsc();
        self.current - self.last > self.interval
    }
    pub fn elapsed(&self) -> u64 {
        self.current - self.last
    }
    pub fn sync(&mut self) {
        self.last = self.current;
    }
}


*/
