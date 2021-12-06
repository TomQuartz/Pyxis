use db::cycles::rdtsc;
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::fs::File;
use std::io::Write as writef;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

const CPU_FREQUENCY: u64 = 2400000000;

pub trait XInterface {
    type X;
    // &mut self?
    // bounce back here
    fn update(&self, delta_x: Self::X);
    fn get(&self) -> Self::X;
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

pub struct ServerLoad {
    cluster_name: String,
    ip2load: HashMap<u32, RwLock<MovingTimeAvg>>,
    // ip2load: HashMap<u32, RwLock<Avg>>,
    #[cfg(feature = "server_stats")]
    ip2trace: HashMap<u32, RwLock<Vec<(usize, (u64, (f64, f64)))>>>,
}

impl ServerLoad {
    pub fn new(
        cluster_name: &str,
        ip_and_ports: Vec<(&String, i32)>,
        moving_exp: f64,
    ) -> ServerLoad {
        let mut ip2load = HashMap::new();
        for &(ip, num_ports) in &ip_and_ports {
            // let mut server_load = vec![];
            // for _ in 0..num_ports {
            //     server_load.push(RwLock::new(MovingAvg::new(exp_decay)));
            // }
            let ip = u32::from(Ipv4Addr::from_str(ip).unwrap());
            ip2load.insert(ip, RwLock::new(MovingTimeAvg::new(moving_exp)));
            // ip2load.insert(ip, RwLock::new(Avg::new()));
        }
        #[cfg(feature = "server_stats")]
        let mut ip2trace = HashMap::new();
        #[cfg(feature = "server_stats")]
        for &(ip, num_ports) in &ip_and_ports {
            // let mut server_trace = vec![];
            // for _ in 0..num_ports {
            //     server_trace.push(RwLock::new(vec![]));
            // }
            let ip = u32::from(Ipv4Addr::from_str(ip).unwrap());
            ip2trace.insert(ip, RwLock::new(Vec::with_capacity(128000)));
        }
        ServerLoad {
            cluster_name: cluster_name.to_string(),
            ip2load: ip2load,
            #[cfg(feature = "server_stats")]
            ip2trace: ip2trace,
        }
    }
    pub fn update(
        &self,
        src_ip: u32,
        src_port: u16,
        curr_rdtsc: u64,
        delta: f64,
    ) -> Result<(), ()> {
        if let Some(server_load) = self.ip2load.get(&src_ip) {
            server_load
                .write()
                .unwrap()
                // TODO: reimpl update of moving_avg to (moving) time avg
                .update(curr_rdtsc, delta);
            Ok(())
        } else {
            Err(())
        }
    }
    #[cfg(feature = "server_stats")]
    pub fn update_trace(
        &self,
        src_ip: u32,
        src_port: u16,
        curr_rdtsc: u64,
        server_ql: f64,
        task_duration_cv: f64,
        id: usize,
    ) {
        if let Some(server_trace) = self.ip2trace.get(&src_ip) {
            server_trace
                .write()
                .unwrap()
                .push((id, (curr_rdtsc, (server_ql, task_duration_cv))));
        }
    }
    pub fn avg_server(&self, ip: u32) -> f64 {
        let server_load = self.ip2load.get(&ip).unwrap();
        // let num_cores = server_load.len() as f64;
        // let mut total = 0f64;
        // for core_load in server_load.iter() {
        //     total += core_load.read().unwrap().avg();
        // }
        // total / num_cores
        server_load.read().unwrap().avg()
    }
    pub fn avg_all(&self) -> f64 {
        let mut total = 0f64;
        let num_servers = self.ip2load.len() as f64;
        for &ip in self.ip2load.keys() {
            total += self.avg_server(ip);
        }
        total / num_servers
    }
    pub fn reset(&self) {
        for &ip in self.ip2load.keys() {
            let server_load = self.ip2load.get(&ip).unwrap();
            server_load.write().unwrap().reset();
        }
    }
    #[cfg(feature = "server_stats")]
    pub fn print_trace(&self) {
        let mut ql_total: f64 = 0.0;
        let mut ql_total_std: f64 = 0.0;
        let mut task_cv_total: f64 = 0.0;
        let mut task_cv_total_std: f64 = 0.0;
        let mut task_cv_total_median: f64 = 0.0;
        for &ip in self.ip2trace.keys() {
            let mut server_trace = self.ip2trace.get(&ip).unwrap().write().unwrap();
            let num_records = server_trace.len();
            let mut duration: u64 = 0;
            let mut ql_mean: f64 = 0.0;
            let mut ql_std: f64 = 0.0;
            let mut task_cv_mean: f64 = 0.0;
            let mut task_cv_std: f64 = 0.0;
            let mut task_cv_median: f64 = 0.0;
            if num_records > 0 {
                let start: u64 = server_trace[0].1 .0;
                let end: u64 = server_trace[num_records - 1].1 .0;
                duration = end - start;
                ql_mean = server_trace.iter().map(|x| x.1 .1 .0).sum::<f64>() / num_records as f64;
                ql_std = (server_trace
                    .iter()
                    .map(|x| (x.1 .1 .0 - ql_mean) * (x.1 .1 .0 - ql_mean))
                    .sum::<f64>()
                    / num_records as f64)
                    .sqrt();
                server_trace.sort_by(|a, b| a.1 .1 .1.partial_cmp(&b.1 .1 .1).unwrap());
                task_cv_median = server_trace[num_records / 2].1 .1 .1;
                task_cv_mean =
                    server_trace.iter().map(|x| x.1 .1 .1).sum::<f64>() / num_records as f64;
                task_cv_std = (server_trace
                    .iter()
                    .map(|x| (x.1 .1 .1 - task_cv_mean) * (x.1 .1 .1 - task_cv_mean))
                    .sum::<f64>()
                    / num_records as f64)
                    .sqrt();
            }
            ql_total += ql_mean;
            ql_total_std += ql_std;
            task_cv_total += task_cv_mean;
            task_cv_total_std += task_cv_std;
            task_cv_total_median += task_cv_median;
            println!(
                "cluster {} ip {} ql_mean {:.2} ql_std {:.2} task_cv_mean {:.2} task_cv_std {:.2} task_cv_median {:.2} duration {} total {} records",
                self.cluster_name,
                ip,
                ql_mean,
                ql_std,
                task_cv_mean,
                task_cv_std,
                task_cv_median,
                duration / 2400,
                num_records,
            )
        }
        let num_servers = self.ip2trace.keys().len() as f64;
        let ql_mean = ql_total / num_servers;
        let ql_std = ql_total_std / num_servers;
        let task_cv_mean = task_cv_total / num_servers;
        let task_cv_std = task_cv_total_std / num_servers;
        let task_cv_median = task_cv_total_median / num_servers;
        println!(
            "{} summary ql_mean {:.2} ql_std {:.2} task_cv_mean {:.2} task_cv_std {:.2} task_cv_median {:.2}",
            self.cluster_name, ql_mean, ql_std, task_cv_mean, task_cv_std, task_cv_median,
        );
    }
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
#[cfg(feature = "server_stats")]
impl fmt::Display for ServerLoad {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "#######{}#######", self.cluster_name)?;
        for &ip in self.ip2load.keys() {
            let server_load = self.ip2load.get(&ip).unwrap().read().unwrap();
            writeln!(f, "ip {} load {}", ip, server_load)?;
            // writeln!(f, "ip {}", ip)?;
            // let server_load = self.ip2load.get(&ip).unwrap();
            // // let mut server_total = 0f64;
            // // let mut server_total_moving = 0f64;
            // // let num_cores = server_load.len() as f64;
            // for (i, core_load) in server_load.iter().enumerate() {
            //     let core_load = core_load.read().unwrap();
            //     // server_total += core_load.avg.E_x;
            //     // server_total_moving += core_load.avg();
            //     writeln!(f, "    core {} {}", i, *core_load)?;
            // }
            // // writeln!(f,"    ip {} avg {} moving {}", ip,server_total/num_cores,server_total_moving/num_cores)?;
        }
        Ok(())
    }
}

struct MovingTimeAvg {
    moving_avg: f64,
    elapsed: f64,
    latest: f64,
    last_time: u64,
    moving_exp: f64,
    // #[cfg(feature = "queue_len")]
    // avg: Avg,
}
impl MovingTimeAvg {
    fn new(moving_exp: f64) -> MovingTimeAvg {
        MovingTimeAvg {
            moving_avg: 0.0,
            elapsed: 0.0,
            latest: 0.0,
            last_time: 0,
            moving_exp: moving_exp,
            // #[cfg(feature = "queue_len")]
            // avg: Avg::new(),
        }
    }
    // reset after xloop changes parition
    fn reset(&mut self) {
        self.moving_avg = 0.0;
        self.elapsed = 0.0;
    }
    fn update(&mut self, current_time: u64, new: f64) {
        let elapsed = (current_time - self.last_time) as f64;
        self.elapsed = self.elapsed * self.moving_exp + elapsed;
        let update_ratio = elapsed / self.elapsed;
        let interval_avg = (self.latest + new) / 2.0;
        self.moving_avg = self.moving_avg * (1.0 - update_ratio) + interval_avg * update_ratio;
        // sync
        self.last_time = current_time;
        self.latest = new;
        // #[cfg(feature = "queue_len")]
        // self.avg.update(delta_avg);
    }
    fn avg(&self) -> f64 {
        self.moving_avg
    }
}
impl fmt::Display for MovingTimeAvg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "moving {:.2} elapsed {:.2} latest {:.2}",
            self.moving_avg, self.elapsed, self.latest
        )
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
    /// update
    pub fn update_x(&mut self, xinterface: &impl XInterface<X = f64>, curr_rdtsc: u64) {
        let ql_storage = self.storage_load.avg_all();
        let ql_compute = self.compute_load.avg_all();
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
            "x {} ql {:?} d_ql {:?} d_x {} step {} range {:?} lr {}",
            x,
            (ql_storage, ql_compute),
            (delta_ql_storage, delta_ql_compute),
            delta_x,
            step,
            (self.lowerbound, self.upperbound),
            self.lr,
        );
    }
}

impl fmt::Display for QueueSweep {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

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

/*
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
