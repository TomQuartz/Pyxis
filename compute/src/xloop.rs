use db::cycles::rdtsc;
use std::fmt::{self, Write};

// pub struct Timer {
//     pub last: u64,
//     pub current: u64,
//     interval: u64,
// }

// impl Timer {
//     pub fn new(factor: u64) -> Timer {
//         Timer {
//             last: rdtsc(),
//             current: 0,
//             interval: 2400000000 / factor,
//         }
//     }
//     pub fn check_time(&mut self) -> bool {
//         self.current = rdtsc();
//         self.current - self.last > self.interval
//     }
//     pub fn elapsed(&self) -> u64 {
//         self.current - self.last
//     }
//     pub fn sync(&mut self) {
//         self.last = self.current;
//     }
// }

const CPU_FREQUENCY: u64 = 2400000000;

pub trait XInterface {
    type X;
    // &mut self?
    // bounce back here
    fn update(&self, delta_x: Self::X);
    fn get(&self) -> Self::X;
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
