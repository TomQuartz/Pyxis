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

use self::bytes::Bytes;
use db::config::{self, *};
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
// use splinter::sched::TaskManager;
use splinter::*;
use zipf::ZipfDistribution;

use self::atomic_float::AtomicF32;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::RwLock;

use db::dispatch::Queue;
use db::e2d2::common::EmptyMetadata;
use db::e2d2::config::{NetbricksConfiguration, PortConfiguration};
use db::e2d2::headers::UdpHeader;
use db::e2d2::scheduler::NetBricksContext as NetbricksContext;
// use dispatch::{CoeffOfVar, ComputeNodeWorker, Dispatcher, Queue};
use splinter::sched::{ComputeNodeWorker, TaskManager};
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

// #[macro_use]
// extern crate cfg_if;
/*
fn setup_dispatcher(
    config: &config::ComputeConfig,
    ports: Vec<CacheAligned<PortQueue>>,
    scheduler: &mut StandaloneScheduler,
    queue: Arc<Queue>,
    reset_vec: Vec<Arc<AtomicBool>>,
    moving_exp: f64,
) {
    match scheduler.add_task(Dispatcher::new(
        &config.src.ip_addr,
        config.max_rx_packets,
        ports[0].clone(),
        queue,
        reset_vec,
        moving_exp,
    )) {
        Ok(_) => {
            info!(
                "Successfully added compute node dispatcher with rx-tx queue {}.",
                ports[0].rxq()
            );
        }
        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

fn setup_worker(
    config: &config::ComputeConfig,
    ports: Vec<CacheAligned<PortQueue>>,
    sibling: Option<CacheAligned<PortQueue>>,
    scheduler: &mut StandaloneScheduler,
    master: Arc<Master>,
    queue: Arc<Queue>,
    reset: Arc<AtomicBool>,
    // task_duration_cv: Arc<RwLock<CoeffOfVar>>,
    #[cfg(feature = "queue_len")] timestamp: Arc<RwLock<Vec<u64>>>,
    #[cfg(feature = "queue_len")] raw_length: Arc<RwLock<Vec<usize>>>,
    #[cfg(feature = "queue_len")] terminate: Arc<AtomicBool>,
) {
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }
    match scheduler.add_task(ComputeNodeWorker::new(
        config,
        queue,
        reset,
        master,
        ports[0].clone(),
        sibling,
        #[cfg(feature = "queue_len")]
        timestamp,
        #[cfg(feature = "queue_len")]
        raw_length,
        #[cfg(feature = "queue_len")]
        terminate,
    )) {
        Ok(_) => {
            info!(
                "Successfully added compute node worker with rx-tx queue {}.",
                ports[0].rxq()
            );
        }
        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}
*/
fn setup_worker(
    config: &config::ComputeConfig,
    scheduler: &mut StandaloneScheduler,
    ports: Vec<CacheAligned<PortQueue>>,
    sib_port: Option<CacheAligned<PortQueue>>,
    queue: Arc<Queue>,
    sib_queue: Option<Arc<Queue>>,
    reset: Vec<Arc<AtomicBool>>,
    // manager: Arc<TaskManager>,
    master: Arc<Master>,
    id: usize,
) {
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }
    match scheduler.add_task(ComputeNodeWorker::new(
        config,
        ports[0].clone(),
        sib_port,
        queue,
        sib_queue,
        reset,
        // manager,
        master,
        id,
    )) {
        Ok(_) => {
            info!(
                "Successfully added compute node worker with rx-tx queue {:?}.",
                (ports[0].rxq(), ports[0].txq()),
            );
        }
        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

fn main() {
    db::env_logger::init().expect("ERROR: failed to initialize logger!");
    // TODO
    let config: ComputeConfig = config::load("compute.toml");
    warn!("Starting up compute node with config {:?}", config);

    let mut master = Master::new(0, 0, 0);
    // Create tenants with extensions.
    let num_tenants: u32 = 1;
    info!("Populating extension for {} tenants", num_tenants);
    for tenant in 1..(num_tenants + 1) {
        master.load_test(tenant);
    }
    // finished populating, now mark as immut
    let master = Arc::new(master);
    let mut net_context: NetbricksContext = config_and_init_netbricks(&config.compute);
    net_context.start_schedulers();
    // setup shared data
    let rx_queues = config.compute.server.rx_queues as usize;
    let workers_per_port = config.compute.num_cores as usize / rx_queues;
    let mut queues = vec![];
    for _ in 0..rx_queues {
        queues.push(Arc::new(Queue::new(config.compute.max_rx_packets)));
    }
    // let queue = Arc::new(Queue::new(config.max_rx_packets));
    let mut reset_vec = vec![];
    for _ in 0..rx_queues {
        let mut reset = vec![];
        for _ in 0..workers_per_port {
            reset.push(Arc::new(AtomicBool::new(false)));
        }
        reset_vec.push(reset);
    }
    // shared task manager
    // let manager = Arc::new(TaskManager::new(master));
    // setup worker
    for (core_id, &core) in net_context.active_cores.clone().iter().enumerate() {
        let cfg = config.clone();
        let port_id = core_id % rx_queues;
        // alternatively
        // let sib_id = (core_id + 1) % rx_queues;
        let sib_id = (port_id + 1) % rx_queues;
        let cqueue = queues[port_id].clone();
        let csib_queue = if port_id != sib_id {
            Some(queues[sib_id].clone())
        } else {
            None
        };
        let creset = reset_vec[port_id].clone();
        // let cmanager = manager.clone();
        let cmaster = master.clone();
        let worker_id = core_id % (workers_per_port as usize);
        net_context.add_pipeline_to_core(
            core,
            Arc::new(
                move |ports, sib_port, scheduler: &mut StandaloneScheduler| {
                    setup_worker(
                        &cfg.clone(),
                        scheduler,
                        ports,
                        sib_port,
                        cqueue.clone(),
                        csib_queue.clone(),
                        creset.clone(),
                        // cmanager.clone(),
                        cmaster.clone(),
                        worker_id,
                    )
                },
            ),
        );
    }
    std::thread::sleep(std::time::Duration::from_secs(2));
    net_context.execute();
    loop {
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
}

/*
fn main() {
    db::env_logger::init().expect("ERROR: failed to initialize logger!");
    // TODO
    let config: ComputeConfig = config::load("compute.toml");
    warn!("Starting up compute node with config {:?}", config);

    let mut master = Master::new(0, 0, 0);
    // Create tenants with extensions.
    let num_tenants: u32 = 1;
    info!("Populating extension for {} tenants", num_tenants);
    for tenant in 1..(num_tenants + 1) {
        master.load_test(tenant);
    }
    // finished populating, now mark as immut
    let master = Arc::new(master);
    // let queue = Arc::new(RwLock::new(VecDeque::with_capacity(config.max_rx_packets)));
    let queue = Arc::new(Queue::new(config.max_rx_packets));
    // let task_duration_cv = Arc::new(RwLock::new(CoeffOfVar::new()));
    let mut reset_vec = vec![];
    for _ in 0..config.src.num_ports {
        reset_vec.push(Arc::new(AtomicBool::new(false)));
    }

    // Setup Netbricks.
    let mut net_context =
        setup::config_and_init_netbricks(config.nic_pci.clone(), config.src.num_ports);

    // let shared_credits = Arc::new(RwLock::new(config.max_credits));

    // Setup the client pipeline.
    net_context.start_schedulers();
    // setup dispatcher
    let cfg = config.clone();
    let cqueue = queue.clone();
    let creset_vec = reset_vec.clone();
    net_context.add_pipeline_to_core(
        0,
        Arc::new(
            move |ports, scheduler: &mut StandaloneScheduler, _core: i32, _sibling| {
                setup_dispatcher(
                    // &config::load::<config::ComputeConfig>("compute.toml"),
                    &cfg,
                    ports,
                    scheduler,
                    cqueue.clone(),
                    creset_vec.clone(),
                    cfg.moving_exp,
                )
            },
        ),
    );
    // setup worker
    for core_id in 1..=config.src.num_ports {
        let cfg = config.clone();
        let cmaster = master.clone();
        let cqueue = queue.clone();
        // let ctask_duration_cv = task_duration_cv.clone();
        let creset = reset_vec[core_id as usize - 1].clone();
        net_context.add_pipeline_to_core(
            core_id as i32,
            Arc::new(
                move |ports, scheduler: &mut StandaloneScheduler, _core: i32, _sibling| {
                    setup_worker(
                        // &config::load::<config::ComputeConfig>("compute.toml"),
                        &cfg,
                        ports,
                        None,
                        scheduler,
                        cmaster.clone(),
                        cqueue.clone(),
                        // ctask_duration_cv.clone(),
                        creset.clone(),
                    )
                },
            ),
        );
    }
    // #[cfg(feature = "queue_len")]
    // let mut timestamps = vec![];
    // #[cfg(feature = "queue_len")]
    // let mut raw_lengths = vec![];
    // #[cfg(feature = "queue_len")]
    // let mut terminates = vec![];
    // cfg_if! {
    //     if #[cfg(feature = "queue_len")]{
    //         for core_id in 0..config.src.num_ports {
    //             let timestamp = Arc::new(RwLock::new(Vec::with_capacity(12800000 as usize)));
    //             let raw_length = Arc::new(RwLock::new(Vec::with_capacity(12800000 as usize)));
    //             let terminate = Arc::new(AtomicBool::new(false));
    //             timestamps.push(timestamp.clone());
    //             raw_lengths.push(raw_length.clone());
    //             terminates.push(terminate.clone());
    //             let cmaster = master.clone();
    //             net_context.add_pipeline_to_core(
    //                 core_id as i32,
    //                 Arc::new(
    //                     move |ports, scheduler: &mut StandaloneScheduler, _core: i32, _sibling| {
    //                         setup_compute(
    //                             &config::load::<config::ComputeConfig>("compute.toml"),
    //                             ports,
    //                             None,
    //                             scheduler,
    //                             cmaster.clone(),
    //                             timestamp.clone(),
    //                             raw_length.clone(),
    //                             terminate.clone(),
    //                         )
    //                     },
    //                 ),
    //             );
    //         }
    //     }else{
    //         for core_id in 0..config.src.num_ports {
    //             let cmaster = master.clone();
    //             net_context.add_pipeline_to_core(
    //                 core_id as i32,
    //                 Arc::new(
    //                     move |ports, scheduler: &mut StandaloneScheduler, _core: i32, _sibling| {
    //                         setup_compute(
    //                             &config::load::<config::ComputeConfig>("compute.toml"),
    //                             ports,
    //                             None,
    //                             scheduler,
    //                             cmaster.clone(),
    //                         )
    //                     },
    //                 ),
    //             );
    //         }
    //     }
    // }
    // net_context.add_pipeline_to_run(Arc::new(
    //     move |ports, scheduler: &mut StandaloneScheduler, _core: i32, _sibling| {
    //         setup_compute(
    //             &config,
    //             ports,
    //             None,
    //             scheduler,
    //             master.clone(),
    //             // &shared_credits,
    //         )
    //     },
    // ));

    // Allow the system to bootup fully.
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Run the client.
    net_context.execute();

    loop {
        std::thread::sleep(std::time::Duration::from_millis(1000));
        // cfg_if! {
        //     if #[cfg(feature = "queue_len")]{
        //         let mut finished = false;
        //         for terminate in terminates.iter() {
        //             if terminate.load(Ordering::Relaxed) {
        //                 finished = true;
        //                 break;
        //             }
        //         }
        //         if finished{
        //             break;
        //         }
        //     }
        // }
    }
    // #[cfg(feature = "queue_len")]
    // for terminate in terminates.iter() {
    //     terminate.store(true, Ordering::Relaxed);
    // }
    // #[cfg(feature = "queue_len")]
    // std::thread::sleep(std::time::Duration::from_millis(1000));
    // #[cfg(feature = "queue_len")]
    // for (i, (time_vec, ql_vec)) in timestamps.iter().zip(raw_lengths.iter()).enumerate() {
    //     let mut f = File::create(format!("core{}.log", i)).unwrap();
    //     for &t in time_vec.read().unwrap().iter() {
    //         write!(f, "{} ", t);
    //     }
    //     writeln!(f, "");
    //     for &l in ql_vec.read().unwrap().iter() {
    //         write!(f, "{} ", l);
    //     }
    //     println!(
    //         "write queue length of core {}, total {}",
    //         i,
    //         time_vec.read().unwrap().len(),
    //     );
    // }
    // Stop the client.
    // net_context.stop();
}
*/
