/* Copyright (c) 2018 University of Utah
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

extern crate db;
extern crate libc;
extern crate nix;
extern crate spin;
extern crate util;

use std::sync::Arc;
use std::thread::{sleep, spawn};
use std::time::Duration;

use db::log::*;

use db::e2d2::allocators::CacheAligned;
use db::e2d2::config::{NetbricksConfiguration, PortConfiguration};
use db::e2d2::interface::*;
use db::e2d2::native::zcsi;
use db::e2d2::scheduler::Executable;
use db::e2d2::scheduler::NetBricksContext as NetbricksContext;
use db::e2d2::scheduler::*;

use db::config::{self, *};
use db::cycles::*;
use db::dispatch::{Dispatch, FAST_PATH};
use db::install::Installer;
use db::master::Master;
use db::sched::RoundRobin;
use db::task::TaskPriority;

// use spin::RwLock;
use std::sync::RwLock;

use libc::ucontext_t;
use nix::sys::signal;
use util::model::{insert_model, MODEL};

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};

use db::dispatch::{Dispatcher, Queue};
use db::e2d2::common::EmptyMetadata;
use db::e2d2::headers::UdpHeader;
use db::sched::{CoeffOfVar, StorageNodeWorker};
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;

/// Interval in milliseconds at which all schedulers in the system will be scanned for misbehaving
/// tasks.
const SCAN_INTERVAL_MS: u64 = 10;

/// A scheduler is considered compromised if it has not updated it's `latest` timestamp in so many
/// milliseconds.
const MALICIOUS_LIMIT_MS: f64 = 1f64;

/// The identifier of the core that all misbehaving schedulers will be migrated to.
const GHETTO: u64 = 20;

/// A simple wrapper around the scheduler, allowing it to be added to a Netbricks pipeline.
struct Server {
    scheduler: Arc<RoundRobin>,
}

// Implementation of methods on Server.
impl Server {
    /// Creates a Server.
    ///
    /// # Arguments
    ///
    /// * `sched`: A scheduler of tasks in the database. This scheduler must already have a
    ///            `Dispatch` task enqueued on it.
    ///
    /// # Return
    ///
    /// A Server that can be added to a Netbricks pipeline.
    pub fn new(sched: Arc<RoundRobin>) -> Server {
        Server { scheduler: sched }
    }
}

// Implementation of the executable trait, allowing Server to be passed into Netbricks.
impl Executable for Server {
    /// This function is called internally by Netbricks to "execute" the server.
    fn execute(&mut self) {
        self.scheduler.poll();
    }

    /// No clue about what this guy is meant to do.
    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

// fn setup_dispatcher(
//     config: &config::StorageConfig,
//     ports: Vec<CacheAligned<PortQueue>>,
//     scheduler: &mut StandaloneScheduler,
//     queue: Arc<Queue>,
//     // task_duration_cv: Arc<RwLock<CoeffOfVar>>,
//     reset_vec: Vec<Arc<AtomicBool>>,
//     moving_exp: f64,
// ) {
//     match scheduler.add_task(Dispatcher::new(
//         &config.src.ip_addr,
//         config.max_rx_packets,
//         ports[0].clone(),
//         queue,
//         reset_vec,
//         // task_duration_cv,
//         moving_exp,
//     )) {
//         Ok(_) => {
//             info!(
//                 "Successfully added storage node dispatcher with rx-tx queue {}.",
//                 ports[0].rxq()
//             );
//         }
//         Err(ref err) => {
//             error!("Error while adding to Netbricks pipeline {}", err);
//             std::process::exit(1);
//         }
//     }
// }

fn setup_worker(
    config: &config::StorageConfig,
    scheduler: &mut StandaloneScheduler,
    ports: Vec<CacheAligned<PortQueue>>,
    sib_port: Option<CacheAligned<PortQueue>>,
    queue: Arc<Queue>,
    sib_queue: Option<Arc<Queue>>,
    reset: Vec<Arc<AtomicBool>>,
    master: Arc<Master>,
    id: usize,
) {
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }
    match scheduler.add_task(StorageNodeWorker::new(
        config,
        ports[0].clone(),
        sib_port,
        queue,
        sib_queue,
        reset,
        master,
        id,
        // task_duration_cv,
    )) {
        Ok(_) => {
            info!(
                "Successfully added storage node worker with rx-tx queue {}.",
                ports[0].rxq()
            );
        }
        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

// fn setup_worker(
//     config: &config::StorageConfig,
//     ports: Vec<CacheAligned<PortQueue>>,
//     scheduler: &mut StandaloneScheduler,
//     master: Arc<Master>,
//     queue: Arc<Queue>,
//     // task_duration_cv: Arc<RwLock<CoeffOfVar>>,
//     reset: Arc<AtomicBool>,
// ) {
//     if ports.len() != 1 {
//         error!("Client should be configured with exactly 1 port!");
//         std::process::exit(1);
//     }
//     match scheduler.add_task(StorageNodeWorker::new(
//         config,
//         ports[0].clone(),
//         master,
//         queue,
//         reset,
//         // task_duration_cv,
//     )) {
//         Ok(_) => {
//             info!(
//                 "Successfully added storage node worker with rx-tx queue {}.",
//                 ports[0].rxq()
//             );
//         }
//         Err(ref err) => {
//             error!("Error while adding to Netbricks pipeline {}", err);
//             std::process::exit(1);
//         }
//     }
// }
/*
/// This function sets up a Sandstorm server's dispatch thread on top
/// of Netbricks.
fn setup_server<S>(
    config: &config::ServerConfig,
    ports: Vec<CacheAligned<PortQueue>>,
    sibling: CacheAligned<PortQueue>,
    scheduler: &mut S,
    core: i32,
    master: &Arc<Master>,
    handles: &Arc<spin::RwLock<Vec<Arc<RoundRobin>>>>,
) where
    S: Scheduler + Sized,
{
    if ports.len() != 1 {
        error!("Server should be configured with exactly 1 port!");
        std::process::exit(1);
    }

    // Get identifier of the thread this scheduler will run on.
    let tid = unsafe { zcsi::get_thread_id() };

    // Create a dispatcher for the server if needed.
    let sched = Arc::new(RoundRobin::new(tid, core));
    let dispatch = Dispatch::new(
        config,
        ports[0].clone(),
        sibling.clone(),
        Arc::clone(master),
        Arc::clone(&sched),
        ports[0].rxq(),
    );
    sched.enqueue(Box::new(dispatch));

    // Add the scheduler to the passed in `handles` vector.
    handles.write().push(Arc::clone(&sched));

    match config.workload.as_str() {
        // If the workload is ANALYSIS, then updated the thread local static variables for the Model.
        //
        // Why not training it here? - For each misbehaving task the training will take place,
        //                             not recommended.
        //
        // Why not just use the Static Global? - Costly because of Mutex/RwLock and also Static
        //                                       Global does an atomic access.
        "ANALYSIS" => {
            if let Some(a_model) = MODEL.lock().unwrap().get(&String::from("analysis")) {
                insert_model(
                    String::from("analysis"),
                    a_model.lr_serialized.clone(),
                    a_model.dr_serialized.clone(),
                    a_model.rf_serialized.clone(),
                );
            }
        }

        "MIX" => {
            if let Some(a_model) = MODEL.lock().unwrap().get(&String::from("analysis")) {
                insert_model(
                    String::from("analysis"),
                    a_model.lr_serialized.clone(),
                    a_model.dr_serialized.clone(),
                    a_model.rf_serialized.clone(),
                );
            }
        }

        _ => {}
    }

    // Add the server to a netbricks pipeline.
    match scheduler.add_task(Server::new(sched)) {
        Ok(_) => {
            info!(
                "Successfully added scheduler(TID {}) with rx,tx,sibling queues {:?} to core {}.",
                tid,
                (ports[0].rxq(), ports[0].txq(), sibling.rxq()),
                core
            );
        }

        Err(ref err) => {
            error!("Error while adding to Netbricks pipeline {}", err);
            std::process::exit(1);
        }
    }
}

/// Returns a struct of type NetbricksConfiguration which can be used to
/// initialize Netbricks with a default set of parameters.
///
/// If used to initialize Netbricks, this struct will run the parent server
/// thread on core 0, and one scheduler on core 1. Packet buffers will be
/// allocated from a 2 GB memory pool, with 64 MB cached at core 1. DPDK will
/// be initialized as a primary process without any additional arguments. A
/// single network interface/port with 1 transmit queue, 1 receive queue, 256
/// receive descriptors, and 256 transmit descriptors will be made available to
/// Netbricks. Loopback, hardware transmit segementation offload, and hardware
/// checksum offload will be disabled on this port.
fn get_default_netbricks_config(config: &config::ServerConfig) -> NetbricksConfiguration {
    // assert!(
    //     (config.server.num_ports as u32).is_power_of_two(),
    //     "the number of ports should be power of two"
    // );
    assert!(
        config.num_cores % config.server.num_ports == 0,
        "the number of server cores should be multiples of the number of ports"
    );
    // General arguments supplied to netbricks.
    let net_config_name = String::from("storage");
    let dpdk_secondary: bool = false;
    let net_primary_core: i32 = 19;
    let net_queues: Vec<i32> = (0..config.server.num_ports).collect();
    let net_strict_cores: bool = false;
    let net_pool_size: u32 = 16384 - 1;
    let net_cache_size: u32 = 512;
    let net_dpdk_args: Option<String> = None;

    // Port configuration. Required to configure the physical network interface.
    let net_port_name = config.nic_pci.clone();
    let net_port_rx_queues: Vec<i32> = net_queues.clone();
    let net_port_tx_queues: Vec<i32> = net_queues.clone();
    let net_port_rxd: i32 = 8192 / config.server.num_ports;
    let net_port_txd: i32 = 8192 / config.server.num_ports;
    let net_port_loopback: bool = false;
    let net_port_tcp_tso: bool = false;
    let net_port_csum_offload: bool = false;

    let net_port_config = PortConfiguration {
        name: net_port_name,
        rx_queues: net_port_rx_queues,
        tx_queues: net_port_tx_queues,
        rxd: net_port_rxd,
        txd: net_port_txd,
        loopback: net_port_loopback,
        tso: net_port_tcp_tso,
        csum: net_port_csum_offload,
    };

    // The set of ports used by netbricks.
    let net_ports: Vec<PortConfiguration> = vec![net_port_config];
    let net_cores: Vec<i32> = (0..config.num_cores).collect();
    NetbricksConfiguration {
        name: net_config_name,
        secondary: dpdk_secondary,
        primary_core: net_primary_core,
        cores: net_cores,
        strict: net_strict_cores,
        ports: net_ports,
        pool_size: net_pool_size,
        cache_size: net_cache_size,
        dpdk_args: net_dpdk_args,
    }
}

/// This function configures and initializes Netbricks. In the case of a
/// failure, it causes the program to exit.
///
/// Returns a Netbricks context which can be used to setup and start the
/// server/client.
fn config_and_init_netbricks(config: &config::ServerConfig) -> NetbricksContext {
    let net_config: NetbricksConfiguration = get_default_netbricks_config(config);

    // Initialize Netbricks and return a handle.
    match initialize_system(&net_config) {
        Ok(net_context) => {
            return net_context;
        }

        Err(ref err) => {
            error!("Error during Netbricks init: {}", err);
            // TODO: Drop NetbricksConfiguration?
            std::process::exit(1);
        }
    }
}
*/

/// Custom signal handler for stack overflows.
extern "C" fn handle_sigsegv(
    _signum: i32,
    _siginfo: *mut libc::siginfo_t,
    context: *mut libc::c_void,
) {
    // We can't do much as it's different stack all together; swapcontext() might help.
    // Enter into an infinite while loop so that the watchdog catches this thread.
    let ucontext: &mut ucontext_t = unsafe { &mut *(context as *mut ucontext_t) };
    info!("Stack pointer {:p}", ucontext.uc_stack.ss_sp);
    loop {}
}

/// Print server startup information.
fn print_info() {
    if cfg!(feature = "pushback") {
        info!("Push-back for invoke operations is ENABLED");
    } else {
        info!("Push-back for invoke operations is DISABLED");
    }
    if FAST_PATH {
        info!("Fast path for native operations is ENABLED");
    } else {
        info!("Fast path for native operations is DISABLED");
    }
    if cfg!(feature = "ml-model") {
        info!("ML Model generation for invoke operations is ENABLED");
    } else {
        info!("ML Model generation for invoke operations is DISABLED");
    }
    println!("\n");
}

fn main() {
    // First off, install a signal handler to catch stack overflows. On catching a
    // SIGSEGV, we allocate a new stack to the thread to prevent a segmentation fault
    // caused by pushing the signal handler onto the overflowed stack (SA_ONSTACK).
    let sig_action = signal::SigAction::new(
        signal::SigHandler::SigAction(handle_sigsegv),
        signal::SaFlags::SA_ONSTACK,
        signal::SigSet::empty(),
    );

    unsafe {
        let _ret = signal::sigaction(signal::SIGSEGV, &sig_action)
            .expect("Failed to install custom handler for stack overflow.");
    }

    // Basic setup and initialization.
    db::env_logger::init().expect("ERROR: failed to initialize logger!");

    print_info();

    let config: StorageConfig = config::load("storage.toml");
    info!("Starting up storage with config {:?}", config);

    let mut master = Master::new(config.key_len, config.value_len, config.record_len);
    match config.workload.as_str() {
        "PUSHBACK" => {
            info!(
                "Populating PUSHBACK data, {} tenants, {} records/tenant",
                config.num_tenants, config.num_records
            );
            for tenant in 1..(config.num_tenants + 1) {
                master.fill_test(tenant, 1, config.num_records);
                master.load_test(tenant);
            }
        }
        _ => {}
    }
    let master = Arc::new(master);
    let mut net_context: NetbricksContext = config_and_init_netbricks(&config.storage);
    net_context.start_schedulers();
    // setup shared data
    let num_ports = config.storage.server.num_ports;
    let workers_per_port = config.storage.num_cores / num_ports;
    let mut queues = vec![];
    for _ in 0..config.storage.server.num_ports {
        queues.push(Arc::new(Queue::new(config.storage.max_rx_packets)));
    }
    // let queue = Arc::new(Queue::new(config.max_rx_packets));
    let mut reset_vec = vec![];
    for _ in 0..config.storage.server.num_ports {
        let mut reset = vec![];
        for _ in 0..workers_per_port {
            reset.push(Arc::new(AtomicBool::new(false)));
        }
        reset_vec.push(reset);
    }
    // setup worker
    for (core_id, &core) in net_context.active_cores.clone().iter().enumerate() {
        let cfg = config.clone();
        let port_id = core_id % (num_ports as usize);
        let sib_id = (port_id + 1) % (num_ports as usize);
        let cqueue = queues[port_id].clone();
        let csib_queue = Some(queues[sib_id].clone());
        let creset = reset_vec[port_id].clone();
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
                        cmaster.clone(),
                        worker_id,
                    )
                },
            ),
        );
    }
    // // setup dispatcher
    // let cfg = config.clone();
    // let cqueue = queue.clone();
    // // let ctask_duration_cv = task_duration_cv.clone();
    // let creset_vec = reset_vec.clone();
    // net_context.add_pipeline_to_core(
    //     0,
    //     Arc::new(
    //         move |ports, scheduler: &mut StandaloneScheduler, _core: i32, _sibling| {
    //             setup_dispatcher(
    //                 // &config::load::<config::ComputeConfig>("compute.toml"),
    //                 &cfg,
    //                 ports,
    //                 scheduler,
    //                 cqueue.clone(),
    //                 // ctask_duration_cv.clone(),
    //                 creset_vec.clone(),
    //                 cfg.moving_exp,
    //             )
    //         },
    //     ),
    // );
    // // setup worker
    // for core_id in 1..=config.src.num_ports {
    //     let cmaster = master.clone();
    //     let cqueue = queue.clone();
    //     // let ctask_duration_cv = task_duration_cv.clone();
    //     let creset = reset_vec[core_id as usize - 1].clone();
    //     net_context.add_pipeline_to_core(
    //         core_id as i32,
    //         Arc::new(
    //             move |ports, scheduler: &mut StandaloneScheduler, _core: i32, _sibling| {
    //                 setup_worker(
    //                     &config::load::<config::StorageConfig>("storage.toml"),
    //                     ports,
    //                     scheduler,
    //                     cmaster.clone(),
    //                     cqueue.clone(),
    //                     // ctask_duration_cv.clone(),
    //                     creset.clone(),
    //                 )
    //             },
    //         ),
    //     );
    // }
    sleep(std::time::Duration::from_secs(2));
    net_context.execute();
    loop {
        sleep(Duration::from_millis(1000));
    }

    // let config = config::ServerConfig::load();
    // info!("Starting up Sandstorm server with config {:?}", config);

    // // let master = Arc::new(Master::new());
    // let mut master = Master::new(config.key_len, config.value_len, config.record_len);

    // // Create tenants with data and extensions.
    // match config.workload.as_str() {
    //     // "YCSB" => {
    //     //     info!(
    //     //         "Populating YCSB data, {} tenants, {} records/tenant",
    //     //         config.num_tenants, config.num_records
    //     //     );
    //     //     for tenant in 1..(config.num_tenants + 1) {
    //     //         master.fill_test(tenant, 1, config.num_records);
    //     //         master.load_test(tenant);
    //     //     }
    //     // }

    //     // "TAO" => {
    //     //     info!(
    //     //         "Populating TAO data, {} tenants, {} records/tenant",
    //     //         config.num_tenants, config.num_records
    //     //     );
    //     //     for tenant in 1..(config.num_tenants + 1) {
    //     //         master.fill_tao(tenant, config.num_records);
    //     //         master.load_test(tenant);
    //     //     }
    //     // }

    //     // "AGGREGATE" => {
    //     //     info!(
    //     //         "Populating AGGREGATE data, {} tenants, {} records/tenant",
    //     //         config.num_tenants, config.num_records
    //     //     );
    //     //     for tenant in 1..(config.num_tenants + 1) {
    //     //         master.fill_aggregate(tenant, 1, config.num_records);
    //     //         master.load_test(tenant);
    //     //     }
    //     // }

    //     // "ANALYSIS" => {
    //     //     info!(
    //     //         "Populating ANALYSIS data, {} tenants, training dataset/tenant",
    //     //         config.num_tenants
    //     //     );
    //     //     master.fill_analysis(config.num_tenants);
    //     //     for tenant in 1..(config.num_tenants + 1) {
    //     //         master.load_test(tenant);
    //     //     }
    //     //     assert_eq!(cfg!(feature = "ml-model"), true);
    //     // }

    //     // "AUTH" => {
    //     //     info!(
    //     //         "Populating AUTH data, {} tenants, {} records/tenant",
    //     //         config.num_tenants, config.num_records
    //     //     );
    //     //     for tenant in 1..(config.num_tenants + 1) {
    //     //         master.fill_auth(tenant, 1, config.num_records);
    //     //         master.load_test(tenant);
    //     //     }
    //     // }

    //     // "MIX" => {
    //     //     info!("Populating MIX data, {} tenants", config.num_tenants);
    //     //     info!("TAO: {} records/tenants", config.num_records);
    //     //     info!("ANALYSIS: 68000 records/tenant");
    //     //     master.fill_mix(config.num_tenants, config.num_records);
    //     //     for tenant in 1..(config.num_tenants + 1) {
    //     //         master.load_test(tenant);
    //     //     }
    //     //     assert_eq!(cfg!(feature = "ml-model"), true);
    //     // }

    //     // "YCSBT" => {
    //     //     info!(
    //     //         "Populating YCSB-T data, {} tenants, {} records/tenant",
    //     //         config.num_tenants, config.num_records
    //     //     );
    //     //     for tenant in 1..(config.num_tenants + 1) {
    //     //         master.fill_ycsb(tenant, 1, config.num_records);
    //     //         master.load_test(tenant);
    //     //     }
    //     // }

    //     // "CHECKSUM" => {
    //     //     info!(
    //     //         "Populating CHECKSUM data, {} tenants, {} records/tenant",
    //     //         config.num_tenants, config.num_records
    //     //     );
    //     //     for tenant in 1..(config.num_tenants + 1) {
    //     //         master.fill_aggregate(tenant, 1, config.num_records);
    //     //         master.load_test(tenant);
    //     //     }
    //     // }
    //     "PUSHBACK" => {
    //         info!(
    //             "Populating PUSHBACK data, {} tenants, {} records/tenant",
    //             config.num_tenants, config.num_records
    //         );
    //         for tenant in 1..(config.num_tenants + 1) {
    //             master.fill_test(tenant, 1, config.num_records);
    //             master.load_test(tenant);
    //         }
    //     }

    //     _ => {
    //         info!("Populating SANITY data for tenant 100");
    //         master.fill_test(100, 100, 0);
    //         master.load_test(100);
    //     }
    // }
    // // finished adding tenant and ext, wrap into immut arc
    // let master = Arc::new(master);

    // // Setup Netbricks.
    // let mut net_context: NetbricksContext = config_and_init_netbricks(&config);

    // // A handle to every scheduler for pre-emption.
    // let handles = Arc::new(RwLock::new(Vec::with_capacity(config.num_cores as usize)));

    // // Clone `master` and `handle` so that they are still around after the schedulers are
    // // initialized.
    // let cmaster = Arc::clone(&master);
    // let chandle = Arc::clone(&handles);

    // // Copy out the network address that install() RPCs will be received on.
    // let install_addr = config.install_addr.clone();

    // // Setup the server pipeline.
    // net_context.start_schedulers();
    // net_context.add_pipeline_to_run(Arc::new(
    //     move |ports, scheduler: &mut StandaloneScheduler, core: i32, sibling| {
    //         setup_server(&config, ports, sibling, scheduler, core, &cmaster, &chandle)
    //     },
    // ));

    // // // Create a thread to handle the install() RPC request.
    // // let imaster = Arc::clone(&master);
    // // let _install = spawn(move || {
    // //     // Pin to the ghetto core.
    // //     let tid = unsafe { zcsi::get_thread_id() };
    // //     unsafe { zcsi::set_affinity(tid, GHETTO) };

    // //     // Run the installer.
    // //     let mut installer = Installer::new(imaster, install_addr);
    // //     installer.execute();
    // // });

    // // Run the server, and give it some time to bootup.
    // net_context.execute();
    // sleep(Duration::from_millis(1000));

    // // Convert to cycles.
    // let limit = (MALICIOUS_LIMIT_MS / 1000f64) * (cycles_per_second() as f64);

    // // Check for misbehaving tasks here.
    // loop {
    //     // Scan schedulers every few milliseconds.
    //     sleep(Duration::from_millis(SCAN_INTERVAL_MS));
    //     #[cfg(feature = "queue_len")]
    //     let mut finished = false;
    //     #[cfg(feature = "queue_len")]
    //     for sched in handles.write().iter_mut() {
    //         if sched.terminate.load(Ordering::Relaxed) {
    //             finished = true;
    //             break;
    //         }
    //     }
    //     #[cfg(feature = "queue_len")]
    //     if finished {
    //         break;
    //     }
    //     /*
    //     for sched in handles.write().iter_mut() {
    //         // Get the current time stamp to compare scheduler time stamps against.
    //         let current = rdtsc();

    //         // Get the latest timestamp at which the scheduler executed.
    //         let latest = sched.latest();

    //         // If the scheduler executed after `current` was measured, continue checking others.
    //         if latest > current {
    //             continue;
    //         }

    //         // If this scheduler executed less than "MALICIOUS_LIMIT_MS" milliseconds before, then
    //         // continue checking others.
    //         if current - latest < limit as u64 {
    //             continue;
    //         }

    //         let tid = sched.thread();
    //         let core = sched.core();
    //         warn!(
    //             "Detected misbehaving task {} on core {}. Current: {}, Latest: {}, Cycles/s {}",
    //             tid,
    //             core,
    //             current,
    //             latest,
    //             cycles_per_second(),
    //         );

    //         // There might be an uncooperative task on this scheduler. Dequeue it's tasks and any
    //         // pending response packets.
    //         let mut tasks = sched.dequeue_all();
    //         let mut resps = sched.responses();

    //         // Retain only non-dispatch tasks.
    //         tasks.retain(|task| task.priority() != TaskPriority::DISPATCH);

    //         // Set the compromised flag on the scheduler and then migrate it. Stop the scheduler.
    //         sched.compromised();
    //         unsafe { zcsi::set_affinity(tid, GHETTO) };
    //         net_context.stop_core(core);

    //         // Create and setup a new scheduler on the core.
    //         let temp = Arc::new(RwLock::new(Vec::with_capacity(1)));
    //         let cmaster = Arc::clone(&master);
    //         let ctemp = Arc::clone(&temp);
    //         net_context.start_scheduler(core);
    //         let _res = net_context.add_pipeline_to_core(
    //             core,
    //             Arc::new(
    //                 move |ports, scheduler: &mut StandaloneScheduler, core: i32, sibling| {
    //                     setup_server(
    //                         &config::ServerConfig::load(),
    //                         ports,
    //                         sibling,
    //                         scheduler,
    //                         core,
    //                         &cmaster,
    //                         &ctemp,
    //                     )
    //                 },
    //             ),
    //         );

    //         // Start the new scheduler, and give it some time to boot.
    //         net_context.execute_core(core);

    //         // Wait for the new scheduler to be created.
    //         while temp.read().len() == 0 {}

    //         // Enqueue all tasks and response packets from the previous scheduler.
    //         let new = temp
    //             .write()
    //             .pop()
    //             .expect("Failed to retrieve added scheduler.");
    //         *sched = new;
    //         sched.enqueue_many(tasks);
    //         sched.append_resps(&mut resps);
    //     }
    //     */
    // }
    // #[cfg(feature = "queue_len")]
    // for sched in handles.write().iter_mut() {
    //     sched.terminate.store(true, Ordering::Relaxed);
    // }
    // #[cfg(feature = "queue_len")]
    // sleep(Duration::from_millis(1000));
    // #[cfg(feature = "queue_len")]
    // for (i, sched) in handles.write().iter_mut().enumerate() {
    //     let mut f = File::create(format!("core{}.log", i)).unwrap();
    //     for &t in sched.timestamp.borrow().iter() {
    //         write!(f, "{} ", t);
    //     }
    //     writeln!(f, "");
    //     for &l in sched.raw_length.borrow().iter() {
    //         write!(f, "{} ", l);
    //     }
    //     println!(
    //         "write queue length of core {}, total {}",
    //         i,
    //         sched.timestamp.borrow().len()
    //     );
    // }
    // // Stop the server.
    // // net_context.stop();
    // // _install.join();
}
