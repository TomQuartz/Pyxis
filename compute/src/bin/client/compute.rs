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

use self::atomic_float::AtomicF32;
use std::sync::atomic::{AtomicUsize, Ordering};

use dispatch::ComputeNodeDispatcher;

fn setup_compute(
    config: &config::ComputeConfig,
    ports: Vec<CacheAligned<PortQueue>>,
    sibling: Option<CacheAligned<PortQueue>>,
    scheduler: &mut StandaloneScheduler,
    master: Arc<Master>,
) {
    if ports.len() != 1 {
        error!("Client should be configured with exactly 1 port!");
        std::process::exit(1);
    }
    match scheduler.add_task(ComputeNodeDispatcher::new(
        config,
        master,
        ports[0].clone(),
        sibling,
        32,
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

fn main() {
    db::env_logger::init().expect("ERROR: failed to initialize logger!");
    // TODO
    let config: config::ComputeConfig = config::load("compute.toml");
    warn!("Starting up compute node with config {:?}", config);

    let mut master = Master::new();
    // Create tenants with extensions.
    let num_tenants: u32 = 1;
    info!("Populating extension for {} tenants", num_tenants);
    for tenant in 1..(num_tenants + 1) {
        master.load_test(tenant);
    }
    // finished populating, now mark as immut
    let master = Arc::new(master);

    // Setup Netbricks.
    let mut net_context =
        setup::config_and_init_netbricks(config.nic_pci.clone(), config.src.num_ports);

    // Setup the client pipeline.
    net_context.start_schedulers();
    net_context.add_pipeline_to_run(Arc::new(
        move |ports, scheduler: &mut StandaloneScheduler, _core: i32, _sibling| {
            setup_compute(&config, ports, None, scheduler, master.clone())
        },
    ));

    // Allow the system to bootup fully.
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Run the client.
    net_context.execute();

    // Stop the client.
    // net_context.stop();
}
