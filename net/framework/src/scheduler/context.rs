use allocators::CacheAligned;
use config::NetbricksConfiguration;
use interface::dpdk::{init_system, init_thread};
use interface::{PmdPort, PortQueue, VirtualPort, VirtualQueue};
use scheduler::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle, Thread};

type AlignedPortQueue = CacheAligned<PortQueue>;
type AlignedVirtualQueue = CacheAligned<VirtualQueue>;

/// A handle to schedulers paused on a barrier.
pub struct BarrierHandle<'a> {
    threads: Vec<&'a Thread>,
}

impl<'a> BarrierHandle<'a> {
    /// Release all threads. This consumes the handle as expected.
    pub fn release(self) {
        for thread in &self.threads {
            thread.unpark();
        }
    }

    /// Allocate a new BarrierHandle with threads.
    pub fn with_threads(threads: Vec<&'a Thread>) -> BarrierHandle {
        BarrierHandle { threads: threads }
    }
}

/// `NetBricksContext` contains handles to all schedulers, and provides mechanisms for coordination.
#[derive(Default)]
pub struct NetBricksContext {
    pub ports: HashMap<String, Arc<PmdPort>>,
    pub core2queues: HashMap<i32, Vec<CacheAligned<PortQueue>>>,
    pub active_cores: Vec<i32>,
    pub siblings: HashMap<i32, CacheAligned<PortQueue>>,
    pub virtual_ports: HashMap<i32, Arc<VirtualPort>>,
    scheduler_channels: HashMap<i32, SyncSender<SchedulerCommand>>,
    scheduler_handles: HashMap<i32, JoinHandle<()>>,
}

impl NetBricksContext {
    /// Boot up all schedulers.
    pub fn start_schedulers(&mut self) {
        let cores = self.active_cores.clone();
        for core in &cores {
            self.start_scheduler(*core);
        }
    }

    #[inline]
    pub fn start_scheduler(&mut self, core: i32) {
        let builder = thread::Builder::new();
        let (sender, receiver) = sync_channel(8);
        self.scheduler_channels.insert(core, sender);
        let join_handle = builder
            .name(format!("sched-{}", core).into())
            .spawn(move || {
                init_thread(core, core);
                // Other init?
                let mut sched = StandaloneScheduler::new_with_channel(receiver);
                sched.handle_requests()
            })
            .unwrap();
        self.scheduler_handles.insert(core, join_handle);
    }

    /// Run a function (which installs a pipeline) on all schedulers in the system.
    pub fn add_pipeline_to_run<T>(&mut self, run: Arc<T>)
    where
        T: Fn(Vec<AlignedPortQueue>, Option<AlignedPortQueue>, &mut StandaloneScheduler) + Send + Sync + 'static,
        // T: Fn(Vec<AlignedPortQueue>, &mut StandaloneScheduler, i32, AlignedPortQueue) + Send + Sync + 'static,
    {
        for (core, channel) in &self.scheduler_channels {
            let ports = match self.core2queues.get(core) {
                Some(v) => v.clone(),
                None => vec![],
            };
            // let id = *core;
            let sibling = match self.siblings.get(core) {
                Some(v) => Some(v.clone()),
                None => None,
            };
            let boxed_run = run.clone();
            channel
                .send(SchedulerCommand::Run(Arc::new(move |s| {
                    boxed_run(ports.clone(), sibling.clone(), s)
                    // boxed_run(ports.clone(), s, id, sibling.clone())
                })))
                .unwrap();
        }
    }

    pub fn add_test_pipeline<T>(&mut self, run: Arc<T>)
    where
        T: Fn(Vec<AlignedVirtualQueue>, &mut StandaloneScheduler) + Send + Sync + 'static,
    {
        for (core, channel) in &self.scheduler_channels {
            let port = self.virtual_ports.entry(*core).or_insert(VirtualPort::new(1).unwrap());
            let boxed_run = run.clone();
            let queue = port.new_virtual_queue(1).unwrap();
            channel
                .send(SchedulerCommand::Run(Arc::new(move |s| {
                    boxed_run(vec![queue.clone()], s)
                })))
                .unwrap();
        }
    }

    pub fn add_test_pipeline_to_core<
        T: Fn(Vec<AlignedVirtualQueue>, &mut StandaloneScheduler) + Send + Sync + 'static,
    >(
        &mut self,
        core: i32,
        run: Arc<T>,
    ) -> Result<()> {
        if let Some(channel) = self.scheduler_channels.get(&core) {
            let port = self.virtual_ports.entry(core).or_insert(VirtualPort::new(1).unwrap());
            let boxed_run = run.clone();
            let queue = port.new_virtual_queue(1).unwrap();
            channel
                .send(SchedulerCommand::Run(Arc::new(move |s| {
                    boxed_run(vec![queue.clone()], s)
                })))
                .unwrap();
            Ok(())
        } else {
            Err(ErrorKind::NoRunningSchedulerOnCore(core).into())
        }
    }

    /// Install a pipeline on a particular core.
    pub fn add_pipeline_to_core<
        T: Fn(Vec<AlignedPortQueue>, Option<AlignedPortQueue>, &mut StandaloneScheduler) + Send + Sync + 'static,
        // T: Fn(Vec<AlignedPortQueue>, &mut StandaloneScheduler, i32, AlignedPortQueue) + Send + Sync + 'static,
    >(
        &mut self,
        core: i32,
        run: Arc<T>,
    ) -> Result<()> {
        if let Some(channel) = self.scheduler_channels.get(&core) {
            // NOTE: for now, only one core is configured
            let ports = match self.core2queues.get(&core) {
                Some(v) => v.clone(),
                None => vec![],
            };
            let sibling = match self.siblings.get(&core) {
                Some(v) => Some(v.clone()),
                None => None,
            };
            let boxed_run = run.clone();
            channel
                .send(SchedulerCommand::Run(Arc::new(move |s| {
                    boxed_run(ports.clone(), sibling.clone(), s)
                    // boxed_run(ports.clone(), s, core, sibling.clone())
                })))
                .unwrap();
            Ok(())
        } else {
            Err(ErrorKind::NoRunningSchedulerOnCore(core).into())
        }
    }

    /// Start scheduling pipelines.
    pub fn execute(&mut self) {
        for (_core, channel) in &self.scheduler_channels {
            channel.send(SchedulerCommand::Execute).unwrap();
            // println!("Starting scheduler on {}", core);
        }
    }

    pub fn execute_core(&mut self, core: i32) {
        let channel = self.scheduler_channels.get(&core).unwrap();
        channel.send(SchedulerCommand::Execute).unwrap();
    }

    /// Pause all schedulers, the returned `BarrierHandle` can be used to resume.
    pub fn barrier(&mut self) -> BarrierHandle {
        // TODO: If this becomes a problem, move this to the struct itself; but make sure to fix `stop` appropriately.
        let channels: Vec<_> = self.scheduler_handles.iter().map(|_| sync_channel(0)).collect();
        let receivers = channels.iter().map(|&(_, ref r)| r);
        let senders = channels.iter().map(|&(ref s, _)| s);
        for ((_, channel), sender) in self.scheduler_channels.iter().zip(senders) {
            channel.send(SchedulerCommand::Handshake(sender.clone())).unwrap();
        }
        for receiver in receivers {
            receiver.recv().unwrap();
        }
        BarrierHandle::with_threads(self.scheduler_handles.values().map(|j| j.thread()).collect())
    }

    /// Stop all schedulers, safely shutting down the system.
    pub fn stop(&mut self) {
        for (_core, channel) in &self.scheduler_channels {
            channel.send(SchedulerCommand::Shutdown).unwrap();
            // println!("Issued shutdown for core {}", core);
        }
        for (_core, join_handle) in self.scheduler_handles.drain() {
            join_handle.join().unwrap();
            // println!("Core {} has shutdown", core);
        }
        // println!("System shutdown");
    }

    pub fn stop_core(&mut self, core: i32) {
        let channel = self.scheduler_channels.get(&core).unwrap();
        channel.send(SchedulerCommand::Shutdown).unwrap();
    }

    pub fn wait(&mut self) {
        for (_core, join_handle) in self.scheduler_handles.drain() {
            join_handle.join().unwrap();
            // println!("Core {} has shutdown", core);
        }
        // println!("System shutdown");
    }

    /// Shutdown all schedulers.
    pub fn shutdown(&mut self) {
        self.stop()
    }
}
/// Initialize the system from a configuration.
pub fn initialize_system(configuration: &NetbricksConfiguration) -> Result<NetBricksContext> {
    init_system(configuration);
    let mut ctx: NetBricksContext = Default::default();
    ctx.active_cores = configuration.cores.clone();
    // let mut cores: HashSet<_> = configuration.cores.iter().cloned().collect();
    assert_eq!(
        configuration.ports.len(),
        1,
        "only a single port configuration is supported for now"
    );
    let port = &configuration.ports[0];
    if ctx.ports.contains_key(&port.name) {
        println!("Port {} appears twice in specification", port.name);
        return Err(ErrorKind::ConfigurationError(format!("Port {} appears twice in specification", port.name)).into());
    } else {
        match PmdPort::new_port_from_configuration(port) {
            Ok(p) => {
                ctx.ports.insert(port.name.clone(), p);
            }
            Err(e) => {
                return Err(ErrorKind::ConfigurationError(format!(
                    "Port {} could not be initialized {:?}",
                    port.name, e
                ))
                .into())
            }
        }

        let port_instance = &ctx.ports[&port.name];
        let nrxq = port.rx_queues.len();
        let ntxq = port.tx_queues.len();
        for (core_id, &core) in configuration.cores.iter().enumerate() {
            let rx_q = port.rx_queues[core_id % nrxq];
            let tx_q = port.tx_queues[core_id % ntxq];
            match PmdPort::new_queue_pair(port_instance, rx_q, tx_q) {
                Ok(q) => {
                    ctx.core2queues.entry(core).or_insert_with(|| vec![]).push(q);
                }
                Err(e) => {
                    return Err(ErrorKind::ConfigurationError(format!(
                        "port {} queue {:?} on core {} could not be \
                            initialized {:?}",
                        port.name,
                        (rx_q, tx_q),
                        core,
                        e
                    ))
                    .into())
                }
            }
        }
        let num_cores = ctx.active_cores.len();
        if nrxq > 1 {
            for core_id in 0..num_cores {
                let sib_id = (core_id + 1) % num_cores;
                let sibling = ctx.core2queues.get(&ctx.active_cores[sib_id]).unwrap()[0].clone();
                ctx.siblings.insert(ctx.active_cores[core_id], sibling);
            }
        }
    }
    Ok(ctx)
}
/*
/// Initialize the system from a configuration.
pub fn initialize_system(configuration: &NetbricksConfiguration) -> Result<NetBricksContext> {
    init_system(configuration);
    let mut ctx: NetBricksContext = Default::default();
    // let mut cores: HashSet<_> = configuration.cores.iter().cloned().collect();
    for port in &configuration.ports {
        if ctx.ports.contains_key(&port.name) {
            println!("Port {} appears twice in specification", port.name);
            return Err(
                ErrorKind::ConfigurationError(format!("Port {} appears twice in specification", port.name)).into(),
            );
        } else {
            match PmdPort::new_port_from_configuration(port) {
                Ok(p) => {
                    ctx.ports.insert(port.name.clone(), p);
                }
                Err(e) => {
                    return Err(ErrorKind::ConfigurationError(format!(
                        "Port {} could not be initialized {:?}",
                        port.name, e
                    ))
                    .into())
                }
            }

            let port_instance = &ctx.ports[&port.name];
            let nrxq = port.rx_queues.len();
            let ntxq = port.tx_queues.len();
            for (core_id, &core) in configuration.cores.iter().enumerate() {
                let rx_q = port.rx_queues[core_id % nrxq];
                let tx_q = port.tx_queues[core_id % ntxq];
                match PmdPort::new_queue_pair(port_instance, rx_q, tx_q) {
                    Ok(q) => {
                        // ctx.core2queues.entry(core).or_insert_with(|| vec![]).push(q);
                        ctx.core2queues.entry(core).or_insert(Some(q));
                    }
                    Err(e) => {
                        return Err(ErrorKind::ConfigurationError(format!(
                            "port {} queue {:?} on core {} could not be \
                                initialized {:?}",
                            port.name,
                            (rx_q, tx_q),
                            core,
                            e
                        ))
                        .into())
                    }
                }
            }
            // for (rx_q, core) in port.rx_queues.iter().enumerate() {
            //     let rx_q = rx_q as i32;
            //     match PmdPort::new_queue_pair(port_instance, rx_q, rx_q) {
            //         Ok(q) => {
            //             // ctx.rx_queues.entry(*core).or_insert_with(|| vec![]).push(q);
            //         }
            //         Err(e) => {
            //             return Err(ErrorKind::ConfigurationError(format!(
            //                 "Queue {} on port {} could not be \
            //                  initialized {:?}",
            //                 rx_q, port.name, e
            //             ))
            //             .into())
            //         }
            //     }
            // }
        }
    }
    // if configuration.strict {
    //     let other_cores: HashSet<_> = ctx.rx_queues.keys().cloned().collect();
    //     let core_diff: Vec<_> = other_cores.difference(&cores).map(|c| c.to_string()).collect();
    //     if !core_diff.is_empty() {
    //         let missing_str = core_diff.join(", ");
    //         return Err(ErrorKind::ConfigurationError(format!(
    //             "Strict configuration selected but core(s) {} appear \
    //              in port configuration but not in cores",
    //             missing_str
    //         ))
    //         .into());
    //     }
    // } else {
    //     cores.extend(ctx.rx_queues.keys());
    // };
    // ctx.active_cores = cores.into_iter().collect();
    // ctx.active_cores.sort();
    ctx.active_cores = configuration.cores.clone();
    // Populate every core's sibling receive queue.
    let num_cores = ctx.active_cores.len();
    for core_id in 0..num_cores {
        let sib_id =
        let rxq = ctx.core2queues.get(&ctx.active_cores[sib_id]).unwrap();
        let sib_rxq = (core_id + 1) % num_cores;
        let sibling = if sib_id != core_id {
            Some(ctx.core2queues.get(&ctx.active_cores[sib_id]).unwrap()[0].clone())
        } else {
            None
        };
        ctx.siblings.insert(ctx.active_cores[core_id], sibling);
        // let sibling = ctx.core2queues.get(&ctx.active_cores[sib_id]).unwrap();
        // ctx.siblings.insert(ctx.active_cores[core_id], sibling[0].clone());
    }

    // // Populate every core's sibling receive queue.
    // for idx in 0..(ctx.active_cores.len() - 1) {
    //     // A core's sibling is it's neighbour.
    //     let sibling = ctx.rx_queues.get(&ctx.active_cores[idx + 1]).unwrap();
    //     ctx.siblings.insert(ctx.active_cores[idx], sibling[0].clone());
    // }

    // // The last core's sibling is the first core's receive queue.
    // {
    //     let last = ctx.active_cores.len() - 1;
    //     let sibling = ctx.rx_queues.get(&ctx.active_cores[0]).unwrap();
    //     ctx.siblings.insert(ctx.active_cores[last], sibling[0].clone());
    // }

    Ok(ctx)
}
*/
