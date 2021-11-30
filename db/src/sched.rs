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

use atomic_float::AtomicF64;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

use super::cycles;
use super::rpc;
use super::task::Task;
use super::task::TaskPriority;
use super::task::TaskState::*;

use e2d2::common::EmptyMetadata;
use e2d2::headers::*;
use e2d2::interface::new_packet;
use wireformat;
use wireformat::*;

use spin::RwLock;
// use std::sync::RwLock;

// use super::dispatch::Resp;

use e2d2::allocators::CacheAligned;
use e2d2::interface::{PacketTx, PortQueue};
use std::rc::Rc;

use config::{self, NetConfig, StorageConfig};
use e2d2::interface::*;
use e2d2::scheduler::Executable;
use master::Master;
use rpc::*;
use sandstorm::common;
use service::Service;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;

// /// The number of resp packets to send in one go
// const TX_PACKETS_THRESH: usize = 8;

// /// The maximum interval between two resp packets transmission, 8us
// const TX_CYCLES_THRESH: u64 = 2400 * 8;

/// The maximum number of tasks the dispatcher can take in one go.
const MAX_RX_PACKETS: usize = 32;

/// Interval in microsecond which each task can use as credit to perform CPU work.
/// Under load shedding, the task which used more than this credit will be pushed-back.
const CREDIT_LIMIT_US: f64 = 0.5f64;

// #[cfg(feature = "queue_len")]
// struct Avg {
//     counter: f64,
//     lastest: f64,
//     E_x: f64,
//     E_x2: f64,
// }
// #[cfg(feature = "queue_len")]
// impl Avg {
//     fn new() -> Avg {
//         Avg {
//             counter: 0.0,
//             lastest: 0.0,
//             E_x: 0.0,
//             E_x2: 0.0,
//         }
//     }
//     fn update(&mut self, delta: f64) {
//         self.counter += 1.0;
//         self.lastest = delta;
//         self.E_x = self.E_x * ((self.counter - 1.0) / self.counter) + delta / self.counter;
//         self.E_x2 =
//             self.E_x2 * ((self.counter - 1.0) / self.counter) + delta * delta / self.counter;
//     }
// }

struct PacketHeaders {
    /// The UDP header that will be appended to every response packet (cached
    /// here to avoid wasting time creating a new one for every response
    /// packet).
    udp_header: UdpHeader,

    /// The IP header that will be appended to every response packet (cached
    /// here to avoid creating a new one for every response packet).
    ip_header: IpHeader,

    /// The MAC header that will be appended to every response packet (cached
    /// here to avoid creating a new one for every response packet).
    mac_header: MacHeader,
}

impl PacketHeaders {
    fn new(src: &NetConfig, dst: &NetConfig) -> PacketHeaders {
        // Create a common udp header for response packets.
        let mut udp_header: UdpHeader = UdpHeader::new();
        udp_header.set_length(common::PACKET_UDP_LEN);
        udp_header.set_checksum(common::PACKET_UDP_CHECKSUM);
        udp_header.set_src_port(common::DEFAULT_UDP_PORT);
        udp_header.set_dst_port(common::DEFAULT_UDP_PORT); // will be changed by the rng of sender
                                                           // Create a common ip header for response packets.
        let mut ip_header: IpHeader = IpHeader::new();
        ip_header.set_ttl(common::PACKET_IP_TTL);
        ip_header.set_version(common::PACKET_IP_VER);
        ip_header.set_ihl(common::PACKET_IP_IHL);
        ip_header.set_length(common::PACKET_IP_LEN);
        ip_header.set_protocol(0x11);
        if let Ok(ip_src_addr) = Ipv4Addr::from_str(&src.ip_addr) {
            ip_header.set_src(u32::from(ip_src_addr));
        }
        if let Ok(ip_dst_addr) = Ipv4Addr::from_str(&dst.ip_addr) {
            ip_header.set_dst(u32::from(ip_dst_addr));
        }

        // Create a common mac header for response packets.
        let mut mac_header: MacHeader = MacHeader::new();
        mac_header.set_etype(common::PACKET_ETYPE);
        if let Ok(mac_src_addr) = config::parse_mac(&src.mac_addr) {
            mac_header.src = mac_src_addr;
        }
        if let Ok(mac_dst_addr) = config::parse_mac(&dst.mac_addr) {
            mac_header.dst = mac_dst_addr;
        }
        PacketHeaders {
            udp_header: udp_header,
            ip_header: ip_header,
            mac_header: mac_header,
        }
    }
    fn create_hdr(src: &NetConfig, src_udp_port: u16, dst: &NetConfig) -> PacketHeaders {
        let mut pkthdr = PacketHeaders::new(src, dst);
        // we need to fix the src port for req_hdrs, which is used as the dst port in resp_hdr at rpc endpoint
        pkthdr.udp_header.set_src_port(src_udp_port);
        pkthdr
    }
    fn create_hdrs(
        src: &NetConfig,
        src_udp_port: u16,
        endpoints: &Vec<NetConfig>,
    ) -> Vec<PacketHeaders> {
        let mut pkthdrs = vec![];
        for dst in endpoints.iter() {
            pkthdrs.push(Self::create_hdr(src, src_udp_port, dst));
        }
        pkthdrs
    }
}

struct Receiver {
    // The network interface over which responses will be received from.
    net_port: CacheAligned<PortQueue>,
    // Sibling port
    sib_port: Option<CacheAligned<PortQueue>>,
    // stealing
    stealing: bool,
    // The maximum number of packets that can be received from the network port in one shot.
    max_rx_packets: usize,
    // ip_addr
    ip_addr: u32,
    // // The total number of responses received.
    // responses_recv: Cell<u64>,
}

// Implementation of methods on Receiver.
impl Receiver {
    pub fn new(
        net_port: CacheAligned<PortQueue>,
        max_rx_packets: usize,
        ip_addr: &str,
    ) -> Receiver {
        Receiver {
            net_port: net_port,
            sib_port: None,
            stealing: false,
            max_rx_packets: max_rx_packets,
            ip_addr: u32::from(Ipv4Addr::from_str(ip_addr).expect("missing src ip for LB")),
        }
    }
    // TODO: makesure src ip is properly set
    // return pkt and (src ip, src udp port)
    pub fn recv(&self) -> Option<Vec<(Packet<UdpHeader, EmptyMetadata>, (u32, u16))>> {
        // Allocate a vector of mutable MBuf pointers into which raw packets will be received.
        let mut mbuf_vector = Vec::with_capacity(self.max_rx_packets);

        // This unsafe block is needed in order to populate mbuf_vector with a bunch of pointers,
        // and subsequently manipulate these pointers. DPDK will take care of assigning these to
        // actual MBuf's.
        unsafe {
            mbuf_vector.set_len(self.max_rx_packets);

            // Try to receive packets from the network port.
            let mut recvd = self
                .net_port
                .recv(&mut mbuf_vector[..])
                .expect("Error on packet receive") as usize;

            // Return if packets weren't available for receive.
            if recvd == 0 && self.stealing {
                recvd = self
                    .sib_port
                    .as_ref()
                    .unwrap()
                    .recv(&mut mbuf_vector[..])
                    .expect("Error on packet receive") as usize;
            }
            if recvd == 0 {
                return None;
            }
            trace!("port {} recv {} raw pkts", self.net_port.rxq(), recvd);
            // // Update the number of responses received.
            // let r = self.responses_recv.get();
            // // if r & 0xffffff == 0 {
            // //     info!("Received many responses...");
            // // }
            // self.responses_recv.set(r + 1);

            // Clear out any dangling pointers in mbuf_vector.
            mbuf_vector.drain(recvd..self.max_rx_packets);

            // Vector to hold packets parsed from mbufs.
            let mut packets = Vec::with_capacity(mbuf_vector.len());

            // Wrap up the received Mbuf's into Packets. The refcount on the mbuf's were set by
            // DPDK, and do not need to be bumped up here. Hence, the call to
            // packet_from_mbuf_no_increment().
            for mbuf in mbuf_vector.iter_mut() {
                let packet = packet_from_mbuf_no_increment(*mbuf, 0);
                if let Some(packet) = self.check_mac(packet) {
                    if let Some((packet, src_ip)) = self.check_ip(packet) {
                        if let Some((packet, src_port)) = self.check_udp(packet) {
                            packets.push((packet, (src_ip, src_port)));
                            continue;
                        }
                    }
                }
            }

            return Some(packets);
        }
    }

    #[inline]
    fn check_mac(
        &self,
        packet: Packet<NullHeader, EmptyMetadata>,
    ) -> Option<Packet<MacHeader, EmptyMetadata>> {
        let packet = packet.parse_header::<MacHeader>();
        // The following block borrows the MAC header from the parsed
        // packet, and checks if the ethertype on it matches what the
        // server expects.
        if packet.get_header().etype().eq(&common::PACKET_ETYPE) {
            Some(packet)
        } else {
            trace!("mac check failed");
            packet.free_packet();
            None
        }
    }

    #[inline]
    fn check_ip(
        &self,
        packet: Packet<MacHeader, EmptyMetadata>,
    ) -> Option<(Packet<IpHeader, EmptyMetadata>, u32)> {
        let packet = packet.parse_header::<IpHeader>();
        // The following block borrows the Ip header from the parsed
        // packet, and checks if it is valid. A packet is considered
        // valid if:
        //      - It is an IPv4 packet,
        //      - It's TTL (time to live) is greater than zero,
        //      - It is not long enough,
        //      - It's destination Ip address matches that of the server.
        const MIN_LENGTH_IP: u16 = common::PACKET_IP_LEN + 2;
        let ip_header: &IpHeader = packet.get_header();
        if (ip_header.version() == 4)
            && (ip_header.ttl() > 0)
            && (ip_header.length() >= MIN_LENGTH_IP)
            && (ip_header.dst() == self.ip_addr)
        {
            let src_ip = ip_header.src();
            trace!("recv from ip {}", src_ip);
            Some((packet, src_ip))
        } else {
            trace!(
                "ip check failed, dst {} local {}",
                ip_header.dst(),
                self.ip_addr
            );
            packet.free_packet();
            None
        }
    }

    #[inline]
    fn check_udp(
        &self,
        packet: Packet<IpHeader, EmptyMetadata>,
    ) -> Option<(Packet<UdpHeader, EmptyMetadata>, u16)> {
        let packet = packet.parse_header::<UdpHeader>();
        // This block borrows the UDP header from the parsed packet, and
        // checks if it is valid. A packet is considered valid if:
        //      - It is not long enough,
        const MIN_LENGTH_UDP: u16 = common::PACKET_UDP_LEN + 2;
        let udp_header: &UdpHeader = packet.get_header();
        if udp_header.length() >= MIN_LENGTH_UDP {
            let src_port = udp_header.src_port();
            Some((packet, src_port))
        } else {
            trace!("udp check failed");
            packet.free_packet();
            None
        }
    }
}

struct TimeAvg {
    time_avg: f64,
    elapsed: f64,
    last_data: f64,
    last_time: u64,
    // #[cfg(feature = "queue_len")]
    // avg: Avg,
}

impl TimeAvg {
    fn new() -> TimeAvg {
        TimeAvg {
            time_avg: 0.0,
            elapsed: 0.0,
            last_data: 0.0,
            last_time: 0,
            // #[cfg(feature = "queue_len")]
            // avg: Avg::new(),
        }
    }
    fn update(&mut self, current_time: u64, delta: f64) {
        let elapsed = (current_time - self.last_time) as f64;
        self.last_time = current_time;
        let delta_avg = (self.last_data + delta) / 2.0;
        self.last_data = delta;
        self.elapsed += elapsed;
        let update_ratio = elapsed / self.elapsed;
        self.time_avg = self.time_avg * (1.0 - update_ratio) + delta_avg * update_ratio;
        // #[cfg(feature = "queue_len")]
        // self.avg.update(delta_avg);
    }
    fn avg(&self) -> f64 {
        self.time_avg
    }
}

struct MovingTimeAvg {
    moving_avg: f64,
    elapsed: f64,
    last_data: f64,
    last_time: u64,
    norm: f64,
    moving_exp: f64,
    // #[cfg(feature = "queue_len")]
    // avg: Avg,
}
impl MovingTimeAvg {
    fn new(moving_exp: f64) -> MovingTimeAvg {
        MovingTimeAvg {
            moving_avg: 0.0,
            elapsed: 0.0,
            last_data: 0.0,
            last_time: 0,
            norm: 0.0,
            moving_exp: moving_exp,
            // #[cfg(feature = "queue_len")]
            // avg: Avg::new(),
        }
    }
    fn update(&mut self, current_time: u64, delta: f64) {
        let elapsed = (current_time - self.last_time) as f64;
        self.last_time = current_time;
        let delta_avg = (self.last_data + delta) / 2.0;
        self.last_data = delta;
        self.elapsed = self.elapsed * self.moving_exp + elapsed;
        // self.norm = self.norm * (1.0 - self.moving_exp) + self.moving_exp;
        let update_ratio = elapsed / self.elapsed;
        self.moving_avg = self.moving_avg * (1.0 - update_ratio) + delta_avg * update_ratio;
        // #[cfg(feature = "queue_len")]
        // self.avg.update(delta_avg);
    }
    fn avg(&self) -> f64 {
        self.moving_avg
    }
}

pub struct Queue {
    pub queue: std::sync::RwLock<VecDeque<Packet<UdpHeader, EmptyMetadata>>>,
    /// length is set by Dispatcher, and cleared by the first worker in that round
    // LB will only update those with positive queue length, thus reducing update frequency and variance
    pub length: AtomicF64,
}
impl Queue {
    pub fn new(capacity: usize) -> Queue {
        Queue {
            queue: std::sync::RwLock::new(VecDeque::with_capacity(capacity)),
            length: AtomicF64::new(-1.0),
        }
    }
}

pub struct Dispatcher {
    receiver: Receiver,
    queue: Arc<Queue>,
    time_avg: MovingTimeAvg,
}

impl Dispatcher {
    pub fn new(
        // config: &ComputeConfig,
        ip_addr: &str,
        max_rx_packets: usize,
        net_port: CacheAligned<PortQueue>,
        queue: Arc<Queue>,
        moving_exp: f64,
    ) -> Dispatcher {
        Dispatcher {
            receiver: Receiver::new(net_port, max_rx_packets, ip_addr),
            queue: queue,
            time_avg: MovingTimeAvg::new(moving_exp),
            // queue: RwLock::new(VecDeque::with_capacity(config.max_rx_packets)),
        }
    }
    pub fn recv(&mut self) {
        if let Some(mut packets) = self.receiver.recv() {
            trace!("dispatcher recv {} packets", packets.len());
            if packets.len() > 0 {
                let current_time = cycles::rdtsc();
                let mut queue = self.queue.queue.write().unwrap();
                // TODO: smooth queue length here
                // let ql = queue.len();
                while let Some((packet, _)) = packets.pop() {
                    queue.push_back(packet);
                }
                let queue_len = queue.len() as f64;
                self.time_avg.update(current_time, queue_len);
                self.queue
                    .length
                    .store(self.time_avg.avg(), Ordering::Relaxed);
            }
        }
    }
    /// get a packet from common queue
    pub fn poll(&self) -> Option<Packet<UdpHeader, EmptyMetadata>> {
        self.queue.queue.write().unwrap().pop_front()
    }
}
impl Executable for Dispatcher {
    fn execute(&mut self) {
        self.recv();
    }
    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

struct TaskManager {
    master_service: Arc<Master>,
    resp_hdr: PacketHeaders,
    // pub ready: VecDeque<Box<Task>>,
    responses: Vec<Packet<IpHeader, EmptyMetadata>>,
}
impl TaskManager {
    fn new(master_service: Arc<Master>, resp_hdr: PacketHeaders) -> TaskManager {
        TaskManager {
            master_service: master_service,
            resp_hdr: resp_hdr,
            // ready: VecDeque::with_capacity(32),
            responses: vec![],
        }
    }
    fn create_response(
        &self,
        request: &Packet<UdpHeader, EmptyMetadata>,
    ) -> Option<Packet<UdpHeader, EmptyMetadata>> {
        if let Some(response) = new_packet() {
            let mut response = response
                .push_header(&self.resp_hdr.mac_header)
                .expect("ERROR: Failed to add response MAC header")
                .push_header(&self.resp_hdr.ip_header)
                .expect("ERROR: Failed to add response IP header")
                .push_header(&self.resp_hdr.udp_header)
                .expect("ERROR: Failed to add response UDP header");

            // Set the destination port on the response UDP header.
            // response
            //     .get_mut_header()
            //     .set_src_port(request.get_header().dst_port());
            response
                .get_mut_header()
                .set_dst_port(request.get_header().src_port());
            Some(response)
        } else {
            warn!("Failed to allocate packet for response");
            None
        }
    }
    fn create_task(&mut self, mut request: Packet<UdpHeader, EmptyMetadata>) -> Option<Box<Task>> {
        let ip = request.deparse_header(common::IP_HDR_LEN);
        // self.resp_hdr.ip_header.set_src(ip.get_header().dst());
        self.resp_hdr.ip_header.set_dst(ip.get_header().src());

        // Set the destination mac address on the response MAC header.
        let mac = ip.deparse_header(common::MAC_HDR_LEN);
        // self.resp_hdr.mac_header.set_src(mac.get_header().dst());
        self.resp_hdr.mac_header.set_dst(mac.get_header().src());

        let request = mac.parse_header::<IpHeader>().parse_header::<UdpHeader>();
        if parse_rpc_service(&request) == wireformat::Service::MasterService {
            match parse_rpc_opcode(&request) {
                OpCode::SandstormInvokeRpc => {
                    let response = self.create_response(&request).unwrap();
                    match self.master_service.dispatch_invoke(request, response) {
                        Ok(task) => Some(task), // self.ready.push_back(task),
                        Err((req, res)) => {
                            // Master returned an error. The allocated request and response packets
                            // need to be freed up.
                            req.free_packet();
                            res.free_packet();
                            None
                        }
                    }
                }
                OpCode::SandstormGetRpc => {
                    let num_responses =
                        self.master_service.value_len / self.master_service.record_len;
                    let mut responses = vec![];
                    for _ in 0..num_responses {
                        responses.push(self.create_response(&request).unwrap());
                    }
                    match self.master_service.get(request, responses) {
                        Ok(task) => Some(task), // self.ready.push_back(task),
                        Err((req, resps)) => {
                            // Master returned an error. The allocated request and response packets
                            // need to be freed up.
                            warn!("failed to dispatch req");
                            req.free_packet();
                            for resp in resps.into_iter() {
                                resp.free_packet();
                            }
                            None
                        }
                    }
                }
                // #[cfg(feature = "queue_len")]
                // OpCode::TerminateRpc => {
                //     self.scheduler.terminate.store(true, Ordering::Relaxed);
                //     request.free_packet();
                // }
                _ => {
                    request.free_packet();
                    None
                }
            }
        } else {
            None
        }
    }
    // TODO: profile overhead in dispatch
    fn run_task(&mut self, mut task: Box<Task>, mut queue_len: f64) {
        if task.run().0 == COMPLETED {
            // The task finished execution, check for request and response packets. If they
            // exist, then free the request packet, and enqueue the response packet.
            if let Some((req, resps)) = unsafe { task.tear(&mut queue_len) } {
                trace!("task complete");
                req.free_packet();
                for resp in resps.into_iter() {
                    self.responses.push(rpc::fixup_header_length_fields(resp));
                }
                // self.responses
                //     .write()
                //     .push(rpc::fixup_header_length_fields(res));
            }
        }
    }
}

pub struct StorageNodeWorker {
    net_port: CacheAligned<PortQueue>,
    queue: Arc<Queue>,
    manager: TaskManager,
}
impl StorageNodeWorker {
    pub fn new(
        config: &StorageConfig,
        net_port: CacheAligned<PortQueue>,
        masterservice: Arc<Master>,
        queue: Arc<Queue>,
    ) -> StorageNodeWorker {
        let resp_hdr =
            PacketHeaders::create_hdr(&config.src, net_port.txq() as u16, &NetConfig::default());
        StorageNodeWorker {
            net_port: net_port,
            queue: queue,
            manager: TaskManager::new(masterservice, resp_hdr),
        }
    }
    fn send_response(&mut self) {
        let responses = &mut self.manager.responses;
        unsafe {
            let mut mbufs = vec![];
            let num_packets = responses.len();
            let mut to_send = num_packets;

            // Extract Mbuf's from the batch of packets.
            while let Some(packet) = responses.pop() {
                mbufs.push(packet.get_mbuf());
            }

            // Send out the above MBuf's.
            loop {
                let sent = self.net_port.send(&mut mbufs).unwrap() as usize;
                // if sent == 0 {
                //     warn!(
                //         "Was able to send only {} of {} packets.",
                //         num_packets - to_send,
                //         num_packets
                //     );
                //     break;
                // }
                if sent < to_send {
                    // warn!("Was able to send only {} of {} packets.", sent, num_packets);
                    to_send -= sent;
                    mbufs.drain(0..sent as usize);
                } else {
                    break;
                }
            }
        }
    }
}

impl Executable for StorageNodeWorker {
    fn execute(&mut self) {
        // let (request, queue_len) = {
        //     let mut queue = self.queue.write().unwrap();
        //     let queue_len = queue.len();
        //     let request = queue.pop_front();
        //     (request, queue_len)
        // };
        let request = self.queue.queue.write().unwrap().pop_front();
        if let Some(request) = request {
            if let Some(task) = self.manager.create_task(request) {
                // TODO: add dispatch overhead to task time
                // TODO: smooth queue_len here
                let queue_len = self.queue.length.swap(-1.0, Ordering::Relaxed) as f64;
                self.manager.run_task(task, queue_len);
                self.send_response();
            }
        }
    }
    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

/// A simple round robin scheduler for Tasks in Sandstorm.
pub struct RoundRobin {
    // The time-stamp at which the scheduler last ran. Required to identify whether there is an
    // uncooperative task running on the scheduler.
    latest: AtomicUsize,

    // Atomic flag indicating whether there is a malicious/long running procedure on this
    // scheduler. If true, the scheduler must return down to Netbricks on the next call to poll().
    compromised: AtomicBool,

    // Identifier of the thread this scheduler is running on. Required for pre-emption.
    thread: AtomicUsize,

    // Identifier of the core this scheduler is running on. Required for pre-emption.
    core: AtomicIsize,

    // Run-queue of tasks waiting to execute. Tasks on this queue have either yielded, or have been
    // recently enqueued and never run before.
    waiting: RwLock<VecDeque<Box<Task>>>,

    // Response packets returned by completed tasks. Will be picked up and sent out the network by
    // the Dispatch task.
    responses: RwLock<Vec<Packet<IpHeader, EmptyMetadata>>>,

    // /// last tx
    // pub last_tx: Cell<u64>,

    // task_completed is incremented after the completion of each task. Reset to zero
    // after every 1M tasks.
    task_completed: RefCell<u64>,
    // // the dispatcher in this roundrobin(FCFS) scheduler
    // dispatcher_port: CacheAligned<PortQueue>,
    queue_length: RefCell<TimeAvg>,
    #[cfg(feature = "queue_len")]
    pub timestamp: RefCell<Vec<u64>>,
    #[cfg(feature = "queue_len")]
    pub raw_length: RefCell<Vec<usize>>,
    #[cfg(feature = "queue_len")]
    pub terminate: AtomicBool,
}

// Implementation of methods on RoundRobin.
impl RoundRobin {
    /// Creates and returns a round-robin scheduler that can run tasks implementing the `Task`
    /// trait.
    ///
    /// # Arguments
    ///
    /// * `thread`: Identifier of the thread this scheduler will run on.
    /// * `core`:   Identifier of the core this scheduler will run on.
    pub fn new(thread: u64, core: i32) -> RoundRobin {
        RoundRobin {
            latest: AtomicUsize::new(cycles::rdtsc() as usize),
            compromised: AtomicBool::new(false),
            thread: AtomicUsize::new(thread as usize),
            core: AtomicIsize::new(core as isize),
            waiting: RwLock::new(VecDeque::new()),
            responses: RwLock::new(Vec::new()),
            task_completed: RefCell::new(0),
            // dispatcher_port: portq.clone(),
            // last_tx: Cell::new(cycles::rdtsc()),
            queue_length: RefCell::new(TimeAvg::new()),
            #[cfg(feature = "queue_len")]
            timestamp: RefCell::new(Vec::with_capacity(12800000 as usize)),
            #[cfg(feature = "queue_len")]
            raw_length: RefCell::new(Vec::with_capacity(12800000 as usize)),
            #[cfg(feature = "queue_len")]
            terminate: AtomicBool::new(false),
        }
    }

    /// Enqueues a task onto the scheduler. The task is enqueued at the end of the schedulers
    /// queue.
    ///
    /// # Arguments
    ///
    /// * `task`: The task to be added to the scheduler. Must implement the `Task` trait.
    #[inline]
    pub fn enqueue(&self, task: Box<Task>) {
        self.waiting.write().push_back(task);
    }

    /// Enqueues multiple tasks onto the scheduler.
    ///
    /// # Arguments
    ///
    /// * `tasks`: A deque of tasks to be added to the scheduler. These tasks will be run in the
    ///            order that they are provided in, and must implement the `Task` trait.
    #[inline]
    pub fn enqueue_many(&self, mut tasks: VecDeque<Box<Task>>) {
        self.waiting.write().append(&mut tasks);
    }

    /// Dequeues all waiting tasks from the scheduler.
    ///
    /// # Return
    ///
    /// A deque of all waiting tasks in the scheduler. This tasks might be in various stages of
    /// execution. Some might have run for a while and yielded, and some might have never run
    /// before. If there are no tasks waiting to run, then an empty vector is returned.
    #[inline]
    pub fn dequeue_all(&self) -> VecDeque<Box<Task>> {
        let mut tasks = self.waiting.write();
        return tasks.drain(..).collect();
    }

    /// Returns a list of pending response packets.
    ///
    /// # Return
    ///
    /// A vector of response packets that were returned by tasks that completed execution. This
    /// packets should be sent out the network. If there are no pending responses, then an empty
    /// vector is returned.
    #[inline]
    pub fn responses(&self) -> Vec<Packet<IpHeader, EmptyMetadata>> {
        let mut responses = self.responses.write();
        return responses.drain(..).collect();
    }

    // #[inline]
    // fn pending_resps(&self) -> Option<Vec<Packet<IpHeader, EmptyMetadata>>> {
    //     let pending = self.responses.read().len();
    //     if pending >= TX_PACKETS_THRESH {
    //         self.last_tx.set(cycles::rdtsc());
    //         Some(self.responses.write().drain(..).collect())
    //     } else if pending > 0 {
    //         let now = cycles::rdtsc();
    //         if now - self.last_tx.get() > TX_CYCLES_THRESH {
    //             self.last_tx.set(now);
    //             Some(self.responses.write().drain(..).collect())
    //         } else {
    //             None
    //         }
    //     } else {
    //         None
    //     }
    // }

    // // no statistics, so that we may use &self instead of mut self
    // fn send_resps(&self, mut packets: Vec<Packet<IpHeader, EmptyMetadata>>) {
    //     unsafe {
    //         let mut mbufs = vec![];
    //         let num_packets = packets.len();

    //         // Extract Mbuf's from the batch of packets.
    //         while let Some(packet) = packets.pop() {
    //             mbufs.push(packet.get_mbuf());
    //         }

    //         // Send out the above MBuf's.
    //         match self.dispatcher_port.send(&mut mbufs) {
    //             Ok(sent) => {
    //                 if sent < num_packets as u32 {
    //                     warn!("Was able to send only {} of {} packets.", sent, num_packets);
    //                 }

    //                 // self.responses_sent += mbufs.len() as u64;
    //             }

    //             Err(ref err) => {
    //                 error!("Error on packet send: {}", err);
    //             }
    //         }
    //     }
    // }

    /// Appends a list of responses to the scheduler.
    ///
    /// # Arguments
    ///
    /// * `resps`: A vector of response packets parsed upto their IP headers.
    pub fn append_resps(&self, resps: &mut Vec<Packet<IpHeader, EmptyMetadata>>) {
        self.responses.write().append(resps);
    }

    /// Returns the time-stamp at which the latest scheduling decision was made.
    #[inline]
    pub fn latest(&self) -> u64 {
        self.latest.load(Ordering::Relaxed) as u64
    }

    /// Sets the compromised flag on the scheduler.
    #[inline]
    pub fn compromised(&self) {
        self.compromised.store(true, Ordering::Relaxed);
    }

    /// Returns the identifier of the thread this scheduler was configured to run on.
    #[inline]
    pub fn thread(&self) -> u64 {
        self.thread.load(Ordering::Relaxed) as u64
    }

    /// Returns the identifier of the core this scheduler was configured to run on.
    #[inline]
    pub fn core(&self) -> i32 {
        self.core.load(Ordering::Relaxed) as i32
    }

    /// Picks up a task from the waiting queue, and runs it until it either yields or completes.
    pub fn poll(&self) {
        let mut total_time: u64 = 0;
        let mut db_time: u64 = 0;
        let credit = (CREDIT_LIMIT_US / 1000000f64) * (cycles::cycles_per_second() as f64);
        let mut overhead_per_req: u64 = 0;

        // XXX: Trigger Pushback if the two dispatcher invocation is 20 us apart.
        let time_trigger: u64 = 2000 * credit as u64;
        let mut previous: u64 = cycles::rdtsc();
        let mut interval: u64 = 0;
        let mut avg_queue_len: f64 = -1.0;
        loop {
            // Set the time-stamp of the latest scheduling decision.
            let current = cycles::rdtsc();
            self.latest.store(current as usize, Ordering::Relaxed);

            // If the compromised flag was set, then return.
            if self.compromised.load(Ordering::Relaxed) {
                return;
            }

            #[cfg(feature = "queue_len")]
            // set by dispatcher upon recving terminate rpc
            if self.terminate.load(Ordering::Relaxed) {
                return;
            }

            // If there are tasks to run, then pick one from the head of the queue, and run it until it
            // either completes or yields back.
            let task = self.waiting.write().pop_front();
            // TODO: run dispatch first, profile dispatch time, and evenly distribute to all tasks with state=INITIALIZED
            if let Some(mut task) = task {
                let mut is_dispatcher: bool = false;
                let mut difference: u64 = 0;
                match task.priority() {
                    TaskPriority::DISPATCH => {
                        is_dispatcher = true;

                        // The time difference include the dispatcher time to account the native
                        // operations.
                        difference = current - previous;
                        interval = difference;
                        previous = current;
                    }

                    _ => {}
                }

                if is_dispatcher {
                    overhead_per_req = task.run().1;
                    // update queue length
                    let current_time = cycles::rdtsc();
                    let queue_length = self.waiting.read().len();
                    self.queue_length
                        .borrow_mut()
                        .update(current_time, queue_length as f64);
                    avg_queue_len = self.queue_length.borrow().avg();
                    self.waiting.write().push_back(task);
                    #[cfg(feature = "queue_len")]
                    if queue_length > 0 || self.timestamp.borrow().len() > 0 {
                        self.timestamp.borrow_mut().push(current_time);
                        // #[cfg(feature = "queue_len")]
                        self.raw_length.borrow_mut().push(queue_length);
                    }
                    continue;
                }
                // handle requests
                if task.state() == INITIALIZED {
                    task.set_time(overhead_per_req);
                }
                if task.run().0 == COMPLETED {
                    // The task finished execution, check for request and response packets. If they
                    // exist, then free the request packet, and enqueue the response packet.
                    if let Some((req, resps)) = unsafe { task.tear(&mut avg_queue_len) } {
                        trace!("task complete");
                        req.free_packet();
                        for resp in resps.into_iter() {
                            self.responses
                                .write()
                                .push(rpc::fixup_header_length_fields(resp));
                        }
                        // self.responses
                        //     .write()
                        //     .push(rpc::fixup_header_length_fields(res));
                    }
                    if cfg!(feature = "execution") {
                        total_time += task.time();
                        db_time += task.db_time();
                        let mut count = self.task_completed.borrow_mut();
                        *count += 1;
                        let every = 1000000;
                        if *count >= every {
                            info!("Total {}, DB {}", total_time / (*count), db_time / (*count));
                            *count = 0;
                            total_time = 0;
                            db_time = 0;
                        }
                    }
                } else {
                    // The task did not complete execution. EITHER add it back to the waiting list so that it
                    // gets to run again OR run the pushback mechanism. The pushback starts only after that
                    // dispatcher task execution. Trigger pushback:-
                    //
                    // if there are MAX_RX_PACKETS /4 yeilded tasks in the queue, OR
                    // if two dispatcher invocations are 2000 us apart, AND
                    // if the current dispatcher invocation received MAX_RX_PACKETS /4 new tasks.
                    /*
                    if cfg!(feature = "pushback")
                        && is_dispatcher == true
                        && (queue_length >= MAX_RX_PACKETS / 8 || difference > time_trigger)
                        && ((self.waiting.read().len() - queue_length) > 0)
                    {
                        for _i in 0..queue_length {
                            let mut yeilded_task = self.waiting.write().pop_front().unwrap();

                            // Compute Ranking/Credit on the go for each task to pushback
                            // some of the tasks whose rank/credit is more than the threshold.
                            if (yeilded_task.state() == YIELDED)
                                && ((yeilded_task.time() - yeilded_task.db_time()) > credit as u64)
                            {
                                yeilded_task.set_state(STOPPED);
                                if let Some((req, resps)) = unsafe { yeilded_task.tear() } {
                                    req.free_packet();
                                    for resp in resps.into_iter() {
                                        self.responses
                                            .write()
                                            .push(rpc::fixup_header_length_fields(resp));
                                    }
                                    // self.responses
                                    //     .write()
                                    //     .push(rpc::fixup_header_length_fields(res));
                                }
                            } else {
                                self.waiting.write().push_front(yeilded_task);
                            }
                        }
                    }
                    */
                    self.waiting.write().push_back(task);
                }
            }
        }
    }
}

// RoundRobin uses atomics and RwLocks. Hence, it is thread-safe. Need to explicitly mark it as
// Send and Sync here because the compiler does not do so. This is because Packet contains a *mut
// MBuf which is not Send and Sync. Similarly, the compiler appears to be having trouble with the
// "Task" trait object.
unsafe impl Send for RoundRobin {}
unsafe impl Sync for RoundRobin {}
