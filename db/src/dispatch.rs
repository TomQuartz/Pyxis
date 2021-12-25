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

#[cfg(feature = "dispatch")]
use std::cell::RefCell;
use std::fmt::Display;
use std::net::Ipv4Addr;
use std::option::Option;
use std::str::FromStr;
use std::sync::Arc;

use super::config;
// #[cfg(feature = "dispatch")]
use super::cyclecounter::CycleCounter;
use super::cycles;
use super::master::Master;
use super::rpc;
use super::rpc::*;
use super::sched::RoundRobin;
use super::service::Service;
use super::task::{Task, TaskPriority, TaskState};
use super::wireformat::*;

use super::e2d2::common::EmptyMetadata;
use super::e2d2::headers::*;
use super::e2d2::interface::*;

use sandstorm::common;

use atomic_float::AtomicF64;
use config::NetConfig;
use e2d2::allocators::*;
use rand::{Rng, SeedableRng, XorShiftRng};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

/// This flag enables or disables fast path for native requests.
/// Later, it will be set from the server.toml file probably.
pub const FAST_PATH: bool = false;

// This is a thread local variable to count the number of occurrences
// of cycle counting to average for 1 M events.
#[cfg(feature = "dispatch")]
thread_local!(static COUNTER: RefCell<u64> = RefCell::new(0));

pub struct PacketHeaders {
    /// The UDP header that will be appended to every response packet (cached
    /// here to avoid wasting time creating a new one for every response
    /// packet).
    pub udp_header: UdpHeader,

    /// The IP header that will be appended to every response packet (cached
    /// here to avoid creating a new one for every response packet).
    pub ip_header: IpHeader,

    /// The MAC header that will be appended to every response packet (cached
    /// here to avoid creating a new one for every response packet).
    pub mac_header: MacHeader,
}

impl PacketHeaders {
    fn parse_ip(config: &NetConfig) -> Option<u32> {
        Ipv4Addr::from_str(&config.ip_addr)
            .and_then(|addr| Ok(u32::from(addr)))
            .ok()
    }
    pub fn sort(configs: &mut Vec<NetConfig>) {
        configs.sort_by_key(|cfg| Self::parse_ip(cfg).unwrap());
    }
    pub fn new(src: &NetConfig, dst: &NetConfig) -> PacketHeaders {
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
        // NOTE: unwrap may panic since dst may be default
        if let Some(src_ip) = Self::parse_ip(src) {
            ip_header.set_src(src_ip);
        }
        if let Some(dst_ip) = Self::parse_ip(dst) {
            ip_header.set_dst(dst_ip);
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
    pub fn create_hdr(src: &NetConfig, src_udp_port: u16, dst: &NetConfig) -> PacketHeaders {
        let mut pkthdr = PacketHeaders::new(src, dst);
        // we need to fix the src port for req_hdrs, which is used as the dst port in resp_hdr at rpc endpoint
        pkthdr.udp_header.set_src_port(src_udp_port);
        pkthdr
    }
    pub fn create_hdrs(
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

pub struct Sender {
    // The network interface over which requests will be sent out.
    net_port: CacheAligned<PortQueue>,
    // number of endpoints, either storage or compute
    // unit is core
    num_endpoints: Cell<usize>,
    // udp+ip+mac header for sending reqs
    req_hdrs: Vec<PacketHeaders>,
    // The number of destination UDP ports a req can be sent to.
    dst_ports: Vec<u16>,
    cumsum_ports: Vec<usize>,
    // // Mapping from type to dst_ports(cores)
    // type2core: HashMap<usize, Vec<u16>>,
    // Rng generator
    rng: RefCell<XorShiftRng>,
    // // Tracks number of packets sent to the server for occasional debug messages.
    // requests_sent: Cell<u64>,
    // shared_credits: Arc<RwLock<u32>>,
    // buffer: RefCell<VecDeque<Packet<IpHeader, EmptyMetadata>>>,
}

impl Sender {
    /// create sender from src and several endpoints
    pub fn new(
        net_port: CacheAligned<PortQueue>,
        src: &NetConfig,
        endpoints: &Vec<NetConfig>,
        // shared_credits: Arc<RwLock<u32>>,
    ) -> Sender {
        let mut endpoints = endpoints.clone();
        PacketHeaders::sort(&mut endpoints);
        let dst_ports = endpoints.iter().map(|x| x.rx_queues as u16).collect();
        let req_hdrs = PacketHeaders::create_hdrs(src, net_port.txq() as u16, &mut endpoints);
        let mut sum = 0usize;
        let mut cumsum_ports = vec![];
        for &nports in &dst_ports {
            sum += nports as usize;
            cumsum_ports.push(sum);
        }
        Sender {
            // id: net_port.rxq() as usize,
            num_endpoints: Cell::new(sum),
            req_hdrs: req_hdrs,
            net_port: net_port, //.clone()
            // TODO: make this a vector, different number of ports for each storage endpoint
            dst_ports: dst_ports,
            cumsum_ports: cumsum_ports,
            rng: {
                let seed: [u32; 4] = rand::random::<[u32; 4]>();
                RefCell::new(XorShiftRng::from_seed(seed))
            },
            // shared_credits: shared_credits,
            // buffer: RefCell::new(VecDeque::new()),
        }
    }

    pub fn set_endpoints(&self, num_endpoints: usize) {
        self.num_endpoints.set(num_endpoints);
    }

    fn get_endpoint(&self) -> (usize, u16) {
        let num_endpoints = self.num_endpoints.get();
        let endpoint_idx = self.rng.borrow_mut().gen::<usize>() % num_endpoints;
        let server_idx = self
            .cumsum_ports
            .partition_point(|&cumsum| cumsum <= endpoint_idx);
        let port_idx = endpoint_idx
            - if server_idx > 0 {
                self.cumsum_ports[server_idx - 1]
            } else {
                0
            };
        (server_idx, port_idx as u16)
    }
    /*
    pub fn return_credit(&self) {
        // let mut buffer = self.buffer.borrow_mut();
        // if let Some(request) = buffer.pop_front() {
        //     self.send_pkt(request);
        // } else {
        //     let credits = self.credits.get();
        //     self.credits.set(credits + 1);
        // }
        let mut shared_credits = self.shared_credits.write().unwrap();
        *shared_credits += 1;
    }

    pub fn try_send_packets(&self) {
        let has_credit = *self.shared_credits.read().unwrap() > 0;
        if !has_credit {
            return;
        }
        let mut buffer = self.buffer.borrow_mut();
        loop {
            if buffer.len() == 0 {
                break;
            }
            let acquired = {
                let mut current_credits = self.shared_credits.write().unwrap();
                if *current_credits > 0 {
                    *current_credits -= 1;
                    true
                } else {
                    false
                }
            };
            if acquired {
                let request = buffer.pop_front().unwrap();
                self.send_pkt(request);
                trace!("send req from buffer");
            } else {
                break;
            }
        }
    }
    */

    /// Creates and sends out a get() RPC request. Network headers are populated based on arguments
    /// passed into new() above.
    ///
    /// # Arguments
    ///
    /// * `tenant`: Id of the tenant requesting the item.
    /// * `table`:  Id of the table from which the key is looked up.
    /// * `key`:    Byte string of key whose value is to be fetched. Limit 64 KB.
    /// * `id`:     RPC identifier.
    #[allow(dead_code)]
    // TODO: hash the target storage with LSB in key
    // for now, the vec len of req_hdrs=1
    pub fn send_get(&self, tenant: u32, table: u64, key: &[u8], id: u64) {
        let (server_idx, port_idx) = self.get_endpoint();
        let request = rpc::create_get_rpc(
            &self.req_hdrs[server_idx].mac_header,
            &self.req_hdrs[server_idx].ip_header,
            &self.req_hdrs[server_idx].udp_header,
            tenant,
            table,
            key,
            id,
            port_idx,
            GetGenerator::SandstormClient,
        );

        self.send_pkt(request);
    }

    /// Creates and sends out a get() RPC request. Network headers are populated based on arguments
    /// passed into new() above.
    ///
    /// # Arguments
    ///
    /// * `tenant`: Id of the tenant requesting the item.
    /// * `table`:  Id of the table from which the key is looked up.
    /// * `key`:    Byte string of key whose value is to be fetched. Limit 64 KB.
    /// * `id`:     RPC identifier.
    #[allow(dead_code)]
    pub fn send_get_from_extension(&self, tenant: u32, table: u64, key: &[u8], id: u64) {
        let (server_idx, port_idx) = self.get_endpoint();
        let request = rpc::create_get_rpc(
            &self.req_hdrs[server_idx].mac_header,
            &self.req_hdrs[server_idx].ip_header,
            &self.req_hdrs[server_idx].udp_header,
            tenant,
            table,
            key,
            id,
            port_idx,
            GetGenerator::SandstormExtension,
        );
        trace!(
            "ext id: {} send key {:?} table {}, from {}, to {}",
            id,
            key,
            table,
            self.req_hdrs[server_idx].ip_header.src(),
            self.req_hdrs[server_idx].ip_header.dst(),
        );
        self.send_pkt(request);
        // let mut buffer = self.buffer.borrow_mut();
        // let has_credit = *self.shared_credits.read().unwrap() > 0;
        // if !has_credit {
        //     buffer.push_back(request);
        //     return;
        // }
        // let acquired = {
        //     let mut current_credits = self.shared_credits.write().unwrap();
        //     if *current_credits > 0 {
        //         *current_credits -= 1;
        //         true
        //     } else {
        //         false
        //     }
        // };
        // if acquired {
        //     if buffer.len() > 0 {
        //         let pending_request = buffer.pop_front().unwrap();
        //         self.send_pkt(pending_request);
        //         buffer.push_back(request);
        //         trace!("send from buffer, push current to queue");
        //     } else {
        //         self.send_pkt(request);
        //         trace!("send directly");
        //     }
        // } else {
        //     buffer.push_back(request);
        //     trace!("failed to acquire");
        // }
    }

    /// Creates and sends out a put() RPC request. Network headers are populated based on arguments
    /// passed into new() above.
    ///
    /// # Arguments
    ///
    /// * `tenant`: Id of the tenant requesting the insertion.
    /// * `table`:  Id of the table into which the key-value pair is to be inserted.
    /// * `key`:    Byte string of key whose value is to be inserted. Limit 64 KB.
    /// * `val`:    Byte string of the value to be inserted.
    /// * `id`:     RPC identifier.
    #[allow(dead_code)]
    pub fn send_put(&self, tenant: u32, table: u64, key: &[u8], val: &[u8], id: u64) {
        let (server_idx, port_idx) = self.get_endpoint();
        let request = rpc::create_put_rpc(
            &self.req_hdrs[server_idx].mac_header,
            &self.req_hdrs[server_idx].ip_header,
            &self.req_hdrs[server_idx].udp_header,
            tenant,
            table,
            key,
            val,
            id,
            port_idx,
        );

        self.send_pkt(request);
    }

    /// Creates and sends out a multiget() RPC request. Network headers are populated based on
    /// arguments passed into new() above.
    ///
    /// # Arguments
    ///
    /// * `tenant`: Id of the tenant requesting the item.
    /// * `table`:  Id of the table from which the key is looked up.
    /// * `k_len`:  The length of each key to be looked up at the server. All keys are
    ///               assumed to be of equal length.
    /// * `n_keys`: The number of keys to be looked up at the server.
    /// * `keys`:   Byte string of keys whose values are to be fetched.
    /// * `id`:     RPC identifier.
    #[allow(dead_code)]
    pub fn send_multiget(
        &self,
        tenant: u32,
        table: u64,
        k_len: u16,
        n_keys: u32,
        keys: &[u8],
        id: u64,
    ) {
        let (server_idx, port_idx) = self.get_endpoint();
        let request = rpc::create_multiget_rpc(
            &self.req_hdrs[server_idx].mac_header,
            &self.req_hdrs[server_idx].ip_header,
            &self.req_hdrs[server_idx].udp_header,
            tenant,
            table,
            k_len,
            n_keys,
            keys,
            id,
            port_idx,
        );

        self.send_pkt(request);
    }

    /// Creates and sends out an invoke() RPC request. Network headers are populated based on
    /// arguments passed into new() above.
    ///
    /// # Arguments
    ///
    /// * `tenant`:   Id of the tenant requesting the invocation.
    /// * `name_len`: The number of bytes at the head of the payload corresponding to the
    ///               extensions name.
    /// * `payload`:  The RPC payload to be written into the packet. Must contain the name of the
    ///               extension followed by it's arguments.
    /// * `id`:       RPC identifier.
    pub fn send_invoke(
        &self,
        tenant: u32,
        name_len: u32,
        payload: &[u8],
        id: u64,
        _type_id: usize,
    ) -> (u32, u16) {
        let (server_idx, port_idx) = self.get_endpoint();
        let request = rpc::create_invoke_rpc(
            &self.req_hdrs[server_idx].mac_header,
            &self.req_hdrs[server_idx].ip_header,
            &self.req_hdrs[server_idx].udp_header,
            tenant,
            name_len,
            payload,
            id,
            // self.get_dst_port(endpoint),
            port_idx,
            // self.get_dst_port_by_type(type_id),
            // (id & 0xffff) as u16 & (self.dst_ports - 1),
        );
        trace!(
            "send req to dst ip {}",
            self.req_hdrs[server_idx].ip_header.dst()
        );
        self.send_pkt(request);
        (self.req_hdrs[server_idx].ip_header.dst(), port_idx)
    }

    pub fn send_reset(&self) {
        for server_idx in 0..self.dst_ports.len() {
            for port_idx in 0..self.dst_ports[server_idx] {
                let request = rpc::create_reset_rpc(
                    &self.req_hdrs[server_idx].mac_header,
                    &self.req_hdrs[server_idx].ip_header,
                    &self.req_hdrs[server_idx].udp_header,
                    port_idx,
                    // self.get_dst_port_by_type(type_id),
                    // (id & 0xffff) as u16 & (self.dst_ports - 1),
                );
                self.send_pkt(request);
            }
        }
    }
    // set provision upon initialization and after scaling
    // this message is sent to ALL compute nodes from lb, regardless of current
    pub fn send_scaling(&self, provision: u32) {
        for server_idx in 0..self.dst_ports.len() {
            for port_idx in 0..self.dst_ports[server_idx] {
                let request = rpc::create_scaling_rpc(
                    &self.req_hdrs[server_idx].mac_header,
                    &self.req_hdrs[server_idx].ip_header,
                    &self.req_hdrs[server_idx].udp_header,
                    port_idx,
                    provision,
                    // self.get_dst_port_by_type(type_id),
                    // (id & 0xffff) as u16 & (self.dst_ports - 1),
                );
                self.send_pkt(request);
            }
        }
    }

    // /// Computes the destination UDP port given a tenant identifier.
    // #[inline]
    // fn get_dst_port(&self, endpoint: usize) -> u16 {
    //     // The two least significant bytes of the tenant id % the total number of destination
    //     // ports.
    //     // (tenant & 0xffff) as u16 & (self.dst_ports - 1)
    //     self.rng.borrow_mut().gen::<u16>() % self.dst_ports[endpoint]
    // }

    // #[inline]
    // fn get_dst_port_by_type(&self, type_id: usize) -> u16 {
    //     let target_cores = &self.type2core[&type_id];
    //     let target_idx = self.rng.borrow_mut().gen::<u32>() % target_cores.len() as u32;
    //     target_cores[target_idx as usize]
    // }

    /// Sends a request/packet parsed upto IP out the network interface.
    #[inline]
    fn send_pkt(&self, request: Packet<IpHeader, EmptyMetadata>) {
        // Send the request out the network.
        unsafe {
            let mut pkts = [request.get_mbuf()];

            let sent = self
                .net_port
                .send(&mut pkts)
                .expect("Failed to send packet!");

            if sent < pkts.len() as u32 {
                warn!("Failed to send all packets!");
            }
        }

        // // Update the number of requests sent out by this generator.
        // let r = self.requests_sent.get();
        // // if r & 0xffffff == 0 {
        // //     info!("Sent many requests...");
        // // }
        // self.requests_sent.set(r + 1);
    }

    pub fn send_pkts(&self, packets: &mut Vec<Packet<IpHeader, EmptyMetadata>>) {
        trace!("send {} resps", packets.len());
        unsafe {
            let mut mbufs = vec![];
            let num_packets = packets.len();
            let mut to_send = num_packets;

            // Extract Mbuf's from the batch of packets.
            while let Some(packet) = packets.pop() {
                mbufs.push(packet.get_mbuf());
            }

            // Send out the above MBuf's.
            loop {
                // let sent = self.dispatcher.receiver.net_port.send(&mut mbufs).unwrap() as usize;
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
        // unsafe {
        //     let mut mbufs = vec![];
        //     let num_packets = packets.len();

        //     // Extract Mbuf's from the batch of packets.
        //     while let Some(packet) = packets.pop() {
        //         mbufs.push(packet.get_mbuf());
        //     }

        //     // Send out the above MBuf's.
        //     match self.net_port.send(&mut mbufs) {
        //         Ok(sent) => {
        //             if sent < num_packets as u32 {
        //                 warn!("Was able to send only {} of {} packets.", sent, num_packets);
        //             }

        //             // self.responses_sent += mbufs.len() as u64;
        //         }

        //         Err(ref err) => {
        //             error!("Error on packet send: {}", err);
        //         }
        //     }
        // }
    }

    // /// Creates and sends out a commit() RPC request. Network headers are populated based on
    // /// arguments passed into new() above.
    // ///
    // /// # Arguments
    // ///
    // /// * `tenant`:   Id of the tenant requesting the invocation.
    // /// * `table_id`: The number of bytes at the head of the payload corresponding to the
    // ///               extensions name.
    // /// * `payload`:  The RPC payload to be written into the packet. Must contain the name of the
    // ///               extension followed by it's arguments.
    // /// * `id`:       RPC identifier.
    // pub fn send_commit(
    //     &self,
    //     tenant: u32,
    //     table_id: u64,
    //     payload: &[u8],
    //     id: u64,
    //     key_len: u16,
    //     val_len: u16,
    // ) {
    //     let request = rpc::create_commit_rpc(
    //         &self.req_hdrs[0].mac_header,
    //         &self.req_hdrs[0].ip_header,
    //         &self.req_hdrs[0].udp_header,
    //         tenant,
    //         table_id,
    //         payload,
    //         id,
    //         self.get_dst_port(tenant),
    //         key_len,
    //         val_len,
    //     );

    //     self.send_pkt(request);
    // }
}

pub struct Receiver {
    // The network interface over which responses will be received from.
    net_port: CacheAligned<PortQueue>,
    // // Sibling port
    // sib_port: Option<CacheAligned<PortQueue>>,
    // // stealing
    // pub stealing: bool,
    // The maximum number of packets that can be received from the network port in one shot.
    max_rx_packets: usize,
    // ip_addr
    ip_addr: u32,
    // // The total number of responses received.
    // responses_recv: Cell<u64>,
    // pub reset: bool,
}

// Implementation of methods on Receiver.
impl Receiver {
    pub fn new(
        net_port: CacheAligned<PortQueue>,
        // sib_port: Option<CacheAligned<PortQueue>>,
        max_rx_packets: usize,
        ip_addr: &str,
    ) -> Receiver {
        Receiver {
            // reset: false,
            // stealing: !sib_port.is_none(),
            net_port: net_port,
            // sib_port: sib_port,
            max_rx_packets: max_rx_packets,
            ip_addr: u32::from(Ipv4Addr::from_str(ip_addr).expect("missing src ip for LB")),
        }
    }
    // TODO: makesure src ip is properly set
    // return pkt and (src ip, src udp port)
    pub fn recv<F>(
        &self,
        mut filter: F,
    ) -> Option<Vec<(Packet<UdpHeader, EmptyMetadata>, (u32, u16))>>
    where
        F: FnMut(Packet<UdpHeader, EmptyMetadata>) -> Option<Packet<UdpHeader, EmptyMetadata>>,
    {
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
            // if recvd == 0 && self.stealing {
            //     recvd = self
            //         .sib_port
            //         .as_ref()
            //         .unwrap()
            //         .recv(&mut mbuf_vector[..])
            //         .expect("Error on packet receive") as usize;
            // }
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
                            if let Some(packet) = filter(packet) {
                                packets.push((packet, (src_ip, src_port)));
                            }
                        }
                    }
                }
            }

            return Some(packets);
        }
    }
    /*
    // need to check self.stealing
    pub fn steal(&self) -> Option<Vec<(Packet<UdpHeader, EmptyMetadata>, (u32, u16))>> {
        assert!(self.stealing);
        // Allocate a vector of mutable MBuf pointers into which raw packets will be received.
        let mut mbuf_vector = Vec::with_capacity(self.max_rx_packets);

        // This unsafe block is needed in order to populate mbuf_vector with a bunch of pointers,
        // and subsequently manipulate these pointers. DPDK will take care of assigning these to
        // actual MBuf's.
        unsafe {
            mbuf_vector.set_len(self.max_rx_packets);

            // Try to receive packets from the network port.
            let mut recvd = self
                .sib_port
                .as_ref()
                .unwrap()
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
            trace!(
                "port {} steal {} raw pkts from {}",
                self.net_port.rxq(),
                recvd,
                self.sib_port.as_ref().unwrap().rxq()
            );
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

            Some(packets)
        }
    }
    */

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

pub struct Queue {
    pub queue: RwLock<VecDeque<Packet<UdpHeader, EmptyMetadata>>>,
    // /// length is set by Dispatcher, and cleared by the first worker in that round
    // LB will only update those with positive queue length, thus reducing update frequency and variance
    // pub length: AtomicF64,
    // pub reset: AtomicBool,
    // pub rx_lock: AtomicBool,
}
impl Queue {
    pub fn new(capacity: usize) -> Queue {
        Queue {
            queue: RwLock::new(VecDeque::with_capacity(capacity)),
            // length: AtomicF64::new(-1.0),
            // reset: AtomicBool::new(false),
            // rx_lock: AtomicBool::new(false),
        }
    }
}

pub struct Dispatcher {
    pub sender: Rc<Sender>,
    pub receiver: Receiver,
    // for all ports
    pub queue: Arc<Queue>,
    pub sib_queue: Option<Arc<Queue>>,
    // for all workers using this port
    // pub reset: Vec<Arc<AtomicBool>>,
    // time_avg: MovingTimeAvg,
    pub length: f64,
    pub reset: Cell<bool>,
}

impl Dispatcher {
    pub fn new(
        net_port: CacheAligned<PortQueue>,
        // sib_port: Option<CacheAligned<PortQueue>>,
        src: &NetConfig,
        endpoints: &Vec<NetConfig>,
        max_rx_packets: usize,
        queue: Arc<Queue>,
        sib_queue: Option<Arc<Queue>>,
        // reset: Vec<Arc<AtomicBool>>,
        // moving_exp: f64,
    ) -> Dispatcher {
        Dispatcher {
            sender: Rc::new(Sender::new(net_port.clone(), src, endpoints)),
            receiver: Receiver::new(net_port, /*sib_port, */ max_rx_packets, &src.ip_addr),
            queue: queue,
            sib_queue: sib_queue,
            // reset: reset,
            // time_avg: MovingTimeAvg::new(moving_exp),
            // queue: RwLock::new(VecDeque::with_capacity(config.max_rx_packets)),
            length: -1.0,
            reset: Cell::new(false),
            // length: MovingTimeAvg::new(moving_exp),
        }
    }
    /// get a packet from common queue
    pub fn poll(&self) -> Option<Packet<UdpHeader, EmptyMetadata>> {
        // reset by sib port
        // if self.queue.reset.load(Ordering::Relaxed) {
        //     self.queue.reset.store(false, Ordering::Relaxed);
        //     self.reset_workers();
        // }
        self.queue.queue.write().unwrap().pop_front()
        // if let Some(packet,_) = self.queue.queue.write().unwrap().pop_front(){
        //     Some(packet)
        // }else{
        //     None
        // }
    }
    pub fn poll_sib(&self) -> Option<Packet<UdpHeader, EmptyMetadata>> {
        if let Some(sib_queue) = &self.sib_queue {
            // if let Some(packet) = sib_queue.queue.write().unwrap().pop_front(){
            //     return Some(packet);
            // }
            sib_queue.queue.write().unwrap().pop_front()
        } else {
            None
        }
    }
    // pub fn reset_workers(&self) {
    //     for reset in &self.reset {
    //         reset.store(true, Ordering::Relaxed);
    //     }
    // }
    // pub fn queue_length(&mut self) -> f64 {
    //     // self.queue.length.swap(-1.0, Ordering::Relaxed)
    //     self.length.avg()
    // }
    // do not report the queue len of sib queue
    // pub fn queue_length_sib(&self) -> f64 {
    //     let sib_queue = self.sib_queue.unwrap();
    //     sib_queue.length.swap(-1.0, Ordering::Relaxed);
    // }
    pub fn recv(&mut self) {
        // let current = cycles::rdtsc();
        let mut reset = false;
        if let Some(packets) = self.receiver.recv(|pkt| match parse_rpc_opcode(&pkt) {
            OpCode::ResetRpc => {
                self.reset.replace(true);
                pkt.free_packet();
                None
            }
            OpCode::ScalingRpc => {
                let pkt = pkt.parse_header::<InvokeRequest>();
                let hdr = pkt.get_header();
                self.sender.set_endpoints(hdr.args_length as usize);
                pkt.free_packet();
                None
            }
            _ => Some(pkt),
        }) {
            if packets.len() > 0 {
                let mut queue = self.queue.queue.write().unwrap();
                queue.extend(packets.into_iter().map(|(packet, _)| packet));
                // queue_length = queue.len() as f64;
                self.length = queue.len() as f64;
                return;
            }
        }
        self.length = 0.0;
    }
    pub fn reset(&self) -> bool {
        self.reset.replace(false)
    }
    /*
    // a wrapper around Receiver.recv
    // mainly for reset and queue length
    // pub fn recv(&mut self) -> Option<Vec<Packet<UdpHeader, EmptyMetadata>>> {
    pub fn recv(&mut self) -> Result<(), ()> {
        // if let Ok(_) =
        //     self.queue
        //         .rx_lock
        //         .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        {
            // NOTE: the rx_lock is immediately released after calling recv
            // but it is not useful since few packets would arrive within a short interval
            let packets = self.receiver.recv();
            // self.queue.rx_lock.store(false, Ordering::Relaxed);
            if let Some(mut packets) = packets {
                trace!("dispatcher recv {} packets", packets.len());
                if packets.len() > 0 {
                    // let current_time = cycles::rdtsc();
                    let mut queue = self.queue.queue.write().unwrap();
                    // let mut queue = vec![];
                    while let Some((packet, _)) = packets.pop() {
                        if parse_rpc_opcode(&packet) == OpCode::ResetRpc {
                            // reset
                            // this will create bottleneck since all workers attempts to write
                            // self.task_duration_cv.write().unwrap().reset();
                            self.reset_workers();
                            // TODO: reset queue
                            packet.free_packet();
                        } else {
                            // queue.push(packet);
                            queue.push_back(packet);
                        }
                    }
                    // NOTE: we only update queue len upon receiving packets
                    let queue_len = queue.len() as f64;
                    // self.time_avg.update(current_time, queue_len);
                    // self.queue.length.store(queue_len, Ordering::Relaxed);
                    self.length = queue_len;
                    return Ok(());
                    // return Some(queue);
                }
            }
        }
        Err(())
        // None
    }
    */
    /*
    pub fn steal(&mut self) -> Result<(), ()> {
        if let Ok(_) = self.sib_queue.as_ref().unwrap().rx_lock.compare_exchange(
            false,
            true,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            let packets = self.receiver.steal();
            self.sib_queue
                .as_ref()
                .unwrap()
                .rx_lock
                .store(false, Ordering::Relaxed);
            if let Some(mut packets) = packets {
                trace!("dispatcher steal {} packets", packets.len());
                if packets.len() > 0 {
                    // let current_time = cycles::rdtsc();
                    let sib_queue = self.sib_queue.as_ref().unwrap();
                    let mut queue = sib_queue.queue.write().unwrap();
                    while let Some((packet, _)) = packets.pop() {
                        if parse_rpc_opcode(&packet) == OpCode::ResetRpc {
                            // reset
                            // this will create bottleneck since all workers attempts to write
                            // self.task_duration_cv.write().unwrap().reset();
                            sib_queue.reset.store(true, Ordering::Relaxed);
                            // TODO: reset queue
                            packet.free_packet();
                        } else {
                            queue.push_back(packet);
                        }
                    }
                    let queue_len = queue.len() as f64;
                    // self.time_avg.update(current_time, queue_len);
                    sib_queue.length.store(queue_len, Ordering::Relaxed);
                    return Ok(());
                }
            }
        }
        Err(())
    }
    */
}

/// This type stores the cycle counter variable for various parts in dispatch stage.
/// poll: Cycle counter for full polling stage.
/// rx_tx: Cycle counter for packets receive and transmit stage.
/// parse: Cycle counter for packet parsing stage.
/// dispatch: Cycle counter for generator and task creation stage.
// #[cfg(feature = "dispatch")]
struct DispatchCounters {
    poll: CycleCounter,
    rx_tx: CycleCounter,
    parse: CycleCounter,
    dispatch: CycleCounter,
}

// #[cfg(feature = "dispatch")]
impl DispatchCounters {
    /// Creates and return an object of DispatchCounters. This object is used for
    /// couting CPU cycles for various parts in dispatch stage.
    ///
    /// # Return
    ///
    /// New instance of DispatchCounters struct
    fn new() -> DispatchCounters {
        DispatchCounters {
            poll: CycleCounter::new(),
            rx_tx: CycleCounter::new(),
            parse: CycleCounter::new(),
            dispatch: CycleCounter::new(),
        }
    }
}

/// This type represents a requests-dispatcher in Sandstorm. When added to a
/// Netbricks scheduler, this dispatcher polls a network port for RPCs,
/// dispatches them to a service, and sends out responses on the same network
/// port.
pub struct Dispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// A ref counted pointer to a master service. The master service
    /// implements the primary interface to the database.
    master_service: Arc<Master>,

    /// A ref counted pointer to the scheduler on which to enqueue tasks,
    /// and from which to receive response packets to be sent back to clients.
    scheduler: Arc<RoundRobin>,

    /// The network port/interface on which this dispatcher receives and
    /// transmits RPC requests and responses on.
    network_port: T,

    /// The receive queue over which this dispatcher steals RPC requests from.
    sibling_port: T,

    /// The IP address of the server. This is required to ensure that the
    /// server does not process packets that were destined to a different
    /// machine.
    network_ip_addr: u32,

    /// The maximum number of packets that the dispatcher can receive from the
    /// network interface in a single burst.
    max_rx_packets: u8,

    /// The UDP header that will be appended to every response packet (cached
    /// here to avoid wasting time creating a new one for every response
    /// packet).
    resp_udp_header: UdpHeader,

    /// The IP header that will be appended to every response packet (cached
    /// here to avoid creating a new one for every response packet).
    resp_ip_header: IpHeader,

    /// The MAC header that will be appended to every response packet (cached
    /// here to avoid creating a new one for every response packet).
    resp_mac_header: MacHeader,

    /// The number of response packets that were sent out by the dispatcher in
    /// the last measurement interval.
    responses_sent: u64,

    /// An indicator of the start of the current measurement interval in cycles.
    measurement_start: u64,

    /// An indicator of the stop of the previous measurement interval in cycles.
    measurement_stop: u64,

    /// The current execution state of the Dispatch task. Can be INITIALIZED, YIELDED, or RUNNING.
    state: TaskState,

    /// The total time for which the Dispatch task has executed on the CPU in cycles.
    time: u64,

    /// The priority of the Dispatch task. Required by the scheduler to determine when to run the
    /// task again.
    priority: TaskPriority,

    /// Unique identifier for a Dispatch task. Currently required for measurement purposes.
    id: i32,

    /// The CPU cycle counter to count the number of cycles per event. Need to use start() and
    /// stop() a code block or function call to profile the events.
    // #[cfg(feature = "dispatch")]
    cycle_counter: DispatchCounters,
}
/*
impl<T> Dispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// This function creates and returns a requests-dispatcher which can be
    /// added to a Netbricks scheduler.
    ///
    /// # Arguments
    ///
    /// * `config`:   A configuration consisting of the IP address, UDP port etc.
    /// * `net_port`: A network port/interface on which packets will be
    ///               received and transmitted.
    /// * `sib_port`: A network port/interface on which packets will be stolen.
    /// * `master`:   A reference to a Master which will be used to construct tasks from received
    ///               packets.
    /// * `sched`:    A reference to a scheduler on which tasks will be enqueued.
    /// * `id`:       The identifier of the dispatcher.
    ///
    /// # Return
    ///
    /// A dispatcher of type ServerDispatch capable of receiving RPCs, and responding to them.
    pub fn new(
        config: &config::ServerConfig,
        net_port: T,
        sib_port: T,
        master: Arc<Master>,
        sched: Arc<RoundRobin>,
        id: i32,
    ) -> Dispatch<T> {
        let rx_batch_size: u8 = config.max_rx_packets as u8;

        // Create a common udp header for response packets.
        let udp_src_port: u16 = config.udp_port;
        let udp_dst_port: u16 = common::DEFAULT_UDP_PORT;
        let udp_length: u16 = common::PACKET_UDP_LEN;
        let udp_checksum: u16 = common::PACKET_UDP_CHECKSUM;

        let mut udp_header: UdpHeader = UdpHeader::new();
        udp_header.set_src_port(udp_src_port);
        udp_header.set_dst_port(udp_dst_port);
        udp_header.set_length(udp_length);
        udp_header.set_checksum(udp_checksum);

        // Create a common ip header for response packets.
        let ip_src_addr: u32 = u32::from(
            Ipv4Addr::from_str(&config.ip_address).expect("Failed to create server IP address."),
        );
        let ip_dst_addr: u32 = u32::from(
            Ipv4Addr::from_str(&config.client_ip).expect("Failed to create client IP address."),
        );
        let ip_ttl: u8 = common::PACKET_IP_TTL;
        let ip_version: u8 = common::PACKET_IP_VER;
        let ip_ihl: u8 = common::PACKET_IP_IHL;
        let ip_length: u16 = common::PACKET_IP_LEN;

        let mut ip_header: IpHeader = IpHeader::new();
        ip_header.set_src(ip_src_addr);
        ip_header.set_dst(ip_dst_addr);
        ip_header.set_ttl(ip_ttl);
        ip_header.set_version(ip_version);
        ip_header.set_ihl(ip_ihl);
        ip_header.set_length(ip_length);
        ip_header.set_protocol(0x11);

        // Create a common mac header for response packets.
        let mac_src_addr: MacAddress = config.parse_mac();
        let mac_dst_addr: MacAddress = config.parse_client_mac();
        let mac_etype: u16 = common::PACKET_ETYPE;

        let mut mac_header: MacHeader = MacHeader::new();
        mac_header.src = mac_src_addr;
        mac_header.dst = mac_dst_addr;
        mac_header.set_etype(mac_etype);

        Dispatch {
            master_service: master,
            scheduler: sched,
            network_port: net_port.clone(),
            sibling_port: sib_port.clone(),
            network_ip_addr: ip_src_addr,
            max_rx_packets: rx_batch_size,
            resp_udp_header: udp_header,
            resp_ip_header: ip_header,
            resp_mac_header: mac_header,
            responses_sent: 0,
            measurement_start: cycles::rdtsc(),
            measurement_stop: 0,
            state: TaskState::INITIALIZED,
            time: 0,
            priority: TaskPriority::DISPATCH,
            id: id,
            // #[cfg(feature = "dispatch")]
            cycle_counter: DispatchCounters::new(),
        }
    }

    /// This function attempts to receive a batch of packets from the
    /// dispatcher's network port.
    ///
    /// # Return
    ///
    /// A vector of packets wrapped up in Netbrick's Packet<NullHeader, EmptyMetadata> type if
    /// there was anything received at the network port.
    fn try_receive_packets(&self) -> Option<Vec<Packet<NullHeader, EmptyMetadata>>> {
        // Allocate a vector of mutable MBuf pointers into which packets will
        // be received.
        let mut mbuf_vector = Vec::with_capacity(self.max_rx_packets as usize);

        // This unsafe block is needed in order to populate mbuf_vector with a
        // bunch of pointers, and subsequently manipulate these pointers. DPDK
        // will take care of assigning these to actual MBuf's.
        unsafe {
            mbuf_vector.set_len(self.max_rx_packets as usize);

            // Try to receive packets from the network port.
            match self.network_port.recv(&mut mbuf_vector[..]) {
                // The receive call returned successfully.
                Ok(num_received) => {
                    if num_received == 0 {
                        // No packets were available for receive.
                        return None;
                    }
                    if num_received == self.max_rx_packets as u32 {
                        warn!("some recvd packets may still be in the rx ring");
                    }
                    trace!("port {} recv {} raw pkts", self.id, num_received);
                    // Allocate a vector for the received packets.
                    let mut recvd_packets = Vec::<Packet<NullHeader, EmptyMetadata>>::with_capacity(
                        self.max_rx_packets as usize,
                    );

                    // Clear out any dangling pointers in mbuf_vector.
                    for _dangling in num_received..self.max_rx_packets as u32 {
                        mbuf_vector.pop();
                    }

                    // Wrap up the received Mbuf's into Packets. The refcount
                    // on the mbuf's were set by DPDK, and do not need to be
                    // bumped up here. Hence, the call to
                    // packet_from_mbuf_no_increment().
                    for mbuf in mbuf_vector.iter_mut() {
                        recvd_packets.push(packet_from_mbuf_no_increment(*mbuf, 0));
                    }

                    return Some(recvd_packets);
                }

                // There was an error during receive.
                Err(ref err) => {
                    error!("Failed to receive packet: {}", err);
                    return None;
                }
            }
        }
    }

    /// This function attempts to steal a batch of packets from the
    /// dispatcher's network port.
    ///
    /// # Return
    ///
    /// A vector of packets wrapped up in Netbrick's Packet<NullHeader, EmptyMetadata> type if
    /// there was anything received at the network port.
    fn try_steal_packets(&self) -> Option<Vec<Packet<NullHeader, EmptyMetadata>>> {
        // Allocate a vector of mutable MBuf pointers into which packets will
        // be received.
        let mut mbuf_vector = Vec::with_capacity(self.max_rx_packets as usize);

        // This unsafe block is needed in order to populate mbuf_vector with a
        // bunch of pointers, and subsequently manipulate these pointers. DPDK
        // will take care of assigning these to actual MBuf's.
        unsafe {
            mbuf_vector.set_len(self.max_rx_packets as usize);

            // Try to receive packets from the sibling.
            match self.sibling_port.recv(&mut mbuf_vector[..]) {
                // The receive call returned successfully.
                Ok(num_received) => {
                    if num_received == 0 {
                        // No packets were available for receive.
                        return None;
                    }
                    trace!(
                        "port {} steal {} raw pkts from sibling port",
                        self.id,
                        num_received
                    );
                    // Allocate a vector for the received packets.
                    let mut recvd_packets = Vec::<Packet<NullHeader, EmptyMetadata>>::with_capacity(
                        self.max_rx_packets as usize,
                    );

                    // Clear out any dangling pointers in mbuf_vector.
                    for _dangling in num_received..self.max_rx_packets as u32 {
                        mbuf_vector.pop();
                    }

                    // Wrap up the received Mbuf's into Packets. The refcount
                    // on the mbuf's were set by DPDK, and do not need to be
                    // bumped up here. Hence, the call to
                    // packet_from_mbuf_no_increment().
                    for mbuf in mbuf_vector.iter_mut() {
                        recvd_packets.push(packet_from_mbuf_no_increment(*mbuf, 0));
                    }

                    return Some(recvd_packets);
                }

                // There was an error during receive.
                Err(ref err) => {
                    error!("Failed to receive packet: {}", err);
                    return None;
                }
            }
        }
    }

    /// This method takes as input a vector of packets and tries to send them
    /// out a network interface.
    ///
    /// # Arguments
    ///
    /// * `packets`: A vector of packets to be sent out the network, parsed upto their UDP headers.
    fn try_send_packets(&mut self, mut packets: Vec<Packet<IpHeader, EmptyMetadata>>) {
        // This unsafe block is required to extract the underlying Mbuf's from
        // the passed in batch of packets, and send them out the network port.
        unsafe {
            let mut mbufs = vec![];
            let num_packets = packets.len();
            let mut to_send = num_packets;

            // Extract Mbuf's from the batch of packets.
            while let Some(packet) = packets.pop() {
                mbufs.push(packet.get_mbuf());
            }

            // Send out the above MBuf's.
            loop {
                let sent = self.network_port.send(&mut mbufs).unwrap() as usize;
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
            // match self.network_port.send(&mut mbufs) {
            //     Ok(sent) => {
            //         if sent < num_packets as u32 {
            //             warn!("Was able to send only {} of {} packets.", sent, num_packets);
            //         }

            //         // self.responses_sent += mbufs.len() as u64;
            //     }

            //     Err(ref err) => {
            //         error!("Error on packet send: {}", err);
            //     }
            // }
        }

        // For every million packets sent out by the dispatcher, print out the
        // amount of time in nano seconds it took to do so.
        // let every = 1000000;
        // if self.responses_sent >= every {
        //     self.measurement_stop = cycles::rdtsc();

        //     debug!(
        //         "Dispatcher {}: {:.0} K/packets/s",
        //         self.id,
        //         (self.responses_sent as f64 / 1e3)
        //             / ((self.measurement_stop - self.measurement_start) as f64
        //                 / (cycles::cycles_per_second() as f64))
        //     );

        //     self.measurement_start = self.measurement_stop;
        //     self.responses_sent = 0;
        // }
    }

    /// This function frees a set of packets that were received from DPDK.
    ///
    /// # Arguments
    ///
    /// * `packets`: A vector of packets wrapped in Netbrick's Packet<> type.
    #[inline]
    fn free_packets<S: EndOffset>(&self, mut packets: Vec<Packet<S, EmptyMetadata>>) {
        while let Some(packet) = packets.pop() {
            packet.free_packet();
        }
    }

    /// This method parses the MAC headers on a vector of input packets.
    ///
    /// This method takes in a vector of packets that were received from
    /// DPDK and wrapped up in Netbrick's Packet<> type, and parses the MAC
    /// headers on the underlying MBufs, effectively rewrapping the packets
    /// into a new type (Packet<MacHeader, EmptyMetadata>).
    ///
    /// Any packets with an unexpected ethertype on the parsed header are
    /// dropped by this method.
    ///
    /// # Arguments
    ///
    /// * `packets`: A vector of packets that were received from DPDK and
    ///              wrapped up in Netbrick's Packet<NullHeader, EmptyMetadata>
    ///              type.
    ///
    /// # Return
    ///
    /// A vector of valid packets with their MAC headers parsed. The packets are of type
    /// `Packet<MacHeader, EmptyMetadata>`.
    #[allow(unused_assignments)]
    fn parse_mac_headers(
        &self,
        mut packets: Vec<Packet<NullHeader, EmptyMetadata>>,
    ) -> Vec<Packet<MacHeader, EmptyMetadata>> {
        // This vector will hold the set of *valid* parsed packets.
        let mut parsed_packets = Vec::with_capacity(self.max_rx_packets as usize);
        // This vector will hold the set of invalid parsed packets.
        let mut ignore_packets = Vec::with_capacity(self.max_rx_packets as usize);

        // Parse the MacHeader on each packet, and check if it is valid.
        while let Some(packet) = packets.pop() {
            let mut valid: bool = true;
            let packet = packet.parse_header::<MacHeader>();

            // The following block borrows the MAC header from the parsed
            // packet, and checks if the ethertype on it matches what the
            // server expects.
            {
                let mac_header: &MacHeader = packet.get_header();
                valid = common::PACKET_ETYPE.eq(&mac_header.etype());
            }

            match valid {
                true => {
                    parsed_packets.push(packet);
                }

                false => {
                    ignore_packets.push(packet);
                }
            }
        }

        // Drop any invalid packets.
        self.free_packets(ignore_packets);

        return parsed_packets;
    }

    /// This method parses the IP header on a vector of packets that have
    /// already had their MAC headers parsed. A vector of valid packets with
    /// their IP headers parsed is returned.
    ///
    /// This method drops a packet if:
    ///     - It is not an IPv4 packet,
    ///     - The TTL field on it is 0,
    ///     - It's destination IP address does not match that of the server,
    ///     - It's IP header and payload are not long enough.
    ///
    /// # Arguments
    ///
    /// * `packets`: A vector of packets with their MAC headers parsed off
    ///              (type Packet<MacHeader, EmptyMetadata>).
    ///
    /// # Return
    ///
    /// A vector of packets with their IP headers parsed, and wrapped up in Netbrick's
    /// `Packet<MacHeader, EmptyMetadata>` type.
    #[allow(unused_assignments)]
    fn parse_ip_headers(
        &self,
        mut packets: Vec<Packet<MacHeader, EmptyMetadata>>,
    ) -> Vec<Packet<IpHeader, EmptyMetadata>> {
        // This vector will hold the set of *valid* parsed packets.
        let mut parsed_packets = Vec::with_capacity(self.max_rx_packets as usize);
        // This vector will hold the set of invalid parsed packets.
        let mut ignore_packets = Vec::with_capacity(self.max_rx_packets as usize);

        // Parse the IpHeader on each packet, and check if it is valid.
        while let Some(packet) = packets.pop() {
            let mut valid: bool = true;
            let packet = packet.parse_header::<IpHeader>();

            // The following block borrows the Ip header from the parsed
            // packet, and checks if it is valid. A packet is considered
            // valid if:
            //      - It is an IPv4 packet,
            //      - It's TTL (time to live) is greater than zero,
            //      - It is not long enough,
            //      - It's destination Ip address matches that of the server.
            {
                const MIN_LENGTH_IP: u16 = common::PACKET_IP_LEN + 2;
                let ip_header: &IpHeader = packet.get_header();
                valid = (ip_header.version() == 4)
                    && (ip_header.ttl() > 0)
                    && (ip_header.length() >= MIN_LENGTH_IP)
                    && (ip_header.dst() == self.network_ip_addr);
                if !valid {
                    trace!(
                        "port {} ip {} drop invalid req: ip hdr {:?}",
                        self.id,
                        self.network_ip_addr,
                        ip_header
                    );
                } else {
                    trace!("port {} recv from ip {}", self.id, ip_header.src());
                }
            }

            match valid {
                true => {
                    parsed_packets.push(packet);
                }

                false => {
                    ignore_packets.push(packet);
                }
            }
        }

        // Drop any invalid packets.
        self.free_packets(ignore_packets);

        return parsed_packets;
    }

    /// This function parses the UDP headers on a vector of packets that have
    /// had their IP headers parsed. A vector of valid packets with their UDP
    /// headers parsed is returned.
    ///
    /// A packet is dropped by this method if:
    ///     - It's destination UDP port does not match that of the server,
    ///     - It's UDP header plus payload is not long enough.
    ///
    /// # Arguments
    ///
    /// * `packets`: A vector of packets with their IP headers parsed.
    ///
    /// # Return
    ///
    /// A vector of packets with their UDP headers parsed. These packets are wrapped in Netbrick's
    /// `Packet<UdpHeader, EmptyMetadata>` type.
    #[allow(unused_assignments)]
    fn parse_udp_headers(
        &self,
        mut packets: Vec<Packet<IpHeader, EmptyMetadata>>,
    ) -> Vec<Packet<UdpHeader, EmptyMetadata>> {
        // This vector will hold the set of *valid* parsed packets.
        let mut parsed_packets = Vec::with_capacity(self.max_rx_packets as usize);
        // This vector will hold the set of invalid parsed packets.
        let mut ignore_packets = Vec::with_capacity(self.max_rx_packets as usize);

        // Parse the UdpHeader on each packet, and check if it is valid.
        while let Some(packet) = packets.pop() {
            let mut valid: bool = true;
            let packet = packet.parse_header::<UdpHeader>();

            // This block borrows the UDP header from the parsed packet, and
            // checks if it is valid. A packet is considered valid if:
            //      - It is not long enough,
            {
                const MIN_LENGTH_UDP: u16 = common::PACKET_UDP_LEN + 2;
                let udp_header: &UdpHeader = packet.get_header();
                valid = udp_header.length() >= MIN_LENGTH_UDP;
            }

            match valid {
                true => {
                    parsed_packets.push(packet);
                }

                false => {
                    ignore_packets.push(packet);
                }
            }
        }

        // Drop any invalid packets.
        self.free_packets(ignore_packets);

        return parsed_packets;
    }

    fn create_response(
        &self,
        request: &Packet<UdpHeader, EmptyMetadata>,
    ) -> Option<Packet<UdpHeader, EmptyMetadata>> {
        if let Some(response) = new_packet() {
            let mut response = response
                .push_header(&self.resp_mac_header)
                .expect("ERROR: Failed to add response MAC header")
                .push_header(&self.resp_ip_header)
                .expect("ERROR: Failed to add response IP header")
                .push_header(&self.resp_udp_header)
                .expect("ERROR: Failed to add response UDP header");

            // Set the destination port on the response UDP header.
            response
                .get_mut_header()
                .set_src_port(request.get_header().dst_port());
            response
                .get_mut_header()
                .set_dst_port(request.get_header().src_port());
            Some(response)
        } else {
            warn!("Failed to allocate packet for response");
            None
        }
    }

    /// This method dispatches requests to the appropriate service. A response
    /// packet is pre-allocated by this method and handed in along with the
    /// request. Once the service returns, this method frees the request packet.
    ///
    /// # Arguments
    ///
    /// * `requests`: A vector of packets parsed upto and including their UDP
    ///               headers that will be dispatched to the appropriate
    ///               service.
    fn dispatch_requests(&mut self, mut requests: Vec<Packet<UdpHeader, EmptyMetadata>>) {
        // This vector will hold the set of packets that were for either an invalid service or
        // operation.
        // trace!("recv {} reqs", requests.len());
        let mut ignore_packets = Vec::with_capacity(self.max_rx_packets as usize);

        // This vector will hold response packets of native requests.
        // It is then appended to scheduler's 'responses' queue
        // so these reponses can be sent out next time dispatch task is run.
        let mut native_responses = Vec::new();

        while let Some(request) = requests.pop() {
            // Set the destination ip address on the response IP header.
            let ip = request.deparse_header(common::IP_HDR_LEN);
            let from = ip.get_header().src();
            // self.resp_ip_header.set_src(ip.get_header().dst());
            self.resp_ip_header.set_dst(ip.get_header().src());
            // Set the destination mac address on the response MAC header.
            let mac = ip.deparse_header(common::MAC_HDR_LEN);
            // self.resp_mac_header.set_src(mac.get_header().dst());
            self.resp_mac_header.set_dst(mac.get_header().src());

            let request = mac.parse_header::<IpHeader>().parse_header::<UdpHeader>();
            if parse_rpc_service(&request) == wireformat::Service::MasterService {
                // The request is for Master, get it's opcode, and call into Master.
                let opcode = parse_rpc_opcode(&request);
                trace!(
                    "port {} dispatch req {:?} from ip {}",
                    self.id,
                    opcode,
                    from
                );
                match opcode {
                    OpCode::SandstormGetRpc => {
                        let num_responses =
                            self.master_service.value_len / self.master_service.record_len;
                        let mut responses = vec![];
                        for _ in 0..num_responses {
                            responses.push(self.create_response(&request).unwrap());
                        }
                        match self.master_service.get(request, responses) {
                            Ok(task) => self.scheduler.enqueue(task),
                            Err((req, resps)) => {
                                // Master returned an error. The allocated request and response packets
                                // need to be freed up.
                                warn!("failed to dispatch req");
                                req.free_packet();
                                for resp in resps.into_iter() {
                                    resp.free_packet();
                                }
                            }
                        }
                    }
                    OpCode::SandstormInvokeRpc => {
                        let response = self.create_response(&request).unwrap();
                        match self.master_service.dispatch_invoke(request, response) {
                            Ok(task) => self.scheduler.enqueue(task),
                            Err((req, res)) => {
                                // Master returned an error. The allocated request and response packets
                                // need to be freed up.
                                req.free_packet();
                                res.free_packet();
                            }
                        }
                    }
                    #[cfg(feature = "queue_len")]
                    OpCode::TerminateRpc => {
                        self.scheduler.terminate.store(true, Ordering::Relaxed);
                        request.free_packet();
                    }
                    _ => request.free_packet(),
                }
            } else {
                // The request is not for Master. The allocated request and response packets need
                // to be freed up.
                trace!("drop req from ip {}", from);
                ignore_packets.push(request);
                // ignore_packets.push(response);
            }
            /*
            // Allocate a packet for the response upfront, and add in MAC, IP, and UDP headers.
            if let Some(response) = new_packet() {
                let mut response = response
                    .push_header(&self.resp_mac_header)
                    .expect("ERROR: Failed to add response MAC header")
                    .push_header(&self.resp_ip_header)
                    .expect("ERROR: Failed to add response IP header")
                    .push_header(&self.resp_udp_header)
                    .expect("ERROR: Failed to add response UDP header");

                // Set the destination port on the response UDP header.
                response
                    .get_mut_header()
                    .set_src_port(request.get_header().dst_port());
                response
                    .get_mut_header()
                    .set_dst_port(request.get_header().src_port());

                if parse_rpc_service(&request) == wireformat::Service::MasterService {
                    // The request is for Master, get it's opcode, and call into Master.
                    let opcode = parse_rpc_opcode(&request);
                    trace!(
                        "port {} dispatch req {:?} from ip {}",
                        self.id,
                        opcode,
                        from
                    );
                    if !FAST_PATH {
                        match self.master_service.dispatch(opcode, request, response) {
                            Ok(task) => {
                                self.scheduler.enqueue(task);
                            }

                            Err((req, res)) => {
                                // Master returned an error. The allocated request and response packets
                                // need to be freed up.
                                warn!("failed to dispatch req");
                                ignore_packets.push(req);
                                ignore_packets.push(res);
                            }
                        }
                    } else {
                        match opcode {
                            wireformat::OpCode::SandstormInvokeRpc => {
                                // The request is for invoke. Dispatch RPC to its handler.
                                match self.master_service.dispatch_invoke(request, response) {
                                    Ok(task) => {
                                        self.scheduler.enqueue(task);
                                    }

                                    Err((req, res)) => {
                                        // Master returned an error. The allocated request and response packets
                                        // need to be freed up.
                                        ignore_packets.push(req);
                                        ignore_packets.push(res);
                                    }
                                }
                            }

                            wireformat::OpCode::SandstormGetRpc
                            | wireformat::OpCode::SandstormPutRpc
                            | wireformat::OpCode::SandstormMultiGetRpc => {
                                // The request is native. Service it right away.
                                match self
                                    .master_service
                                    .service_native(opcode, request, response)
                                {
                                    Ok((req, res)) => {
                                        // Free request packet.
                                        req.free_packet();

                                        // Push response packet on the local queue of responses that are ready to be sent out.
                                        let resp = rpc::fixup_header_length_fields(res);
                                        // assert_eq!(
                                        //     resp.get_header().dst(),
                                        //     self.resp_ip_header.dst(),
                                        //     "self.resp_ip {}",
                                        //     self.resp_ip_header.dst()
                                        // );
                                        native_responses.push(resp);
                                    }

                                    Err((req, res)) => {
                                        // Master returned an error. The allocated request and response packets
                                        // need to be freed up.
                                        ignore_packets.push(req);
                                        ignore_packets.push(res);
                                    }
                                }
                            }

                            _ => {
                                // The request is unknown.
                                ignore_packets.push(request);
                                ignore_packets.push(response);
                            }
                        }
                    }
                } else {
                    // The request is not for Master. The allocated request and response packets need
                    // to be freed up.
                    trace!("drop req from ip {}", from);
                    ignore_packets.push(request);
                    ignore_packets.push(response);
                }
            } else {
                println!("ERROR: Failed to allocate packet for response");
            }
            */
        }

        // Enqueue completed native resps on to scheduler's responses queue
        self.scheduler.append_resps(&mut native_responses);

        // Free the set of ignored packets.
        self.free_packets(ignore_packets);
    }

    #[inline]
    fn dispatch(&mut self, packets: Vec<Packet<NullHeader, EmptyMetadata>>) -> u64 {
        #[cfg(feature = "dispatch")]
        self.cycle_counter.rx_tx.stop(packets.len() as u64);

        // Perform basic network processing on the received packets.
        #[cfg(feature = "dispatch")]
        self.cycle_counter.parse.start();
        let packets = self.parse_mac_headers(packets);
        // trace!("after mac check {} pkts",packets.len());
        let packets = self.parse_ip_headers(packets);
        // trace!("after ip check {} pkts",packets.len());
        let packets = self.parse_udp_headers(packets);
        // trace!("after udp check {} pkts",packets.len());
        let count = packets.len();
        #[cfg(feature = "dispatch")]
        self.cycle_counter.parse.stop(count as u64);
        // Dispatch these packets to the appropriate service.
        #[cfg(feature = "dispatch")]
        self.cycle_counter.dispatch.start();
        self.dispatch_requests(packets);
        #[cfg(feature = "dispatch")]
        self.cycle_counter.dispatch.stop(count as u64);
        count as u64
    }

    /// This method polls the dispatchers network port for any received packets,
    /// dispatches them to the appropriate service, and sends out responses over
    /// the network port.
    ///
    /// # Return
    ///
    /// The number of packets received.
    #[inline]
    fn poll(&mut self) -> u64 {
        // First, send any pending response packets out.
        #[cfg(feature = "dispatch")]
        self.cycle_counter.rx_tx.start();
        let responses = self.scheduler.responses();
        if responses.len() > 0 {
            // self.scheduler.last_tx.set(cycles::rdtsc());
            self.try_send_packets(responses);
        }

        // Next, try to receive packets from the network.
        if let Some(packets) = self.try_receive_packets() {
            self.dispatch(packets)
        } else if let Some(stolen) = self.try_steal_packets() {
            self.dispatch(stolen)
        } else {
            0
        }
    }
}

// Implementation of the Task trait for Dispatch. This will allow Dispatch to be scheduled by the
// database.
impl<T> Task for Dispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    /// Refer to the `Task` trait for Documentation.
    fn run(&mut self) -> (TaskState, u64) {
        let start = cycles::rdtsc();

        // Run the dispatch task, polling for received packets and sending out pending responses.
        self.state = TaskState::RUNNING;
        let count = self.poll();
        self.state = TaskState::YIELDED;

        // Update the time the task spent executing and return.
        let exec = cycles::rdtsc() - start;

        self.time += exec;
        // #[cfg(feature = "dispatch")]
        self.cycle_counter.poll.total_cycles(exec, count);

        #[cfg(feature = "dispatch")]
        COUNTER.with(|count_a| {
            let mut count = count_a.borrow_mut();
            if _count > 0 {
                *count += 1;
            }
            let every = 1000000;
            if *count >= every {
                info!(
                    "Poll {}, RX-TX {}, Parse {}, Dispatch {}",
                    self.cycle_counter.poll.get_average(),
                    self.cycle_counter.rx_tx.get_average(),
                    self.cycle_counter.parse.get_average(),
                    self.cycle_counter.dispatch.get_average()
                );
                *count = 0;
            }
        });
        return (self.state, self.cycle_counter.poll.get_average());
    }

    /// Refer to the `Task` trait for Documentation.
    fn state(&self) -> TaskState {
        self.state.clone()
    }

    /// Refer to the `Task` trait for Documentation.
    fn time(&self) -> u64 {
        self.time.clone()
    }

    /// Refer to the `Task` trait for Documentation.
    fn db_time(&self) -> u64 {
        return 0;
    }

    /// Refer to the `Task` trait for Documentation.
    fn priority(&self) -> TaskPriority {
        self.priority.clone()
    }

    /// Refer to the `Task` trait for Documentation.
    unsafe fn tear(
        &mut self,
        _server_load: &mut f64,
        _task_duration_cv: f64,
    ) -> Option<(
        Packet<UdpHeader, EmptyMetadata>,
        Vec<Packet<UdpHeader, EmptyMetadata>>,
    )> {
        // The Dispatch task does not return any packets.
        None
    }

    /// Refer to the `Task` trait for Documentation.
    fn set_state(&mut self, state: TaskState) {
        self.state = state;
    }

    /// Refer to the `Task` trait for Documentation.
    fn update_cache(&mut self, _record: &[u8], _seg_id: usize, _num_segs: usize) -> bool {
        true
    }

    /// Refer to the `Task` trait for Documentation.
    fn get_id(&self) -> u64 {
        0
    }

    fn set_time(&mut self, _overhead: u64) {}
}

impl<T> Drop for Dispatch<T>
where
    T: PacketRx + PacketTx + Display + Clone + 'static,
{
    fn drop(&mut self) {
        let responses = self.scheduler.responses();
        if responses.len() > 0 {
            self.try_send_packets(responses);
        }
    }
}

// pub trait Resp {
//     fn send_resps(&self);
// }

// impl<T> Resp for Dispatch<T>
// where
//     T: PacketRx + PacketTx + Display + Clone + 'static,
// {
//     fn send_resps(&self) {
//         if let Some(responses) = self.scheduler.pending_resps() {
//             self.send_resps(responses);
//         }
//     }
// }
*/
