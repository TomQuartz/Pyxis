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
extern crate rand;
use std::cell::Cell;
use std::fmt::Display;
use std::net::Ipv4Addr;
use std::str::FromStr;

use self::rand::{Rng, SeedableRng, XorShiftRng};
use db::config;
use db::config::*;
use db::e2d2::allocators::*;
use db::e2d2::common::EmptyMetadata;
use db::e2d2::headers::*;
use db::e2d2::interface::*;
use db::log::*;
use db::rpc;
use db::wireformat::*;
use std::cell::RefCell;
use std::collections::HashMap;

use db::cycles;
use db::master::Master;
use db::rpc::*;
use sandstorm::common;
use sched::TaskManager;
use std::rc::Rc;
use std::sync::Arc;

use db::e2d2::scheduler::Executable;

/// A simple RPC request generator for Sandstorm.
// TODO: the dst_ports available for each endpoint may be different, make dst_ports a vec
pub struct Sender {
    // The network interface over which requests will be sent out.
    net_port: CacheAligned<PortQueue>,
    // number of endpoints, either storage or compute
    num_endpoints: usize,
    // udp+ip+mac header for sending reqs
    req_hdrs: Vec<PacketHeaders>,
    // The number of destination UDP ports a req can be sent to.
    dst_ports: Vec<u16>,
    // // Mapping from type to dst_ports(cores)
    // type2core: HashMap<usize, Vec<u16>>,
    // Rng generator
    rng: RefCell<XorShiftRng>,
    // // Tracks number of packets sent to the server for occasional debug messages.
    // requests_sent: Cell<u64>,
}

impl Sender {
    /// create sender from src and several endpoints
    pub fn new(
        net_port: CacheAligned<PortQueue>,
        src: &NetConfig,
        endpoints: &Vec<NetConfig>,
    ) -> Sender {
        Sender {
            num_endpoints: endpoints.len(),
            req_hdrs: PacketHeaders::get_req_hdrs(src, net_port.txq() as u16, endpoints),
            net_port: net_port, //.clone()
            // TODO: make this a vector, different number of ports for each storage endpoint
            dst_ports: endpoints.iter().map(|x| x.num_ports).collect(),
            rng: {
                let seed: [u32; 4] = rand::random::<[u32; 4]>();
                RefCell::new(XorShiftRng::from_seed(seed))
            },
        }
    }

    fn get_endpoint(&self, _key: &[u8]) -> usize {
        self.rng.borrow_mut().gen::<usize>() % self.num_endpoints
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
    // TODO: hash the target storage with LSB in key
    // for now, the vec len of req_hdrs=1
    pub fn send_get(&self, tenant: u32, table: u64, key: &[u8], id: u64) {
        let endpoint = self.get_endpoint(key);
        let request = rpc::create_get_rpc(
            &self.req_hdrs[endpoint].mac_header,
            &self.req_hdrs[endpoint].ip_header,
            &self.req_hdrs[endpoint].udp_header,
            tenant,
            table,
            key,
            id,
            self.get_dst_port(endpoint),
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
        let endpoint = self.get_endpoint(key);
        let request = rpc::create_get_rpc(
            &self.req_hdrs[endpoint].mac_header,
            &self.req_hdrs[endpoint].ip_header,
            &self.req_hdrs[endpoint].udp_header,
            tenant,
            table,
            key,
            id,
            self.get_dst_port(endpoint),
            GetGenerator::SandstormExtension,
        );
        self.send_pkt(request);
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
        let endpoint = self.get_endpoint(key);
        let request = rpc::create_put_rpc(
            &self.req_hdrs[endpoint].mac_header,
            &self.req_hdrs[endpoint].ip_header,
            &self.req_hdrs[endpoint].udp_header,
            tenant,
            table,
            key,
            val,
            id,
            self.get_dst_port(endpoint),
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
        let endpoint = self.get_endpoint(keys);
        let request = rpc::create_multiget_rpc(
            &self.req_hdrs[endpoint].mac_header,
            &self.req_hdrs[endpoint].ip_header,
            &self.req_hdrs[endpoint].udp_header,
            tenant,
            table,
            k_len,
            n_keys,
            keys,
            id,
            self.get_dst_port(endpoint),
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
        _type_idx: usize,
    ) {
        let endpoint = self.get_endpoint(payload);
        let request = rpc::create_invoke_rpc(
            &self.req_hdrs[endpoint].mac_header,
            &self.req_hdrs[endpoint].ip_header,
            &self.req_hdrs[endpoint].udp_header,
            tenant,
            name_len,
            payload,
            id,
            self.get_dst_port(endpoint),
            // self.get_dst_port_by_type(type_idx),
            // (id & 0xffff) as u16 & (self.dst_ports - 1),
        );
        trace!("send req to dst ip {}", self.req_hdrs[endpoint].ip_header.dst_ip);
        self.send_pkt(request);
    }

    /// Computes the destination UDP port given a tenant identifier.
    #[inline]
    fn get_dst_port(&self, endpoint: usize) -> u16 {
        // The two least significant bytes of the tenant id % the total number of destination
        // ports.
        // (tenant & 0xffff) as u16 & (self.dst_ports - 1)
        self.rng.borrow_mut().gen::<u16>() % self.dst_ports[endpoint]
    }

    // #[inline]
    // fn get_dst_port_by_type(&self, type_idx: usize) -> u16 {
    //     let target_cores = &self.type2core[&type_idx];
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

            // Extract Mbuf's from the batch of packets.
            while let Some(packet) = packets.pop() {
                mbufs.push(packet.get_mbuf());
            }

            // Send out the above MBuf's.
            match self.net_port.send(&mut mbufs) {
                Ok(sent) => {
                    if sent < num_packets as u32 {
                        warn!("Was able to send only {} of {} packets.", sent, num_packets);
                    }

                    // self.responses_sent += mbufs.len() as u64;
                }

                Err(ref err) => {
                    error!("Error on packet send: {}", err);
                }
            }
        }
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

/// A Receiver of responses to RPC requests.
pub struct Receiver {
    // The network interface over which responses will be received from.
    net_port: CacheAligned<PortQueue>,
    // Sibling port
    sib_port: Option<CacheAligned<PortQueue>>,
    // stealing
    stealing: bool,
    // The maximum number of packets that can be received from the network port in one shot.
    max_rx_packets: usize,
    // // The total number of responses received.
    // responses_recv: Cell<u64>,
}

// Implementation of methods on Receiver.
impl Receiver {
    pub fn recv(&self) -> Option<Vec<Packet<UdpHeader, EmptyMetadata>>> {
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
                let packet = packet_from_mbuf_no_increment(*mbuf, 0)
                    .parse_header::<MacHeader>()
                    .parse_header::<IpHeader>()
                    .parse_header::<UdpHeader>();

                packets.push(packet);
            }

            return Some(packets);
        }
    }
}

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

    fn get_req_hdrs(
        src: &NetConfig,
        src_udp_port: u16,
        endpoints: &Vec<NetConfig>,
    ) -> Vec<PacketHeaders> {
        let mut req_hdrs = vec![];
        for dst in endpoints.iter() {
            let mut pkthdr = PacketHeaders::new(src, dst);
            // we need to fix the src port for req_hdrs, which is used as the dst port in resp_hdr at rpc endpoint
            pkthdr.udp_header.set_src_port(src_udp_port);
            // NOTE: for now, sender only use vec[0]
            req_hdrs.push(pkthdr);
        }
        req_hdrs
    }
}

pub struct LBDispatcher {
    pub sender2compute: Sender,
    pub sender2storage: Sender,
    pub receiver: Receiver,
}

impl LBDispatcher {
    pub fn new(
        config: &LBConfig,
        net_port: CacheAligned<PortQueue>,
        max_rx_packets: usize,
    ) -> LBDispatcher {
        LBDispatcher {
            sender2compute: Sender::new(net_port.clone(), &config.src, &config.compute),
            sender2storage: Sender::new(net_port.clone(), &config.src, &config.storage),
            receiver: Receiver {
                net_port: net_port,
                sib_port: None,
                stealing: false,
                max_rx_packets: max_rx_packets,
            },
        }
    }
}

/// sender&receiver for compute node
// 1. does not precompute mac and ip addr for LB and storage
// 2. repeated parse and reparse header
pub struct ComputeNodeDispatcher {
    sender: Rc<Sender>,
    receiver: Receiver,
    manager: TaskManager,
    // this is dynamic, i.e. resp dst is set as req src upon recving new reqs
    resp_hdr: PacketHeaders,
}

// TODO: move req_ports& into config?
impl ComputeNodeDispatcher {
    pub fn new(
        config: &ComputeConfig,
        masterservice: Arc<Master>,
        net_port: CacheAligned<PortQueue>,
        sib_port: Option<CacheAligned<PortQueue>>,
        // req_ports: u16, // for sender
        max_rx_packets: usize,
    ) -> ComputeNodeDispatcher {
        ComputeNodeDispatcher {
            sender: Rc::new(Sender::new(net_port.clone(), &config.src, &config.storage)),
            receiver: Receiver {
                stealing: !sib_port.is_none(),
                net_port: net_port,
                sib_port: sib_port,
                max_rx_packets: max_rx_packets,
            },
            manager: TaskManager::new(Arc::clone(&masterservice)),
            resp_hdr: PacketHeaders::new(&config.src, &NetConfig::default()),
        }
    }

    pub fn poll(&mut self) {
        if let Some(mut packets) = self.receiver.recv() {
            trace!("recv {} packets", packets.len());
            while let Some(packet) = packets.pop() {
                self.dispatch(packet);
            }
        }
    }

    pub fn run_extensions(&mut self) {
        self.manager.execute_tasks();
    }

    pub fn send_resps(&mut self) {
        let resps = &mut self.manager.responses;
        if resps.len() > 0 {
            self.sender.send_pkts(resps);
        }
    }

    // 1. invoke_req: lb to compute
    // 2. get_resp: storage to compute
    pub fn dispatch(&mut self, packet: Packet<UdpHeader, EmptyMetadata>) {
        match parse_rpc_opcode(&packet) {
            OpCode::SandstormInvokeRpc => {
                trace!("recv invoke rpc");
                self.dispatch_invoke(packet);
            }
            OpCode::SandstormGetRpc => {
                self.process_get_resp(packet);
            }
            _ => {}
        }
    }

    fn process_get_resp(&mut self, response: Packet<UdpHeader, EmptyMetadata>) {
        let p = response.parse_header::<GetResponse>();
        let hdr = p.get_header();
        let timestamp = hdr.common_header.stamp; // this is the timestamp when this ext is inserted in taskmanager
        let table_id = hdr.table_id as usize;
        let records = p.get_payload();
        let recordlen = records.len();
        self.manager
            .update_rwset(timestamp, table_id, records, recordlen);
        p.free_packet();
    }

    fn dispatch_invoke(&mut self, request: Packet<UdpHeader, EmptyMetadata>) {
        let ip = request.deparse_header(common::IP_HDR_LEN);
        // self.resp_hdr.ip_header.set_src(ip.get_header().dst());
        self.resp_hdr.ip_header.set_dst(ip.get_header().src());

        // Set the destination mac address on the response MAC header.
        let mac = ip.deparse_header(common::MAC_HDR_LEN);
        // self.resp_hdr.mac_header.set_src(mac.get_header().dst());
        self.resp_hdr.mac_header.set_dst(mac.get_header().src());

        let request = mac.parse_header::<IpHeader>().parse_header::<UdpHeader>();

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
            self.add_to_manager(request, response);
        } else {
            println!("ERROR: Failed to allocate packet for response");
        }
    }

    fn add_to_manager(
        &mut self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) {
        let req = req.parse_header::<InvokeRequest>();
        let hdr = req.get_header();
        let tenant_id = hdr.common_header.tenant;
        let name_length = hdr.name_length as usize;
        let args_length = hdr.args_length as usize;
        let rpc_stamp = hdr.common_header.stamp;

        let res = res
            .push_header(&InvokeResponse::new(
                rpc_stamp,
                OpCode::SandstormInvokeRpc,
                tenant_id,
            ))
            .expect("Failed to push InvokeResponse");
        trace!("add req to manager");
        self.manager.create_task(
            cycles::rdtsc(),
            req,
            res,
            tenant_id,
            name_length,
            self.sender.clone(),
        );
    }
}

impl Executable for ComputeNodeDispatcher {
    fn execute(&mut self) {
        self.poll();
        self.run_extensions();
        self.send_resps();
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

impl Drop for ComputeNodeDispatcher {
    fn drop(&mut self) {
        self.send_resps();
    }
}

// pub trait ShipData

// pub trait NetPort {
//     type Port;
// }

// pub trait SendRecv {
//     fn send();
//     fn recv();
// }

// impl<T> SendRecv for T
// where
//     T: NetPort,
//     <T as NetPort>::Port: PacketTx + PacketRx,
// {
//     fn send() {}
//     fn recv() {}
// }
