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
use sandstorm::common;
// use sched::TaskManager;
use std::rc::Rc;
use std::sync::Arc;

use atomic_float::AtomicF64;
use db::dispatch::{Queue, Receiver, Sender};
use db::e2d2::scheduler::Executable;
use db::sched::MovingTimeAvg;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::RwLock;

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
    pub fn recv(&mut self) -> Option<Vec<Packet<UdpHeader, EmptyMetadata>>> {
        if let Some(mut packets) = self.receiver.recv() {
            let num_recvd = packets.len();
            if num_recvd > 0 {
                let mut queue = self.queue.queue.write().unwrap();
                let mut get_resps = vec![];
                for (packet, _) in packets.into_iter() {
                    if rpc::parse_rpc_opcode(&packet) == OpCode::SandstormGetRpc {
                        get_resps.push(packet);
                    } else {
                        queue.push_back(packet);
                    }
                }
                // queue.extend(packets.into_iter().map(|(packet, _)| packet));
                self.length = num_recvd as f64;
                return Some(get_resps);
            }
        }
        self.length = 0.0;
        None
    }
    pub fn reset(&mut self) -> bool {
        if self.receiver.reset {
            self.receiver.reset = false;
            true
        } else {
            false
        }
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

// /// A simple RPC request generator for Sandstorm.
// // TODO: the dst_ports available for each endpoint may be different, make dst_ports a vec
// pub struct Sender {
//     // The network interface over which requests will be sent out.
//     net_port: CacheAligned<PortQueue>,
//     // number of endpoints, either storage or compute
//     num_endpoints: usize,
//     // udp+ip+mac header for sending reqs
//     req_hdrs: Vec<PacketHeaders>,
//     // The number of destination UDP ports a req can be sent to.
//     dst_ports: Vec<u16>,
//     // // Mapping from type to dst_ports(cores)
//     // type2core: HashMap<usize, Vec<u16>>,
//     // Rng generator
//     rng: RefCell<XorShiftRng>,
//     // // Tracks number of packets sent to the server for occasional debug messages.
//     // requests_sent: Cell<u64>,
//     // shared_credits: Arc<RwLock<u32>>,
//     // buffer: RefCell<VecDeque<Packet<IpHeader, EmptyMetadata>>>,
// }

// impl Sender {
//     /// create sender from src and several endpoints
//     pub fn new(
//         net_port: CacheAligned<PortQueue>,
//         src: &NetConfig,
//         endpoints: &Vec<NetConfig>,
//         // shared_credits: Arc<RwLock<u32>>,
//     ) -> Sender {
//         Sender {
//             // id: net_port.rxq() as usize,
//             num_endpoints: endpoints.len(),
//             req_hdrs: PacketHeaders::create_hdrs(src, net_port.txq() as u16, endpoints),
//             net_port: net_port, //.clone()
//             // TODO: make this a vector, different number of ports for each storage endpoint
//             dst_ports: endpoints.iter().map(|x| x.num_ports).collect(),
//             rng: {
//                 let seed: [u32; 4] = rand::random::<[u32; 4]>();
//                 RefCell::new(XorShiftRng::from_seed(seed))
//             },
//             // shared_credits: shared_credits,
//             // buffer: RefCell::new(VecDeque::new()),
//         }
//     }

//     fn get_endpoint(&self, _key: &[u8]) -> usize {
//         self.rng.borrow_mut().gen::<usize>() % self.num_endpoints
//     }
//     /*
//     pub fn return_credit(&self) {
//         // let mut buffer = self.buffer.borrow_mut();
//         // if let Some(request) = buffer.pop_front() {
//         //     self.send_pkt(request);
//         // } else {
//         //     let credits = self.credits.get();
//         //     self.credits.set(credits + 1);
//         // }
//         let mut shared_credits = self.shared_credits.write().unwrap();
//         *shared_credits += 1;
//     }

//     pub fn try_send_packets(&self) {
//         let has_credit = *self.shared_credits.read().unwrap() > 0;
//         if !has_credit {
//             return;
//         }
//         let mut buffer = self.buffer.borrow_mut();
//         loop {
//             if buffer.len() == 0 {
//                 break;
//             }
//             let acquired = {
//                 let mut current_credits = self.shared_credits.write().unwrap();
//                 if *current_credits > 0 {
//                     *current_credits -= 1;
//                     true
//                 } else {
//                     false
//                 }
//             };
//             if acquired {
//                 let request = buffer.pop_front().unwrap();
//                 self.send_pkt(request);
//                 trace!("send req from buffer");
//             } else {
//                 break;
//             }
//         }
//     }
//     */
//     /// Creates and sends out a get() RPC request. Network headers are populated based on arguments
//     /// passed into new() above.
//     ///
//     /// # Arguments
//     ///
//     /// * `tenant`: Id of the tenant requesting the item.
//     /// * `table`:  Id of the table from which the key is looked up.
//     /// * `key`:    Byte string of key whose value is to be fetched. Limit 64 KB.
//     /// * `id`:     RPC identifier.
//     #[allow(dead_code)]
//     // TODO: hash the target storage with LSB in key
//     // for now, the vec len of req_hdrs=1
//     pub fn send_get(&self, tenant: u32, table: u64, key: &[u8], id: u64) {
//         let endpoint = self.get_endpoint(key);
//         let request = rpc::create_get_rpc(
//             &self.req_hdrs[endpoint].mac_header,
//             &self.req_hdrs[endpoint].ip_header,
//             &self.req_hdrs[endpoint].udp_header,
//             tenant,
//             table,
//             key,
//             id,
//             self.get_dst_port(endpoint),
//             GetGenerator::SandstormClient,
//         );

//         self.send_pkt(request);
//     }

//     /// Creates and sends out a get() RPC request. Network headers are populated based on arguments
//     /// passed into new() above.
//     ///
//     /// # Arguments
//     ///
//     /// * `tenant`: Id of the tenant requesting the item.
//     /// * `table`:  Id of the table from which the key is looked up.
//     /// * `key`:    Byte string of key whose value is to be fetched. Limit 64 KB.
//     /// * `id`:     RPC identifier.
//     #[allow(dead_code)]
//     pub fn send_get_from_extension(&self, tenant: u32, table: u64, key: &[u8], id: u64) {
//         let endpoint = self.get_endpoint(key);
//         let request = rpc::create_get_rpc(
//             &self.req_hdrs[endpoint].mac_header,
//             &self.req_hdrs[endpoint].ip_header,
//             &self.req_hdrs[endpoint].udp_header,
//             tenant,
//             table,
//             key,
//             id,
//             self.get_dst_port(endpoint),
//             GetGenerator::SandstormExtension,
//         );
//         trace!(
//             "ext id: {} send key {:?}, from {}, to {}",
//             id,
//             key,
//             self.req_hdrs[endpoint].ip_header.src(),
//             self.req_hdrs[endpoint].ip_header.dst(),
//         );
//         self.send_pkt(request);
//         // let mut buffer = self.buffer.borrow_mut();
//         // let has_credit = *self.shared_credits.read().unwrap() > 0;
//         // if !has_credit {
//         //     buffer.push_back(request);
//         //     return;
//         // }
//         // let acquired = {
//         //     let mut current_credits = self.shared_credits.write().unwrap();
//         //     if *current_credits > 0 {
//         //         *current_credits -= 1;
//         //         true
//         //     } else {
//         //         false
//         //     }
//         // };
//         // if acquired {
//         //     if buffer.len() > 0 {
//         //         let pending_request = buffer.pop_front().unwrap();
//         //         self.send_pkt(pending_request);
//         //         buffer.push_back(request);
//         //         trace!("send from buffer, push current to queue");
//         //     } else {
//         //         self.send_pkt(request);
//         //         trace!("send directly");
//         //     }
//         // } else {
//         //     buffer.push_back(request);
//         //     trace!("failed to acquire");
//         // }
//     }

//     /// Creates and sends out a put() RPC request. Network headers are populated based on arguments
//     /// passed into new() above.
//     ///
//     /// # Arguments
//     ///
//     /// * `tenant`: Id of the tenant requesting the insertion.
//     /// * `table`:  Id of the table into which the key-value pair is to be inserted.
//     /// * `key`:    Byte string of key whose value is to be inserted. Limit 64 KB.
//     /// * `val`:    Byte string of the value to be inserted.
//     /// * `id`:     RPC identifier.
//     #[allow(dead_code)]
//     pub fn send_put(&self, tenant: u32, table: u64, key: &[u8], val: &[u8], id: u64) {
//         let endpoint = self.get_endpoint(key);
//         let request = rpc::create_put_rpc(
//             &self.req_hdrs[endpoint].mac_header,
//             &self.req_hdrs[endpoint].ip_header,
//             &self.req_hdrs[endpoint].udp_header,
//             tenant,
//             table,
//             key,
//             val,
//             id,
//             self.get_dst_port(endpoint),
//         );

//         self.send_pkt(request);
//     }

//     /// Creates and sends out a multiget() RPC request. Network headers are populated based on
//     /// arguments passed into new() above.
//     ///
//     /// # Arguments
//     ///
//     /// * `tenant`: Id of the tenant requesting the item.
//     /// * `table`:  Id of the table from which the key is looked up.
//     /// * `k_len`:  The length of each key to be looked up at the server. All keys are
//     ///               assumed to be of equal length.
//     /// * `n_keys`: The number of keys to be looked up at the server.
//     /// * `keys`:   Byte string of keys whose values are to be fetched.
//     /// * `id`:     RPC identifier.
//     #[allow(dead_code)]
//     pub fn send_multiget(
//         &self,
//         tenant: u32,
//         table: u64,
//         k_len: u16,
//         n_keys: u32,
//         keys: &[u8],
//         id: u64,
//     ) {
//         let endpoint = self.get_endpoint(keys);
//         let request = rpc::create_multiget_rpc(
//             &self.req_hdrs[endpoint].mac_header,
//             &self.req_hdrs[endpoint].ip_header,
//             &self.req_hdrs[endpoint].udp_header,
//             tenant,
//             table,
//             k_len,
//             n_keys,
//             keys,
//             id,
//             self.get_dst_port(endpoint),
//         );

//         self.send_pkt(request);
//     }

//     /// Creates and sends out an invoke() RPC request. Network headers are populated based on
//     /// arguments passed into new() above.
//     ///
//     /// # Arguments
//     ///
//     /// * `tenant`:   Id of the tenant requesting the invocation.
//     /// * `name_len`: The number of bytes at the head of the payload corresponding to the
//     ///               extensions name.
//     /// * `payload`:  The RPC payload to be written into the packet. Must contain the name of the
//     ///               extension followed by it's arguments.
//     /// * `id`:       RPC identifier.
//     pub fn send_invoke(
//         &self,
//         tenant: u32,
//         name_len: u32,
//         payload: &[u8],
//         id: u64,
//         _type_id: usize,
//     ) {
//         let endpoint = self.get_endpoint(payload);
//         let request = rpc::create_invoke_rpc(
//             &self.req_hdrs[endpoint].mac_header,
//             &self.req_hdrs[endpoint].ip_header,
//             &self.req_hdrs[endpoint].udp_header,
//             tenant,
//             name_len,
//             payload,
//             id,
//             self.get_dst_port(endpoint),
//             // self.get_dst_port_by_type(type_id),
//             // (id & 0xffff) as u16 & (self.dst_ports - 1),
//         );
//         trace!(
//             "send req to dst ip {}",
//             self.req_hdrs[endpoint].ip_header.dst()
//         );
//         self.send_pkt(request);
//     }
//     #[cfg(feature = "queue_len")]
//     pub fn send_terminate(&self) {
//         for endpoint in 0..self.num_endpoints {
//             let mut request = rpc::create_terminate_rpc(
//                 &self.req_hdrs[endpoint].mac_header,
//                 &self.req_hdrs[endpoint].ip_header,
//                 &self.req_hdrs[endpoint].udp_header,
//                 self.get_dst_port(endpoint),
//                 // self.get_dst_port_by_type(type_id),
//                 // (id & 0xffff) as u16 & (self.dst_ports - 1),
//             );
//             self.send_pkt(request);
//         }
//     }

//     pub fn send_reset(&self) {
//         for endpoint in 0..self.num_endpoints {
//             let mut request = rpc::create_reset_rpc(
//                 &self.req_hdrs[endpoint].mac_header,
//                 &self.req_hdrs[endpoint].ip_header,
//                 &self.req_hdrs[endpoint].udp_header,
//                 self.get_dst_port(endpoint),
//                 // self.get_dst_port_by_type(type_id),
//                 // (id & 0xffff) as u16 & (self.dst_ports - 1),
//             );
//             self.send_pkt(request);
//         }
//     }

//     /// Computes the destination UDP port given a tenant identifier.
//     #[inline]
//     fn get_dst_port(&self, endpoint: usize) -> u16 {
//         // The two least significant bytes of the tenant id % the total number of destination
//         // ports.
//         // (tenant & 0xffff) as u16 & (self.dst_ports - 1)
//         self.rng.borrow_mut().gen::<u16>() % self.dst_ports[endpoint]
//     }

//     // #[inline]
//     // fn get_dst_port_by_type(&self, type_id: usize) -> u16 {
//     //     let target_cores = &self.type2core[&type_id];
//     //     let target_idx = self.rng.borrow_mut().gen::<u32>() % target_cores.len() as u32;
//     //     target_cores[target_idx as usize]
//     // }

//     /// Sends a request/packet parsed upto IP out the network interface.
//     #[inline]
//     fn send_pkt(&self, request: Packet<IpHeader, EmptyMetadata>) {
//         // Send the request out the network.
//         unsafe {
//             let mut pkts = [request.get_mbuf()];

//             let sent = self
//                 .net_port
//                 .send(&mut pkts)
//                 .expect("Failed to send packet!");

//             if sent < pkts.len() as u32 {
//                 warn!("Failed to send all packets!");
//             }
//         }

//         // // Update the number of requests sent out by this generator.
//         // let r = self.requests_sent.get();
//         // // if r & 0xffffff == 0 {
//         // //     info!("Sent many requests...");
//         // // }
//         // self.requests_sent.set(r + 1);
//     }

//     pub fn send_pkts(&self, packets: &mut Vec<Packet<IpHeader, EmptyMetadata>>) {
//         trace!("send {} resps", packets.len());
//         unsafe {
//             let mut mbufs = vec![];
//             let num_packets = packets.len();
//             let mut to_send = num_packets;

//             // Extract Mbuf's from the batch of packets.
//             while let Some(packet) = packets.pop() {
//                 mbufs.push(packet.get_mbuf());
//             }

//             // Send out the above MBuf's.
//             loop {
//                 // let sent = self.dispatcher.receiver.net_port.send(&mut mbufs).unwrap() as usize;
//                 let sent = self.net_port.send(&mut mbufs).unwrap() as usize;
//                 // if sent == 0 {
//                 //     warn!(
//                 //         "Was able to send only {} of {} packets.",
//                 //         num_packets - to_send,
//                 //         num_packets
//                 //     );
//                 //     break;
//                 // }
//                 if sent < to_send {
//                     // warn!("Was able to send only {} of {} packets.", sent, num_packets);
//                     to_send -= sent;
//                     mbufs.drain(0..sent as usize);
//                 } else {
//                     break;
//                 }
//             }
//         }
//         // unsafe {
//         //     let mut mbufs = vec![];
//         //     let num_packets = packets.len();

//         //     // Extract Mbuf's from the batch of packets.
//         //     while let Some(packet) = packets.pop() {
//         //         mbufs.push(packet.get_mbuf());
//         //     }

//         //     // Send out the above MBuf's.
//         //     match self.net_port.send(&mut mbufs) {
//         //         Ok(sent) => {
//         //             if sent < num_packets as u32 {
//         //                 warn!("Was able to send only {} of {} packets.", sent, num_packets);
//         //             }

//         //             // self.responses_sent += mbufs.len() as u64;
//         //         }

//         //         Err(ref err) => {
//         //             error!("Error on packet send: {}", err);
//         //         }
//         //     }
//         // }
//     }

//     // /// Creates and sends out a commit() RPC request. Network headers are populated based on
//     // /// arguments passed into new() above.
//     // ///
//     // /// # Arguments
//     // ///
//     // /// * `tenant`:   Id of the tenant requesting the invocation.
//     // /// * `table_id`: The number of bytes at the head of the payload corresponding to the
//     // ///               extensions name.
//     // /// * `payload`:  The RPC payload to be written into the packet. Must contain the name of the
//     // ///               extension followed by it's arguments.
//     // /// * `id`:       RPC identifier.
//     // pub fn send_commit(
//     //     &self,
//     //     tenant: u32,
//     //     table_id: u64,
//     //     payload: &[u8],
//     //     id: u64,
//     //     key_len: u16,
//     //     val_len: u16,
//     // ) {
//     //     let request = rpc::create_commit_rpc(
//     //         &self.req_hdrs[0].mac_header,
//     //         &self.req_hdrs[0].ip_header,
//     //         &self.req_hdrs[0].udp_header,
//     //         tenant,
//     //         table_id,
//     //         payload,
//     //         id,
//     //         self.get_dst_port(tenant),
//     //         key_len,
//     //         val_len,
//     //     );

//     //     self.send_pkt(request);
//     // }
// }

// /// A Receiver of responses to RPC requests.
// pub struct Receiver {
//     // The network interface over which responses will be received from.
//     net_port: CacheAligned<PortQueue>,
//     // Sibling port
//     sib_port: Option<CacheAligned<PortQueue>>,
//     // stealing
//     stealing: bool,
//     // The maximum number of packets that can be received from the network port in one shot.
//     max_rx_packets: usize,
//     // ip_addr
//     ip_addr: u32,
//     // // The total number of responses received.
//     // responses_recv: Cell<u64>,
// }

// // Implementation of methods on Receiver.
// impl Receiver {
//     pub fn new(
//         net_port: CacheAligned<PortQueue>,
//         max_rx_packets: usize,
//         ip_addr: &str,
//     ) -> Receiver {
//         Receiver {
//             net_port: net_port,
//             sib_port: None,
//             stealing: false,
//             max_rx_packets: max_rx_packets,
//             ip_addr: u32::from(Ipv4Addr::from_str(ip_addr).expect("missing src ip for LB")),
//         }
//     }
//     // TODO: makesure src ip is properly set
//     // return pkt and (src ip, src udp port)
//     pub fn recv(&self) -> Option<Vec<(Packet<UdpHeader, EmptyMetadata>, (u32, u16))>> {
//         // Allocate a vector of mutable MBuf pointers into which raw packets will be received.
//         let mut mbuf_vector = Vec::with_capacity(self.max_rx_packets);

//         // This unsafe block is needed in order to populate mbuf_vector with a bunch of pointers,
//         // and subsequently manipulate these pointers. DPDK will take care of assigning these to
//         // actual MBuf's.
//         unsafe {
//             mbuf_vector.set_len(self.max_rx_packets);

//             // Try to receive packets from the network port.
//             let mut recvd = self
//                 .net_port
//                 .recv(&mut mbuf_vector[..])
//                 .expect("Error on packet receive") as usize;

//             // Return if packets weren't available for receive.
//             if recvd == 0 && self.stealing {
//                 recvd = self
//                     .sib_port
//                     .as_ref()
//                     .unwrap()
//                     .recv(&mut mbuf_vector[..])
//                     .expect("Error on packet receive") as usize;
//             }
//             if recvd == 0 {
//                 return None;
//             }
//             trace!("port {} recv {} raw pkts", self.net_port.rxq(), recvd);
//             // // Update the number of responses received.
//             // let r = self.responses_recv.get();
//             // // if r & 0xffffff == 0 {
//             // //     info!("Received many responses...");
//             // // }
//             // self.responses_recv.set(r + 1);

//             // Clear out any dangling pointers in mbuf_vector.
//             mbuf_vector.drain(recvd..self.max_rx_packets);

//             // Vector to hold packets parsed from mbufs.
//             let mut packets = Vec::with_capacity(mbuf_vector.len());

//             // Wrap up the received Mbuf's into Packets. The refcount on the mbuf's were set by
//             // DPDK, and do not need to be bumped up here. Hence, the call to
//             // packet_from_mbuf_no_increment().
//             for mbuf in mbuf_vector.iter_mut() {
//                 let packet = packet_from_mbuf_no_increment(*mbuf, 0);
//                 if let Some(packet) = self.check_mac(packet) {
//                     if let Some((packet, src_ip)) = self.check_ip(packet) {
//                         if let Some((packet, src_port)) = self.check_udp(packet) {
//                             packets.push((packet, (src_ip, src_port)));
//                             continue;
//                         }
//                     }
//                 }
//             }

//             return Some(packets);
//         }
//     }

//     #[inline]
//     fn check_mac(
//         &self,
//         packet: Packet<NullHeader, EmptyMetadata>,
//     ) -> Option<Packet<MacHeader, EmptyMetadata>> {
//         let packet = packet.parse_header::<MacHeader>();
//         // The following block borrows the MAC header from the parsed
//         // packet, and checks if the ethertype on it matches what the
//         // server expects.
//         if packet.get_header().etype().eq(&common::PACKET_ETYPE) {
//             Some(packet)
//         } else {
//             trace!("mac check failed");
//             packet.free_packet();
//             None
//         }
//     }

//     #[inline]
//     fn check_ip(
//         &self,
//         packet: Packet<MacHeader, EmptyMetadata>,
//     ) -> Option<(Packet<IpHeader, EmptyMetadata>, u32)> {
//         let packet = packet.parse_header::<IpHeader>();
//         // The following block borrows the Ip header from the parsed
//         // packet, and checks if it is valid. A packet is considered
//         // valid if:
//         //      - It is an IPv4 packet,
//         //      - It's TTL (time to live) is greater than zero,
//         //      - It is not long enough,
//         //      - It's destination Ip address matches that of the server.
//         const MIN_LENGTH_IP: u16 = common::PACKET_IP_LEN + 2;
//         let ip_header: &IpHeader = packet.get_header();
//         if (ip_header.version() == 4)
//             && (ip_header.ttl() > 0)
//             && (ip_header.length() >= MIN_LENGTH_IP)
//             && (ip_header.dst() == self.ip_addr)
//         {
//             let src_ip = ip_header.src();
//             trace!("recv from ip {}", src_ip);
//             Some((packet, src_ip))
//         } else {
//             trace!(
//                 "ip check failed, dst {} local {}",
//                 ip_header.dst(),
//                 self.ip_addr
//             );
//             packet.free_packet();
//             None
//         }
//     }

//     #[inline]
//     fn check_udp(
//         &self,
//         packet: Packet<IpHeader, EmptyMetadata>,
//     ) -> Option<(Packet<UdpHeader, EmptyMetadata>, u16)> {
//         let packet = packet.parse_header::<UdpHeader>();
//         // This block borrows the UDP header from the parsed packet, and
//         // checks if it is valid. A packet is considered valid if:
//         //      - It is not long enough,
//         const MIN_LENGTH_UDP: u16 = common::PACKET_UDP_LEN + 2;
//         let udp_header: &UdpHeader = packet.get_header();
//         if udp_header.length() >= MIN_LENGTH_UDP {
//             let src_port = udp_header.src_port();
//             Some((packet, src_port))
//         } else {
//             trace!("udp check failed");
//             packet.free_packet();
//             None
//         }
//     }
// }

// struct PacketHeaders {
//     /// The UDP header that will be appended to every response packet (cached
//     /// here to avoid wasting time creating a new one for every response
//     /// packet).
//     udp_header: UdpHeader,

//     /// The IP header that will be appended to every response packet (cached
//     /// here to avoid creating a new one for every response packet).
//     ip_header: IpHeader,

//     /// The MAC header that will be appended to every response packet (cached
//     /// here to avoid creating a new one for every response packet).
//     mac_header: MacHeader,
// }

// impl PacketHeaders {
//     fn new(src: &NetConfig, dst: &NetConfig) -> PacketHeaders {
//         // Create a common udp header for response packets.
//         let mut udp_header: UdpHeader = UdpHeader::new();
//         udp_header.set_length(common::PACKET_UDP_LEN);
//         udp_header.set_checksum(common::PACKET_UDP_CHECKSUM);
//         udp_header.set_src_port(common::DEFAULT_UDP_PORT);
//         udp_header.set_dst_port(common::DEFAULT_UDP_PORT); // will be changed by the rng of sender
//                                                            // Create a common ip header for response packets.
//         let mut ip_header: IpHeader = IpHeader::new();
//         ip_header.set_ttl(common::PACKET_IP_TTL);
//         ip_header.set_version(common::PACKET_IP_VER);
//         ip_header.set_ihl(common::PACKET_IP_IHL);
//         ip_header.set_length(common::PACKET_IP_LEN);
//         ip_header.set_protocol(0x11);
//         if let Ok(ip_src_addr) = Ipv4Addr::from_str(&src.ip_addr) {
//             ip_header.set_src(u32::from(ip_src_addr));
//         }
//         if let Ok(ip_dst_addr) = Ipv4Addr::from_str(&dst.ip_addr) {
//             ip_header.set_dst(u32::from(ip_dst_addr));
//         }

//         // Create a common mac header for response packets.
//         let mut mac_header: MacHeader = MacHeader::new();
//         mac_header.set_etype(common::PACKET_ETYPE);
//         if let Ok(mac_src_addr) = config::parse_mac(&src.mac_addr) {
//             mac_header.src = mac_src_addr;
//         }
//         if let Ok(mac_dst_addr) = config::parse_mac(&dst.mac_addr) {
//             mac_header.dst = mac_dst_addr;
//         }
//         PacketHeaders {
//             udp_header: udp_header,
//             ip_header: ip_header,
//             mac_header: mac_header,
//         }
//     }
//     fn create_hdr(src: &NetConfig, src_udp_port: u16, dst: &NetConfig) -> PacketHeaders {
//         let mut pkthdr = PacketHeaders::new(src, dst);
//         // we need to fix the src port for req_hdrs, which is used as the dst port in resp_hdr at rpc endpoint
//         pkthdr.udp_header.set_src_port(src_udp_port);
//         pkthdr
//     }
//     fn create_hdrs(
//         src: &NetConfig,
//         src_udp_port: u16,
//         endpoints: &Vec<NetConfig>,
//     ) -> Vec<PacketHeaders> {
//         let mut pkthdrs = vec![];
//         for dst in endpoints.iter() {
//             pkthdrs.push(Self::create_hdr(src, src_udp_port, dst));
//         }
//         pkthdrs
//     }
// }

pub struct LBDispatcher {
    pub sender2compute: Sender,
    pub sender2storage: Sender,
    pub receiver: Receiver,
}

impl LBDispatcher {
    pub fn new(config: &LBConfig, net_port: CacheAligned<PortQueue>) -> LBDispatcher {
        LBDispatcher {
            sender2compute: Sender::new(
                net_port.clone(),
                &config.lb.server,
                &config.compute,
                // Arc::new(RwLock::new(0)),
            ),
            sender2storage: Sender::new(
                net_port.clone(),
                &config.lb.server,
                &config.storage,
                // Arc::new(RwLock::new(0)),
            ),
            receiver: Receiver::new(
                net_port,
                // None,
                config.lb.max_rx_packets,
                &config.lb.server.ip_addr,
            )
            // receiver: Receiver {
            //     net_port: net_port,
            //     sib_port: None,
            //     stealing: false,
            //     max_rx_packets: config.lb.max_rx_packets,
            //     ip_addr: u32::from(
            //         Ipv4Addr::from_str(&config.lb.server.ip_addr).expect("missing src ip for LB"),
            //     ),
            // },
        }
    }
}

pub struct KayakDispatcher {
    pub sender2compute: Sender,
    pub sender2storage: Sender,
    pub receiver: Receiver,
}

impl KayakDispatcher {
    pub fn new(config: &KayakConfig, net_port: CacheAligned<PortQueue>) -> KayakDispatcher {
        KayakDispatcher {
            sender2compute: Sender::new(
                net_port.clone(),
                &config.lb.server,
                &config.compute,
                // Arc::new(RwLock::new(0)),
            ),
            sender2storage: Sender::new(
                net_port.clone(),
                &config.lb.server,
                &config.storage,
                // Arc::new(RwLock::new(0)),
            ),
            receiver: Receiver::new(
                net_port,
                // None,
                config.lb.max_rx_packets,
                &config.lb.server.ip_addr,
            )
            // receiver: Receiver {
            //     net_port: net_port,
            //     sib_port: None,
            //     stealing: false,
            //     max_rx_packets: config.lb.max_rx_packets,
            //     ip_addr: u32::from(
            //         Ipv4Addr::from_str(&config.lb.server.ip_addr).expect("missing src ip for LB"),
            //     ),
            // },
        }
    }
}

// pub struct Queue {
//     pub queue: std::sync::RwLock<VecDeque<Packet<UdpHeader, EmptyMetadata>>>,
//     /// length is set by Dispatcher, and cleared by the first worker in that round
//     // LB will only update those with positive queue length, thus reducing update frequency and variance
//     pub length: AtomicF64,
//     // TODO: add moving avg here
// }
// impl Queue {
//     pub fn new(capacity: usize) -> Queue {
//         Queue {
//             queue: std::sync::RwLock::new(VecDeque::with_capacity(capacity)),
//             length: AtomicF64::new(-1.0),
//         }
//     }
// }

/*
pub struct Dispatcher {
    receiver: Receiver,
    queue: Arc<Queue>,
    reset: Vec<Arc<AtomicBool>>,
    // time_avg: MovingTimeAvg,
}

impl Dispatcher {
    pub fn new(
        // config: &ComputeConfig,
        ip_addr: &str,
        max_rx_packets: usize,
        net_port: CacheAligned<PortQueue>,
        queue: Arc<Queue>,
        reset: Vec<Arc<AtomicBool>>,
        moving_exp: f64,
    ) -> Dispatcher {
        Dispatcher {
            receiver: Receiver::new(net_port, max_rx_packets, ip_addr),
            queue: queue,
            reset: reset,
            // time_avg: MovingTimeAvg::new(moving_exp),
            // queue: RwLock::new(VecDeque::with_capacity(config.max_rx_packets)),
        }
    }
    pub fn recv(&mut self) {
        if let Some(mut packets) = self.receiver.recv() {
            trace!("dispatcher recv {} packets", packets.len());
            if packets.len() > 0 {
                let current_time = cycles::rdtsc();
                let mut queue = self.queue.queue.write().unwrap();
                while let Some((packet, _)) = packets.pop() {
                    if parse_rpc_opcode(&packet) == OpCode::ResetRpc {
                        // reset
                        // this will create bottleneck since all workers attempts to write
                        // self.task_duration_cv.write().unwrap().reset();
                        for reset in &self.reset {
                            reset.store(true, Ordering::Relaxed);
                        }
                        // TODO: reset queue
                        packet.free_packet();
                    } else {
                        queue.push_back(packet);
                    }
                }
                let queue_len = queue.len() as f64;
                // self.time_avg.update(current_time, queue_len);
                self.queue.length.store(queue_len, Ordering::Relaxed);
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

pub struct CoeffOfVar {
    counter: f64,
    E_x: f64,
    E_x2: f64,
}
impl CoeffOfVar {
    pub fn new() -> CoeffOfVar {
        CoeffOfVar {
            counter: 0.0,
            E_x: 0.0,
            E_x2: 0.0,
        }
    }
    fn reset(&mut self) {
        self.counter = 0.0;
        self.E_x = 0.0;
        self.E_x2 = 0.0;
    }
    fn update(&mut self, delta: f64) {
        self.counter += 1.0;
        self.E_x = self.E_x * ((self.counter - 1.0) / self.counter) + delta / self.counter;
        self.E_x2 =
            self.E_x2 * ((self.counter - 1.0) / self.counter) + delta * delta / self.counter;
    }
    fn mean(&self) -> f64 {
        self.E_x
    }
    fn std(&self) -> f64 {
        (self.E_x2 - self.E_x * self.E_x).sqrt()
        // ((self.E_x2 - self.E_x * self.E_x) * (self.counter / (self.counter - 1.0))).sqrt()
    }
    fn cv(&self) -> f64 {
        (self.E_x2 - self.E_x * self.E_x) / (self.E_x * self.E_x + 1e-6)
        // * (self.counter / (self.counter - 1.0))
    }
}

/// sender&receiver for compute node
// 1. does not precompute mac and ip addr for LB and storage
// 2. repeated parse and reparse header
pub struct ComputeNodeWorker {
    sender: Rc<Sender>,
    receiver: Receiver,
    queue: Arc<Queue>,
    // this will bottleneck since all workers attempts to write
    // task_duration_cv: Arc<RwLock<CoeffOfVar>>,
    task_duration_cv: CoeffOfVar,
    reset: Arc<AtomicBool>,
    manager: TaskManager,
    // this is dynamic, i.e. resp dst is set as req src upon recving new reqs
    resp_hdr: PacketHeaders,
    last_run: u64,
    #[cfg(feature = "queue_len")]
    timestamp: Arc<RwLock<Vec<u64>>>,
    #[cfg(feature = "queue_len")]
    raw_length: Arc<RwLock<Vec<usize>>>,
    #[cfg(feature = "queue_len")]
    terminate: Arc<AtomicBool>,
}

// TODO: move req_ports& into config?
impl ComputeNodeWorker {
    pub fn new(
        config: &ComputeConfig,
        queue: Arc<Queue>,
        // task_duration_cv: Arc<RwLock<CoeffOfVar>>,
        reset: Arc<AtomicBool>,
        masterservice: Arc<Master>,
        net_port: CacheAligned<PortQueue>,
        sib_port: Option<CacheAligned<PortQueue>>,
        #[cfg(feature = "queue_len")] timestamp: Arc<RwLock<Vec<u64>>>,
        #[cfg(feature = "queue_len")] raw_length: Arc<RwLock<Vec<usize>>>,
        #[cfg(feature = "queue_len")] terminate: Arc<AtomicBool>,
    ) -> ComputeNodeWorker {
        ComputeNodeWorker {
            queue: queue,
            // task_duration_cv: task_duration_cv,
            task_duration_cv: CoeffOfVar::new(),
            reset: reset,
            resp_hdr: PacketHeaders::create_hdr(
                &config.src,
                net_port.txq() as u16,
                &NetConfig::default(),
            ),
            sender: Rc::new(Sender::new(
                net_port.clone(),
                &config.src,
                &config.storage,
                // shared_credits.clone(),
            )),
            receiver: Receiver {
                stealing: !sib_port.is_none(),
                net_port: net_port,
                sib_port: sib_port,
                max_rx_packets: config.max_rx_packets,
                ip_addr: u32::from(
                    Ipv4Addr::from_str(&config.src.ip_addr).expect("missing src ip for LB"),
                ),
            },
            manager: TaskManager::new(Arc::clone(&masterservice)),
            last_run: 0,
            #[cfg(feature = "queue_len")]
            timestamp: timestamp,
            #[cfg(feature = "queue_len")]
            raw_length: raw_length,
            #[cfg(feature = "queue_len")]
            terminate: terminate,
        }
    }

    fn poll(&mut self) -> f64 {
        if let Some(mut packets) = self.receiver.recv() {
            trace!("worker recv {} resps", packets.len());
            while let Some((packet, _)) = packets.pop() {
                self.dispatch(packet);
            }
            if self.manager.ready.len() > 0 {
                let queue_len = self.queue.length.swap(-1.0, Ordering::Relaxed) as f64;
                // return queue_len + 1.0;
                return queue_len;
            }
        }
        // let (request, queue_len) = {
        //     let mut queue = self.queue.queue.write().unwrap();
        //     let queue_len = queue.len();
        //     let request = queue.pop_front();
        //     (request, queue_len)
        // };
        let request = self.queue.queue.write().unwrap().pop_front();
        if let Some(request) = request {
            self.dispatch(request);
            // return queue_len as f64;
            return self.queue.length.swap(-1.0, Ordering::Relaxed) as f64;
        }
        return -1.0;
    }

    // fn poll_response(&mut self) -> usize {
    //     // let mut num_dispatched: u32 = 0;
    //     if let Some(mut packets) = self.receiver.recv() {
    //         trace!("worker recv {} resps", packets.len());
    //         while let Some((packet, _)) = packets.pop() {
    //             self.dispatch(packet);
    //         }
    //     }
    // }

    // fn poll_request(&mut self) {
    //     let packet = self.queue.write().unwrap().pop_front();
    //     if let Some(packet) = packet {
    //         self.dispatch(packet);
    //     }
    // }

    // pub fn run_extensions(&mut self, queue_len: f64) {
    //     self.manager.execute_tasks(queue_len);
    // }

    pub fn send_response(&mut self) {
        let resps = &mut self.manager.responses;
        if resps.len() > 0 {
            self.sender.send_pkts(resps);
        }
    }

    // 1. invoke_req: lb to compute
    // 2. get_resp: storage to compute
    pub fn dispatch(&mut self, packet: Packet<UdpHeader, EmptyMetadata>) -> Result<(), ()> {
        match parse_rpc_opcode(&packet) {
            OpCode::SandstormInvokeRpc => {
                if parse_rpc_service(&packet) == Service::MasterService {
                    self.dispatch_invoke(packet);
                    Ok(())
                } else {
                    trace!("invalid rpc req, ignore");
                    packet.free_packet();
                    Err(())
                }
            }
            OpCode::SandstormGetRpc => {
                self.process_get_resp(packet);
                Ok(())
            }
            #[cfg(feature = "queue_len")]
            OpCode::TerminateRpc => {
                self.terminate.store(true, Ordering::Relaxed);
                packet.free_packet();
                println!("core {} terminate", self.sender.net_port.rxq());
                Ok(())
            }
            // OpCode::ResetRpc => {
            //     packet.free_packet();
            //     // println!("core {} resets", self.sender.net_port.rxq());
            //     self.task_duration_cv.write().unwrap().reset();
            //     // TODO: reset dispatcher moving avg queue len
            //     // self.queue.moving_avg.reset()
            //     Ok(())
            // }
            _ => {
                trace!("pkt is not for compute node, ignore");
                packet.free_packet();
                Err(())
            }
        }
    }

    fn process_get_resp(&mut self, response: Packet<UdpHeader, EmptyMetadata>) {
        let p = response.parse_header::<GetResponse>();
        let hdr = p.get_header();
        let timestamp = hdr.common_header.stamp; // this is the timestamp when this ext is inserted in taskmanager
        let num_segments = hdr.num_segments as usize;
        let segment_id = hdr.segment_id as usize;
        // let table_id = hdr.table_id as usize;
        let records = p.get_payload();
        let recordlen = records.len();
        if p.get_header().common_header.status == RpcStatus::StatusOk {
            // self.manager
            //     .update_rwset(timestamp, table_id, records, recordlen);
            self.manager
                .update_rwset(timestamp, records, recordlen, segment_id, num_segments);
            // self.sender.return_credit();
        } else {
            warn!(
                "kv req of ext id: {} failed with status {:?}",
                timestamp,
                p.get_header().common_header.status
            );
        }
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

impl Executable for ComputeNodeWorker {
    fn execute(&mut self) {
        // #[cfg(feature = "queue_len")]
        // if self.terminate.load(Ordering::Relaxed) {
        //     return;
        // }
        // let current = cycles::rdtsc();
        // let eventloop_interval = if self.last_run > 0 {
        //     current - self.last_run
        // } else {
        //     0
        // };
        // self.last_run = current;
        // event loop
        // TODO: smooth queue_len
        let queue_len = self.poll();
        if self.manager.ready.len() > 0 {
            // let cv = self.task_duration_cv.read().unwrap().cv();
            let cv = self.task_duration_cv.cv();
            let start = cycles::rdtsc();
            self.manager.execute_task(queue_len, cv);
            // TODO: add transmission overhead in send_response(in case of multi-packet resp)
            self.send_response();
            let end = cycles::rdtsc();
            let task_time = (end - start) as f64;
            if self.reset.load(Ordering::Relaxed) {
                self.task_duration_cv.reset();
                self.reset.store(false, Ordering::Relaxed);
            }
            // self.task_duration_cv.write().unwrap().update(task_time);
            self.task_duration_cv.update(task_time);
        }
        // TODO: pass in queue_len
        // self.manager.execute_tasks(queue_len);
        // self.run_extensions(0.0);
        // let current_time = cycles::rdtsc();
        // let queue_length = self.manager.ready.len();
        // #[cfg(feature = "queue_len")]
        // if (queue_length > 0 || self.timestamp.read().unwrap().len() > 0)
        //     && !self.terminate.load(Ordering::Relaxed)
        // {
        //     self.timestamp.write().unwrap().push(current_time);
        //     // #[cfg(feature = "queue_len")]
        //     self.raw_length.write().unwrap().push(queue_length);
        // }
        // // TODO: smooth this queue_length
        // self.run_extensions(queue_length as f64);
        self.send_response();
    }

    fn dependencies(&mut self) -> Vec<usize> {
        vec![]
    }
}

impl Drop for ComputeNodeWorker {
    fn drop(&mut self) {
        self.send_response();
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
*/
