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
pub trait Workload {
    fn gen(&mut self, rng: &mut impl Rng) -> &[u8];
    fn name_len(&self) -> u32;
}

pub fn create_workload(config: &WorkloadConfig, table: &TableConfig) -> Box<dyn Workload> {
    match config.extension.as_str() {
        "pushback" => Box::new(Synthetic::new(config, table)),
        "vector" => Box::new(VectorQuery::new(config, table)),
    }
}

struct Synthetic {
    key_rng: Box<ZipfDistribution>,
    // tenant_rng: Box<ZipfDistribution>,
    payload: Vec<u8>,
    // name of extension, e.g. pushback
    name_len: u32,
    // key_len: usize,
    key_offset: usize,
}
// TODO: add intra-type variation
impl Synthetic {
    fn new(config: &WorkloadConfig, table: &TableConfig) -> Synthetic {
        let extension = config.extension.as_bytes();
        // let key_offset =
        //     extension.len() + mem::size_of::<u64>() + mem::size_of::<u32>() + mem::size_of::<u32>();
        // let payload_len = key_offset + table.key_len;
        // let mut payload = Vec::with_capacity(payload_len);
        // payload.extend_from_slice(extension);
        // // TODO: introduce variation in kv and order, these fields will not be static
        // payload.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(config.table_id.to_le()) });
        // payload.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(config.kv.to_le()) });
        // payload.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(config.order.to_le()) });
        // payload.resize(payload_len, 0);
        let mut payload = vec![];
        payload.extend_from_slice(extension);
        // payload.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(config.table_id.to_le()) });
        payload.extend_from_slice(&config.table_id.to_le_bytes());
        payload.extend_from_slice(&table.value_len.to_le_bytes());
        payload.extend_from_slice(&config.kv.to_le_bytes());
        payload.extend_from_slice(&config.order.to_le_bytes());
        let key_offset = payload.len();
        let payload_len = key_offset + table.key_len;
        payload.resize(payload_len, 0);
        Synthetic {
            // rng: {
            //     let seed: [u32; 4] = rand::random::<[u32; 4]>();
            //     Box::new(XorShiftRng::from_seed(seed))
            // },
            key_rng: Box::new(
                ZipfDistribution::new(table.num_records as usize, config.skew)
                    .expect("Couldn't create key RNG."),
            ),
            payload: payload,
            name_len: extension.len() as u32,
            // key_len: config.key_len,
            key_offset: key_offset,
        }
    }
}
impl Workload for Synthetic {
    fn name_len(&self) -> u32 {
        self.name_len
    }
    fn gen(&mut self, rng: &mut impl Rng) -> &[u8] {
        let key = self.key_rng.sample(rng) as u32;
        // let key: [u8; 4] = unsafe { transmute(key.to_le()) };
        let key = key.to_le_bytes();
        self.payload[self.key_offset..self.key_offset + 4].copy_from_slice(&key);
        &self.payload
    }
}

struct VectorQuery {
    key_rng: Box<ZipfDistribution>,
    // tenant_rng: Box<ZipfDistribution>,
    payload: Vec<u8>,
    // name of extension, e.g. pushback
    name_len: u32,
    key_len: usize,
    key_offset: usize,
    opcode: u8,
    // table: TableConfig,
}
impl VectorQuery {
    fn new(config: &WorkloadConfig, table: &TableConfig) -> VectorQuery {
        let extension = config.extension.as_bytes();
        let mut payload = vec![];
        payload.extend_from_slice(extension);
        payload.extend_from_slice(&config.table_id.to_le_bytes());
        // set value_len and record_len
        let value_len = match config.opcode {
            // for topk(multiget)
            2 => table.record_len as usize,
            // for auth, record len is used as passwd len
            1 | 3 | 4 => table.value_len,
            _ => panic!("invalid opcode {}", config.opcode),
        };
        payload.extend_from_slice(&value_len.to_le_bytes());
        payload.extend_from_slice(&table.record_len.to_le_bytes());
        payload.extend_from_slice(&config.opcode.to_le_bytes());
        let key_offset = payload.len();
        let mut payload_len = key_offset + table.key_len;
        if config.opcode == 4 {
            // for auth, add passwd
            payload_len += tabel.record_len as usize;
        }
        payload.resize(payload_len, 0);
        Synthetic {
            // rng: {
            //     let seed: [u32; 4] = rand::random::<[u32; 4]>();
            //     Box::new(XorShiftRng::from_seed(seed))
            // },
            key_rng: Box::new(
                ZipfDistribution::new(table.num_records as usize, config.skew)
                    .expect("Couldn't create key RNG."),
            ),
            payload: payload,
            name_len: extension.len() as u32,
            key_len: table.key_len,
            key_offset: key_offset,
            opcode: config.opcode,
            // table: table.clone(),
        }
    }
}
impl Workload for VectorQuery {
    fn name_len(&self) -> u32 {
        self.name_len
    }
    fn gen(&mut self, rng: &mut impl Rng) -> &[u8] {
        let key = self.key_rng.sample(rng) as u32;
        // let key: [u8; 4] = unsafe { transmute(key.to_le()) };
        let key = key.to_le_bytes();
        self.payload[self.key_offset..self.key_offset + 4].copy_from_slice(&key);
        if self.opcode == 4 {
            // auth, fill passwd
            let offset = self.key_offset + self.key_len;
            self.payload[offset..offset + 4].copy_from_slice(&key);
        }
        &self.payload
    }
}

/*
use db::e2d2::common::EmptyMetadata;
use db::e2d2::interface::*;
use db::wireformat::{GetResponse, MultiGetResponse, OpCode, PutResponse};

/// Definition of the Workload trait that will allow clients to implement
/// application specific code. This trait mostly helps for native side execution;
/// the only invoke() side exeuction function is to generate the request using
/// get_invoke_request()
pub trait Workload {
    /// This method helps the client to decide on the type of the next
    /// request(get/put/multiget) based on the operation distribution
    /// specified in the configuration file.
    ///
    /// # Return
    ///
    /// The enum OpCode which indicates the operation type.
    fn next_optype(&mut self) -> OpCode;

    /// This method returns the name length used in the invoke() payload. It helps
    /// both client and server to parse the argument passed in the request payload.
    ///
    /// # Return
    ///
    /// The name length for the extension name in the request payload.
    fn name_length(&self) -> u32;

    /// This method decides the arguements for the next invoke() operation.
    /// Usually, it changes call specific arguments in a vector and which
    /// is later added to the request payload.
    ///
    /// # Return
    ///
    /// The tenant-id and request payload reference.
    fn get_invoke_request(&mut self) -> (u32, &[u8]);

    /// This method decides the arguments for the next get() request.
    /// The tenant and the key are sampled through a pre-decided distribution
    /// and a from specific range.
    ///
    /// # Return
    ///
    /// The tenant-od and key-id for the get() request.
    fn get_get_request(&mut self) -> (u32, &[u8]);

    /// This method decides the arguments for the next put() request.
    /// The tenant, key and the value are sampled through a pre-decided
    /// distribution and from a specific range.
    ///
    /// # Return
    ///
    /// The tenant-id, key-id, and value for the put() request.
    fn get_put_request(&mut self) -> (u32, &[u8], &[u8]);

    /// This method decides the arguments for the next multiget() request.
    /// The tenant, number of keys and key-ids are sampled through a pre-decided
    /// distribution and from a specific range.
    ///
    /// # Return
    ///
    /// The tenant-id, key-id, and value for the put() request.
    fn get_multiget_request(&mut self) -> (u32, u32, &[u8]);

    /// This method handles the `GetResponse` based on application specific logic.
    /// And it is called only for the response for the native execution.
    ///
    /// # Arguments
    /// *`packet`: The reference to the `GetResponse` packet.
    fn process_get_response(&mut self, packet: &Packet<GetResponse, EmptyMetadata>);

    /// This method handles the `PutResponse` based on application specific logic.
    /// And it is called only for the response for the native execution.
    ///
    /// # Arguments
    /// *`packet`: The reference to the `PutResponse` packet.
    fn process_put_response(&mut self, packet: &Packet<PutResponse, EmptyMetadata>);

    /// This method handles the `MultiGetResponse` based on application specific logic.
    /// And it is called only for the response for the native execution.
    ///
    /// # Arguments
    /// *`packet`: The reference to the `MultiGetResponse` packet.
    fn process_multiget_response(&mut self, packet: &Packet<MultiGetResponse, EmptyMetadata>);
}
*/
