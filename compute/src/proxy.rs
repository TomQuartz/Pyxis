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

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::{mem, slice};

use db::cycles::*;
use db::wireformat::*;

use sandstorm::buf::{MultiReadBuf, ReadBuf, WriteBuf};
use sandstorm::db::DB;

use super::dispatch::*;

use db::dispatch::Sender;
use db::e2d2::common::EmptyMetadata;
use db::e2d2::interface::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

extern crate bytes;
use self::bytes::{Bytes, BytesMut};
use util::model::Model;

const NSEGMENTS: usize = 32;
const NBYTES: usize = 1024;

/// This struct represents a record for a read/write set. Each record in the read/write set will
/// be of this type.
#[derive(Clone)]
pub struct KV {
    /// This variable stores the Version for the record.
    pub version: Bytes,

    /// This variable stores the Key for the record.
    pub key: Bytes,

    /// This variable stores the Value for the record.
    pub value: Bytes,
}

impl KV {
    /// This method creates and returns a record consists of key and value.
    ///
    /// # Arguments
    /// * `rversion`: The version in the record.
    /// * `rkey`: The key in the record.
    /// * `rvalue`: The value in the record.
    ///
    /// # Return
    ///
    /// A record with a key and a value.
    pub fn new(rversion: Bytes, rkey: Bytes, rvalue: Bytes) -> KV {
        KV {
            version: rversion,
            key: rkey,
            value: rvalue,
        }
    }
}

/// A proxy to the database on the client side; which searches the
/// local cache before issuing the operations to the server.
pub struct ProxyDB {
    // The tenant-id for which the invoke() function was called for the parent request.
    tenant: u32,

    // After pushback, each subsequent request(get/put) will have the same packet identifier
    // as the first request.
    parent_id: u64,

    // The buffer consisting of the RPC payload that invoked the extension. This is required
    // to potentially pass in arguments to an extension. For example, a get() extension might
    // require a key and table identifier to be passed in.
    // req: Vec<u8>,
    req: Packet<InvokeRequest, EmptyMetadata>,

    resp: RefCell<Packet<InvokeResponse, EmptyMetadata>>,

    // The offset inside the request packet/buffer's payload at which the
    // arguments to the extension begin.
    args_offset: usize,

    // The flag to indicate if the current extension is waiting for the DB operation to complete.
    // This flag will be used by the scheduler to avoid scheduling the task until the response comes.
    waiting: RefCell<bool>,

    // Network stack required to actually send RPC requests out the network.
    sender: Rc<Sender>,

    // A list of the records in Read-set for the extension.
    readset: RefCell<Vec<KV>>,

    // A list of records in the write-set for the extension.
    writeset: RefCell<Vec<KV>>,

    // The credit which the extension has earned by making the db calls.
    db_credit: RefCell<u64>,
    // The model for a given extension which is stored based on the name of the extension.
    model: Option<Arc<Model>>,
    // // This maintains the read-write records accessed by the extension.
    kv_buffer: RwLock<Vec<RefCell<Vec<u8>>>>,
    /// number of responses to wait for, protected by the rwlock above
    // pub pending_resps: Cell<usize>,
    pending_resps: AtomicUsize,

    // the key_len for each table
    // TODO: make this a hashmap indexed by table_id
    // NOTE: for proxyDB, the key_len is set by the ext code, via the GET macro, in search_get_in_cache method
    key_len: Cell<usize>,
}

impl ProxyDB {
    pub fn get_keylen(&self) -> usize {
        self.key_len.get()
    }
    /// This method creates and returns the `ProxyDB` object. This DB issues the remote RPC calls
    /// instead of local table lookups.
    ///
    /// #Arguments
    ///
    /// * `tenant_id`: Tenant id will be needed reuqest generation.
    /// * `id`: This is unique-id for the request and consecutive requests will have same id.
    /// * `request`: A reference to the request sent by the client, it will be helpful in task creation
    ///             if the requested is pushed back.
    /// * `name_length`: This will be useful in parsing the request and find out the argument for consecutive requests.
    /// * `sender_service`: A reference to the service which helps in the RPC request generation.
    ///
    /// # Return
    ///
    /// A DB object which either manipulates the record in RW set or perform remote RPCs.
    pub fn new(
        tenant_id: u32,
        id: u64,
        // request: Vec<u8>,
        req: Packet<InvokeRequest, EmptyMetadata>,
        resp: Packet<InvokeResponse, EmptyMetadata>,
        name_length: usize,
        sender_service: Rc<Sender>,
        model: Option<Arc<Model>>,
    ) -> ProxyDB {
        let mut kv_buffer = vec![];
        for _ in 0..NSEGMENTS {
            kv_buffer.push(RefCell::new(Vec::with_capacity(NBYTES)));
        }
        ProxyDB {
            tenant: tenant_id,
            parent_id: id,
            req: req,
            resp: RefCell::new(resp),
            args_offset: name_length,
            waiting: RefCell::new(false),
            sender: sender_service,
            readset: RefCell::new(Vec::with_capacity(4)),
            writeset: RefCell::new(Vec::with_capacity(4)),
            db_credit: RefCell::new(0),
            model: model,
            // commit_payload: RefCell::new(Vec::new()),
            key_len: Cell::new(0),
            kv_buffer: RwLock::new(kv_buffer),
            // pending_resps: Cell::new(0),
            pending_resps: AtomicUsize::new(0),
        }
    }

    /// This method can change the waiting flag to true/false. This flag is used to move the
    /// task between suspended or blocked task queue.
    ///
    /// # Arguments
    ///
    /// * `value`: A boolean value, which can be true or false.
    fn set_waiting(&self, value: bool) {
        *self.waiting.borrow_mut() = value;
    }

    /// This method return the current value of waiting flag. This flag is used to move the
    /// task between suspended or blocked task queue.
    pub fn get_waiting(&self) -> bool {
        self.waiting.borrow().clone()
    }
    /*
    /// This method is used to add a record to the read set. The return value of get()/multiget()
    /// goes the read set.
    ///
    /// # Arguments
    /// * `record`: A reference to a record with a key and a value.
    // TODO: make read/write set a hashmap indexed by hash(key)
    pub fn set_read_record(&self, record: &[u8], keylen: usize) {
        let ptr = &OpType::SandstormRead as *const _ as *const u8;
        // let optype = unsafe { slice::from_raw_parts(ptr, mem::size_of::<OpType>()) };
        // self.commit_payload.borrow_mut().extend_from_slice(optype);
        // self.commit_payload.borrow_mut().extend_from_slice(record);
        let (version, entry) = record.split_at(8);
        let (key, value) = entry.split_at(keylen);
        self.readset.borrow_mut().push(KV::new(
            Bytes::from(version),
            Bytes::from(key),
            Bytes::from(value),
        ));
    }
    */

    /// the get resp is scattered in multiple packets
    pub fn collect_resp(
        &self,
        record: &[u8],
        key_len: usize,
        segment_id: usize,
        num_segments: usize,
    ) -> bool {
        let (version, entry) = record.split_at(8);
        let (key, value) = entry.split_at(key_len);
        let value_len = value.len();

        self.pending_resps
            .compare_exchange(0, num_segments, Ordering::SeqCst, Ordering::Acquire);
        if self.kv_buffer.read().unwrap().len() < num_segments {
            let mut kv_buffer = self.kv_buffer.write().unwrap();
            kv_buffer.resize_with(num_segments, || RefCell::new(Vec::with_capacity(NBYTES)));
        }
        // invariant: kv buffer has at least num_segments elements
        // wrapped in brackets because refcell.borrow_mut returns a RAII guard
        if let Ok(kv_buffer) = self.kv_buffer.read() {
            // let kv_buffer = self.kv_buffer.read().unwrap();
            let mut segment = kv_buffer[segment_id].borrow_mut();
            if segment.len() < value_len {
                segment.resize(value_len, 0);
            }
            segment[..value_len].copy_from_slice(value);
        }
        // check if this is the last segment
        if self.pending_resps.fetch_sub(1, Ordering::Relaxed) == 1 {
            // if we are here, then the mut ref to refcell of all segments must have gone out of scope
            let version = Bytes::from(version);
            let key = Bytes::from(key);
            let mut value = Bytes::with_capacity(num_segments * value_len);
            for segment_id in 0..num_segments {
                if let Ok(kv_buffer) = self.kv_buffer.read() {
                    let segment = kv_buffer[segment_id].borrow();
                    value.extend_from_slice(&segment[..value_len]);
                }
            }
            self.readset.borrow_mut().push(KV::new(version, key, value));
            true
        } else {
            false
        }
        // let mut kv_buffer = self.kv_buffer.borrow_mut();
        // kv_buffer[segment_id * value_len..(segment_id + 1) * value_len].copy_from_slice(value);
        // let mut pending_resps = self.pending_resps.get();
        // pending_resps -= 1;
        // self.pending_resps.set(pending_resps);
        // if pending_resps == 0 {
        //     self.readset.borrow_mut().push(KV::new(
        //         Bytes::from(version),
        //         Bytes::from(key),
        //         Bytes::from(&kv_buffer[..]),
        //     ));
        //     true
        // } else {
        //     false
        // }
    }

    /// This method is used to add a record to the write set. The return value of put()
    /// goes the write set.
    ///
    /// # Arguments
    /// * `record`: A reference to a record with a key and a value.
    pub fn set_write_record(&self, record: &[u8], keylen: usize) {
        let ptr = &OpType::SandstormWrite as *const _ as *const u8;
        // let optype = unsafe { slice::from_raw_parts(ptr, mem::size_of::<OpType>()) };
        // self.commit_payload.borrow_mut().extend_from_slice(optype);
        // self.commit_payload.borrow_mut().extend_from_slice(record);
        let (version, entry) = record.split_at(8);
        let (key, value) = entry.split_at(keylen);
        self.writeset.borrow_mut().push(KV::new(
            Bytes::from(version),
            Bytes::from(key),
            Bytes::from(value),
        ));
    }

    /// This method search the a list of records to find if a record with the given key
    /// exists or not.
    ///
    /// # Arguments
    ///
    /// * `list`: A list of records, which can be the read set or write set for the extension.
    /// * `key`: A reference to a key to be looked up in the list.
    ///
    /// # Return
    ///
    /// The index of the element, if present. 1024 otherwise.
    pub fn search_cache(&self, list: Vec<KV>, key: &[u8]) -> usize {
        let length = list.len();
        for i in 0..length {
            if list[i].key == key {
                return i;
            }
        }
        //Return some number way bigger than the cache size.
        return 1024;
    }

    /// This method returns the value of the credit which an extension has accumulated over time.
    /// The extension credit is increased whenever it makes a DB function call; like get(),
    /// multiget(), put(), etc.
    ///
    /// # Return
    ///
    /// The current value of the credit for the extension.
    pub fn db_credit(&self) -> u64 {
        self.db_credit.borrow().clone()
    }

    /// This method send a request to the server to commit the transaction.
    pub fn commit(
        self,
    ) -> (
        Packet<InvokeRequest, EmptyMetadata>,
        Packet<InvokeResponse, EmptyMetadata>,
    ) {
        (self.req, self.resp.into_inner())
    }

    // pub fn commit(&self) {
    //     if self.readset.borrow().len() > 0 || self.writeset.borrow().len() > 0 {
    //         let mut table_id = 0;
    //         let mut key_len = 0;
    //         let mut val_len = 0;

    //         // find the table_id for the transaction.
    //         let args = self.args();
    //         let (table, _) = args.split_at(8);
    //         for (idx, e) in table.iter().enumerate() {
    //             table_id |= (*e as u64) << (idx << 3);
    //         }

    //         // Find the key length and value length for records in RWset.
    //         if self.readset.borrow().len() > 0 {
    //             key_len = self.readset.borrow()[0].key.len();
    //             val_len = self.readset.borrow()[0].value.len();
    //         }

    //         if key_len == 0 && self.writeset.borrow().len() > 0 {
    //             key_len = self.writeset.borrow()[0].key.len();
    //             val_len = self.writeset.borrow()[0].value.len();
    //         }
    //         if cfg!(feature = "checksum") {
    //             key_len = 30;
    //             val_len = 100;
    //             let commit_payload = self.commit_payload.borrow();
    //             let payload = commit_payload.split_at(377).1;
    //             self.sender.send_commit(
    //                 self.tenant,
    //                 table_id,
    //                 payload,
    //                 self.parent_id,
    //                 key_len as u16,
    //                 val_len as u16,
    //             );
    //             return;
    //         }

    //         self.sender.send_commit(
    //             self.tenant,
    //             table_id,
    //             &self.commit_payload.borrow(),
    //             self.parent_id,
    //             key_len as u16,
    //             val_len as u16,
    //         );
    //     }
    // }
}

// TODO:
// 1. reimpl search_cache, using table_id and key as index
// 2. reimpl readset as hashmap instead of vec
// 3. add mapping from table to key_len
// 3. set key_len for table in search_get_in_cache
impl DB for ProxyDB {
    /// Lookup the `DB` trait for documentation on this method.
    fn get(&self, _table: u64, key: &[u8]) -> Option<ReadBuf> {
        let start = rdtsc();
        self.set_waiting(false);
        let index = self.search_cache(self.readset.borrow().to_vec(), key);
        let value = self.readset.borrow()[index].value.clone();
        *self.db_credit.borrow_mut() += rdtsc() - start;
        unsafe { Some(ReadBuf::new(value)) }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn multiget(&self, _table: u64, key_len: u16, keys: &[u8]) -> Option<MultiReadBuf> {
        let mut objs = Vec::new();
        for key in keys.chunks(key_len as usize) {
            if key.len() != key_len as usize {
                break;
            }
            let index = self.search_cache(self.readset.borrow().to_vec(), key);
            if index != 1024 {
                let value = self.readset.borrow()[index].value.clone();
                objs.push(value);
            } else {
                info!("Multiget Failed");
            }
        }
        unsafe { Some(MultiReadBuf::new(objs)) }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn alloc(&self, table: u64, key: &[u8], val_len: u64) -> Option<WriteBuf> {
        unsafe {
            // Alloc for version, key and value.
            let mut writebuf = WriteBuf::new(
                table,
                BytesMut::with_capacity(8 + key.len() + val_len as usize),
            );
            writebuf.write_slice(&[0; 8]);
            writebuf.write_slice(key);
            Some(writebuf)
        }
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn put(&self, buf: WriteBuf) -> bool {
        unsafe {
            let (_table_id, buf) = buf.freeze();
            assert_eq!(buf.len(), 138);
            self.set_write_record(&buf, 30);
        }
        return true;
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn del(&self, _table: u64, _key: &[u8]) {}

    /// Lookup the `DB` trait for documentation on this method.
    fn args(&self) -> &[u8] {
        self.req.get_payload().split_at(self.args_offset).1
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn resp(&self, data: &[u8]) {
        self.resp
            .borrow_mut()
            .add_to_payload_tail(data.len(), data)
            .expect("fail to write response");
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn debug_log(&self, _message: &str) {}

    /// Lookup the `DB` trait for documentation on this method.
    fn search_get_in_cache(&self, table: u64, key: &[u8]) -> (bool, bool, Option<ReadBuf>) {
        self.key_len.set(key.len());
        let start = rdtsc();
        let index = self.search_cache(self.readset.borrow().to_vec(), key);
        if index != 1024 {
            let value = self.readset.borrow()[index].value.clone();
            *self.db_credit.borrow_mut() += rdtsc() - start;
            return (false, true, unsafe { Some(ReadBuf::new(value)) });
        }
        trace!("ext id: {} yield due to missing key", self.parent_id);
        self.set_waiting(true);
        self.sender
            .send_get_from_extension(self.tenant, table, key, self.parent_id);
        *self.db_credit.borrow_mut() += rdtsc() - start;
        (false, false, None)
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn search_multiget_in_cache(
        &self,
        table: u64,
        key_len: u16,
        keys: &[u8],
    ) -> (bool, bool, Option<MultiReadBuf>) {
        let start = rdtsc();
        let mut objs = Vec::new();
        for key in keys.chunks(key_len as usize) {
            if key.len() != key_len as usize {
                return (false, false, None);
            }

            let index = self.search_cache(self.readset.borrow().to_vec(), key);
            if index != 1024 {
                let value = self.readset.borrow()[index].value.clone();
                objs.push(value);
            } else {
                self.set_waiting(true);
                self.sender
                    .send_get_from_extension(self.tenant, table, key, self.parent_id);
                *self.db_credit.borrow_mut() += rdtsc() - start;
                return (false, false, None);
            }
        }
        *self.db_credit.borrow_mut() += rdtsc() - start;
        return (false, true, unsafe { Some(MultiReadBuf::new(objs)) });
    }

    /// Lookup the `DB` trait for documentation on this method.
    fn get_model(&self) -> Option<Arc<Model>> {
        match self.model {
            Some(ref model) => Some(Arc::clone(&model)),
            None => None,
        }
    }
}
