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

#![crate_type = "dylib"]
// Disable this because rustc complains about no_mangle being unsafe
//#![forbid(unsafe_code)]
#![feature(generators, generator_trait)]
#![allow(bare_trait_objects)]

extern crate db;
#[macro_use]
extern crate sandstorm;

#[macro_use]
extern crate cfg_if;

use std::ops::Generator;
use std::pin::Pin;
use std::rc::Rc;

use sandstorm::db::DB;
use sandstorm::pack::pack;
use std::convert::TryInto;

const K: usize = 5; // for topk

enum QueryOp {
    Vector = 1, // TODO: impl multiget
    TopK = 2,
    Scalar = 3,
    Auth = 4,
}

/// Converts a u8 into a TaoOp.
impl From<u8> for QueryOp {
    fn from(opcode: u8) -> Self {
        match opcode {
            1 => Self::Vector,
            2 => Self::TopK,
            3 => Self::Scalar,
            4 => Self::Auth,
            _ => panic!("Invalid opcode for vector query."),
        }
    }
}

fn dispatch(db: Rc<DB>) -> u64 {
    let args = db.args();
    let (table, args) = args.split_at(8);
    let (value_len, args) = args.split_at(8);
    // let (value_len, args) = args.split_at(8);
    let (record_len, args) = args.split_at(4);
    let (opcode, key) = args.split_at(1);
    let table = u64::from_le_bytes(table.try_into().unwrap());
    let record_len = u32::from_le_bytes(record_len.try_into().unwrap());
    let opcode: QueryOp = opcode[0].into();
    let key = key.to_vec();
    return 0;
}

// TODO: transmute this into vec of f32
fn vector_query_handler(
    db: Rc<DB>,
    table: u64,
    value_len: usize,
    record_len: usize,
    key: Vec<u8>,
) -> u64 {
    let mut obj = None;
    GET!(db, table, key, value_len, obj);
    if let Some(val) = obj {
        let mut res = vec![0usize; record_len];
        let mut num_records = 0usize;
        for v in val.read().chunks(record_len) {
            num_records += 1;
            for (x, &y) in res.iter_mut().zip(v.iter()) {
                *x += y as usize;
            }
        }
        let res = res.iter().map(|&x| (x / num_records) as u8).collect();
        db.resp(&res[..]);
        return 0;
    }
    let error = "Object does not exist";
    db.resp(error.as_bytes());
    return 1;
}
// TODO: transmute this into vec of f32
fn topk(src_vec: &[u8], assoc_vecs: Vec<&[u8]>) -> [usize; K] {
    let mut order: Vec<usize> = (0..assoc_vecs.len()).collect();
    order.sort_by_key(|&idx| {
        -assoc_vecs[idx]
            .iter()
            .zip(src_vec.iter())
            .map(|(&a, &b)| (a as i32) * (b as i32))
            .sum::<i32>()
    });
    order[..K].try_into().unwrap()
}

fn topk_query_handler(db: Rc<DB>, table: u64, record_len: usize, mut key: Vec<u8>) -> u64 {
    let key_len = key.len() as u16;
    // expected table order: id_assoc = id_vector + 1
    let assoc_table = table + 1;
    // key is extended by assoc
    ASSOC_GET!(db, assoc_table, key);
    let mut objs = None;
    // NOTE: for now, assume record_len < full table entry size
    // i.e., topk is computed over the first vector record
    // for full size, pass size = 0 to multiget; see proxy.rs for explanation
    MULTIGET!(db, table, key_len, key, record_len, objs);
    if let Some(vals) = objs {
        let (src_key, assoc_keys) = key.split_at(key_len as usize);
        let (src_vec, assoc_vecs) = vals.split_at(record_len);
        let topk = topk(src_vec, assoc_vecs);
        for &idx in &topk {
            let offset = idx * key_len as usize;
            db.resp(&assoc_keys[offset..offset + key_len as usize]);
            return 0;
        }
    }
    let error = "Object does not exist";
    db.resp(error.as_bytes());
    return 1;
}

#[no_mangle]
#[allow(unreachable_code)]
#[allow(unused_assignments)]
pub fn init(db: Rc<DB>) -> Pin<Box<Generator<Yield = u64, Return = u64>>> {
    Box::pin(move || {
        let mut obj = None;
        let mut t_table: u64 = 0;
        let mut num: u32 = 0;
        let mut ord: u32 = 0;
        let mut keys: Vec<u8> = Vec::with_capacity(30);

        {
            // First off, retrieve the arguments to the extension.
            let args = db.args();

            // Check that the arguments received is long enough to contain an
            // 8 byte table id and a key to be looked up. If not, then write
            // an error message to the response and return to the database.
            if args.len() <= 8 {
                let error = "Invalid args";
                db.resp(error.as_bytes());
                return 1;
            }

            // Next, split the arguments into a view over the table identifier
            // (first eight bytes), and a view over the key to be looked up.
            // De-serialize the table identifier into a u64.
            let (table, value) = args.split_at(8);
            let (number, value) = value.split_at(4);
            let (order, key) = value.split_at(4);
            keys.extend_from_slice(key);

            for (idx, e) in table.iter().enumerate() {
                t_table |= (*e as u64) << (idx << 3);
            }

            for (idx, e) in number.iter().enumerate() {
                num |= (*e as u32) << (idx << 3);
            }

            for (idx, e) in order.iter().enumerate() {
                ord |= (*e as u32) << (idx << 3);
            }
        }

        let mut mul: u64 = 0;

        for i in 0..num {
            // println!("round {} key {:?}", i, keys);
            GET!(db, t_table, keys, obj);

            if i == num - 1 {
                match obj {
                    // If the object was found, use the response.
                    Some(val) => {
                        mul = val.read()[0] as u64;
                    }

                    // If the object was not found, write an error message to the
                    // response.
                    None => {
                        let error = "Object does not exist";
                        db.resp(error.as_bytes());
                        return 1;
                    }
                }
            } else {
                // find the key for the second request.
                match obj {
                    // If the object was found, find the key from the response.
                    Some(val) => {
                        keys[0..4].copy_from_slice(&val.read()[0..4]);
                    }

                    // If the object was not found, write an error message to the
                    // response.
                    None => {
                        let error = "Object does not exist";
                        db.resp(error.as_bytes());
                        return 1;
                    }
                }
            }
        }

        // if ord >= 600 {
        //     let start = cycles::rdtsc();
        //     while cycles::rdtsc() - start < 600 as u64 {}
        //     ord -= 600;
        //     yield 0;
        // }

        // Compute part for this extension
        cfg_if! {
            if #[cfg(feature = "yield")]{
                loop {
                    if ord <= 2000 {
                        let start = cycles::rdtsc();
                        while cycles::rdtsc() - start < ord as u64 {}
                        break;
                    } else {
                        let start = cycles::rdtsc();
                        while cycles::rdtsc() - start < 2000 as u64 {}
                        ord -= 2000;
                        yield 0;
                    }
                }
            }else{
                let start = cycles::rdtsc();
                while cycles::rdtsc() - start < ord as u64 {}
            }
        }

        db.resp(pack(&mul));
        return 0;

        yield 0;
    })
}
