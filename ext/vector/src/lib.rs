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

use std::ops::Generator;
use std::pin::Pin;
use std::rc::Rc;

use sandstorm::db::DB;
use sandstorm::pack::pack;
use std::convert::TryInto;

extern crate openssl;
use openssl::aes::{aes_ige, AesKey};
use openssl::symm::Mode;
extern crate crypto;
use crypto::scrypt::{scrypt, ScryptParams};

const ARGS_OFFSET: usize = 8 + 8 + 4 + 1;
// for topk
const K: usize = 5;
// for auth
const SCRYPT_PARAMS: (u8, u32, u32) = (4, 2, 2);
const AES_KEY: &[u8] = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F";
const AES_IV: &[u8] = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F\
        \x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F";

enum QueryOp {
    Vector = 1,
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

#[no_mangle]
#[allow(unreachable_code)]
#[allow(unused_assignments)]
pub fn init(db: Rc<DB>) -> Pin<Box<Generator<Yield = u64, Return = u64>>> {
    // Box::pin(move || {
    //     return dispatch(db);
    //     yield 0;
    // })
    let args = db.args();
    let (table, args) = args.split_at(8);
    let (value_len, args) = args.split_at(8);
    let (record_len, args) = args.split_at(4);
    let (opcode, args) = args.split_at(1);
    let table = u64::from_le_bytes(table.try_into().unwrap());
    let value_len = usize::from_le_bytes(value_len.try_into().unwrap());
    let record_len = u32::from_le_bytes(record_len.try_into().unwrap()) as usize;
    // println!(
    //     "QUERY table {} value {} record {} opcode {}",
    //     table, value_len, record_len, opcode[0]
    // );
    let opcode: QueryOp = opcode[0].into();
    match opcode {
        QueryOp::Vector => vector_query_handler(db, table, value_len, record_len),
        QueryOp::TopK => topk_query_handler(db, table, value_len, record_len),
        QueryOp::Scalar => scalar_query_handler(db, table, value_len, record_len),
        QueryOp::Auth => auth_query_handler(db, table, value_len, record_len),
        _ => Box::pin(move || {
            return 1;
            yield 0;
        }),
    }
}

// fn dispatch(db: Rc<DB>) -> Pin<Box<Generator<Yield = u64, Return = u64>>> {
//     let args = db.args();
//     let (table, args) = args.split_at(8);
//     let (value_len, args) = args.split_at(8);
//     let (record_len, args) = args.split_at(4);
//     let (opcode, args) = args.split_at(1);
//     let table = u64::from_le_bytes(table.try_into().unwrap());
//     let value_len = usize::from_le_bytes(record_len.try_into().unwrap());
//     let record_len = u32::from_le_bytes(record_len.try_into().unwrap()) as usize;
//     let opcode: QueryOp = opcode[0].into();
//     match opcode {
//         QueryOp::Vector => vector_query_handler(db, table, value_len, record_len, args),
//         // QueryOp::TopK => topk_query_handler(db, table, value_len, record_len, args),
//         // QueryOp::Scalar => scalar_query_handler(db, table, value_len, record_len, args),
//         // QueryOp::Auth => auth_query_handler(db, table, value_len, record_len, args),
//         _=>Box::pin(move || {
//             return 1;
//             yield 0;
//         })
//     }
// }

fn reinterpret<T>(src: &[u8]) -> &[T] {
    // let (_, v, _) = unsafe { src.align_to::<T>() };
    // v
    unsafe { src.align_to::<T>().1 }
}

fn vectorize<T>(val: &[u8], record_len: usize) -> Vec<&[T]> {
    val.chunks(record_len)
        .map(|record| reinterpret(record))
        .collect::<Vec<_>>()
}

fn add(to: &mut [f32], from: &[f32]) {
    for (x, &y) in to.iter_mut().zip(from.iter()) {
        *x += y;
    }
}

fn dot(x: &[f32], y: &[f32]) -> f32 {
    x.iter().zip(y.iter()).map(|(&a, &b)| a * b).sum::<f32>()
}

#[allow(unreachable_code)]
fn vector_query_handler(
    db: Rc<DB>,
    table: u64,
    value_len: usize,
    record_len: usize,
    // args: &[u8],
    // args_offset: usize,
) -> Pin<Box<Generator<Yield = u64, Return = u64>>> {
    Box::pin(move || {
        let args = db.args().split_at(ARGS_OFFSET).1;
        let key = args.to_vec();
        let mut obj = None;
        GET!(db, table, key, value_len, obj);
        if let Some(val) = obj {
            let mut mean = vec![0f32; record_len / 4];
            let mut num_records = 0f32;
            for v in val.read().chunks(record_len) {
                num_records += 1.0;
                let v = reinterpret(v);
                add(&mut mean, v);
            }
            for x in mean.iter_mut() {
                *x /= num_records;
            }
            unsafe {
                db.resp(std::slice::from_raw_parts(
                    mean.as_ptr() as *const u8,
                    mean.len() * 4,
                ));
            }
            return 0;
        }
        let error = "Object does not exist";
        db.resp(error.as_bytes());
        return 1;
        yield 0;
    })
    // let key = args.to_vec();
    // let mut obj = None;
    // GET!(db, table, key, value_len, obj);
    // if let Some(val) = obj {
    //     let mut mean = vec![0f32; record_len / 4];
    //     let mut num_records = 0f32;
    //     for v in val.read().chunks(record_len) {
    //         num_records += 1.0;
    //         let v = reinterpret(v);
    //         add(&mut mean, v);
    //     }
    //     for x in mean.iter_mut() {
    //         *x /= num_records;
    //     }
    //     db.resp(std::slice::from_raw_parts(
    //         mean.as_ptr() as *const u8,
    //         mean.len() * 4,
    //     ));
    //     return 0;
    // }
    // let error = "Object does not exist";
    // db.resp(error.as_bytes());
    // return 1;
}

fn topk(src_vec: Vec<&[f32]>, assoc_vecs: Vec<Vec<&[f32]>>) -> [usize; K] {
    let mut scores = vec![];
    for assoc_vec in &assoc_vecs {
        let mut score = 1f32;
        for (&x, &y) in assoc_vec.iter().zip(src_vec.iter()) {
            score *= dot(x, y);
        }
        scores.push(score);
    }
    let mut order: Vec<usize> = (0..scores.len()).collect();
    // in descending order
    order.sort_by(|&idx1, &idx2| scores[idx2].partial_cmp(&scores[idx1]).unwrap());
    order[..K].try_into().unwrap()
}

#[allow(unreachable_code)]
fn topk_query_handler(
    db: Rc<DB>,
    table: u64,
    value_len: usize,
    record_len: usize,
    // args: &[u8],
) -> Pin<Box<Generator<Yield = u64, Return = u64>>> {
    Box::pin(move || {
        let args = db.args().split_at(ARGS_OFFSET).1;
        let mut keys = args.to_vec();
        let key_len = keys.len() as u16;
        // expected table order: id_assoc = id_vector + 1
        let assoc_table = table + 1;
        let mut assoc = None;
        ASSOCGET!(db, assoc_table, keys, assoc);
        // key is extended by assoc
        if let Some(assoc) = assoc {
            // println!("ASSOC len {}", assoc.read().len());
            keys.extend_from_slice(assoc.read());
        } else {
            let error = "Key does not exist";
            db.resp(error.as_bytes());
            return 1;
        }
        let mut objs = None;
        // NOTE: for now, assume record_len < full table entry size
        // i.e., topk is computed over the first vector record
        // for full size, pass size = 0 to multiget; see proxy.rs for explanation
        MULTIGET!(db, table, key_len, keys, value_len, objs);
        if let Some(vals) = objs {
            // extract keys
            let (src_key, assoc_keys) = keys.split_at(key_len as usize);
            let assoc_keys = vectorize::<u8>(assoc_keys, key_len as usize);
            // extract vals
            let mut vals = vals.read();
            let src_vec = vectorize::<f32>(vals[0], record_len);
            let assoc_vecs = vals
                .drain(1..)
                .map(|val| vectorize::<f32>(val, record_len))
                .collect::<Vec<_>>();
            // println!(
            //     "keys {} num {} src {} assoc {} record {}",
            //     keys.len(),
            //     assoc_keys.len(),
            //     src_vec.len(),
            //     assoc_vecs.len(),
            //     src_vec[0].len()
            // );
            let topk = topk(src_vec, assoc_vecs);
            for &idx in &topk {
                // let offset = idx * key_len as usize;
                // db.resp(&assoc_keys[offset..offset + key_len as usize]);
                db.resp(assoc_keys[idx]);
            }
            return 0;
        }
        let error = "Assocs does not exist";
        db.resp(error.as_bytes());
        return 1;
        yield 0;
    })
    // let args = db.args().split_at(ARGS_OFFSET).1;
    // let mut keys = args.to_vec();
    // let key_len = keys.len() as u16;
    // // expected table order: id_assoc = id_vector + 1
    // let assoc_table = table + 1;
    // // key is extended by assoc
    // ASSOC_GET!(db, assoc_table, keys);
    // let mut objs = None;
    // // NOTE: for now, assume record_len < full table entry size
    // // i.e., topk is computed over the first vector record
    // // for full size, pass size = 0 to multiget; see proxy.rs for explanation
    // MULTIGET!(db, table, key_len, keys, value_len, objs);
    // if let Some(vals) = objs {
    //     // extract keys
    //     let (src_key, assoc_keys) = keys.split_at(key_len as usize);
    //     let assoc_keys = vectorize::<u8>(assoc_keys, key_len as usize);
    //     // extract vals
    //     let mut vals = vals.read();
    //     let src_vec = vectorize::<f32>(vals[0], record_len);
    //     let assoc_vecs = vals
    //         .drain(1..)
    //         .map(|val| vectorize::<f32>(val, record_len))
    //         .collect::<Vec<_>>();
    //     let topk = topk(src_vec, assoc_vecs);
    //     for &idx in &topk {
    //         // let offset = idx * key_len as usize;
    //         // db.resp(&assoc_keys[offset..offset + key_len as usize]);
    //         db.resp(assoc_keys[idx]);
    //     }
    //     return 0;
    // }
    // let error = "Object does not exist";
    // db.resp(error.as_bytes());
    // return 1;
}

#[allow(unreachable_code)]
fn scalar_query_handler(
    db: Rc<DB>,
    table: u64,
    value_len: usize,
    _record_len: usize,
    // args: &[u8],
) -> Pin<Box<Generator<Yield = u64, Return = u64>>> {
    Box::pin(move || {
        let args = db.args().split_at(ARGS_OFFSET).1;
        let key = args.to_vec();
        let mut obj = None;
        GET!(db, table, key, value_len, obj);
        // for scalar, record len = 4
        if let Some(val) = obj {
            let mut arr = vec![];
            let mut sum_x = 0f32;
            let mut sum_x2 = 0f32;
            for v in val.read().chunks(4) {
                let x = reinterpret::<f32>(v)[0];
                sum_x += x;
                sum_x2 += x * x;
                arr.push(x);
            }
            arr.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let num_records = arr.len() as f32;
            let mean = sum_x / num_records;
            let std = (sum_x2 / num_records - mean * mean).sqrt();
            let median = arr[num_records as usize / 2];
            db.resp(pack(&mean));
            db.resp(pack(&std));
            db.resp(pack(&median));
            return 0;
        }
        let error = "Object does not exist";
        db.resp(error.as_bytes());
        return 1;
        yield 0;
    })
}

#[allow(unreachable_code)]
fn auth_query_handler(
    db: Rc<DB>,
    table: u64,
    value_len: usize,
    passwd_len: usize,
    // args: &[u8],
) -> Pin<Box<Generator<Yield = u64, Return = u64>>> {
    Box::pin(move || {
        let args = db.args().split_at(ARGS_OFFSET).1;
        // decrypt aes-encrypted passwd
        let key_len = args.len() - passwd_len;
        let (userid, passwd) = args.split_at(key_len);
        let userid = userid.to_vec();
        let mut passwd = passwd.to_vec();
        let mut decrpyted = [0u8; 16];
        let ekey = AesKey::new_decrypt(AES_KEY).unwrap();
        let mut iv = AES_IV.to_vec();
        aes_ige(
            &passwd[0..16],
            &mut decrpyted,
            &ekey,
            &mut iv,
            Mode::Decrypt,
        );
        passwd[0..16].copy_from_slice(&decrpyted[0..16]);
        // hash passwd and compare
        let mut obj = None;
        GET!(db, table, userid, value_len, obj);
        if let Some(val) = obj {
            let (hash, salt) = val.read().split_at(24);
            let mut scrypt_hash = [0u8; 24];
            scrypt(
                &passwd,
                salt,
                &ScryptParams::new(SCRYPT_PARAMS.0, SCRYPT_PARAMS.1, SCRYPT_PARAMS.2),
                &mut scrypt_hash,
            );
            if scrypt_hash == hash {
                db.resp("Ok".as_bytes());
            } else {
                db.resp("Failed".as_bytes());
            }
            return 0;
        }
        let error = "Object does not exist";
        db.resp(error.as_bytes());
        return 1;
    })
}
