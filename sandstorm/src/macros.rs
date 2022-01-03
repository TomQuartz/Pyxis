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

/// The macro for get() operation. This macro calls db.get() on the server side and checks
/// the local cache for the data on the client side. If the data is not present in the cache
/// then it requests the data using the RPC call.
#[macro_export]
macro_rules! GET {
    ($db:ident, $table:ident, $key:ident, $size:ident, $obj:ident) => {
        let (server, found, val) = $db.search_get_in_cache($table, &$key, $size);
        if server == false {
            if found == false {
                yield 0;
                $obj = $db.get($table, &$key, $size);
            } else {
                $obj = val;
            }
        } else {
            $obj = $db.get($table, &$key, $size);
        }
    };
}

#[macro_export]
macro_rules! ASSOC_GET {
    ($db:ident, $table:ident, $key:ident) => {
        let (server, found, assoc) = $db.search_get_in_cache($table, &$key);
        if server == false {
            if found == false {
                yield 0;
                // $obj = $db.get($table, &$key);
                let assoc = $db.get($table, &$key).unwrap().read();
                key.extend_from_slice(assoc);
            } else {
                // $obj = val;
                key.extend_from_slice(assoc);
            }
        } else {
            // $obj = $db.get($table, &$key);
            let assoc = $db.get($table, &$key).unwrap().read();
            key.extend_from_slice(assoc);
        }
    };
}

#[macro_export]
macro_rules! MULTIGET {
    ($db:ident, $table:ident, $key_len:ident, $keys:ident, $size:ident, $objs:ident) => {
        let (server, found, val) = $db.search_multiget_in_cache($table, $key_len, &$keys, $size);
        if server == false {
            if found == false {
                yield 0;
                $obj = $db.multiget($table, $key_len, &$keys, $size);
            } else {
                $obj = val;
            }
        } else {
            $obj = $db.multiget($table, $key_len, &$keys, $size);
        }
    };
}

/*
/// TODO: Change it later, not implemented fully.
#[macro_export]
macro_rules! MULTIGET {
    ($db:ident, $table:ident, $keylen:ident, $keys:ident, $buf:ident) => {
        let mut is_server = false;
        let mut objs = Vec::new();
        for key in $keys.chunks($keylen as usize) {
            if key.len() != $keylen as usize {
                break;
            }
            let (server, _, val) = $db.search_get_in_cache($table, key);
            if server == true {
                $buf = $db.multiget($table, $keylen, &$keys);
                is_server = server;
                break;
            } else {
                objs.push(Bytes::from(val.unwrap().read()))
            }
        }
        if is_server == false {
            unsafe {
                $buf = Some(MultiReadBuf::new(objs));
            }
        }
    };
}
*/
