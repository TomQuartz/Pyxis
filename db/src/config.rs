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

use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::Read;

use super::e2d2::headers::*;
use super::toml;
use e2d2::config::{NetbricksConfiguration, PortConfiguration};
// use e2d2::scheduler::NetBricksContext as NetbricksContext;
use e2d2::scheduler::*;
use std::collections::HashMap;

/// To show the error while parsing the MAC address.
#[derive(Debug, Clone)]
pub struct ParseError;

impl Error for ParseError {
    fn description(&self) -> &str {
        "Malformed MAC address."
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Malformed MAC address.")
    }
}

/// Parses str into a MacAddress or returns ParseError.
/// str must be formatted six colon-separated hex literals.
pub fn parse_mac(mac: &str) -> Result<MacAddress, ParseError> {
    let bytes: Result<Vec<_>, _> = mac.split(':').map(|s| u8::from_str_radix(s, 16)).collect();

    match bytes {
        Ok(bytes) => {
            if bytes.len() == 6 {
                Ok(MacAddress::new_from_slice(&bytes))
            } else {
                Err(ParseError {})
            }
        }
        Err(_) => Err(ParseError {}),
    }
}

// /// Load a config from `filename` otherwise return a default structure.
// fn load_config(filename: &str) -> ServerConfig {
//     let mut contents = String::new();

//     let _ = File::open(filename).and_then(|mut file| file.read_to_string(&mut contents));

//     match toml::from_str(&contents) {
//         Ok(config) => config,
//         Err(e) => {
//             warn!("Failure paring config file {}: {}", filename, e);
//             ServerConfig::default()
//         }
//     }
// }

/// Load a config from `filename` otherwise return a default structure.
fn load_config_cl(filename: &str) -> ClientConfig {
    let mut contents = String::new();

    let _ = File::open(filename).and_then(|mut file| file.read_to_string(&mut contents));

    match toml::from_str(&contents) {
        Ok(config) => config,
        Err(e) => {
            warn!("Failure paring config file {}: {}", filename, e);
            ClientConfig::default()
        }
    }
}
/*
/// All of the various configuration options needed to run a server, both optional and required.
/// Normally this config is recovered from a server.toml file (an example of which is in
/// server.toml-example). If this file is malformed or missing, the server will typically
/// crash when it cannot determine a MAC address to bind to.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ServerConfig {
    /// Server MAC Address.
    mac_address: String,
    /// Server IP Address.
    pub ip_address: String,
    /// Server UDP port for response packets.
    pub udp_port: u16,
    /// PCI address for the NIC.
    pub nic_pci: String,
    /// Client MAC Address.
    client_mac: String,
    /// Client IP Address.
    pub client_ip: String,
    /// Number of tenants to intialize the tables.
    pub num_tenants: u32,
    /// Network endpoint to install new extensions.
    pub install_addr: String,
    /// Type of workload; TAO, YCSB, AGGREGATE etc.
    pub workload: String,
    /// Number of records in the table for each tenant.
    pub num_records: u32,
    /// Number of cores on storage
    pub num_cores: i32,
    /// rx batch size
    pub max_rx_packets: usize,
    pub key_len: usize,
    pub value_len: usize,
    pub record_len: usize,
}

impl ServerConfig {
    /// Load server config from server.toml file in the current directory or otherwise return a
    /// default structure.
    pub fn load() -> ServerConfig {
        load_config("server.toml")
    }

    /// Parse `mac_address` into NetBrick's format or panic if malformed.
    /// Linear time, so ideally we'd store this in ServerConfig, but TOML parsing makes that tricky.
    pub fn parse_mac(&self) -> MacAddress {
        parse_mac(&self.mac_address)
            .expect("Missing or malformed mac_address field in server config.")
    }

    /// Parse `client_mac` into NetBrick's format or panic if malformed.
    /// Linear time, so ideally we'd store this in ServerConfig, but TOML parsing makes that tricky.
    pub fn parse_client_mac(&self) -> MacAddress {
        parse_mac(&self.client_mac)
            .expect("Missing or malformed mac_address field in server config.")
    }
}
*/

/// All of the various configuration options needed to run a client, both optional and required.
/// Normally this config is recovered from a client.toml file (an example of which is in
/// client.toml-example). If this file is malformed or missing, the client will typically
/// crash when it cannot determine a MAC address to bind to.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ClientConfig {
    /// Client MAC Address.
    mac_address: String,
    /// Client IP Address.
    pub ip_address: String,
    /// PCI address for the NIC.
    pub nic_pci: String,

    /// Server MAC Address.
    server_mac_address: String,
    /// Server IP Address.
    pub server_ip_address: String,
    /// Number of UDP ports to send requests to.
    pub server_udp_ports: u16,

    /// Number of tenants for requests generation.
    pub num_tenants: u32,
    /// Server network endpoint to install new extensions.
    pub install_addr: String,

    /// Type of workload; YCSBT etc.
    pub workload: String,
    /// This parameter decides the load type; open-loop load or closed-loop load.
    pub open_load: bool,
    /// This parameter decides the requests type; native or extension.
    pub use_invoke: bool,

    /// SLO target, unit: Cycles
    pub slo: u64,
    /// Kayak R Loop slowdown factor
    pub kayak_rloop_factor: usize,
    /// Kayak X Loop slowdown factor
    pub kayak_xloop_factor: usize,
    /// Initial X for Kayak, range [0,100]
    pub invoke_p: usize,
    /// Initial outstanding requests
    pub max_out: usize,

    /// Switch Bimodal mode on
    pub bimodal: bool,
    /// Bimodal mode only: Bimodal Interval, unit: Cycles
    pub bimodal_interval: u64,
    /// Bimodal mode only: Computation weight of the 2nd mode, in addition to order
    pub order2: u32,

    /// Length of the key for requests generation.
    pub key_len: usize,
    /// Length of the value for requests generation and response parsing.
    pub value_len: usize,
    /// Number of records in the table, needed in requests generation.
    pub n_keys: usize,
    /// Percentage of put() requests for YCSB workload.
    pub put_pct: usize,
    /// Used for enabling or disabling scan requests for YCSB workload.
    pub enable_scan: bool,
    /// Used for indicating the range of the scan query.
    pub scan_range: u32,
    /// Skew in Zipf distribution used for YCSB workload.
    pub skew: f64,
    /// Tenant skew to show the gain due to workstealing on the server side.
    pub tenant_skew: f64,

    /// Total number of requets generated by the client for one run.
    pub num_reqs: usize,
    /// Number of requets generated per second.
    pub req_rate: usize,

    /// Number of records aggregated per requests for AGGREGATE workload.
    pub num_aggr: u32,
    /// Number of multiplications done per aggregation for AGGREGATE workload.
    pub order: u32,

    /// If true, then an invoke() based run will use native requests for an obj_get.
    pub combined: bool,
    /// The percentage of assoc_range() requests.
    pub assocs_p: usize,

    /// The percentage of invoke() based requests that are long running.
    pub long_pct: usize,
    /// The frequency at which the extension should yield to the database.
    pub yield_f: u8,

    ///The number of bad requests to generate for every 10 million operations.
    pub bad_ptm: usize,

    /// num kv for multi-type
    pub multi_kv: Vec<u32>,
    /// cpu computation for multi-type
    pub multi_ord: Vec<u32>,
    /// ratio for each type
    pub multi_ratio: Vec<f32>,
    /// compute rpc rate for each type or not
    pub multi_rpc: bool,
    /// split each type into several steps
    // pub multi_steps: Vec<u32>,
    /// our partition
    pub partition: i32,
    /// mapping from type to remote core
    pub type2core: Vec<Vec<u16>>,
    /// sender threads
    pub num_sender: usize,
    /// compute the rpc rate using kayak's xloop
    pub learnable: bool,
    /// interval for the second type in bimodal
    pub bimodal_interval2: u64,
    /// ratio of the first type in two intervals
    pub bimodal_ratio: Vec<u32>,
    /// two rpc rate for each modal
    pub bimodal_rpc: Vec<u32>,
    /// how many cycles between each stats output
    pub output_factor: u64,
    // pub max_credits: u32,
}

impl ClientConfig {
    /// Load client config from client.toml file in the current directory or otherwise return a
    /// default structure.
    pub fn load() -> ClientConfig {
        load_config_cl("client.toml")
    }

    /// Parse `mac_address` into NetBrick's format or panic if malformed.
    /// Linear time, so ideally we'd store this in ClientConfig, but TOML parsing makes that tricky.
    pub fn parse_mac(&self) -> MacAddress {
        parse_mac(&self.mac_address)
            .expect("Missing or malformed mac_address field in client config.")
    }

    /// Parse `server_mac_address` into NetBrick's format or panic if malformed.
    /// Linear time, so ideally we'd store this in ClientConfig, but TOML parsing makes that tricky.
    pub fn parse_server_mac(&self) -> MacAddress {
        parse_mac(&self.server_mac_address)
            .expect("Missing or malformed server_mac_address field in client config.")
    }
}

#[cfg(test)]
mod tests {
    use super::parse_mac;

    #[test]
    fn empty_str() {
        if let Err(e) = parse_mac("") {
            assert_eq!("Malformed MAC address.", e.to_string());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn ok_str() {
        if let Ok(m) = parse_mac("A1:b2:C3:d4:E5:f6") {
            assert_eq!(0xa1, m.addr[0]);
            assert_eq!(0xb2, m.addr[1]);
            assert_eq!(0xc3, m.addr[2]);
            assert_eq!(0xd4, m.addr[3]);
            assert_eq!(0xe5, m.addr[4]);
            assert_eq!(0xf6, m.addr[5]);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn bad_examples() {
        if let Ok(_) = parse_mac("A1:b2:C3:d4:E5:g6") {
            assert!(false);
        } else {
        }
        if let Ok(_) = parse_mac("A1:b2:C3:d4:E5: 6") {
            assert!(false);
        } else {
        }
        if let Ok(_) = parse_mac("A1:b2:C3:d4:E5") {
            assert!(false);
        } else {
        }
        if let Ok(_) = parse_mac(":::::") {
            assert!(false);
        } else {
        }
    }
}

/// load
pub fn load<T>(path: &str) -> T
where
    T: Default + serde::de::DeserializeOwned,
{
    let mut contents = String::new();

    let _ = File::open(path).and_then(|mut file| file.read_to_string(&mut contents));
    match toml::from_str(&contents) {
        Ok(config) => config,
        Err(e) => {
            warn!("Failure paring config file {}: {}", path, e);
            T::default()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct NetConfig {
    pub rx_queues: i32,
    pub ip_addr: String,
    pub mac_addr: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct ServerConfig {
    pub nic_pci: String,
    pub num_cores: i32,
    pub max_rx_packets: usize,
    pub server: NetConfig,
}
/*
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct LBConfig {
    pub num_tenants: u32,
    pub key_len: usize,
    pub value_len: usize,
    pub n_keys: usize,
    pub put_pct: usize,
    pub skew: f64,
    pub tenant_skew: f64,
    pub num_reqs: usize,
    pub multi_kv: Vec<u32>,
    pub multi_ord: Vec<u32>,
    pub multi_ratio: Vec<f32>,
    pub multi_rpc: bool,
    // pub type2core: Vec<Vec<u16>>,
    pub learnable: bool,
    pub invoke_p: usize,
    pub partition: f64,
    pub max_out: u32,
    pub xloop_factor: u64,
    pub moving_exp: f64,
    // tput
    pub lr: f64,
    pub max_step_rel: f64,
    pub max_step_abs: f64,
    pub min_step_rel: f64,
    pub min_step_abs: f64,
    pub min_interval: f64,
    pub max_err: f64,
    // pub max_ql_diff: f64,
    // pub min_ql_diff: f64,
    // pub ql_lowerbound: f64,
    // pub max_step: f64,
    // pub min_lr: f64,
    // qgrad
    pub thresh_ql: f64,
    // pub thresh_tput: f64,
    // pub lr: f64,
    pub lr_decay: f64,
    // pub max_step: f64,
    // pub min_step: f64,
    // // queue
    // pub ql_diff_thresh: f64,
    // // pivot
    // pub pivot: f64,
    // pub range: f64,
    // pub step_large: f64,
    // pub step_small: f64,
    // // sweep
    // pub min_range: f64,
    // pub monotone_thresh: f64,
    // pub max_step: f64,
    // pub lr_decay: f64,
    // bimodal, not used for now
    pub bimodal: bool,
    pub bimodal_interval: u64,
    pub bimodal_interval2: u64,
    pub bimodal_ratio: Vec<u32>,
    pub bimodal_rpc: Vec<u32>,
    pub output_factor: u64,
    // network configuration
    pub lb: ServerConfig,
    pub compute: Vec<NetConfig>,
    pub storage: Vec<NetConfig>,
}
*/

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct KayakConfig {
    pub num_tenants: usize,
    pub tenant_skew: f64,
    // stop after duration
    pub duration: u64,
    pub learnable: bool,
    pub partition: f64,
    pub max_out: u32,
    pub xloop_factor: u64,
    pub output_factor: u64,
    // TODO: add kayak configurations here
    pub xloop_learning_rate: f64,
    // network configuration
    pub lb: ServerConfig,
    pub provision: ProvisionConfig,
    // [[]]
    pub compute: Vec<NetConfig>,
    pub storage: Vec<NetConfig>,
    // workload configuration
    pub tables: Vec<TableConfig>,
    pub workloads: Vec<WorkloadConfig>,
    pub phases: Vec<PhaseConfig>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct XloopConfig {
    pub factor: u64,
    pub lr: f64,
    pub max_step_rel: f64,
    pub max_step_abs: f64,
    pub min_step_rel: f64,
    pub min_step_abs: f64,
    // the major criterion
    pub min_interval: f64,
    // not useful
    pub min_delta_rel: f64,
    pub min_delta_abs: f64,
    pub max_err: f64,
    pub tolerance: u32,
    // not useful
    pub min_err: f64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct ElasticConfig {
    pub elastic: bool,
    pub max_load: f64,
    pub min_load: f64,
    // pub max_load_abs: f64,
    // pub max_load_rel: f64,
    // pub min_load_abs: f64,
    // pub min_load_rel: f64,
    pub max_step: i32,
    // pub convergence: u32,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct LBConfig {
    pub num_tenants: usize,
    pub tenant_skew: f64,
    // stop after duration
    pub duration: u64,
    pub learnable: bool,
    pub partition: f64,
    pub max_out: u32,
    pub output_factor: u64,
    // profile ratio, cost, and sort
    pub sample_factor: u64,
    // xloop with tput
    pub xloop: XloopConfig,
    // elastic
    pub elastic: ElasticConfig,
    // network configuration
    pub lb: ServerConfig,
    // TODO: add utilization config
    // NOTE: this is the number of storage ports, equal duration
    // NOTE: assumes no workstealing
    // [[]]
    pub compute: Vec<NetConfig>,
    pub storage: Vec<NetConfig>,
    // workload configuration
    pub tables: Vec<TableConfig>,
    pub workloads: Vec<WorkloadConfig>,
    pub phases: Vec<PhaseConfig>,
    pub provisions: Vec<ProvisionConfig>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct ComputeConfig {
    // pub max_credits: u32,
    pub moving_exp: f64,
    pub compute: ServerConfig,
    // NOTE: all exts are loaded by default
    // pub exts: Vec<String>,
    // [[]]
    pub storage: Vec<NetConfig>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct TableConfig {
    pub description: String,
    // pub table_id: u64,
    pub key_len: usize,
    // length of all entries corresponding to the key
    pub value_len: usize,
    // length of a single entry
    pub record_len: u32,
    pub num_records: u32,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct WorkloadConfig {
    pub extension: String,
    // TODO: add variation
    pub kv: u32,
    pub order: u32,
    pub table_id: u64,
    pub opcode: u8,
    pub skew: f64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct PhaseConfig {
    /// workload ids
    // pub workloads: Vec<usize>,
    // NOTE: this ratio is over all workloads and may include zero
    // this is ok since ratios are cumsumed
    pub ratios: Vec<f32>,
    pub duration: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct ProvisionConfig {
    pub storage: u32,
    pub compute: u32,
    pub duration: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
pub struct StorageConfig {
    /// Number of tenants to intialize the tables.
    pub num_tenants: u32,
    // /// Type of workload; TAO, YCSB, AGGREGATE etc.
    // pub workload: String,
    // /// Number of records in the table for each tenant.
    // pub num_records: u32,
    // /// rx batch size
    // pub key_len: usize,
    // pub value_len: usize,
    // pub record_len: usize,
    // not used
    pub moving_exp: f64,
    pub storage: ServerConfig,
    // NOTE: all exts are loaded by default
    // pub exts: Vec<String>,
    // [[]]
    pub tables: Vec<TableConfig>,
}

pub fn get_default_netbricks_config(config: &ServerConfig) -> NetbricksConfiguration {
    // General arguments supplied to netbricks.
    let net_config_name = String::from("storage");
    let dpdk_secondary: bool = false;
    let net_primary_core: i32 = 19;
    let net_queues: Vec<i32> = (0..config.server.rx_queues).collect();
    let net_strict_cores: bool = false;
    let net_pool_size: u32 = 32768 - 1;
    let net_cache_size: u32 = 512;
    let net_dpdk_args: Option<String> = None;

    // Port configuration. Required to configure the physical network interface.
    let net_port_name = config.nic_pci.clone();
    let net_port_rx_queues: Vec<i32> = net_queues.clone();
    let net_port_tx_queues: Vec<i32> = net_queues.clone();
    let net_port_rxd: i32 = 8192 / config.server.rx_queues;
    let net_port_txd: i32 = 8192 / config.server.rx_queues;
    let net_port_loopback: bool = false;
    let net_port_tcp_tso: bool = false;
    let net_port_csum_offload: bool = false;

    let net_port_config = PortConfiguration {
        name: net_port_name,
        rx_queues: net_port_rx_queues,
        tx_queues: net_port_tx_queues,
        rxd: net_port_rxd,
        txd: net_port_txd,
        loopback: net_port_loopback,
        tso: net_port_tcp_tso,
        csum: net_port_csum_offload,
    };

    // The set of ports used by netbricks.
    let net_ports: Vec<PortConfiguration> = vec![net_port_config];
    let net_cores: Vec<i32> = (0..config.num_cores).collect();
    NetbricksConfiguration {
        name: net_config_name,
        secondary: dpdk_secondary,
        primary_core: net_primary_core,
        cores: net_cores,
        strict: net_strict_cores,
        ports: net_ports,
        pool_size: net_pool_size,
        cache_size: net_cache_size,
        dpdk_args: net_dpdk_args,
    }
}

/// This function configures and initializes Netbricks. In the case of a
/// failure, it causes the program to exit.
///
/// Returns a Netbricks context which can be used to setup and start the
/// server/client.
pub fn config_and_init_netbricks(config: &ServerConfig) -> NetBricksContext {
    let net_config: NetbricksConfiguration = get_default_netbricks_config(config);

    // Initialize Netbricks and return a handle.
    match initialize_system(&net_config) {
        Ok(net_context) => {
            return net_context;
        }

        Err(ref err) => {
            error!("Error during Netbricks init: {}", err);
            // TODO: Drop NetbricksConfiguration?
            std::process::exit(1);
        }
    }
}
