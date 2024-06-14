# Pyxis

Pyxis is scheduler for heterogenous workloads in disaggregated datacenter. Pyxis is a fork of [Kayak](https://github.com/SymbioticLab/Kayak) that overcomes its limitation of supporting only homogenous tasks with a novel "all-or-nothing" scheduling algorithm.

## Setting up Pyxis

Pyxis is deployed on [CloudLab](https://www.cloudlab.us/) using `xl170` machine in Utah cluster:
1. Instantiate a CloudLab cluster using the python profile `cloudlab_profile.py`. This should provide a cluster of bare-metal machines running Ubuntu 18.04 with kernel 4.15.0-147 and gcc 7.5.0.
2. Run the provided `setup.sh`. This will install [MLNX OFED driver]((https://content.mellanox.com/ofed/MLNX_OFED-4.9-2.2.4.0/MLNX_OFED_LINUX-4.9-2.2.4.0-ubuntu18.04-x86_64.tgz)), DPDK as well as the latest version of Rust.
3. `make`.

## Running Pyxis

### Configuration

Pyxis consists of three parts: the scheduler (with load generator colocated), the compute nodes, and storage nodes. Each takes a configuration file in the TOML format.

- Load balancer: `compute/lb-elastic.toml`
- Compute Nodes: `compute/compute.toml`
- Storage Nodes: `db/storage.toml`

You may find in the respective directories example TOML config files. Please copy and modify these files as needed. Note that the config files should be placed on the respective machines where components run.

### Compute and storage nodes

The compute and storage nodes are long-running processes that should be started on the respective machines. 

- To start one compute node, run
  `./scripts/run-compute`
- To start one storage node, run
  `./scripts/run-storage`

### Pyxis scheduler

After the compute and storage nodes are running, start the Pyxis scheduler with `./scripts/run-elastic`

### Kayak

Alternatively, start the Kayak scheduler with `./scripts/run-kayak`


## Acknowledgements

Thanks to Jie You, Jingfeng Wu, Xin Jin, and Mosharaf Chowdhury for the [Kayak repo](https://github.com/SymbioticLab/Kayak).

Thanks to Chinmay Kulkarni, Ankit Bhardwaj and Ryan Stutsman for the [Splinter repo](https://github.com/utah-scs/splinter).
