#!/usr/bin/env bash
set -exu

workspace=$PWD

cd ..
wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-2.2.4.0/MLNX_OFED_LINUX-4.9-2.2.4.0-ubuntu18.04-x86_64.tgz
tar xvf MLNX_OFED_LINUX-4.9-2.2.4.0-ubuntu18.04-x86_64.tgz
cd MLNX_OFED_LINUX-4.9-2.2.4.0-ubuntu18.04-x86_64
echo "y" | sudo ./mlnxofedinstall 
sudo /etc/init.d/openibd restart
cd $workspace

sudo apt-get update && sudo apt-get install -y libnuma-dev clang numactl

bash ./net/3rdparty/get-dpdk.sh

# cd net/native
# make
# cd ../..
# mkdir -p net/target/native
# cp net/native/libzcsi.so net/target/native/libzcsi.so

# Make sure rust is up-to-date
if [ ! -x "$(command -v rustup)" ] ; then
    # curl https://sh.rustup.rs -sSf | sh -s -- -y
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain none -y
fi

source $HOME/.cargo/env
# Use nightly-2021-03-24
rustup toolchain install nightly-2023-01-26 --force
rustup default nightly-2023-01-26
# rustup default nightly
# rustup update

source ../.bashrc
make

sudo sed -i 's/GRUB_CMDLINE_LINUX_DEFAULT=""/GRUB_CMDLINE_LINUX_DEFAULT="default_hugepagesz=1G hugepagesz=1G hugepages=32 intel_idle.max_cstate=0 idle=halt"/g' /etc/default/grub
sudo update-grub

# PCI=$(python ./net/3rdparty/dpdk/usertools/dpdk-devbind.py --status-dev=net | grep ens1f1 | grep Active | tail -1 | awk '{ print $1 }')
# MAC=$(ethtool -P ens1f1 | awk '{ print $3 }')

# echo "pci: $PCI" > nic_info
# echo "mac: $MAC" >> nic_info

# default_hugepagesz=1G hugepagesz=1G hugepages=16 intel_idle.max_cstate=0 idle=halt 
