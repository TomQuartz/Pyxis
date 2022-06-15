#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

LOG_PATH="../logs/multi-type/"

date=`date +%Y-%m-%d`

cp lb-4.toml lb.toml
cp kayak-4.toml kayak.toml
./multi_type.sh 2

cp lb-8.toml lb.toml
cp kayak-8.toml kayak.toml
./multi_type.sh 3

cp lb-12.toml lb.toml
cp kayak-12.toml kayak.toml
./multi_type.sh 4

# cp lb-16.toml lb.toml
# cp kayak-16.toml kayak.toml
# ./multi_type.sh 1