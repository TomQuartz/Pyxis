#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

line_max_load=29
line_min_load=30

num=4
./out.sh ${num} 88
let "num+=1"

./out.sh ${num} 89
let "num+=1"

./out.sh ${num} 90
let "num+=1"
