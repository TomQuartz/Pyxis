#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

line_max_load=29
line_min_load=30

num=1
sed -i -e "${line_max_load}c max_load = 0.6" ${LB_PATH}
sed -i -e "${line_min_load}c min_load = 0.55" ${LB_PATH}
./elastic_curve.sh ${num}
let "num+=1"

sed -i -e "${line_max_load}c max_load = 0.7" ${LB_PATH}
sed -i -e "${line_min_load}c min_load = 0.65" ${LB_PATH}
./elastic_curve.sh ${num}
let "num+=1"

# sed -i -e "${line_max_load}c max_load = 0.8" ${LB_PATH}
# sed -i -e "${line_min_load}c min_load = 0.75" ${LB_PATH}
# ./elastic_curve.sh ${num}
# let "num+=1"
