#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

line_ratio_lb=138
line_compute_lb=142
line_ratio_kayak=119
line_compute_kayak=123

comp=()
num=7
sed -i -e "${line_ratio_lb}c ratios = [20, 80]" ${LB_PATH}
sed -i -e "${line_ratio_kayak}c ratios = [20, 80]" ${KAYAK_PATH}
for c in ${comp[@]}
do
    sed -i -e "${line_compute_lb}c compute = ${c}" ${LB_PATH}
    sed -i -e "${line_compute_kayak}c compute = ${c}" ${KAYAK_PATH}
    ./out.sh ${num}
    let "num+=1"
done

comp=(128 64 32 16 8 4)
sed -i -e "${line_ratio_lb}c ratios = [40, 60]" ${LB_PATH}
sed -i -e "${line_ratio_kayak}c ratios = [40, 60]" ${KAYAK_PATH}
for c in ${comp[@]}
do
    sed -i -e "${line_compute_lb}c compute = ${c}" ${LB_PATH}
    sed -i -e "${line_compute_kayak}c compute = ${c}" ${KAYAK_PATH}
    ./out.sh ${num}
    let "num+=1"
done

sed -i -e "${line_ratio_lb}c ratios = [80, 20]" ${LB_PATH}
sed -i -e "${line_ratio_kayak}c ratios = [80, 20]" ${KAYAK_PATH}
for c in ${comp[@]}
do
    sed -i -e "${line_compute_lb}c compute = ${c}" ${LB_PATH}
    sed -i -e "${line_compute_kayak}c compute = ${c}" ${KAYAK_PATH}
    ./out.sh ${num}
    let "num+=1"
done

