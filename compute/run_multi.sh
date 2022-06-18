#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

# 2-type 20-80
cp lb-2.toml lb.toml
cp kayak-2.toml kayak.toml
line_ratio_lb=132
line_ratio_kayak=121
line_compute_lb=198
line_compute_kayak=46
sed -i -e "${line_ratio_lb}c ratios = [20, 80]" ${LB_PATH}
sed -i -e "${line_ratio_kayak}c ratios = [20, 80]" ${KAYAK_PATH}
sed -i -e "${line_compute_lb}c compute = 128" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 128" ${KAYAK_PATH}
# ./nCmS.sh 1
# ./nCmS_points.sh 9

sed -i -e "${line_compute_lb}c compute = 64" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 64" ${KAYAK_PATH}
# ./nCmS.sh 2
# ./nCmS_points.sh 10

sed -i -e "${line_compute_lb}c compute = 32" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 32" ${KAYAK_PATH}
# ./nCmS.sh 3
# ./nCmS_points.sh 11

sed -i -e "${line_compute_lb}c compute = 16" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 16" ${KAYAK_PATH}
# ./nCmS.sh 4
# ./nCmS_points.sh 12

cp lb-4.toml lb.toml
cp kayak-4.toml kayak.toml
./multi_type.sh 1

cp lb-8.toml lb.toml
cp kayak-8.toml kayak.toml
./multi_type.sh 2

cp lb-12.toml lb.toml
cp kayak-12.toml kayak.toml
./multi_type.sh 3

cp lb-16.toml lb.toml
cp kayak-16.toml kayak.toml
./multi_type.sh 4

# 50-50, 80-20
cp lb-2.toml lb.toml
cp kayak-2.toml kayak.toml
sed -i -e "${line_ratio_lb}c ratios = [50, 50]" ${LB_PATH}
sed -i -e "${line_ratio_kayak}c ratios = [50, 50]" ${KAYAK_PATH}
sed -i -e "${line_compute_lb}c compute = 128" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 128" ${KAYAK_PATH}
# ./nCmS-noplus.sh 1

sed -i -e "${line_compute_lb}c compute = 64" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 64" ${KAYAK_PATH}
# ./nCmS-noplus.sh 2

sed -i -e "${line_compute_lb}c compute = 32" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 32" ${KAYAK_PATH}
# ./nCmS-noplus.sh 3

sed -i -e "${line_compute_lb}c compute = 16" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 16" ${KAYAK_PATH}
# ./nCmS-noplus.sh 4

sed -i -e "${line_ratio_lb}c ratios = [80, 20]" ${LB_PATH}
sed -i -e "${line_ratio_kayak}c ratios = [80, 20]" ${KAYAK_PATH}
sed -i -e "${line_compute_lb}c compute = 128" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 128" ${KAYAK_PATH}
# ./nCmS-noplus.sh 5

sed -i -e "${line_compute_lb}c compute = 64" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 64" ${KAYAK_PATH}
# ./nCmS-noplus.sh 6

sed -i -e "${line_compute_lb}c compute = 32" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 32" ${KAYAK_PATH}
# ./nCmS-noplus.sh 7

sed -i -e "${line_compute_lb}c compute = 16" ${LB_PATH}
sed -i -e "${line_compute_kayak}c compute = 16" ${KAYAK_PATH}
# ./nCmS-noplus.sh 8
