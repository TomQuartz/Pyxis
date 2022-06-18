#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

LOG_PATH="../logs/perf-2-type/"

date=`date +%Y-%m-%d`

if [ ! -d ${LOG_PATH} ]; then
    mkdir -p ${LOG_PATH}
fi

cd ${LOG_PATH}
OUTPUT_FILE="${date}_$1.log"
if [ -e ${OUTPUT_FILE} ]; then
    rm ${OUTPUT_FILE}
fi
cd -
OUTPUT=${LOG_PATH}${OUTPUT_FILE}


# lb
line_learnable_lb=18
line_partition_lb=7
line_output_lb=9
line_num_cores_lb=52
line_max_out_lb=85
line_slo_lb=44
slo=(3 6 10 15 20 30 40 50 60 80)
cores=(4)
echo "lb:" >> ${OUTPUT}
sed -i -e "${line_learnable_lb}c learnable = true" ${LB_PATH}
sed -i -e "${line_partition_lb}c partition = 50" ${LB_PATH}
sed -i -e "${line_output_lb}c output = false" ${LB_PATH}
sed -i -e "${line_slo_lb}c slo = 2" ${LB_PATH}
for c in ${cores[@]}
do
    sed -i -e "${line_num_cores_lb}c num_cores = ${c}" ${LB_PATH}
    echo "num_cores = ${c}" >> ${OUTPUT}
    sudo ../scripts/run-elastic >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done
sed -i -e "${line_num_cores_lb}c num_cores = 8" ${LB_PATH}
for t in ${slo[@]}
do
    sed -i -e "${line_slo_lb}c slo = ${t}" ${LB_PATH}
    echo "slo = ${t}" >> ${OUTPUT}
    sudo ../scripts/run-elastic >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done


# kayak
line_learnable=6
line_partition=7
line_max_out_kayak=8
line_num_cores_kayak=38
line_slo_kayak=30
line_output_kayak=10
maxout=(1 2 4 6 8 12 16)
cores=(2 4)
echo "kayak:" >> ${OUTPUT}
sed -i -e "${line_learnable}c learnable = true" ${KAYAK_PATH}
sed -i -e "${line_partition}c partition = 50" ${KAYAK_PATH}
sed -i -e "${line_output_kayak}c output = false" ${KAYAK_PATH}
sed -i -e "${line_max_out_kayak}c max_out = 1" ${KAYAK_PATH}
for c in ${cores[@]}
do
    sed -i -e "${line_num_cores_kayak}c num_cores = ${c}" ${KAYAK_PATH}
    echo "num_cores = ${c}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done
sed -i -e "${line_num_cores_kayak}c num_cores = 8" ${KAYAK_PATH}
maxout=(1 2 4 6 8 12 16 32)
for t in ${maxout[@]}
do
    sed -i -e "${line_max_out_kayak}c max_out = ${t}" ${KAYAK_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done


# kayak-plus
line_duration=4
sed -i -e "${line_duration}c duration = 90" ${KAYAK_PATH}
echo "kayakplus:" >> ${OUTPUT}
sed -i -e "${line_learnable}c learnable = true" ${KAYAK_PATH}
sed -i -e "${line_partition}c partition = 50" ${KAYAK_PATH}
sed -i -e "${line_output_kayak}c output = false" ${KAYAK_PATH}
sed -i -e "${line_max_out_kayak}c max_out = 1" ${KAYAK_PATH}
cores=(4)
for c in ${cores[@]}
do
    sed -i -e "${line_num_cores_kayak}c num_cores = ${c}" ${KAYAK_PATH}
    echo "num_cores = ${c}" >> ${OUTPUT}
    sudo ../scripts/run-kayak-plus >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done
sed -i -e "${line_num_cores_kayak}c num_cores = 8" ${KAYAK_PATH}
maxout=(1 2 4 8 16 32 64 128)
for q in ${maxout[@]}
do
    sed -i -e "${line_max_out_kayak}c max_out = ${q}" ${KAYAK_PATH}
    echo "max_out = ${q}" >> ${OUTPUT}
    sudo ../scripts/run-kayak-plus >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done
sed -i -e "${line_duration}c duration = 20" ${KAYAK_PATH}

cat ${LB_PATH} >> ${OUTPUT}

# python3 ../logs/perform.py ${OUTPUT}