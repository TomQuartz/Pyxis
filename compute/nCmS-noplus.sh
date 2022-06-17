#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

# Only usable to 2-type table(50-50 and 80-20)

LOG_PATH="../logs/perf-2-type-table/"

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
line_learnable_lb=6
line_partition_lb=7
line_output_lb=9
line_num_cores_lb=52
line_max_out_lb=85
line_slo_lb=44
slo=(10 20 30 50 80)
echo "lb:" >> ${OUTPUT}
sed -i -e "${line_learnable_lb}c learnable = true" ${LB_PATH}
sed -i -e "${line_partition_lb}c partition = 50" ${LB_PATH}
sed -i -e "${line_output_lb}c output = false" ${LB_PATH}
sed -i -e "${line_slo_lb}c slo = 2" ${LB_PATH}
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
echo "kayak:" >> ${OUTPUT}
sed -i -e "${line_learnable}c learnable = true" ${KAYAK_PATH}
sed -i -e "${line_partition}c partition = 50" ${KAYAK_PATH}
sed -i -e "${line_output_kayak}c output = false" ${KAYAK_PATH}
sed -i -e "${line_max_out_kayak}c max_out = 1" ${KAYAK_PATH}
sed -i -e "${line_num_cores_kayak}c num_cores = 8" ${KAYAK_PATH}
maxout=(1 2 4 8 16)
for t in ${maxout[@]}
do
    sed -i -e "${line_max_out_kayak}c max_out = ${t}" ${KAYAK_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

cat ${LB_PATH} >> ${OUTPUT}