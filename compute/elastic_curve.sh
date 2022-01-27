#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

LOG_PATH="../logs/elastic_curve_rerun/"

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
line_elas_lb=28
line_num_cores_lb=38
line_max_out_lb=118
line_comp_lb=178

maxout=(1 2 4 8 16 32 64 128 192)
cores=()
echo "lb:" >> ${OUTPUT}

sed -i -e "${line_learnable_lb}c learnable = true" ${LB_PATH}
sed -i -e "${line_elas_lb}c elastic = false" ${LB_PATH}
sed -i -e "${line_partition_lb}c partition = 50" ${LB_PATH}
sed -i -e "${line_max_out_lb}c max_out = 1" ${LB_PATH}
sed -i -e "${line_comp_lb}c compute = 128" ${LB_PATH}
for c in ${cores[@]}
do
    sed -i -e "${line_num_cores_lb}c num_cores = ${c}" ${LB_PATH}
    echo "num_cores = ${c}" >> ${OUTPUT}
    # sudo ../scripts/run-elastic >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

sed -i -e "${line_num_cores_lb}c num_cores = 8" ${LB_PATH}
for t in ${maxout[@]}
do
    sed -i -e "${line_max_out_lb}c max_out = ${t}" ${LB_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    # sudo ../scripts/run-elastic >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

# elastic
echo "lb:" >> ${OUTPUT}
sed -i -e "${line_elas_lb}c elastic = true" ${LB_PATH}
sed -i -e "${line_max_out_lb}c max_out = 1" ${LB_PATH}
sed -i -e "${line_comp_lb}c compute = 64" ${LB_PATH}
for c in ${cores[@]}
do
    sed -i -e "${line_num_cores_lb}c num_cores = ${c}" ${LB_PATH}
    echo "num_cores = ${c}" >> ${OUTPUT}
    sudo ../scripts/run-elastic >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

sed -i -e "${line_num_cores_lb}c num_cores = 8" ${LB_PATH}
for t in ${maxout[@]}
do
    sed -i -e "${line_max_out_lb}c max_out = ${t}" ${LB_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    sudo ../scripts/run-elastic >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

cat ${LB_PATH} >> ${OUTPUT}

# python3 ../logs/perform.py ${OUTPUT}