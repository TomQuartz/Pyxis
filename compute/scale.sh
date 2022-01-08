#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

LOG_PATH="../logs/scalability/"

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
line_num_cores_lb=38
line_max_out_lb=85
line_provision_compute=122

num_compute=(16 32 48 64)
maxout=(8 12 16 24 32)
cores=()
echo "lb:" >> ${OUTPUT}

sed -i -e "${line_learnable_lb}c learnable = true" ${LB_PATH}
sed -i -e "${line_partition_lb}c partition = 50" ${LB_PATH}
for t in ${maxout[@]}
do
    sed -i -e "${line_max_out_lb}c max_out = ${t}" ${LB_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    for k in ${num_compute[@]}
    do
        sed -i -e "${line_provision_compute}c compute = ${k}" ${LB_PATH}
        echo "compute = ${k}" >> ${OUTPUT}
        sudo ../scripts/run-elastic >> ${OUTPUT}
        echo "" >> ${OUTPUT}
    done
done

# python3 ../logs/perform.py ${OUTPUT}