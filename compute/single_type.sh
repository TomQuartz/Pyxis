#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

LOG_PATH="../logs/multi-rpc/"

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


# kayak
line_learnable_kayak=6
line_partition_kayak=7
line_max_out_kayak=8

maxout=(32)
partition=(50)

sed -i -e "${line_learnable_kayak}c learnable = true" ${KAYAK_PATH}
echo "kayak:" >> ${OUTPUT}

for p in ${partition[@]}
do
    sed -i -e "${line_partition_kayak}c partition = ${p}" ${KAYAK_PATH}
    echo "partition = ${p}" >> ${OUTPUT}
    for t in ${maxout[@]}
    do
        sed -i -e "${line_max_out_kayak}c max_out = ${t}" ${KAYAK_PATH}
        echo "max_out = ${t}" >> ${OUTPUT}
        sudo ../scripts/run-kayak >> ${OUTPUT}
        echo "" >> ${OUTPUT}
    done
    echo "" >> ${OUTPUT}
done

# cat ${KAYAK_PATH} >> ${OUTPUT}
