#!/bin/bash
set -exu

KAYAK_PATH="kayak.toml"

LOG_PATH="../logs/kayak_test/"

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
# line_max_out_kayak=8
# line_num_cores_kayak=19

# sed -i -e "${line_learnable_kayak}c learnable = true" ${KAYAK_PATH}
# sed -i -e "${line_partition_kayak}c partition = 50" ${KAYAK_PATH}
# echo "learn" >> ${OUTPUT}
# sudo ../scripts/run-kayak >> ${OUTPUT}
# echo "" >> ${OUTPUT}

sed -i -e "${line_learnable_kayak}c learnable = false" ${KAYAK_PATH}
# sed -i -e "${line_max_out_kayak}c max_out = 16" ${KAYAK_PATH}
# sed -i -e "${line_num_cores_kayak}c num_cores = 8" ${KAYAK_PATH}
partition=(0 100)
for p in ${partition[@]}
do
    sed -i -e "${line_partition_kayak}c partition = ${p}" ${KAYAK_PATH}
    echo "partition = ${p}" >> ${OUTPUT}
    sudo ../scripts/run-kayak-plus >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

cat ${KAYAK_PATH} >> ${OUTPUT}
