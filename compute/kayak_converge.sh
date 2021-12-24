#!/bin/bash
set -exu

KAYAK_PATH="kayak.toml"

LOG_PATH="../logs/kayak_converge/"

date="20211223"

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

# # kayak
line_learnable_kayak=6
line_partition_kayak=7
line_max_out_kayak=8

sed -i -e "${line_learnable_kayak}c learnable = true" ${KAYAK_PATH}
sed -i -e "${line_partition_kayak}c partition = 50" ${KAYAK_PATH}
echo "learn" >> ${OUTPUT}
sudo ../scripts/run-kayak >> ${OUTPUT}
echo "" >> ${OUTPUT}

sed -i -e "${line_learnable_kayak}c learnable = false" ${KAYAK_PATH}
partition=(0 10 20 30 40 50 60 70 80 90 100)
echo "sweep" >> ${OUTPUT}
for p in ${partition[@]}
do
    sed -i -e "${line_partition_kayak}c partition = ${p}" ${KAYAK_PATH}
    echo "partition = ${p}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

cat ${KAYAK_PATH} >> ${OUTPUT}
