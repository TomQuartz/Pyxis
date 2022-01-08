#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

LOG_PATH="../logs/test_single-type-cost/"

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
line_kv_kayak=86
line_order_kayak=87
line_table_kayak=88
sed -i -e "${line_learnable_kayak}c learnable = false" ${KAYAK_PATH}

kv=3
table=6
order=55200

# S = 8 / tput
echo "S" >> ${OUTPUT}
maxout=(64)
partition=(100)
sed -i -e "${line_kv_kayak}c kv = ${kv}" ${KAYAK_PATH}
sed -i -e "${line_table_kayak}c table_id = ${table}" ${KAYAK_PATH}
sed -i -e "${line_order_kayak}c order = ${order}" ${KAYAK_PATH}
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

# S' = 8 / tput
echo "S'" >> ${OUTPUT}
maxout=(64)
partition=(0)
sed -i -e "${line_kv_kayak}c kv = ${kv}" ${KAYAK_PATH}
sed -i -e "${line_table_kayak}c table_id = ${table}" ${KAYAK_PATH}
sed -i -e "${line_order_kayak}c order = 0" ${KAYAK_PATH}
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

# C = 64 / tput
echo "C" >> ${OUTPUT}
maxout=(128)
partition=(0)
sed -i -e "${line_kv_kayak}c kv = 0" ${KAYAK_PATH}
sed -i -e "${line_table_kayak}c table_id = ${table}" ${KAYAK_PATH}
sed -i -e "${line_order_kayak}c order = ${order}" ${KAYAK_PATH}
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

cat ${KAYAK_PATH} >> ${OUTPUT}
