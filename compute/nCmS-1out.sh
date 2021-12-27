#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

LOG_PATH="../logs/nCmS/"

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
line_max_out_lb=8
line_num_cores_lb=36
sed -i -e "${line_learnable_lb}c learnable = true" ${LB_PATH}
sed -i -e "${line_partition_lb}c partition = 50" ${LB_PATH}
sed -i -e "${line_max_out_lb}c max_out = 1" ${LB_PATH}
echo "lb:" >> ${OUTPUT}
cores=(1 2 4)
for c in ${cores[@]}
do
    sed -i -e "${line_num_cores_lb}c num_cores = ${c}" ${LB_PATH}
    echo "num_cores = ${c}" >> ${OUTPUT}
    sudo ../scripts/run-elastic >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done


# kayak
line_learnable=6
line_partition=7
line_max_out_kayak=8
line_xloop_factor=10
line_learn_rate=12
line_num_cores_kayak=19
# x_factor=4000
# learn_rate=0.25

sed -i -e "${line_learnable}c learnable = true" ${KAYAK_PATH}
sed -i -e "${line_partition}c partition = 50" ${KAYAK_PATH}
sed -i -e "${line_max_out_kayak}c max_out = 1" ${KAYAK_PATH}
# sed -i -e "${line_xloop_factor}c xloop_factor = ${x_factor}" ${KAYAK_PATH}
# sed -i -e "${line_learn_rate}c xloop_learning_rate = ${learn_rate}" ${KAYAK_PATH}  

echo "kayak:" >> ${OUTPUT}
# echo "xloop_factor = ${x_factor}" >> ${OUTPUT}
# echo "xloop_learning_rate = ${learn_rate}" >> ${OUTPUT}

for c in ${cores[@]}
do
    sed -i -e "${line_num_cores_kayak}c num_cores = ${c}" ${KAYAK_PATH}
    echo "num_cores = ${c}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done


# only C
sed -i -e "${line_learnable}c learnable = false" ${KAYAK_PATH}
sed -i -e "${line_partition}c partition = 0" ${KAYAK_PATH}
echo "only C:" >> ${OUTPUT}
for c in ${cores[@]}
do
    sed -i -e "${line_num_cores_kayak}c num_cores = ${c}" ${KAYAK_PATH}
    echo "num_cores = ${c}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done


# only S
sed -i -e "${line_learnable}c learnable = false" ${KAYAK_PATH}
sed -i -e "${line_partition}c partition = 100" ${KAYAK_PATH}
echo "only S:" >> ${OUTPUT}
for c in ${cores[@]}
do
    sed -i -e "${line_num_cores_kayak}c num_cores = ${c}" ${KAYAK_PATH}
    echo "num_cores = ${c}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

cat ${LB_PATH} >> ${OUTPUT}

# python3 ../logs/perform.py ${OUTPUT}