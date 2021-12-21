#!/bin/bash
set -exu

KAYAK_PATH="./kayak.toml"
LB_PATH="lb.toml"

LOG_PATH="../logs/nCmS_2type/"

date="20211221"

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
line_learnable=6
line_partition=7
line_max_out_kayak=8
line_xloop_factor=10
line_learn_rate=12

maxout=(1 2 4 8 16 32 64)

x_factor=4000
learn_rate=0.25

sed -i -e "${line_learnable}c learnable = true" ${KAYAK_PATH}
sed -i -e "${line_partition}c partition = 50" ${KAYAK_PATH}
# sed -i -e "${line_xloop_factor}c xloop_factor = ${x_factor}" ${KAYAK_PATH}
# sed -i -e "${line_learn_rate}c xloop_learning_rate = ${learn_rate}" ${KAYAK_PATH}  

echo "kayak:" >> ${OUTPUT}
echo "xloop_factor = ${x_factor}" >> ${OUTPUT}
echo "xloop_learning_rate = ${learn_rate}" >> ${OUTPUT}

for t in ${maxout[@]}
do
    sed -i -e "${line_max_out_kayak}c max_out = ${t}" ${KAYAK_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done


# # lb
line_learnable_lb=6
line_max_out_lb=8
maxout=(1 2 4 8 16 32 64 128 256)

sed -i -e "${line_learnable_lb}c learnable = true" ${LB_PATH}
echo "lb:" >> ${OUTPUT}
for t in ${maxout[@]}
do
    sed -i -e "${line_max_out_lb}c max_out = ${t}" ${LB_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    sudo ../scripts/run-lb >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done


# only C
maxout=(1 2 4 8 16)
sed -i -e "${line_learnable}c learnable = false" ${KAYAK_PATH}
sed -i -e "${line_partition}c partition = 0" ${KAYAK_PATH}
echo "only C:" >> ${OUTPUT}
for t in ${maxout[@]}
do
    sed -i -e "${line_max_out_kayak}c max_out = ${t}" ${KAYAK_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done


# only S
maxout=(1 2 4 8 16)
sed -i -e "${line_learnable}c learnable = false" ${KAYAK_PATH}
sed -i -e "${line_partition}c partition = 100" ${KAYAK_PATH}
echo "only S:" >> ${OUTPUT}
for t in ${maxout[@]}
do
    sed -i -e "${line_max_out_kayak}c max_out = ${t}" ${KAYAK_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    sudo ../scripts/run-kayak >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

python3 ../logs/perform.py ${OUTPUT}