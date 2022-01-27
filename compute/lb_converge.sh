#!/bin/bash
set -exu

LB_PATH="lb.toml"

LOG_PATH="../logs/multi-type_opt/"

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

# # lb
line_learnable_lb=6
line_partition_lb=7
line_output_factor_lb=9
line_max_out_lb=123

sed -i -e "${line_learnable_lb}c learnable = false" ${LB_PATH}
sed -i -e "${line_output_factor_lb}c output_factor = 0" ${LB_PATH}

partition=(98 99 100)
for p in ${partition[@]}
do
    sed -i -e "${line_partition_lb}c partition = ${p}" ${LB_PATH}
    echo "partition = ${p}" >> ${OUTPUT}
    sudo ../scripts/run-elastic >> ${OUTPUT}
    echo "" >> ${OUTPUT}
done

# sed -i -e "${line_learnable_lb}c learnable = false" ${LB_PATH}
# partition=()
# echo "sweep" >> ${OUTPUT}
# for p in ${partition[@]}
# do
#     sed -i -e "${line_partition_lb}c partition = ${p}" ${LB_PATH}
#     echo "partition = ${p}" >> ${OUTPUT}
#     sudo ../scripts/run-elastic >> ${OUTPUT}
#     echo "" >> ${OUTPUT}
# done

cat ${LB_PATH} >> ${OUTPUT}

# python3 ../logs/ratio-tput.py ${OUTPUT} 
