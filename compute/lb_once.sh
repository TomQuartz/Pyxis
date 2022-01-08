#!/bin/bash
set -exu

LB_PATH="lb.toml"

LOG_PATH="../logs/lb_convergence/"

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
line_max_out_lb=85

sed -i -e "${line_learnable_lb}c learnable = true" ${LB_PATH}
sed -i -e "${line_partition_lb}c partition = 0" ${LB_PATH}
echo "learn" >> ${OUTPUT}
sudo ../scripts/run-elastic >> ${OUTPUT}
echo "" >> ${OUTPUT}

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

python3 ../logs/ratio-tput.py ${OUTPUT} 
