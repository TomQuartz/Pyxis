#!/bin/bash
set -exu

LB_PATH="lb.toml"

LOG_PATH="../logs/convergence_with_slo/"

date=`date +%Y-%m-%d`

if [ ! -d ${LOG_PATH} ]; then
    mkdir -p ${LOG_PATH}
fi

cd ${LOG_PATH}
# OUTPUT_FILE="${date}_$1.log"
OUTPUT_FILE="dyn-r.log"
if [ -e ${OUTPUT_FILE} ]; then
    rm ${OUTPUT_FILE}
fi
cd -
OUTPUT=${LOG_PATH}${OUTPUT_FILE}

cat ${LB_PATH} >> ${OUTPUT}

echo "" >> ${OUTPUT}
echo "##############################" >> ${OUTPUT}
sudo ../scripts/run-elastic >> ${OUTPUT}

# echo "rloop" >> ${OUTPUT}
# cat "rloop.log" >> ${OUTPUT}
cd ../logs
python3 dyn.py
