#!/bin/bash
set -exu


LBTOML_PATH="./lb.toml"

date="20211121"
expriment="multiple"

# naive implementation
# Set the related line index up manually 


line_multi_kv=11
line_multi_ord=12
line_multi_ratio=13
line_learnable=16
line_invoke_p=17
line_partition=18
line_max_out=19

max_out=(1 2 4 8)

sed -i -e "${line_learnable}c learnable = false" ${LBTOML_PATH}


kv=(16)
ord=(0)
ratio=(100)
sed -i -e "${line_multi_kv}c multi_kv = [${kv[0]}]" ${LBTOML_PATH}
sed -i -e "${line_multi_ord}c multi_ord = [${ord[0]}]" ${LBTOML_PATH}
sed -i -e "${line_multi_ratio}c multi_ratio = [${ratio[0]}]" ${LBTOML_PATH}


invoke_p=(100 0) 

LOG_PATH="../logs/${date}_${expriment}/"
if [ ! -d ${LOG_PATH} ]; then
    mkdir -p ${LOG_PATH}
fi

# Run Kayak

cd ${LOG_PATH}
OUTPUT="multiple_$1.log"
if [ -e ${OUTPUT} ]; then
    rm ${OUTPUT}
fi
cd -
OUTPUT=${LOG_PATH}${OUTPUT}

echo "multi_type = ${kv[@]}" >> ${OUTPUT}
echo "multi_ord = ${ord[@]}" >> ${OUTPUT}
echo "multi_ratio = ${ratio[@]}" >> ${OUTPUT}

sed -i -e "${line_partition}c partition = -1" ${LBTOML_PATH}

for p in ${invoke_p[@]}
do
    sed -i -e "${line_invoke_p}c invoke_p = ${p}" ${LBTOML_PATH}
    echo "invoke_p = ${p}" >> ${OUTPUT}
    for t in ${max_out[@]}
    do
        sed -i -e "${line_max_out}c max_out = ${t}" ${LBTOML_PATH}
        echo "max_out = ${t}" >> ${OUTPUT}
        echo "" >> ${OUTPUT}
        sudo env RUST_LOG=debug LD_LIBRARY_PATH=../net/target/native ./target/release/lb >> ${OUTPUT}
    done
    echo "" >> ${OUTPUT}
done
