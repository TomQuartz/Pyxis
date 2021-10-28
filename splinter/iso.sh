#!/bin/bash
set -exu


CLIENTTOML_PATH="./client.toml"

date="20211027"

# naive implementation
# Set the related line index up manually 
line_invoke_p=84
line_max_out=87
line_bimodal=90
line_bi_interval=96
line_multi_kv=98
line_multi_ord=99
line_multi_ratio=100
line_type_core=101
line_partition=102
line_learnable=103
line_bi_interval2=105
line_bi_rpc=107

no=$1

max_out=(2 4 8 16 32)


LOG_PATH="../logs/${date}_isolation/${no}/"
if [ ! -d ${LOG_PATH} ]; then
    mkdir -p ${LOG_PATH}
fi


i=34
while ((i<=34))
do
    let "j=100-i"

    cd ${LOG_PATH}
    OUTPUT="ours_iso_ratio${i}.log"
    if [ -e ${OUTPUT} ]; then
        rm ${OUTPUT}
    fi
    cd -
    OUTPUT=${LOG_PATH}${OUTPUT}

    sed -i -e "${line_multi_ratio}c multi_ratio = [${i}, ${j}]" ${CLIENTTOML_PATH}
    echo "multi_ratio = [${i}, ${j}]" >> ${OUTPUT}

    multi_ord="multi_ord = [200, 4000]"
    sed -i -e "${line_multi_ord}c ${multi_ord}" ${CLIENTTOML_PATH}
    echo ${multi_ord} >> ${OUTPUT}
    echo "" >> ${OUTPUT}

    type_to_core="type2core = [[0,1],[2, 3,4,5,6,7]]"
    sed -i -e "${line_type_core}c ${type_to_core}" ${CLIENTTOML_PATH}
    echo ${type_to_core} >> ${OUTPUT}
    echo "" >> ${OUTPUT}
    for t in ${max_out[@]}
    do
        sed -i -e "${line_max_out}c max_out = ${t}" ${CLIENTTOML_PATH}
        echo "max_out = ${t}" >> ${OUTPUT}
        sudo env RUST_LOG=info LD_LIBRARY_PATH=../net/target/native ./target/release/ours >> ${OUTPUT}
    done
    echo "" >> ${OUTPUT}

    type_to_core="type2core = [[],[]]"
    sed -i -e "${line_type_core}c ${type_to_core}" ${CLIENTTOML_PATH}
    echo ${type_to_core} >> ${OUTPUT}
    echo "" >> ${OUTPUT}
    for t in ${max_out[@]}
    do
        sed -i -e "${line_max_out}c max_out = ${t}" ${CLIENTTOML_PATH}
        echo "max_out = ${t}" >> ${OUTPUT}
        sudo env RUST_LOG=info LD_LIBRARY_PATH=../net/target/native ./target/release/ours >> ${OUTPUT}
    done
    echo "" >> ${OUTPUT}

    ((i+=2))
done