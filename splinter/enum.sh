#!/bin/bash
set -exu


CLIENTTOML_PATH="./client.toml"

date="20211019"

# naive implementation
# Set the related line index up manually 
line_invoke_p=84
line_max_out=87
line_bimodal=90
line_bi_interval=96
line_multi_kv=98
line_multi_ord=99
line_partition=101
line_learnable=104
line_bi_interval2=105
line_bi_rpc=107

num_type=$1
max_out=(1 2 4 8 16 32)

sed -i -e "${line_bimodal}c bimodal = true" ${CLIENTTOML_PATH}

# fixed type
if ((num_type==1)); then
    kv=(1)
    ord=(12800)
    sed -i -e "${line_multi_kv}c multi_kv = [${kv[0]}]" ${CLIENTTOML_PATH}
    sed -i -e "${line_multi_ord}c multi_ord = [${ord[0]}]" ${CLIENTTOML_PATH}
elif ((num_type==2)); then
    kv=(8 1)
    ord=(200 12800)
    sed -i -e "${line_multi_kv}c multi_kv = [${kv[0]}, ${kv[1]}]" ${CLIENTTOML_PATH}
    sed -i -e "${line_multi_ord}c multi_ord = [${ord[0]}, ${ord[1]}]" ${CLIENTTOML_PATH}
elif ((num_type==4)); then
    kv=(8 4 2 1)
    ord=(50 400 3200 25600)
    sed -i -e "${line_multi_kv}c multi_kv = \
[${kv[0]}, ${kv[1]}, ${kv[2]}, ${kv[3]}]" ${CLIENTTOML_PATH}
    sed -i -e "${line_multi_ord}c multi_ord = \
[${ord[0]}, ${ord[1]}, ${ord[2]}, ${ord[3]}]" ${CLIENTTOML_PATH}
else  # num_type == 8
    kv=(8 8 4 4 2 2 1 1)
    ord=(25 50 200 400 1600 3200 12800 25600)
    sed -i -e "${line_multi_kv}c multi_kv = \
[${kv[0]}, ${kv[1]}, ${kv[2]}, ${kv[3]}, ${kv[4]}, ${kv[5]}, ${kv[6]}, ${kv[7]}]" ${CLIENTTOML_PATH}
    sed -i -e "${line_multi_ord}c multi_ord = \
[${ord[0]}, ${ord[1]}, ${ord[2]}, ${ord[3]}, ${ord[4]}, ${ord[5]}, ${ord[6]}, ${ord[7]}]" ${CLIENTTOML_PATH}
fi


# Run Kayak
LOG_PATH="../logs/${date}_enumerate/"
if [ ! -d ${LOG_PATH} ]; then
    mkdir -p ${LOG_PATH}
fi
cd ${LOG_PATH}
OUTPUT="kayak_type${num_type}.log"
if [ -e ${OUTPUT} ]; then
    rm ${OUTPUT}
fi
cd -
OUTPUT=${LOG_PATH}${OUTPUT}


sed -i -e "${line_partition}c partition = -1" ${CLIENTTOML_PATH}
sed -i -e "${line_invoke_p}c invoke_p = 100" ${CLIENTTOML_PATH}
# Print configuration
echo "Kayak configuration:" >> ${OUTPUT}
echo "partition = -1" >> ${OUTPUT}
echo "invoke_p = 100" >> ${OUTPUT}
for t in ${max_out[@]}
do
    sed -i -e "${line_max_out}c max_out = ${t}" ${CLIENTTOML_PATH}
    echo "max_out = ${t}" >> ${OUTPUT}
    echo "" >> ${OUTPUT}
    sudo env RUST_LOG=debug LD_LIBRARY_PATH=../net/target/native ./target/release/pushback-ours >> ${OUTPUT}
done


# Run ours

# Configuration could be modified here.
if ((num_type==1)); then
    partition=(0)
elif ((num_type==2)); then
    partition=(0 1)
elif ((num_type==4)); then
    partition=(1 2)
else  # num_type == 8
    partition=(2 3 4 5)
fi
invoke_p=(0 10 20 30 40 50 60 70 80 90 100)

for t in ${max_out[@]}
do
    sed -i -e "${line_max_out}c max_out = ${t}" ${CLIENTTOML_PATH}

    cd ${LOG_PATH}
    OUTPUT="ours_type${num_type}_out${t}.log"
    if [ -e ${OUTPUT} ]; then
        rm ${OUTPUT}
    fi
    cd -
    OUTPUT=${LOG_PATH}${OUTPUT}

    echo "max_out = ${t}" >> ${OUTPUT}
    echo "" >> ${OUTPUT}

    # loop partition
    for p in ${partition[@]}
    do
        sed -i -e "${line_partition}c partition = ${p}" ${CLIENTTOML_PATH}
        # Print one configuration
        echo "partition = ${p}" >> ${OUTPUT}
        sudo env RUST_LOG=debug LD_LIBRARY_PATH=../net/target/native ./target/release/pushback-ours >> ${OUTPUT}
        echo "" >> ${OUTPUT}
    done
done
