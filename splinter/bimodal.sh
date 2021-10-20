#!/bin/bash
set -exu

date="20211021"

CLIENTTOML_PATH="./client.toml"

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


num_type=2
max_out=8
i1=960000000
i2=3840000000


LOG_PATH="../logs/${date}_bimodal/"
if [ ! -d ${LOG_PATH} ]; then
    mkdir -p ${LOG_PATH}
fi
cd ${LOG_PATH}
OUTPUT="type${num_type}_out${max_out}_i1${i1}_i2${i2}.log"
if [ -e ${OUTPUT} ]; then
    rm ${OUTPUT}
fi
cd -
OUTPUT=${LOG_PATH}${OUTPUT}


sed -i -e "${line_bimodal}c bimodal = true" ${CLIENTTOML_PATH}
sed -i -e "${line_bi_interval}c bimodal_interval = ${i1}" ${CLIENTTOML_PATH}
sed -i -e "${line_bi_interval2}c bimodal_interval2 = ${i2}" ${CLIENTTOML_PATH}


# Run Kayak
sed -i -e "${line_partition}c partition = -1" ${CLIENTTOML_PATH}
sed -i -e "${line_learnable}c learnable = true" ${CLIENTTOML_PATH}
echo "Kayak: " >> ${OUTPUT}
sudo env RUST_LOG=debug LD_LIBRARY_PATH=../net/target/native ./target/release/pushback-ours >> ${OUTPUT}


# Run ours
sed -i -e "${line_partition}c partition = 1" ${CLIENTTOML_PATH}
sed -i -e "${line_learnable}c learnable = false" ${CLIENTTOML_PATH}
echo "Ours: " >> ${OUTPUT}
# loop bimodal_rpc rate
rpc_rate=40
while ((rpc_rate<=40))
do
    # sed -i -e "${line_bi_rpc}c bimodal_rpc = [0, ${rpc_rate}]" ${CLIENTTOML_PATH}
    sed -i -e "${line_bi_rpc}c bimodal_rpc = [0, ${rpc_rate}]" ${CLIENTTOML_PATH}
    # Print one configuration
    echo "bimodal_rpc = ${rpc_rate}" >> ${OUTPUT}
    sudo env RUST_LOG=debug LD_LIBRARY_PATH=../net/target/native ./target/release/pushback-ours >> ${OUTPUT}
    echo "" >> ${OUTPUT}
    ((rpc_rate+=10))
done
