#!/bin/bash
set -exu


CLIENTTOML_PATH="./client.toml"

# naive implementation
# Set the related line index up manually 
line_invoke_p=84
line_max_out=87
line_multi_kv=98
line_multi_ord=99
line_partition=101

num_type=$1
max_out=(1 2 4 8 16 32)

# # enumerate type combination
# i=0
# while ((i<len))
# do
#     let "j=i+1"
#     while ((j<len))
#     do
#         ...
#         ((++j))
#     done
#     ((++i))
# done

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
    ord=(200 800 3200 12800)
    sed -i -e "${line_multi_kv}c multi_kv = \
[${kv[0]}, ${kv[1]}, ${kv[2]}, ${kv[3]}]" ${CLIENTTOML_PATH}
    sed -i -e "${line_multi_ord}c multi_ord = \
[${ord[0]}, ${ord[1]}, ${ord[2]}, ${ord[3]}]" ${CLIENTTOML_PATH}
else  # num_type == 8
    kv=(128 64 32 16 8 4 2 1)
    ord=(10 25 50 100 200 800 3200 12800)
    sed -i -e "${line_multi_kv}c multi_kv = \
[${kv[0]}, ${kv[1]}, ${kv[2]}, ${kv[3]}, ${kv[4]}, ${kv[5]}, ${kv[6]}, ${kv[7]}]" ${CLIENTTOML_PATH}
    sed -i -e "${line_multi_ord}c multi_ord = \
[${ord[0]}, ${ord[1]}, ${ord[2]}, ${ord[3]}, ${ord[4]}, ${ord[5]}, ${ord[6]}, ${ord[7]}]" ${CLIENTTOML_PATH}
fi


# Run Kayak
OUTPUT="./kayak_type${num_type}.log"
if [ -e ${OUTPUT} ]; then
    rm ${OUTPUT}
fi

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
    # sudo env RUST_LOG=debug LD_LIBRARY_PATH=../net/target/native ./target/release/pushback-ours >> ${OUTPUT}
done


# Run ours

OUTPUT="./test_ours_type${num_type}.log"
if [ -e ${OUTPUT} ]; then
    rm ${OUTPUT}
fi

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
    
    OUTPUT="./ours_type${num_type}_out${t}.log"
    if [ -e ${OUTPUT} ]; then
        rm ${OUTPUT}
    fi

    echo "max_out = ${t}" >> ${OUTPUT}
    echo "" >> ${OUTPUT}

    # loop partition
    for p in ${partition[@]}
    do
        sed -i -e "${line_partition}c partition = ${p}" ${CLIENTTOML_PATH}
        # loop invoke_p
        for r in ${invoke_p[@]}
        do
            sed -i -e "${line_invoke_p}c invoke_p = ${r}" ${CLIENTTOML_PATH}
            # Print one configuration
            echo "partition = ${p}" >> ${OUTPUT}
            echo "invoke_p = ${r}" >> ${OUTPUT}
            # sudo env RUST_LOG=debug LD_LIBRARY_PATH=../net/target/native ./target/release/pushback-ours >> ${OUTPUT}
            echo "" >> ${OUTPUT}
        done
    done
done
