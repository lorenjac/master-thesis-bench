#!/bin/bash

sc_name=$1
data_file=$2
workload_file=$3
num_threads_max=$4
num_runs=$5

# echo "dataset: $data_file"
# echo "workload: $workload_file"
# echo "num_threads_max: $num_threads_max"
# echo "#runs per config: $num_runs"

folder="log/`date +%Y%m%d-%H%M`-echo-$sc_name"
mkdir $folder

for ((n=1; n<=$num_threads_max; n=2*n))
do
    echo "running benchmark with $n thread(s)"
    for ((i=1; i<=$num_runs; i++))
    do
        echo -n "starting run $i/$num_runs ... "
        echo "--------------------------------" >> $folder/echo-$sc_name-$n.log
        echo "num_threads=$n" >> $folder/echo-$sc_name-$n.log
        rm -f /dev/shm/nvdimm_echo && ./bin/echo-scaling --data $data_file --workload $workload_file --num-threads $n --num-retries 3 >> $folder/echo-$sc_name-$n.log
        echo "done!"
    done
done
