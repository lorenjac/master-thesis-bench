#!/bin/bash

data_file=$1
workload_file=$2
num_threads_max=$3
num_runs=$4

# echo "dataset: $data_file"
# echo "workload: $workload_file"
# echo "num_threads_max: $num_threads_max"
# echo "#runs per config: $num_runs"

folder=log/midas-`date +%Y-%m-%d-%H%M`
mkdir $folder

for ((n=1; n<=$num_threads_max; n=2*n))
do
    echo "running benchmark with $n thread(s)"
    for ((i=1; i<=$num_runs; i++))
    do
        echo -n "starting run $i/$num_runs ..."
        echo "--------------------------------" >> $folder/midas-medium-$n.log
        echo "num_threads=$n" >> log/midas-medium-$n.log
        rm -f /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling --data $data_file --workload $workload_file --num-threads $n >> $folder/midas-medium-$n.log
        echo "done!"
    done
done

