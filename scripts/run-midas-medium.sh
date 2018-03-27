#!/bin/bash

data_file=assets/data/medium-128-1024-65536.csv
workload_file=assets/workloads/medium-oltp-2-64-1000-1.json
num_threads_max=32

num_runs=$1

echo "dataset: $data_file"
echo "workload: $workload_file"
echo "num_threads_max: $num_threads_max"
echo "#runs per config: $num_runs"

for ((n=1; n<=$num_threads_max; n=2*n))
do
    echo "running benchmark with $n threads"
    for ((i=1; i<=$num_runs; i++))
    do
        echo -n "starting run $i/$num_runs ..."
        rm -f /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling --data $data_file --workload $workload_file --num-threads $n >> log/midas-medium-$n.log
        echo "done!"
    done
done
