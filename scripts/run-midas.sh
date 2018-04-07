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

folder="log/`date +%Y%m%d-%H%M%S`-midas-$sc_name"
mkdir $folder

for ((n=1; n<=$num_threads_max; n=2*n))
do
    echo "running benchmark with $n thread(s)"
    for ((i=1; i<=$num_runs; i++))
    do
        echo -n "starting run $i/$num_runs ... "
        echo "--------------------------------" >> $folder/midas-$sc_name-$n.log
        echo "num_threads=$n" >> $folder/midas-$sc_name-$n.log
        rm -f /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling --data $data_file --workload $workload_file --num-threads $n --num-retries 3 >> $folder/midas-$sc_name-$n.log
        #rm -f /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling --data $data_file --workload $workload_file --num-threads $n >> $folder/midas-$sc_name-$n.log
        echo "done!"
    done
done

