#!/bin/bash

data_file=assets/data/medium-128-1024-65536.csv
workload_file=assets/workloads/medium-oltp-2-64-1000-1.json

num_cpus=$(grep "physical id" /proc/cpuinfo | sort | uniq | wc -l)
num_cores_per_cpu=$(grep "cpu cores" /proc/cpuinfo | sort | uniq | sed -e 's/^.*\([1-9]\)/\1/')
num_cores=$((num_cpus * num_cores_per_cpu))
num_threads_max=$num_cores

num_runs=$1

echo "dataset: $data_file"
echo "workload: $workload_file"
echo "num_cpus: $num_cpus"
echo "num_cores: $num_cores"
echo "num_threads_max: $num_threads_max"
echo "#runs per config: $num_runs"

folder=log/`date +%Y-%m-%d-%H%M`
mkdir $folder

for ((n=1; n<=$num_threads_max; n=2*n))
do
    echo "running benchmark with $n thread(s)"
    for ((i=1; i<=$num_runs; i++))
    do
        echo -n "starting run $i/$num_runs ..."
        rm -f /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling --data $data_file --workload $workload_file --num-threads $n >> $folder/midas-medium-$n.log
        echo "done!"
    done
done
