#!/bin/bash

<<<<<<< HEAD:scripts/run-midas-medium.sh
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
=======
data_file=$1
workload_file=$2
num_threads_max=$3
num_runs=$4

# echo "dataset: $data_file"
# echo "workload: $workload_file"
# echo "num_threads_max: $num_threads_max"
# echo "#runs per config: $num_runs"
>>>>>>> c84b4ecd13117a1711f348b657cad47279beab98:scripts/run-midas.sh

folder=log/`date +%Y-%m-%d-%H%M`
mkdir $folder

for ((n=1; n<=$num_threads_max; n=2*n))
do
    echo "running benchmark with $n thread(s)"
    for ((i=1; i<=$num_runs; i++))
    do
        echo -n "starting run $i/$num_runs ..."
<<<<<<< HEAD:scripts/run-midas-medium.sh
        rm -f /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling --data $data_file --workload $workload_file --num-threads $n >> $folder/midas-medium-$n.log
=======
        echo "--------------------------------" >> log/midas-medium-$n.log
        echo "num_threads=$n" >> log/midas-medium-$n.log
        rm -f /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling --data $data_file --workload $workload_file --num-threads $n >> log/midas-medium-$n.log
>>>>>>> c84b4ecd13117a1711f348b657cad47279beab98:scripts/run-midas.sh
        echo "done!"
    done
done
