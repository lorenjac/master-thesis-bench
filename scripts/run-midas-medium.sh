#!/bin/bash

num_threads=$1
num_runs=$2

echo "rm -f /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling -d assets/data/medium-128-1024-65536.csv -w assets/workloads/medium-oltp-2-64-1000-1.json -t $num_threads" > "midas-medium-t$num_threads-magni.log"

#for i in {1..$num_runs}
for ((i=1; i<=$num_runs; i++))
do 
    echo "starting run #$i ..."
    rm -f /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling -d assets/data/medium-128-1024-65536.csv -w assets/workloads/medium-oltp-2-64-1000-1.json -t $num_threads >> "midas-medium-t$num_threads-magni.log"
    echo "done!"
done
