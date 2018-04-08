echo -n "Measuring latency of GET on SMALL database..."
rm /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-baseline get -p assets/data/small.csv > log/midas-get-small.log
echo " done!"

echo -n "Measuring latency of GET on LARGE database..."
rm /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-baseline get -p assets/data/large.csv > log/midas-get-large.log
echo " done!"

echo -n "Measuring latency of PUT on SMALL database..."
rm /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-baseline put -p assets/data/small.csv > log/midas-put-small.log
echo " done!"

echo -n "Measuring latency of PUT on LARGE database..."
rm /dev/shm/nvdimm_midas && PMEM_IS_PMEM_FORCE=1 ./bin/midas-baseline put -p assets/data/large.csv > log/midas-put-large.log
echo " done!"

