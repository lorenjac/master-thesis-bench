echo -n "Measuring latency of GET on SMALL database..."
rm /dev/shm/nvdimm_echo && ./bin/echo-baseline get -p assets/data/small.csv > log/echo-get-small.log
echo " done!"

echo -n "Measuring latency of GET on LARGE database..."
rm /dev/shm/nvdimm_echo && ./bin/echo-baseline get -p assets/data/large.csv > log/echo-get-large.log
echo " done!"

echo -n "Measuring latency of PUT on SMALL database..."
rm /dev/shm/nvdimm_echo && ./bin/echo-baseline put -p assets/data/small.csv > log/echo-put-small.log
echo " done!"

echo -n "Measuring latency of PUT on LARGE database..."
rm /dev/shm/nvdimm_echo && ./bin/echo-baseline put -p assets/data/large.csv > log/echo-put-large.log
echo " done!"

