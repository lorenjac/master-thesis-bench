key_len=128
val_len=1024
num_txs=1000
short_min=2
short_max=32
long_min=32
long_max=512

# build key-value generator if necessary
if [ ! -d "bin/kv-gen" ]; then
    make kv-gen
fi

# build workload generator if necessary
if [ ! -d "bin/workload-gen" ]; then
    make workload-gen
fi

# generate workloads with small database
./bin/kv-gen $key_len $val_len 1000 assets/data/small.csv
./bin/workload-gen --data assets/data/small.csv --tx-profile assets/profiles/sap-oltp.json --num-txs $num_txs --tx-length-min $short_min --tx-length-max $short_max -o assets/workloads/ss-1000.json
./bin/workload-gen --data assets/data/small.csv --tx-profile assets/profiles/sap-oltp.json --num-txs $num_txs --tx-length-min $long_min --tx-length-max $long_max -o assets/workloads/sl-1000.json

# generate workloads with large database
./bin/kv-gen $key_len $val_len 100000 assets/data/large.csv
./bin/workload-gen --data assets/data/large.csv --tx-profile assets/profiles/sap-oltp.json --num-txs $num_txs --tx-length-min $short_min --tx-length-max $short_max -o assets/workloads/ls-1000.json
./bin/workload-gen --data assets/data/large.csv --tx-profile assets/profiles/sap-oltp.json --num-txs $num_txs --tx-length-min $long_min --tx-length-max $long_max -o assets/workloads/ll-1000.json

