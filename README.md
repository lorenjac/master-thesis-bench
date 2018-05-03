# Benchmark Suite

## Prequisites

* clone and build [pmdk](https://github.com/pmem/pmdk)
* clone and build [libcuckoo](https://github.com/efficient/libcuckoo)
* clone and build [midas](https://git.informatik.tu-cottbus.de/lorenjac/thesis-code)
    * build with `make lib` (this creates a required static library)
* clone and build [echo](https://github.com/snalli/echo)
    * build with `make lib` (this creates a required static library)
* in folder `midas`
    * create folders `bin` and `lib`
    * in folder `lib` add the following symlinks
        * libcuckoo -> ... libcuckoo
        * midas -> ... thesis-code
        * pmdk -> ... pmdk
    * run `make`
* in folder `echo`
    * create folders `bin` and `lib`
    * in folder `lib` add the following symlinks
        * echo -> ... echo
    * run `make`

## Sample Data

* sample data sets can be generated with `kv-gen`

```
make kv-gen
./bin/kv-gen <key_size> <value_size> <num_pairs> <output_file>
```

## Latency Benchmark

For Midas, run

```
PMEM_IS_PMEM_FORCE=1 ./bin/midas-baseline [params]
```

For Echo, run

```
./bin/echo-baseline [params]`
```

* for echo, persistence is already enabled
* for midas, set `PMEM_IS_PMEM_FORCE=1` before running the benchmark
* it is recommended to use tmpfs (e.g. `/dev/shm/`)
* run with `./bin/<kvs>-baseline -h` for instructions

## Throughput Benchmark

For Midas, run

```
PMEM_IS_PMEM_FORCE=1 ./bin/midas-scaling [params]
```

For Echo, run

```
./bin/echo-scaling [params]`
```

* for echo, persistence is already enabled
* for midas, set `PMEM_IS_PMEM_FORCE=1` before running the benchmark
* it is recommended to use tmpfs (e.g. `/dev/shm/`)
* run with `./bin/<kvs>-scaling -h` for help
* workloads can generated using `workload-gen`
* dummy key-value pairs can be generated using `kv-gen`
* transaction profiles are stored in `assets/`
* there are also scripts in `scripts` to do that
