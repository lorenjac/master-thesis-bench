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

* sample data are located in folder `data`
* further data sets can be generated with `gen/gen`

```
cd gen
./gen <key_size> <value_size> <num_pairs> <output_file>
```

## Latency Benchmark

* run with `./bin/baseline <operation> [-p <data_file>] [-r <num_repeats>] [-u <time_unit>] [-v]`
* for echo, persistence is already enabled
* for midas, set `PMEM_IS_PMEM_FORCE=1` before running the benchmark

