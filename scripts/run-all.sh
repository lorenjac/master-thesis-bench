# assets
data_small=assets/data/small.csv
data_large=assets/data/large.csv
work_small_short=assets/workloads/ss-1000.json
work_small_long=assets/workloads/sl-1000.json
work_large_short=assets/workloads/ls-1000.json
work_large_long=assets/workloads/ll-1000.json

# parameters
num_threads_max=32
num_runs=10

./run-midas.sh ss $data_small $work_small_short $num_threads_max $num_runs
./run-midas.sh sl $data_small $work_small_long $num_threads_max $num_runs
./run-midas.sh ls $data_large $work_large_short $num_threads_max $num_runs
./run-midas.sh ll $data_large $work_large_long $num_threads_max $num_runs

./run-echo.sh ss $data_small $work_small_short $num_threads_max $num_runs
./run-echo.sh sl $data_small $work_small_long $num_threads_max $num_runs
./run-echo.sh ls $data_large $work_large_short $num_threads_max $num_runs
./run-echo.sh ll $data_large $work_large_long $num_threads_max $num_runs

