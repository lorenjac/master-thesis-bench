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

./run-midas.sh $data_small $work_small_short $num_threads_max $num_runs
./run-midas.sh $data_small $work_small_long $num_threads_max $num_runs
./run-midas.sh $data_large $work_large_short $num_threads_max $num_runs
./run-midas.sh $data_large $work_large_long $num_threads_max $num_runs

./run-echo.sh $data_small $work_small_short $num_threads_max $num_runs
./run-echo.sh $data_small $work_small_long $num_threads_max $num_runs
./run-echo.sh $data_large $work_large_short $num_threads_max $num_runs
./run-echo.sh $data_large $work_large_long $num_threads_max $num_runs

