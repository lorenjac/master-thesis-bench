# assets
data_small=...
data_large=...
work_small_short=...
work_small_long=...
work_large_short=...
work_large_long=...

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
