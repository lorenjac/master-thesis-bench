#include <iostream> // std::cout, std::endl
#include <vector>   // std::vector
#include <chrono>   // std::chrono::high_resolution_clock, std::chrono::duration
#include <sstream>  // std::stringstream

#include <sys/sysinfo.h>
#include <pthread.h>

#define PERSISTENT_HEAP "/dev/shm/nvdimm_echo"

extern "C" {
#include "kp_kv_local.h"    // local store
#include "kp_kv_master.h"   // master store
#include "kp_macros.h"      // kp_die()
#include "clibpm.h"         // PMSIZE, pmemalloc_init
#include "kp_recovery.h"
}

// #define NUM_CPUS 2
// #define CPU_OFFSET 0
// #define RET_STRING_LEN 64
// #define MASTER_EXPECTED_MAX_NO_KEYS 1024
// #define LOCAL_EXPECTED_MAX_NO_KEYS 1024

#include "utils.hpp"
#include "opcode.hpp"
#include "workload.hpp"

namespace bench {

enum {
    NUM_CPUS = 4,
    CPU_OFFSET = 0,
    MAX_THREADS = 256
};

struct BenchThreadResult {
    std::size_t num_failures = 0;
    std::size_t num_rw_conflicts = 0;
    std::size_t num_ww_conflicts = 0;
    std::size_t num_r_snapshot_misses = 0;
    std::size_t num_w_snapshot_misses = 0;
    std::size_t num_invalid_txs = 0;
    std::size_t num_canceled_txs = 0;
    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point end;
};

struct BenchThreadArgs {
    unsigned id;
    cpu_set_t cpu_set;
    ProgramArgs* pargs;

    kp_kv_master* master;

    std::vector<KVPair>* pairs;
    tools::workload_t* workload;
    BenchThreadResult result;
};

void* worker_routine(void* arg)
{
    BenchThreadArgs* worker_args = (BenchThreadArgs *) arg;
    ProgramArgs* prog_args = worker_args->pargs;

    const auto id = worker_args->id;
    const auto master = worker_args->master;
    const auto pairs = worker_args->pairs;
    const auto time_unit = prog_args->unit;
    // const auto num_retries_max = prog_args->num_retries;
    const auto& workload = *worker_args->workload;


    kp_kv_local *local;
    // int rc = kp_kv_local_create(master, &local, LOCAL_EXPECTED_MAX_NO_KEYS, false);
    int rc = kp_kv_local_create(master, &local, pairs->size(), false);

    std::string result;

    // Counters
    std::size_t num_failures = 0;
    std::size_t num_rw_conflicts = 0;
    std::size_t num_ww_conflicts = 0;
    std::size_t num_r_snapshot_misses = 0;
    std::size_t num_w_snapshot_misses = 0;
    std::size_t num_invalid_txs = 0;
    std::size_t num_canceled_txs = 0;

    if (prog_args->verbose) {
        std::stringstream ss;
        auto cpu = sched_getcpu();
        ss << "worker " << id << " runs on core " << cpu << std::endl;
        std::cout << ss.str();
    }

    // FIXME division remainder may be lost here
    const std::size_t num_steps_max = workload.size() / prog_args->num_threads;
    const std::size_t begin = id * num_steps_max;
    const std::size_t end = begin + num_steps_max;
    // std::size_t num_retries = 0;

    // ########################################################################
    // ## START ###############################################################
    // ########################################################################

    const auto time_start = std::chrono::high_resolution_clock::now();

    // if (prog_args->verbose) {
    //     std::stringstream ss;
    //     ss << "id = " << id << std::endl;
    //     ss << "workload_size = " << workload.size() << std::endl;
    //     ss << "num_steps_max = " << num_steps_max << std::endl;
    //     ss << "begin = " << begin << std::endl;
    //     ss << "end = " << end << std::endl;
    //     std::cout << ss.str();
    // }

    for (std::size_t step = begin; step < end; ) {
        const auto& workload_tx = workload[step];

        // begin transaction
        // auto tx = store->begin();
        PM_START_TX();

        for (const auto& workload_cmd : workload_tx) {

            // select pair
            const auto& [key, val] = (*pairs)[workload_cmd.pos];

            // perform operation
            switch (workload_cmd.opcode) {
            case tools::tx_opcode_t::Get:
                // if (auto ret = store->read(tx, key, result); ret != midas::Store::OK) {
                //     if (ret == midas::Store::VALUE_NOT_FOUND)
                //         ++num_r_snapshot_misses;
                // }
                {
                    const char* key_ = key.c_str();
                    char* val_;
                    std::size_t size;
                    rc = kp_local_get(local, key_, (void**)&val_, &size);
                }
                break;

            case tools::tx_opcode_t::Put:
                // if (auto ret = store->write(tx, key, val); ret != midas::Store::OK) {
                //     if (ret == midas::Store::VALUE_NOT_FOUND)
                //         ++num_w_snapshot_misses;
                // }
                {
                    const char* key_ = key.c_str();
                    const char* val_ = val.c_str();
                    const std::size_t size = val.size();
                    rc = kp_local_put(local, key_, val_, size);
                }
                break;

            default:
                throw std::runtime_error("error: unexpected operation type");
            }
        }

        rc = kp_local_commit(local, NULL);
        PM_END_TX();
        ++step;

        // Test if the current transaction has failed due to the previous operation
        // if (tx->getStatus() == midas::Transaction::FAILED) {
        //     ++num_failures;
        //     if (num_retries_max) {
        //         if (num_retries < num_retries_max) {
        //             ++num_retries;
        //         }
        //         else {
        //             num_retries = 0;
        //             ++num_canceled_txs;
        //             ++step;
        //         }
        //     }
        //     else {
        //         ++num_canceled_txs;
        //         ++step;
        //     }
        // }
        // else {
        //     // commit transaction; increase error counters if necessary
        //     const auto status = store->commit(tx);
        //     if (status != midas::Store::OK) {
        //         ++num_failures;
        //         if (status == midas::Store::WW_CONFLICT)
        //             ++num_ww_conflicts;
        //         else if (status == midas::Store::RW_CONFLICT)
        //             ++num_rw_conflicts;
        //         else if (status == midas::Store::INVALID_TX)
        //             ++num_invalid_txs;
        //
        //         if (num_retries_max) {
        //             if (num_retries < num_retries_max) {
        //                 ++num_retries;
        //             }
        //             else {
        //                 num_retries = 0;
        //                 ++num_canceled_txs;
        //                 ++step;
        //             }
        //         }
        //         else {
        //             ++num_canceled_txs;
        //             ++step;
        //         }
        //     }
        //     else {
        //         ++step;
        //     }
        // }
    }

    const auto time_end = std::chrono::high_resolution_clock::now();

    // ########################################################################
    // ## END #################################################################
    // ########################################################################

    worker_args->result.num_failures = num_failures;
    worker_args->result.num_canceled_txs = num_canceled_txs;
    worker_args->result.num_rw_conflicts = num_rw_conflicts;
    worker_args->result.num_ww_conflicts = num_ww_conflicts;
    worker_args->result.num_r_snapshot_misses = num_r_snapshot_misses;
    worker_args->result.num_w_snapshot_misses = num_w_snapshot_misses;
    worker_args->result.num_invalid_txs = num_invalid_txs;
    worker_args->result.start = time_start;
    worker_args->result.end = time_end;

    return nullptr;
}

int run(ProgramArgs* pargs)
{
    // load sample data
    std::vector<KVPair> pairs;
    if (read_pairs(pargs->data_file, pairs)) {
        std::cout << "error: could not read pairs from file " << pargs->data_file << "!\n";
        return 1;
    }

    // load workload
    tools::workload_t workload;
    if (tools::parseWorkload(pargs->workload_file, workload)) {
        std::cout << "error: could not read workload from file " << pargs->workload_file << "!\n";
        return 1;
    }

    if (workload.size() < pargs->num_threads) {
        std::cout << "error: too many threads for given size of workload (must be less or equal)!\n";
        return 1;
    }

    if (pargs->verbose)
        std::cout << "initializing store..." << std::endl;

    const char* path = PERSISTENT_HEAP;
    void *pmp;
    if ((pmp = pmemalloc_init(path, (size_t)PMSIZE)) == NULL) {
        printf("Unable to allocate memory pool\n");
        exit(0);
    }

    kp_kv_master* master;
    auto ret = kp_kv_master_create(
            &master,
            MODE_SNAPSHOT,
            // MASTER_EXPECTED_MAX_NO_KEYS,  // expected max no keys
            pairs.size(),  // expected max no keys
            true, // enable conflict detection
            true  // enable NVM usage
    );
    if (ret)
        std::cout << "error: master store could not be created!\n";

    // ########################################################################
    // Populate store
    // ########################################################################

    if (pargs->verbose)
        std::cout << "populating..." << std::endl;

    if (pairs.size()) {
        kp_kv_local *local;
        int rc = kp_kv_local_create(master, &local, pairs.size(), false);

        PM_START_TX();
        for (auto [key, value] : pairs) {
            rc = kp_local_put(local, key.c_str(), value.c_str(), value.size());
            if (rc)
                std::cout << "status code: " << rc << std::endl;
        }
        rc = kp_local_commit(local, NULL);
        PM_END_TX();

        kp_kv_local_destroy(&local);
    }

    // ########################################################################
    // Run benchmark
    // ########################################################################

    int rc;
    int cpu;
    pthread_attr_t attr;
    pthread_t threads[MAX_THREADS];
    BenchThreadArgs thread_args[MAX_THREADS];

    int cpu_count = get_nprocs();
    std::cout << "physical cpus: " << (cpu_count / 2) << std::endl;
    std::cout << "hyper threads: " << cpu_count << std::endl;

    for (std::size_t i = 0; i < pargs->num_threads; i++) {

        thread_args[i].pargs = pargs;
        thread_args[i].master = master;
        thread_args[i].pairs = &pairs;
        thread_args[i].workload = &workload;
        thread_args[i].id = i;

        /* Create Attributes */
        rc = pthread_attr_init(&attr);
        if(rc != 0)
            std::printf("pthread_attr_init() returned error=%d\n", rc);

        /* Setup CPU for everybody: don't spawn yet */
        // cpu = CPU_OFFSET + (i % NUM_CPUS);
        cpu = CPU_OFFSET + (i % NUM_CPUS);
        CPU_ZERO(&(thread_args[i].cpu_set));
        CPU_SET(cpu, &(thread_args[i].cpu_set));

        /* Set affinity */
        rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &(thread_args[i].cpu_set));
        if(rc != 0)
            std::printf("pthread_attr_setaffinity_np() returned error=%d\n", rc);

        if (pargs->verbose)
            std::cout << "launching worker..." << std::endl;

        /* Start thread */
        rc = pthread_create(&threads[i], &attr, &worker_routine, (void *)(&thread_args[i]));
        if(rc != 0)
            std::printf("pthread_create() returned error=%d\n", rc);
    }

    // ########################################################################
    // Barrier
    // ########################################################################

    /* Wait for all workers to complete! */
    for (std::size_t i = 0; i < pargs->num_threads; ++i) {
        rc = pthread_join(threads[i], nullptr);
        if(rc != 0)
            std::printf("pthread_join() returned error=%d\n", rc);
    }

    const auto time_unit = pargs->unit;
    std::size_t num_failures = 0;
    std::size_t num_rw_conflicts = 0;
    std::size_t num_ww_conflicts = 0;
    std::size_t num_r_snapshot_misses = 0;
    std::size_t num_w_snapshot_misses = 0;
    std::size_t num_invalid_txs = 0;
    std::size_t num_canceled_txs = 0;
    std::chrono::high_resolution_clock::time_point glob_start;
    std::chrono::high_resolution_clock::time_point glob_end;
    for (std::size_t i=0; i<pargs->num_threads; ++i) {
        if (pargs->verbose) {
            std::cout << "----------------------------------------\n";
            std::cout << "results for thread-" << i << '\n';
            std::cout << "----------------------------------------\n";
            std::cout << "failures      = " << thread_args[i].result.num_failures << std::endl;
            std::cout << "canceled      = " << thread_args[i].result.num_canceled_txs << std::endl;
            std::cout << "r snap misses = " << thread_args[i].result.num_r_snapshot_misses << std::endl;
            std::cout << "w snap misses = " << thread_args[i].result.num_w_snapshot_misses << std::endl;
            // std::cout << "invalid txs   = " << thread_args[i].result.num_invalid_txs << std::endl;
            std::cout << "w/w conflicts = " << (thread_args[i].result.num_ww_conflicts + thread_args[i].result.num_w_snapshot_misses) << std::endl;
            std::cout << "r/w conflicts = " << thread_args[i].result.num_rw_conflicts << std::endl;
            std::cout << "duration      = " << convert_duration(
                thread_args[i].result.end - thread_args[i].result.start,
                time_unit) << time_unit << std::endl;
        }
        num_failures += thread_args[i].result.num_failures;
        num_rw_conflicts += thread_args[i].result.num_rw_conflicts;
        num_ww_conflicts += thread_args[i].result.num_ww_conflicts;
        num_r_snapshot_misses += thread_args[i].result.num_r_snapshot_misses;
        num_w_snapshot_misses += thread_args[i].result.num_w_snapshot_misses;
        num_invalid_txs += thread_args[i].result.num_invalid_txs;
        num_canceled_txs += thread_args[i].result.num_canceled_txs;
        if (i == 0) {
            glob_start = thread_args[i].result.start;
            glob_end = thread_args[i].result.end;
        }
        else {
            if (thread_args[i].result.start < glob_start)
                glob_start = thread_args[i].result.start;
            if (thread_args[i].result.end > glob_end)
                glob_end = thread_args[i].result.end;
        }
    }

    if (pargs->verbose) {
        std::cout << "----------------------------------------\n";
        std::cout << "summary" << '\n';
        std::cout << "----------------------------------------\n";
    }
    const auto duration = convert_duration(glob_end - glob_start, time_unit);
    std::cout << "time          = " << duration << ' ' << time_unit << std::endl;
    std::cout << "failures      = " << num_failures << std::endl;
    std::cout << "canceled      = " << num_canceled_txs << std::endl;
    std::cout << "r snap misses = " << num_r_snapshot_misses << std::endl;
    std::cout << "w snap misses = " << num_w_snapshot_misses << std::endl;
    std::cout << "invalid txs   = " << num_invalid_txs << std::endl;
    std::cout << "w/w conflicts = " << (num_ww_conflicts + num_w_snapshot_misses) << std::endl;
    std::cout << "r/w conflicts = " << num_rw_conflicts << std::endl;
    std::cout << "throughput    = " << ((workload.size() - num_canceled_txs) / duration) << "/" << time_unit << std::endl;

    // ########################################################################
    // Cleanup
    // ########################################################################

    kp_kv_master_destroy(master);

    rc = pthread_attr_destroy(&attr);
    if(rc != 0)
        std::printf("pthread_attr_destroy() returned error=%d\n", rc);

    // pop.close();
    return 0;
}

} // end namespace bench

int main(int argc, char* argv[])
{
    using namespace bench;

    if (argc < 2) {
        usage();
        exit(0);
    }

    ProgramArgs pargs;
    parse_args(argc, argv, pargs);
    if (validate_args(pargs)) {
        if (pargs.verbose)
            print_args(pargs);
        run(&pargs);
    }
    else {
        usage();
    }
    return 0;
}
