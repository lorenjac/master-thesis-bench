#include <iostream> // std::cout, std::endl
#include <iomanip>  // std::setw, std::setfill
#include <vector>   // std::vector
#include <fstream>  // std::ifstream
#include <chrono>   // std::chrono::high_resolution_clock, std::chrono::duration
#include <cmath>    // std::ceil, std::log10
#include <algorithm>// std::min_element, std::max_element
#include <random>   // std::random_device, std::uniform_int_distribution
#include <sstream>

#include <sys/sysinfo.h>
#include <pthread.h>

#include "midas.hpp"

#include "utils.hpp"
#include "opcode.hpp"

namespace bench {

const std::string STORE_FILE = "/dev/shm/nvdimm_midas";
// const size_t POOL_SIZE = 64ULL * 1024 * 1024;
const size_t POOL_SIZE = 256ULL * 1024 * 1024;

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
    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point end;
};

struct BenchThreadArgs {
    unsigned id;
    cpu_set_t cpu_set;
    ProgramArgs* pargs;
    midas::Store* store;
    std::vector<KVPair>* pairs;
    tools::workload_t* workload;
    BenchThreadResult result;
};

std::string print_midas_ret(unsigned code)
{
    switch (code) {
    case midas::Store::OK:
        return "OK";

    case midas::Store::INVALID_TX:
        return "INVALID_TX";

    case midas::Store::RW_CONFLICT:
        return "RW_CONFLICT";

    case midas::Store::WW_CONFLICT:
        return "WW_CONFLICT";

    case midas::Store::VALUE_NOT_FOUND:
        return "VALUE_NOT_FOUND";

    default:
        return "UNKNOWN STATUS CODE";
    }
}

void* worker_routine(void* arg)
{
    BenchThreadArgs* worker_args = (BenchThreadArgs *) arg;
    ProgramArgs* prog_args = worker_args->pargs;

    // auto pid = pthread_self();
    // auto pid = worker_args->id;

    const auto store = worker_args->store;
    const auto pairs = worker_args->pairs;
    const auto workload = worker_args->workload;
    const auto time_unit = prog_args->unit;

    std::string result;

    // Counters
    std::size_t num_failures = 0;
    std::size_t num_rw_conflicts = 0;
    std::size_t num_ww_conflicts = 0;
    std::size_t num_r_snapshot_misses = 0;
    std::size_t num_w_snapshot_misses = 0;
    std::size_t num_invalid_txs = 0;

    if (prog_args->verbose) {
        std::stringstream ss;
        auto cpu = sched_getcpu();
        ss << "worker " << worker_args->id << " runs on core " << cpu << std::endl;
        std::cout << ss.str();
    }

    // ########################################################################
    // ## START ###############################################################
    // ########################################################################

    const auto time_start = std::chrono::high_resolution_clock::now();

    for (const auto& workload_tx : *workload) {

        // begin transaction
        auto tx = store->begin();

        for (const auto& workload_cmd : workload_tx) {

            // select pair
            const auto& [key, val] = (*pairs)[workload_cmd.pos];

            // perform operation
            switch (workload_cmd.opcode) {
            case tools::tx_opcode_t::Get:
                if (auto ret = store->read(tx, key, result); ret != midas::Store::OK) {
                    if (ret == midas::Store::VALUE_NOT_FOUND)
                        ++num_r_snapshot_misses;
                    // ss << "[" << pid << "] Get -> " << print_midas_ret(ret);
                    // ss << "[" << pid << ": tx=" << i << '/' << num_txs;
                    // ss << ", op=" << step << '/' << tx_length;
                    // ss << " | id=" << tx->getId();
                    // ss << ", ts=" << tx->getBegin() << "] ";
                    // ss << "Get -> " << print_midas_ret(ret);
                    // std::cout << ss.str() << std::endl;
                    // ss.str("");
                    // exit(0);
                }
                break;

            case tools::tx_opcode_t::Put:
                if (auto ret = store->write(tx, key, val); ret != midas::Store::OK) {
                    if (ret == midas::Store::VALUE_NOT_FOUND)
                        ++num_w_snapshot_misses;
                    // ss << "[" << pid << ": tx=" << i << '/' << num_txs;
                    // ss << ", op=" << step << '/' << tx_length;
                    // ss << " | id=" << tx->getId();
                    // ss << ", ts=" << tx->getBegin() << "] ";
                    // ss << "Put -> " << print_midas_ret(ret);
                    // std::cout << ss.str() << std::endl;
                    // std::cerr << ss.str() << std::endl;
                    // ss.str("");
                    // store->print();
                    // exit(0);
                }
                break;

            default:
                throw std::runtime_error("error: unexpected operation type");
            }
        }

        if (tx->getStatus().load() == midas::Transaction::FAILED) {
            ++num_failures;
            continue;
        }

        // commit transaction; increase error counters if necessary
        const auto status = store->commit(tx);
        // ss << "[" << pid << "] commit -> " << print_midas_ret(status) << std::endl;
        // std::cout << ss.str();
        // ss.str("");
        if (status != midas::Store::OK) {
            ++num_failures;
            if (status == midas::Store::WW_CONFLICT)
                ++num_ww_conflicts;
            else if (status == midas::Store::RW_CONFLICT)
                ++num_rw_conflicts;
            else if (status == midas::Store::INVALID_TX) {
                ++num_invalid_txs;
            }
        }
    }

    const auto time_end = std::chrono::high_resolution_clock::now();

    // ########################################################################
    // ## END #################################################################
    // ########################################################################

    worker_args->result.num_failures = num_failures;
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
    if (read_pairs(pargs->data_path, pairs)) {
        std::cout << "error: could not read pairs from file " << pargs->data_path << "!\n";
        return 1;
    }

    // load workloads
    std::vector<tools::workload_t> workloads;
    if (read_workloads(pargs->workload_file, workloads)) {
        std::cout << "error: could not read workloads from file " << pargs->workload_file << "!\n";
        return 1;
    }

    if (workloads.size() < pargs->num_threads) {
        std::cout << "error: too many threads for given number of workloads (must be less or equal)!\n";
        return 1;
    }

    if (pargs->verbose)
        std::cout << "initializing store..." << std::endl;

    midas::pop_type pop;
    if (!midas::init(pop, STORE_FILE, POOL_SIZE)) {
        std::cout << "error: could not open file <" << STORE_FILE << ">!\n";
        return 1;
    }
    midas::Store store{pop};

    // ########################################################################
    // Populate store
    // ########################################################################

    if (pargs->verbose)
        std::cout << "populating..." << std::endl;

    if (pairs.size()) {
        auto tx = store.begin();
        for (auto [key, value] : pairs) {
            store.write(tx, key, value);
        }
        store.commit(tx);
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
        thread_args[i].store = &store;
        thread_args[i].pairs = &pairs;
        thread_args[i].workload = &workloads[i];
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
    std::chrono::high_resolution_clock::time_point glob_start;
    std::chrono::high_resolution_clock::time_point glob_end;
    for (std::size_t i=0; i<pargs->num_threads; ++i) {
        if (pargs->verbose) {
            std::cout << "----------------------------------------\n";
            std::cout << "results for thread-" << i << '\n';
            std::cout << "----------------------------------------\n";
            std::cout << "failures      = " << thread_args[i].result.num_failures << std::endl;
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
    std::cout << "time          = " << convert_duration(glob_end - glob_start, time_unit) << time_unit << std::endl;
    std::cout << "failures      = " << num_failures << std::endl;
    std::cout << "r snap misses = " << num_r_snapshot_misses << std::endl;
    std::cout << "w snap misses = " << num_w_snapshot_misses << std::endl;
    std::cout << "invalid txs   = " << num_invalid_txs << std::endl;
    std::cout << "w/w conflicts = " << (num_ww_conflicts + num_w_snapshot_misses) << std::endl;
    std::cout << "r/w conflicts = " << num_rw_conflicts << std::endl;

    // ########################################################################
    // Cleanup
    // ########################################################################

    rc = pthread_attr_destroy(&attr);
    if(rc != 0)
        std::printf("pthread_attr_destroy() returned error=%d\n", rc);

    pop.close();
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
