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

#include "utils-rand.hpp"
#include "midas.hpp"

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
    std::vector<TransactionProfile::Ptr>* tx_profiles;
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
    const auto num_txs = prog_args->num_txs;
    const auto tx_profiles = worker_args->tx_profiles;
    const auto tx_len_min = prog_args->tx_len_min;
    const auto tx_len_max = prog_args->tx_len_max;
    const auto time_unit = prog_args->unit;

    // Pseudo-random number generator
    std::random_device dev;
    std::mt19937 rng(dev());

    // Distribution for selecting profiles, operations
    std::uniform_int_distribution<> prob_dist{1, 100};

    // Distribution for selecting pairs
    std::uniform_int_distribution<> pair_dist{0, static_cast<int>(pairs->size() - 1)};

    // Distribution for selecting #ops in a transaction
    double tx_len_mean = (tx_len_max - tx_len_min) / 2.0;
    std::normal_distribution<> len_dist{
            tx_len_mean,
            std::sqrt(tx_len_mean - tx_len_min)};

    // Runtime variables
    int rand = 0;
    OpCode opcode = OpCode::Get;
    TransactionProfile::Ptr prof;
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

    for (std::size_t i=0; i<num_txs; ++i) {
        // select random tx profile (using probabilities from profiles)
        rand = prob_dist(rng);
        for (auto const& p : *tx_profiles) {
            if (rand <= p->prob) {
                prof = p;
                break;
            }
            rand -= p->prob;
        }

        // std::cout << pid << "> selected profile: " << prof->name << std::endl;
        // ss << "[" << pid << "] selected profile: " << prof->name << std::endl;
        // ss << "[" << pid << ": tx=" << i << '/' << num_txs << "] ";
        // ss << "selected profile: " << prof->name << std::endl;
        // std::cout << ss.str();
        // ss.str("");

        // select random tx length (using normal distribution based on profile)
        auto tx_length = std::round(len_dist(rng));

        // std::cout << pid << "> selected tx length: " << tx_length << std::endl;
        // ss << "[" << pid << "] selected tx length: " << tx_length << std::endl;
        // ss << "[" << pid << ": tx=" << i << '/' << num_txs << "] ";
        // ss << "selected tx length: " << tx_length << std::endl;
        // std::cout << ss.str();
        // ss.str("");

        // begin transaction
        auto tx = store->begin();

        // loop for $tx_length steps
        for (std::size_t step=0; step<tx_length; ++step) {
            // select random operation (using custom distribution in profile)
            rand = prob_dist(rng);
            for (auto const& [prob, op] : prof->ops) {
                if (rand <= prob) {
                    opcode = op;
                    break;
                }
                rand -= prob;
            }

            // std::cout << pid << "> selected operation: " << opcode << std::endl;
            // ss << "[" << pid << "] selected operation: " << opcode << std::endl;
            // ss << "[" << pid << ": tx=" << i << '/' << num_txs;
            // ss << ", op=" << step << '/' << tx_length;
            // ss << " | id=" << tx->getId();
            // ss << ", ts=" << tx->getBegin() << "] ";
            // ss << "selected operation: " << opcode << std::endl;
            // std::cout << ss.str();
            // ss.str("");

            // select random pair (using uniform distribution)
            const auto& [key, val] = (*pairs)[pair_dist(rng)];

            // std::cout << pid << "> selected pair: [" << key << ", " << val << "]" << std::endl;
            // ss << "[" << pid << "] selected pair: [" << key << ", " << val << "]" << std::endl;
            // ss << "[" << pid << ": tx=" << i << '/' << num_txs;
            // ss << ", op=" << step << '/' << tx_length;
            // ss << " | id=" << tx->getId();
            // ss << ", ts=" << tx->getBegin() << "] ";
            // ss << "selected pair: [" << key << ", " << val << "]" << std::endl;
            // std::cout << ss.str();
            // ss.str("");

            // perform operation
            switch (opcode) {
            case OpCode::Get:
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

            case OpCode::Put:
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

            case OpCode::Ins:
                if (auto ret = store->write(tx, key, val); ret != midas::Store::OK) {
                    // ss << "[" << pid << "] Ins -> " << print_midas_ret(ret);
                    // std::cout << ss.str() << std::endl;
                    // ss.str("");
                    // exit(0);
                }
                break;

            case OpCode::Del:
                if (auto ret = store->drop(tx, key); ret != midas::Store::OK) {
                    // ss << "[" << pid << "] Del -> " << print_midas_ret(ret);
                    // std::cout << ss.str() << std::endl;
                    // ss.str("");
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
    auto pairs = fetch_data(pargs->data_path);

    // load profiles
    std::vector<TransactionProfile::Ptr> profiles;
    for (const auto& fpath : pargs->tx_profile_paths) {
        auto prof = std::make_shared<TransactionProfile>();
        load_profile(fpath, prof);
        profiles.push_back(prof);
    }
    std::sort(profiles.begin(), profiles.end(),
            [](auto a, auto b){ return a->prob <= b->prob; });

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
        thread_args[i].tx_profiles = &profiles;
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
