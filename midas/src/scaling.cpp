#include <iostream> // std::cout, std::endl
#include <iomanip>  // std::setw, std::setfill
#include <vector>   // std::vector
#include <fstream>  // std::ifstream
#include <chrono>   // std::chrono::high_resolution_clock, std::chrono::duration
#include <cmath>    // std::ceil, std::log10
#include <algorithm>// std::min_element, std::max_element
#include <random>   // std::random_device, std::uniform_int_distribution
#include <pthread.h>

#include "utils.hpp"
#include "midas.hpp"

namespace bench {

const std::string STORE_FILE = "/dev/shm/nvdimm_midas";
const size_t POOL_SIZE = 64ULL * 1024 * 1024;

enum {
    NUM_CPUS = 2,
    CPU_OFFSET = 0
};

struct BenchThreadArgs {
    cpu_set_t cpu_set;
    ProgramArgs* pargs;
    midas::Store* store;
    std::vector<KVPair> pairs;
    std::vector<TransactionProfile::Ptr> tx_profiles;
};

void* worker_routine(void* arg)
{
    BenchThreadArgs* worker_args = (BenchThreadArgs *) arg;
    ProgramArgs* prog_args = worker_args->pargs;

    const auto store = worker_args->store;
    const auto& pairs = worker_args->pairs;
    const auto num_txs = prog_args->num_txs;
    // const auto num_retries_max = prog_args->num_retries;
    const auto& tx_profiles = worker_args->tx_profiles;
    const auto tx_len_min = prog_args->tx_len_min;
    const auto tx_len_max = prog_args->tx_len_max;
    const auto time_unit = prog_args->unit;

    // Pseudo-random number generator
    std::random_device dev;
    std::mt19937 rng(dev());

    // Distribution for selecting profiles, operations
    std::uniform_int_distribution<> prob_dist{1, 100};

    // Distribution for selecting pairs
    std::uniform_int_distribution<> pair_dist{0, static_cast<int>(pairs.size() - 1)};

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

    // ########################################################################
    // ## START ###############################################################
    // ########################################################################

    const auto time_start = std::chrono::high_resolution_clock::now();

    for (std::size_t i=0; i<num_txs; ++i) {
        // select random tx profile (using probabilities from profiles)
        rand = prob_dist(rng);
        for (auto const& p : tx_profiles) {
            if (rand <= p->prob) {
                prof = p;
                break;
            }
            rand -= p->prob;
        }

        std::cout << "selected profile: " << prof->name << std::endl;

        // select random tx length (using normal distribution based on profile)
        auto tx_length = std::round(len_dist(rng));

        std::cout << "selected tx length: " << tx_length << std::endl;

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

            std::cout << "selected operation: " << opcode << std::endl;

            // select random pair (using uniform distribution)
            const auto& [key, val] = pairs[pair_dist(rng)];

            std::cout << "selected pair: [" << key << ", " << val << "]" << std::endl;

            // perform operation
            switch (opcode) {
            case OpCode::Get:
                store->read(tx, key, result);
                break;

            case OpCode::Put:
                store->write(tx, key, val);
                break;

            case OpCode::Ins:
                store->write(tx, key, val);
                break;

            case OpCode::Del:
                store->drop(tx, key);
                break;

            default:
                throw std::runtime_error("error: unexpected operation type");
            }
        }

        // commit transaction; increase error counters if necessary
        const auto status = store->commit(tx);
        if (status != midas::Store::OK) {
            ++num_failures;
            if (status == midas::Store::WW_CONFLICT)
                ++num_ww_conflicts;
            else if (status == midas::Store::RW_CONFLICT)
                ++num_rw_conflicts;

            // TODO make sure this tx is retried (do we need that?)
        }
    }

    const auto time_end = std::chrono::high_resolution_clock::now();

    // ########################################################################
    // ## END #################################################################
    // ########################################################################

    std::cout << "time elapsed = " << convert_duration(time_end - time_start, time_unit) << time_unit << std::endl;
    std::cout << "#failures = " << num_failures << std::endl;
    std::cout << "#w/w conflicts = " << num_ww_conflicts << std::endl;
    std::cout << "#r/w conflicts = " << num_rw_conflicts << std::endl;

    return nullptr;
}

int run(ProgramArgs* pargs)
{
    BenchThreadArgs thread_args;
    thread_args.pargs = pargs;

    if (!pargs->data_path.empty()) {
        thread_args.pairs = fetch_data(pargs->data_path);
    }

    for (const auto& fpath : pargs->tx_profile_paths) {
        auto prof = std::make_shared<TransactionProfile>();
        load_profile(fpath, prof);
        thread_args.tx_profiles.push_back(prof);
    }
    std::sort(thread_args.tx_profiles.begin(), thread_args.tx_profiles.end(),
            [](auto a, auto b){ return a->prob <= b->prob; });

    std::cout << "initializing store..." << std::endl;
    midas::pop_type pop;
    if (!midas::init(pop, STORE_FILE, POOL_SIZE)) {
        std::cout << "error: could not open file <" << STORE_FILE << ">!\n";
        return 0;
    }
    midas::Store store{pop};
    thread_args.store = &store;

    // ########################################################################
    // Populate store
    // ########################################################################

    std::cout << "populating..." << std::endl;

    if (thread_args.pairs.size()) {
        auto tx = store.begin();
        for (auto [key, value] : thread_args.pairs) {
            store.write(tx, key, value);
        }
        store.commit(tx);
    }

    // pthread_spin_init(&tot_epoch_lock, PTHREAD_PROCESS_SHARED);

    int rc, i = 0;
    int cpu;
    pthread_attr_t attr;
    pthread_t thread;
    void *ret_thread;

    /* Setup CPU for everybody: don't spawn yet */
    cpu = CPU_OFFSET + (i % NUM_CPUS);
    CPU_ZERO(&(thread_args.cpu_set));
    CPU_SET(cpu, &(thread_args.cpu_set));

    /* Create Attributes */
    rc = pthread_attr_init(&attr);
    if(rc != 0)
        std::printf("pthread_attr_init() returned error=%d\n", rc);

    /* Set affinity */
    rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &(thread_args.cpu_set));
    if(rc != 0)
        std::printf("pthread_attr_setaffinity_np() returned error=%d\n", rc);

    std::cout << "launching worker..." << std::endl;

    /* Start thread */
    rc = pthread_create(&thread, &attr, &worker_routine, (void *)(&thread_args));
    if(rc != 0)
        std::printf("pthread_create() returned error=%d\n", rc);

    /* Get that worker back and cleanup! */
    rc = pthread_join(thread, &ret_thread);
    if(rc != 0)
        std::printf("pthread_join() returned error=%d\n", rc);

    if(ret_thread) {
        free(ret_thread);
        ret_thread = NULL;
    }

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
        print_args(pargs);
        run(&pargs);
    }
    else {
        usage();
    }
    return 0;
}
