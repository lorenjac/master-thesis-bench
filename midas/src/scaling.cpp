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
    std::vector<KVPair>* pairs;
    std::vector<TransactionProfile>* tx_profiles;
};

void* worker_routine(void* arg)
{
    BenchThreadArgs* worker_args = (BenchThreadArgs *) arg;
    ProgramArgs* pargs = worker_args->pargs;

    const auto& pairs = *worker_args->pairs;
    const auto num_txs = pargs->num_txs;
    const auto num_retries = pargs->num_retries;
    const auto& tx_profiles = *worker_args->tx_profiles;
    const auto tx_len_min = pargs->tx_len_min;
    const auto tx_len_max = pargs->tx_len_max;

    std::random_device dev;
    std::mt19937 rng(dev());

    for (std::size_t i=0; i<num_txs; ++i) {
        // select random tx profile (using custom distribution in profiles)
        // select random tx length (using normal distribution based on profile)
        // begin transaction
        // loop for $tx_length steps
            // select random operation (using custom distribution in profile)
            // select random pair (using uniform distribution)
        // loop $num_retries times if tx fails to commit
            // commit transaction
    }
    return nullptr;
}

int run(ProgramArgs* pargs)
{
    std::vector<KVPair> pairs;
    if (!pargs->data_path.empty())
        pairs = fetch_data(pargs->data_path);

    std::vector<TransactionProfile> profiles;
    for (const auto& fpath : pargs->tx_profile_paths) {
        load_profile(fpath, profiles.emplace_back());
    }

    std::cout << "initializing store..." << std::endl;
    midas::pop_type pop;
    if (!midas::init(pop, STORE_FILE, POOL_SIZE)) {
        std::cout << "error: could not open file <" << STORE_FILE << ">!\n";
        return 0;
    }
    midas::Store store{pop};
    
    // ########################################################################
    // Populate store
    // ########################################################################

    std::cout << "populating..." << std::endl;

    if (pairs.size()) {
        auto tx = store.begin();
        for (auto [key, value] : pairs) {
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
    BenchThreadArgs thread_args;

    thread_args.pargs = pargs;
    thread_args.store = &store;
    thread_args.pairs = &pairs;

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
    print_args(pargs);
    //run(&pargs);

    return 0;
}
