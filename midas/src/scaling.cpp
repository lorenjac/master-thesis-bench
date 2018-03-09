#include <iostream> // std::cout, std::endl
#include <iomanip>  // std::setw, std::setfill
#include <vector>   // std::vector
#include <fstream>  // std::ifstream
#include <chrono>   // std::chrono::high_resolution_clock, std::chrono::duration
#include <cmath>    // std::ceil, std::log10
#include <algorithm>// std::min_element, std::max_element
#include <random>   // std::random_device, std::uniform_int_distribution
#include <stdexcept>// std::invalid_argument

#include "midas.hpp"

#include <getopt.h> // getopt_long

#include <pthread.h>

namespace bench {

const std::string STORE_FILE = "/dev/shm/nvdimm_midas";
const size_t POOL_SIZE = 64ULL * 1024 * 1024;

enum {
    NUM_CPUS = 2,
    CPU_OFFSET = 0
};

using KVPair = std::pair<std::string, std::string>;

struct ProgramArgs {
    std::string data_path;
    std::size_t num_threads;
    std::size_t num_txs;
    std::size_t num_retries;
    std::vector<std::string> tx_profile_paths;
    std::size_t tx_len_min;
    std::size_t tx_len_max;
    std::string unit = "s";
};

struct BenchThreadArgs {
    cpu_set_t cpu_set;
    ProgramArgs* pargs;
    midas::Store* store;
    std::vector<KVPair>* pairs;
};

std::vector<KVPair> fetch_data(const std::string& path)
{
    std::ifstream ifs{path};
    if (!ifs.is_open())
        return {};

    std::string line;
    std::vector<KVPair> pairs;
    while (std::getline(ifs, line)) {
        if (!line.empty()) {
            auto pos_delim = line.find(';');
            if (pos_delim != std::string::npos) {
                pairs.emplace_back(
                    line.substr(0, pos_delim), // get chars before delimiter
                    line.substr(pos_delim + 1) // get chars after delimiter
                );
            }
            else {
                throw std::invalid_argument("error: missing delimiter (;) in line");
            }
        }
    }
    return pairs;
}

double convert_duration(std::chrono::duration<double> dur, const std::string& unit = "")
{
    if (unit == "ms")
        return std::chrono::duration<double, std::milli>{dur}.count();

    if (unit == "us")
        return std::chrono::duration<double, std::micro>(dur).count();

    if (unit == "ns")
        return std::chrono::duration<double, std::nano>(dur).count();

    return dur.count();
}

void* worker_routine(void* arg)
{
    BenchThreadArgs* args = (BenchThreadArgs *) arg;
    // unsigned long tid = pthread_self();


    return nullptr;
}

int run(ProgramArgs* pargs)
{
    std::vector<KVPair> pairs;
    if (!pargs->data_path.empty())
        pairs = fetch_data(pargs->data_path);

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

void usage()
{
    std::cout << "NAME\n";
    std::cout << "\tscaling - determine transaction throughput\n";
    std::cout << "\nSYNOPSIS\n";
    std::cout << "\tscaling options\n";
    std::cout << "\nDESCRIPTION\n";
    std::cout << "\nOPTIONS\n";
    std::cout << "\t-u, --unit UNIT\n";
    std::cout << "\t\tSets the time unit of used when printing results. Can be one of {s | ms | us | ns} (default = s).\n";
    std::cout << "\t-h, --help\n";
    std::cout << "\t\tShow this help text.\n";
}

void parse_args(int argc, char* argv[], ProgramArgs& args)
{
    static struct option longopts[] = {
        { "data"          , required_argument , NULL , 'd' },
        { "num-threads"   , required_argument , NULL , 't' },
        { "num-txs"       , required_argument , NULL , 'n' },
        { "num-retries"   , required_argument , NULL , 'r' },
//        { "mixed-profile" , required_argument , NULL , 'm' },
//        { "ronly-profile" , required_argument , NULL , 'o' },
        { "tx-profile"    , required_argument , NULL , 'p' },
        { "tx-length-min" , required_argument , NULL , 'i' },
        { "tx-length-max" , required_argument , NULL , 'a' },
        { "unit"          , required_argument , NULL , 'u' },
        { "help"          , no_argument       , NULL , 'h' },
        { NULL            , 0                 , NULL , 0 }
    };

    char ch;
    // while ((ch = getopt_long(argc, argv, "d:t:n:r:m:o:i:a:u:h", longopts, NULL)) != -1) {
    while ((ch = getopt_long(argc, argv, "d:t:n:r:p:i:a:u:h", longopts, NULL)) != -1) {
        switch (ch) {
        case 'd': // path to data set
            args.data_path = optarg;
            break;

        case 't': // number of threads
            args.num_threads = std::stoll(optarg);
            break;

        case 'n': // number of transactions executed by each thread
            args.num_txs = std::stoll(optarg);
            break;

        case 'r': // number of times a transaction can be retried upon failure
            args.num_retries = std::stoll(optarg);
            break;

        // case 'm': // path to profile for mixed transactions
        //     break;

        // case 'o': // path to profile for read-only transactions
        //     break;

        case 'p': // add path to transaction profile
            args.tx_profile_paths.emplace_back(optarg);
            break;

        case 'i': // minimum number of operations in a transaction
            args.tx_len_min = std::stoll(optarg);
            break;

        case 'a': // maximum number of operations in a transaction
            args.tx_len_max = std::stoll(optarg);
            break;

        case 'u': // time unit
            args.unit = optarg;
            break;

        case 'h':
            usage();
            exit(0);
            break;

        default:
            usage();
            exit(0);
        }
    }
    argc -= optind;
    argv += optind;
}

void print_args(ProgramArgs& args)
{
    std::cout << "data_path: " << args.data_path << std::endl;
    std::cout << "num_threads: " << args.num_threads << std::endl;
    std::cout << "num_txs: " << args.num_txs << std::endl;
    std::cout << "num_retries: " << args.num_retries << std::endl;
    std::cout << "profile_paths: " << std::endl;
    for (const auto& p : args.tx_profile_paths)
        std::cout << "\t" << p << std::endl;
    std::cout << "tx_len_min: " << args.tx_len_min << std::endl;
    std::cout << "tx_len_max: " << args.tx_len_max << std::endl;
    std::cout << "unit: " << args.unit << std::endl;
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
