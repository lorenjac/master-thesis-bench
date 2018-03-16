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

//#define _GNU_SOURCE
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
    std::string opcode;
    std::string data_file;
    std::size_t num_repeats = 1000;
    std::string unit = "s";
    bool verbose = false;
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
    // std::getline(ifs, line);
    // std::size_t key_size = std::stoi(line);
    // std::getline(ifs, line);
    // std::size_t val_size = std::stoi(line);
    // std::getline(ifs, line);
    // std::size_t num_pairs = std::stoi(line);

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

void measure_populated_store(BenchThreadArgs* thread_args, std::vector<std::chrono::duration<double>>& latencies)
{
    midas::Store* store = thread_args->store;
    const auto& pairs = *thread_args->pairs;
    const auto opcode = thread_args->pargs->opcode;
    const auto num_repeats = thread_args->pargs->num_repeats;
    const auto verbose = thread_args->pargs->verbose;

    latencies.reserve(num_repeats);

    std::random_device rand_dev;
    std::mt19937 rng(rand_dev());
    std::uniform_int_distribution<> dist(0, pairs.size() - 1);

    if (opcode == "get") {
        auto tx = store->begin();
        for (size_t i=0; i<num_repeats; ++i) {
            const auto& [key, val] = pairs[dist(rng)];
            (void)val;
            if (verbose) {
                std::cout << "get(\n";
                std::cout << "\tkey = " << key << '\n';
                std::cout << ")\n";
            }
            std::string result;
            (void)result;

            auto start = std::chrono::high_resolution_clock::now();
            store->read(tx, key, result);
            auto end = std::chrono::high_resolution_clock::now();
            latencies.emplace_back(end - start);
        }
        store->commit(tx);
    }
    else if (opcode == "put") {
        auto tx = store->begin();
        for (size_t i=0; i<num_repeats; ++i) {
            const auto& [key, val] = pairs[dist(rng)];
            if (verbose) {
                std::cout << "put(\n";
                std::cout << "\tkey = " << key << '\n';
                std::cout << "\tval = " << val << '\n';
                std::cout << ")\n";
            }

            auto start = std::chrono::high_resolution_clock::now();
            store->write(tx, key, val);
            auto end = std::chrono::high_resolution_clock::now();
            latencies.emplace_back(end - start);
        }
        store->commit(tx);
    }
    else if (opcode == "ins") {
        // TODO do I need this?
    }
    else if (opcode == "del") {
        auto tx = store->begin();
        for (size_t i=0; i<num_repeats; ++i) {
            const auto& [key, val] = pairs[dist(rng)];
            (void)val;
            if (verbose) {
                std::cout << "del(\n";
                std::cout << "\tkey = " << key << '\n';
                std::cout << ")\n";
            }

            auto start = std::chrono::high_resolution_clock::now();
            store->drop(tx, key);
            auto end = std::chrono::high_resolution_clock::now();
            latencies.emplace_back(end - start);
        }
        store->commit(tx);
    }
}

void measure(BenchThreadArgs* thread_args, std::vector<std::chrono::duration<double>>& latencies)
{
    if (thread_args->pairs->size())
        measure_populated_store(thread_args, latencies);
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

void evaluate(BenchThreadArgs* thread_args, const std::vector<std::chrono::duration<double>>& latencies)
{
    const auto unit = thread_args->pargs->unit;
    if (thread_args->pargs->verbose) {
        std::cout << "--------------------------------------------------\n";
        size_t index_width = std::ceil(std::log10(latencies.size()));
        for (std::size_t i=0; i<latencies.size(); i++) {
            std::cout << std::setw(index_width) << std::setfill('0') << i;
            std::cout << ": " << convert_duration(latencies[i], unit) << unit << std::endl;
        }
        std::cout << "--------------------------------------------------\n";
    }

    auto min = std::min_element(std::begin(latencies), std::end(latencies));
    auto max = std::max_element(std::begin(latencies), std::end(latencies));
    std::cout << "min;" << convert_duration(*min, unit) << std::endl;
    // std::cout << "min;" << convert_duration(*min, unit) << unit << std::endl;
    std::cout << "max;" << convert_duration(*max, unit) << std::endl;
    // std::cout << "max;" << convert_duration(*max, unit) << unit << std::endl;

    auto size = latencies.size();
    if (size % 2) {
        std::cout << "med;" << convert_duration(latencies[size / 2], unit) << std::endl;
        // std::cout << "med;" << convert_duration(latencies[size / 2], unit) << unit << std::endl;
    }
    else {
        auto lower = latencies[size / 2 - 1];
        auto upper = latencies[size / 2];
        std::cout << "med;" << convert_duration((lower + upper) / 2, unit) << std::endl;
        // std::cout << "med;" << convert_duration((lower + upper) / 2, unit) << unit << std::endl;
    }

    std::chrono::duration<double> sum;
    for (const auto v : latencies)
        sum += v;
    std::cout << "avg;" << convert_duration(sum / latencies.size(), unit) << std::endl;
    // std::cout << "avg;" << convert_duration(sum / latencies.size(), unit) << unit << std::endl;
}

/* Packages up single threaded evaluations so we can use it from within
   a single worker setup */
void* latency_benchmark(void* arg)
{
    // unsigned long tid = pthread_self();

    BenchThreadArgs* thread_args = (BenchThreadArgs *) arg;
    ProgramArgs* prog_args = thread_args->pargs;

    // ########################################################################
    // Populate store
    // ########################################################################

    if (prog_args->verbose)
        std::cout << "populating..." << std::endl;

    auto store = thread_args->store;
    auto& pairs = *thread_args->pairs;
    if (pairs.size()) {
        auto tx = store->begin();
        for (auto [key, value] : pairs) {
            store->write(tx, key, value);
        }
        store->commit(tx);
    }

    // ########################################################################
    // Measure operation latency
    // ########################################################################

    if (prog_args->verbose)
        std::cout << "measuring..." << std::endl;

    std::vector<std::chrono::duration<double>> latencies;
    measure(thread_args, latencies);

    // ########################################################################
    // Evaluate results
    // ########################################################################

    if (prog_args->verbose)
        std::cout << "evaluating..." << std::endl;

    evaluate(thread_args, latencies);

    // ########################################################################
    // End benchmark
    // ########################################################################
    return nullptr;
}

int run(ProgramArgs* pargs)
{
    std::vector<KVPair> pairs;
    if (!pargs->data_file.empty())
        pairs = fetch_data(pargs->data_file);

    if (pargs->verbose)
        std::cout << "initializing store..." << std::endl;

    midas::pop_type pop;
    if (!midas::init(pop, STORE_FILE, POOL_SIZE)) {
        std::cout << "error: could not open file <" << STORE_FILE << ">!\n";
        return 0;
    }
    midas::Store store{pop};

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

    if (pargs->verbose)
        std::cout << "launching worker..." << std::endl;

    /* Start thread */
    rc = pthread_create(&thread, &attr, &latency_benchmark, (void *)(&thread_args));
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
    std::cout << "\tbaseline - determine average operation latency\n";
    std::cout << "\nSYNOPSIS\n";
    std::cout << "\tbaseline opcode [options]\n";
    std::cout << "\nDESCRIPTION\n";
    std::cout << "\tThis program is used to determine the average latency of ";
    std::cout << "individual database\n";
    std::cout << "\toperations. The opcode denotes the operation to be ";
    std::cout << "measured and is a required\n";
    std::cout << "\targument. The measurement may be configured via several ";
    std::cout << "options.\n";
    std::cout << "\nOPCODES\n";
    std::cout << "\tput\n";
    std::cout << "\t\tUpdate.\n";
    std::cout << "\tins\n";
    std::cout << "\t\tInsertion (currently not supported).\n";
    std::cout << "\tget\n";
    std::cout << "\t\tRetrieval.\n";
    std::cout << "\tdel\n";
    std::cout << "\t\tDeletion.\n";
    std::cout << "\nOPTIONS\n";
    std::cout << "\t-p, --populate FILE\n";
    std::cout << "\t\tPopulates the database with data from the specified file.\n";
    std::cout << "\t-r, --repeats NUM\n";
    std::cout << "\t\tSets the number of repetitions for the given operation (default = 1000).\n";
    std::cout << "\t-u, --unit UNIT\n";
    std::cout << "\t\tSets the time unit of used when printing results. Can be one of {s | ms | us | ns} (default = s).\n";
    std::cout << "\t-v, --verbose\n";
    std::cout << "\t\tEnables verbose mode. With this, all intermediate results will be shown.\n";
    std::cout << "\t-h, --help\n";
    std::cout << "\t\tShow this help text.\n";
}

void parse_args(int argc, char* argv[], ProgramArgs& pargs)
{
    pargs.opcode = argv[1];
    if (pargs.opcode != "get"
            && pargs.opcode != "put"
            && pargs.opcode != "ins"
            && pargs.opcode != "del") {
        usage();
        exit(0);
    }
    ++optind;

    static struct option longopts[] = {
        { "repeats"  , required_argument , NULL , 'r' },
        { "populate" , required_argument , NULL , 'p' },
        { "unit"     , required_argument , NULL , 'u' },
        { "verbose"  , no_argument       , NULL , 'v' },
        { "help"     , no_argument       , NULL , 'h' },
        { NULL       , 0                 , NULL , 0 }
    };

    char ch;
    while ((ch = getopt_long(argc, argv, "r:p:u:v", longopts, NULL)) != -1) {
        switch (ch) {
        case 'r':
            pargs.num_repeats = std::stoll(optarg);
            break;

        case 'p':
            pargs.data_file = optarg;
            break;

        case 'u':
            pargs.unit = optarg;
            break;

        case 'v':
            pargs.verbose = true;
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

void print_args(ProgramArgs& pargs)
{
    std::cout << std::boolalpha;
    std::cout << "opcode  : " << pargs.opcode << std::endl;
    std::cout << "repeats : " << pargs.num_repeats << std::endl;
    std::cout << "datafile: " << pargs.data_file << std::endl;
    std::cout << "unit    : " << pargs.unit << std::endl;
    std::cout << "verbose : " << pargs.verbose << std::endl;
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
    if (pargs.verbose)
        print_args(pargs);
    run(&pargs);

    return 0;
}
