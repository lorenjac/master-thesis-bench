#include <iostream> // std::cout, std::endl
#include <iomanip>  // std::setw, std::setfill
#include <vector>   // std::vector
#include <fstream>  // std::ifstream
#include <chrono>   // std::chrono::high_resolution_clock, std::chrono::duration
#include <cmath>    // std::ceil, std::log10
#include <algorithm>// std::min_element, std::max_element
#include <random>   // std::random_device, std::uniform_int_distribution
#include <stdexcept>// std::invalid_argument

#include <getopt.h> // getopt_long

#define PERSISTENT_HEAP "/dev/shm/nvdimm_echo"

extern "C" {
#include "kp_kv_local.h"    // local store
#include "kp_kv_master.h"   // master store
#include "kp_macros.h"      // kp_die()
#include "clibpm.h"         // PMSIZE, pmemalloc_init
#include "kp_recovery.h"
// void *pmemalloc_init(const char *path, size_t size);
}

#define NUM_CPUS 2
#define CPU_OFFSET 0
#define RET_STRING_LEN 64
#define MASTER_EXPECTED_MAX_NO_KEYS 1024
#define LOCAL_EXPECTED_MAX_NO_KEYS 1024

struct program_args {
    std::string opcode;
    std::string data_file;
    std::size_t num_repeats = 1000;
    std::string unit = "s";
    bool verbose = false;
};

using kvpair_t = std::pair<std::string, std::string>;

// typedef struct random_ints_ {
//   int *array;
//   unsigned int count;
//   unsigned int idx;
// } random_ints;

/* Structure to push arguments to the worker */
typedef struct benchmark_args_struct {
    cpu_set_t cpu_set;
    void *master;
    program_args *pargs;
    std::vector<kvpair_t> *pairs;
    // int num_threads;
    // int starting_ops;
    // pthread_cond_t *bench_cond;
    // pthread_mutex_t *bench_mutex;
    // bool slam_local;
    // bool split_keys;
    // int my_id;
    // bool do_measure;
    // random_ints *ints;
} benchmark_thread_args;

/**
 * Prevent reordering of instructions even if implementation is known.
 *
 * Surround non-reordered code with each one call to this function.
 * The first one receives an input parameter of the code, the second
 * receives a return value of the code. If timing is required, put
 * that before the first call and after the last call of the same
 * context.
 *
 * For more information see: 
 *   https://stackoverflow.com/questions/37786547/enforcing-statement-order-in-c
 */
template <class T>
__attribute__((always_inline)) inline void DoNotOptimize(const T &value) {
      asm volatile("" : "+m"(const_cast<T &>(value)));
}

std::vector<kvpair_t> fetch_data(std::string path)
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

    std::vector<kvpair_t> pairs;
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

void measure_empty_store(kp_kv_local *local, benchmark_thread_args* args, std::vector<std::chrono::duration<double>>& latencies)
{
    // TODO how to determine key size and value size for empty store?
}

void measure_populated_store(kp_kv_local *local, benchmark_thread_args* thread_args, std::vector<std::chrono::duration<double>>& latencies)
{
    const auto& pairs = *thread_args->pairs;
    const auto& opcode = thread_args->pargs->opcode;
    const auto& num_repeats = thread_args->pargs->num_repeats;

    latencies.reserve(num_repeats);

    std::random_device rand_dev;
    std::mt19937 rng(rand_dev());
    std::uniform_int_distribution<> dist(0, pairs.size() - 1); // use: dist(rng)

    if (opcode == "get") {
        int rc;
        for (size_t i=0; i<num_repeats; ++i) {
            const auto& [_key, _val] = pairs[dist(rng)];
            (void)_val;
            if (thread_args->pargs->verbose) {
                std::cout << "get(\n";
                std::cout << "\tkey = " << _key << '\n';
                std::cout << ")\n";
            }
            const char* key = _key.c_str();
            char* val;
            std::size_t siz;

            const auto start = std::chrono::high_resolution_clock::now();
            DoNotOptimize(local); 
            rc = kp_local_get(local, key, (void**)&val, &siz);
            (void)rc;
            DoNotOptimize(rc); 
            const auto end = std::chrono::high_resolution_clock::now();

            latencies.emplace_back(end - start);

            // FIXME produces a memory leak: 'val' is never free'ed before going out of scope (which heap is it on???)
        }
    }
    else if (opcode == "put") {
        int rc;
        for (size_t i=0; i<num_repeats; ++i) {
            const auto& [_key, _val] = pairs[dist(rng)];
            if (thread_args->pargs->verbose) {
                std::cout << "put(\n";
                std::cout << "\tkey = " << _key << '\n';
                std::cout << "\tval = " << _val << '\n';
                std::cout << ")\n";
            }
            const char* key = _key.c_str();
            const char* val = _val.c_str();
            const std::size_t siz = _val.size();

            const auto start = std::chrono::high_resolution_clock::now();
            DoNotOptimize(local); 
            rc = kp_local_put(local, key, val, siz);
            (void)rc;
            DoNotOptimize(rc);
            const auto end = std::chrono::high_resolution_clock::now();

            latencies.emplace_back(end - start);
        }
    }
    else if (opcode == "ins") {
        // TODO not really required (not used in throughput benchmark) 
    }
    else if (opcode == "del") {
        // TODO not really required (not used in throughput benchmark) 
    }

    // // Starting a transaction
    // // Note: Transactions are always started implicitly for each local store
    // // But we still have to start a NVM transaction here
    // PM_START_TX();

    // // Committing a transaction
    // rc = kp_local_commit(local, NULL);
    // PM_END_TX();
}

void measure(kp_kv_local *local, benchmark_thread_args* thread_args, std::vector<std::chrono::duration<double>>& latencies)
{
    if (thread_args->pairs->empty())
        measure_empty_store(local, thread_args, latencies);
    else
        measure_populated_store(local, thread_args, latencies);
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

void evaluate(benchmark_thread_args* thread_args, const std::vector<std::chrono::duration<double>>& latencies)
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
    std::cout << "min: " << convert_duration(*min, unit) << unit << std::endl;
    std::cout << "max: " << convert_duration(*max, unit) << unit << std::endl;

    auto size = latencies.size();
    if (size % 2) {
        std::cout << "med: " << convert_duration(latencies[size / 2], unit) << unit << std::endl;
    }
    else {
        auto lower = latencies[size / 2 - 1];
        auto upper = latencies[size / 2];
        std::cout << "med: " << convert_duration((lower + upper) / 2, unit) << unit << std::endl;
    }

    std::chrono::duration<double> sum;
    for (const auto v : latencies)
        sum += v;
    std::cout << "avg: " << convert_duration(sum / latencies.size(), unit) << unit << std::endl;
}

/* Packages up single threaded evaluations so we can use it from within
   a single worker setup */
void *little_latency_wrapper(void *arg)
{
    /* setup thread */
    unsigned long tid = pthread_self();
    benchmark_thread_args* thread_args = (benchmark_thread_args *) arg;
    kp_kv_master* master = (kp_kv_master*)thread_args->master;

    /* Create worker */
    kp_kv_local *local;
    int rc = kp_kv_local_create(master, &local, LOCAL_EXPECTED_MAX_NO_KEYS, false);
    if(rc != 0)
        kp_die("thread_%lu: kp_kv_local_create() returned error=%d\n", tid, rc);

    // ########################################################################
    // Populate store
    // ########################################################################

    auto& pairs = *thread_args->pairs;
    if (pairs.size()) {
        PM_START_TX();
        for (auto [key, value] : pairs) {
            rc = kp_local_put(local, key.c_str(), value.c_str(), value.size());
            if (rc)
            std::cout << "status code: " << rc << std::endl;
        }
        rc = kp_local_commit(local, NULL);
        PM_END_TX();
    }

    // ########################################################################
    // Measure operation latency
    // ########################################################################

    std::vector<std::chrono::duration<double>> latencies;
    measure(local, thread_args, latencies);

    // ########################################################################
    // Evaluate results
    // ########################################################################

    evaluate(thread_args, latencies);

    // ########################################################################
    // End benchmark
    // ########################################################################

    /* Destroy worker */
    kp_kv_local_destroy(&local);

    /* Cleanup and return */
    char *ret_string;
    ret_string = (char*)malloc(RET_STRING_LEN);
    if (!ret_string)
        kp_die("malloc(ret_string) failed\n");

    snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);

    return (void *) ret_string;
}

int run(program_args* pargs)
{
    std::vector<kvpair_t> pairs;
    if (!pargs->data_file.empty())
        pairs = fetch_data(pargs->data_file);

    // for (auto [key, val] : pairs) {
    //     std::cout << key.substr(0,3) << "..." << key.substr(key.size() - 3);
    //     std::cout << " -> " << val.substr(0,3) << "..." << val.substr(val.size() - 3) << '\n';
    // }

    const char* path = PERSISTENT_HEAP;
    void *pmp;
    if ((pmp = pmemalloc_init(path, (size_t)PMSIZE)) == NULL) {
        printf("Unable to allocate memory pool\n");
        exit(0);
    }

    pthread_spin_init(&tot_epoch_lock, PTHREAD_PROCESS_SHARED);

    int rc, i = 0;
    void *ret_thread;
    int cpu;
    pthread_attr_t attr;
    pthread_t thread;
    benchmark_thread_args thread_args;

    /* Setup CPU for everybody: don't spawn yet */
    cpu = CPU_OFFSET + (i % NUM_CPUS);
    CPU_ZERO(&(thread_args.cpu_set));
    CPU_SET(cpu, &(thread_args.cpu_set));

    /* Create Attributes */
    rc = pthread_attr_init(&attr);
    if(rc != 0)
        kp_die("pthread_attr_init() returned error=%d\n", rc);

    /* Set affinity */
    rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &(thread_args.cpu_set));
    if(rc != 0)
        kp_die("pthread_attr_setaffinity_np() returned error=%d\n", rc);

    /* Create master store */
    kp_kv_master* master;
    auto ret = kp_kv_master_create(
            &master,
            MODE_SNAPSHOT,
            MASTER_EXPECTED_MAX_NO_KEYS,  // expected max no keys
            true, // enable conflict detection
            true  // enable NVM usage
    );
    if (ret)
        std::cout << "error: master store could not be created!\n";

    thread_args.master = master;
    thread_args.pargs = pargs;
    thread_args.pairs = &pairs;

    /* Start thread */
    rc = pthread_create(&thread, &attr, &little_latency_wrapper, (void *)(&thread_args));
    if(rc != 0)
        kp_die("pthread_create() returned error=%d\n", rc);

    /* Get that worker back and cleanup! */
    rc = pthread_join(thread, &ret_thread);
    if(rc != 0)
        kp_die("pthread_join() returned error=%d\n", rc);

    if(ret_thread) {
        free(ret_thread);
        ret_thread = NULL;
    }

    kp_kv_master_destroy(master);

    rc = pthread_attr_destroy(&attr);
    if(rc != 0)
        kp_die("pthread_attr_destroy() returned error=%d\n", rc);

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
    std::cout << "\t\tDeletion (currently not supported).\n";
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

void parse_args(int argc, char* argv[], program_args& pargs)
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

        // case 0:
        //     break;

        default:
            usage();
            exit(0);
        }
    }
    argc -= optind;
    argv += optind;
}

void print_args(program_args& pargs)
{
    std::cout << std::boolalpha;
    std::cout << "opcode  : " << pargs.opcode << std::endl;
    std::cout << "repeats : " << pargs.num_repeats << std::endl;
    std::cout << "datafile: " << pargs.data_file << std::endl;
    std::cout << "unit    : " << pargs.unit << std::endl;
    std::cout << "verbose : " << pargs.verbose << std::endl;
}

int main(int argc, char* argv[])
{
//#ifdef FLUSH_IT
//    std::cout << "flushing: enabled" << std::endl;
//#else
//    std::cout << "flushing: disabled" << std::endl;
//#endif

    if (argc < 2) {
        usage();
        exit(0);
    }

    program_args pargs;
    parse_args(argc, argv, pargs);
    if (pargs.verbose)
        print_args(pargs);
    run(&pargs);

    return 0;
}
