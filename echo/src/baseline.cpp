#include <iostream>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <getopt.h>

#define PERSISTENT_HEAP "/dev/shm/efile"

extern "C" {
#include "kp_kv_local.h"    // local store
#include "kp_kv_master.h"   // master store
#include "kp_macros.h"      // kp_die()
#include "clibpm.h"         // PMSIZE, pmemalloc_init

// void *pmemalloc_init(const char *path, size_t size);
}

#define NUM_CPUS 2
#define CPU_OFFSET 0
#define RET_STRING_LEN 64
#define MASTER_EXPECTED_MAX_NO_KEYS 512
#define LOCAL_EXPECTED_MAX_NO_KEYS 512

struct program_args {
    std::string opcode;
    std::string data_file;
    std::size_t num_repeats = 1000;
    std::string unit = "ns";
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
            auto pos = line.find(';');
            if (pos != std::string::npos) {
                pairs.emplace_back(
                    line.substr(0, pos),
                    line.substr(pos + 1)
                );
            }
            else {
                throw std::invalid_argument("error: missing delimiter (;) in line");
            }
        }
    }
    return pairs;
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

    PM_START_TX();
    auto& pairs = *thread_args->pairs;
    for (auto [key, value] : pairs) {
        rc = kp_local_put(local, key.c_str(), value.c_str(), value.size());
        if (rc)
            std::cout << "status code: " << rc << std::endl;
    }
    rc = kp_local_commit(local, NULL);
    PM_END_TX();

    // ########################################################################
    // Perform operation
    // ########################################################################

    // Retrieve a key
    // char *key;
    // char **value;
    // size_t* size;
    // rc = kp_local_get(local, key, (void **)value, size);

    auto [key, value] = thread_args->pairs->at(0);
    char* result;
    size_t size;
    rc = kp_local_get(local, key.c_str(), (void **)&result, &size);
    if (rc)
        std::cout << "status code: " << rc << std::endl;
    
    // // Insert/update a key
    // const char *key;
    // const char *value;
    // const size_t size;
    // rc = kp_local_put(local, key, value, size);

    // // Delete a key
    // char *key;
    // rc = kp_local_delete_key(local, key);

    // // Starting a transaction
    // // Note: Transactions are always started implicitly for each local store
    // // But we still have to start a NVM transaction here
    // PM_START_TX();

    // // Committing a transaction
    // rc = kp_local_commit(local, NULL);
    // PM_END_TX();

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

    for (auto [key, val] : pairs) {
        std::cout << key.substr(0,3) << "..." << key.substr(key.size() - 3);
        std::cout << " -> " << val.substr(0,3) << "..." << val.substr(val.size() - 3) << '\n';
    }

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
    std::cout << "\tbaseline is used to determine the average latency of of single database\n";
    std::cout << "\toperations. opcode denotes the operation to be measured and is a required\n";
    std::cout << "\targument. options may be used to configure the measurement but are not required.\n";
    std::cout << "\nOPTIONS\n";
    std::cout << "\t-p, --populate FILE\n";
    std::cout << "\t\tPopulates the database with data from the specified file.\n";
    std::cout << "\t-r, --repeats NUM\n";
    std::cout << "\t\tSets the number of repetitions for the given operation.\n";
    std::cout << "\t-u, --unit UNIT\n";
    std::cout << "\t\tSets the time unit of used when printing results. Can be one of {s | ms | us | ns}.\n";
    std::cout << "\t-v, --verbose\n";
    std::cout << "\t\tEnables verbose mode. With this, all intermediate results will be shown.\n";
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
    if (argc < 2) {
        usage();
        exit(0);
    }

    program_args pargs;
    parse_args(argc, argv, pargs);
    print_args(pargs);
    run(&pargs);

    return 0;
}
