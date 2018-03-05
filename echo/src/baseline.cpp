#include <iostream>

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

typedef struct random_ints_ {
  int *array;
  unsigned int count;
  unsigned int idx;
} random_ints;

/* Structure to push arguments to the worker */
typedef struct benchmark_args_struct {
    cpu_set_t cpu_set;
    void *master;
    int num_threads;
    int starting_ops;
    pthread_cond_t *bench_cond;
    pthread_mutex_t *bench_mutex;
    bool slam_local;
    bool split_keys;
    int my_id;
    bool do_measure;
    random_ints *ints;
} benchmark_thread_args;

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
    int rc = kp_kv_local_create(master, &local, 256, false);
    if(rc != 0)
        kp_die("thread_%lu: kp_kv_local_create() returned error=%d\n", tid, rc);

    // Retrieve a key
    // char *key;
    // char **value;
    // size_t* size;
    // rc = kp_local_get(local, key, (void **)value, size);

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

int main()
{
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
