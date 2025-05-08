#include <iostream>
#include <thread>
#include <mutex>
#include <atomic>
#include <random>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <pthread.h>
#include <cmath>
#include <sys/mman.h>
#include <immintrin.h>


using namespace std;
using namespace chrono;

constexpr uint32_t NUM_TUPLES = 1 << 24; // 2^24 tuples (16 MB)
constexpr uint32_t CACHE_LINE_SIZE = 64; // Bytes - verified it on this system it is indeed 64 Bytes by default
constexpr uint32_t PAGE_SIZE = 4096;  // 4KB
volatile bool start_flag = false;

struct Tuple
{
    uint64_t key;
    uint64_t payload;

    Tuple(uint64_t k = 0, uint64_t p = 0) : key(k), payload(p) {}
};

/* Structure to hold partitioning metadata and buffers
   Align each struct for a separate cache line when allocated the first time to prevent false sharing
*/
struct alignas(CACHE_LINE_SIZE) PartitionBuffer
{
    uint32_t write_index; 
    Tuple *buffer;                                    
};

struct ThreadData
{
    uint32_t thread_id;
    uint32_t num_partitions;
    uint32_t num_tuples_to_handle;
    uint32_t buffer_size;
    Tuple *tuples; 
    PartitionBuffer *output_buffers;
};

/* `inline` tells the compiler to replace the function call with it's actual code (reduces function call overhead)
    The number formed with the last b bits of the key defined the partition index the tuple would be assigned to
*/
inline int hash_function(uint64_t key, int num_partitions) // no real parameter "copy" since it is inline
{                                     
    return key & (num_partitions - 1); // Faster than modulo - key % (2^b) (extracts the lowest b bits of the key)
}


// Allign memory to PAGE_SIZE
/*
The OS reserves a virtual memory region of `size` bytes.
No physical memory is allocated yet (just a virtual address range).
The OS will allocate physical memory only when you access (write to) the memory (TOUCH)
Now tuples array is page aligned at PAGE_SIZE (4KB) boundary
*/
Tuple* allocate_memory(uint32_t num_tuples) {
    uint32_t size = num_tuples * sizeof(Tuple);
    void* ptr = aligned_alloc(PAGE_SIZE, size); 
    if (!ptr) {
        cerr << "Memory allocation failed.\n";
        exit(EXIT_FAILURE);
    }
    return static_cast<Tuple*>(ptr);
}



/* Touch all preallocated pages before running to avoid page faults during measurements
   Writing to tuples[i] forces the OS to allocate a physical page in RAM
*/
void initialize_memory(Tuple* tuples, uint32_t num_tuples) {
    uint32_t step = PAGE_SIZE / sizeof(Tuple);
    for (uint32_t i = 0; i < num_tuples; i += step) {
        tuples[i].key = 0; 
    }
}

// Function ran by every thread for the `Independent Output` Partitioning
void *independent_output(void *args)
{
    ThreadData *thread = static_cast<ThreadData *>(args);

    // Set CPU Affinity
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    uint32_t core_id;
    if (thread->thread_id < 8) 
    {
        // NUMA 0, physical cores: PU#0,2,4,6,8,10,12,14
        core_id = thread->thread_id * 2;
    } 
    else if (thread->thread_id < 16) 
    {
        // NUMA 1, physical cores: PU#16,18,20,22,24,26,28,30
        core_id = (thread->thread_id - 8) * 2 + 16;
    } 
    else if (thread->thread_id < 24) 
    {
        // NUMA 0, hyperthreads: PU#1,3,5,7,9,11,13,15
        core_id = (thread->thread_id - 16) * 2 + 1;
    } 
    else 
    {
        // NUMA 1, hyperthreads: PU#17,19,21,23,25,27,29,31
        core_id = (thread->thread_id - 24) * 2 + 17;
    }

    CPU_SET(core_id, &cpuset); // map current thread to core_id

    pthread_t current_thread = pthread_self();
    int rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }

    // Wait until the main thread sets the flag
    while (!start_flag) {
        _mm_pause(); // Hint to CPU to reduce power usage while spinning
    }
    _mm_mfence(); // Ensure memory synchronization before proceeding


    Tuple *tuples = thread->tuples;
    uint32_t num_tuples_to_handle = thread->num_tuples_to_handle;
    uint32_t num_partitions = thread->num_partitions;

    // Buffers for each partition
    thread->output_buffers = new PartitionBuffer[num_partitions];


    // Calculate buffer size
    uint32_t buffer_size = thread->buffer_size;

    // Initialize buffers
    uint32_t i;
    for (i = 0; i < num_partitions; i++)
    {
        thread->output_buffers[i].write_index = 0;
        thread->output_buffers[i].buffer = new Tuple[buffer_size];
    }

    // Partition tuples
    for (i = 0; i < num_tuples_to_handle; i++)
    {
        uint64_t key = tuples[i].key;                           
        uint32_t partition_index = hash_function(key, num_partitions);

        // Write tuple to buffer
        uint32_t idx = thread->output_buffers[partition_index].write_index++;
        if (idx >= buffer_size)
        {
            cerr << "Buffer overflow detected!";
            exit(EXIT_FAILURE);
        }
        thread->output_buffers[partition_index].buffer[idx] = tuples[i];
    }

    return nullptr;
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        cerr << "Usage: " << argv[0] << " <num_threads> <hash_bits>\n";
        return -1;
    }

    uint32_t num_threads = atoi(argv[1]);
    uint32_t hash_bits = atoi(argv[2]);
    uint32_t num_partitions = 1 << hash_bits;  // 2^b                
    uint32_t num_tuples_to_handle = NUM_TUPLES / num_threads;     // num_threads will always be a power of 2 so it is evenly divisible
    uint32_t buffer_size = num_tuples_to_handle / num_partitions; 
    buffer_size *= (num_partitions >= (1 << 17)) ? 7 : (num_partitions >= (1 << 14)) ? 4 : 2; // dynamic allocation

    // Allocate memory with PAGE_SIZE alignment
    Tuple* tuples = allocate_memory(NUM_TUPLES);
    initialize_memory(tuples, NUM_TUPLES);

    // Tuple *tuples = new Tuple[NUM_TUPLES];
    // initialize_memory(tuples,NUM_TUPLES);

    // Generate random tuples
    mt19937_64 gen(time(0));
    uniform_int_distribution<uint64_t> dis(0, (1 << hash_bits) - 1); // generates random numbers in the interval uniformly

    // Initialize each tuple with a random key and a 0 payload
    uint32_t i;
    for (i = 0; i < NUM_TUPLES; i++)
    {
        tuples[i] = Tuple(dis(gen), 0);
    }

    // Thread management
    pthread_t *threads = new pthread_t[num_threads];
    ThreadData *thread_data = new ThreadData[num_threads];

    uint32_t base = NUM_TUPLES / num_threads;
    uint32_t remainder = NUM_TUPLES % num_threads;
    uint32_t offset = 0;

    

    // Global variables may not be cache friendly - thread-local data = faster access
    for (i = 0; i < num_threads; i++)
    {
        uint32_t count = base + (i < remainder ? 1 : 0); // each thread gets + 1 tuple to handle, only the last one/s get none

        thread_data[i].thread_id = i;
        thread_data[i].num_tuples_to_handle = count;
        thread_data[i].num_partitions = num_partitions;
        thread_data[i].buffer_size = buffer_size;
        thread_data[i].tuples = tuples + offset;  // so the current thread starts from it's assigned region
        pthread_create(&threads[i], NULL, independent_output, &thread_data[i]);
        offset += count;
    }

    // Memory fence to ensure thread visibility
    _mm_mfence();
    start_flag = true; // Release the barrier
    _mm_mfence();      // Ensure all threads see the updated flag

    auto start_time = high_resolution_clock::now();

    // Join threads
    for (i = 0; i < num_threads; i++)
    {
        pthread_join(threads[i], NULL);
    }

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();

    cout << "Partitioning completed in " << duration << " ms.\n";
    cout << "Throughput: " << (NUM_TUPLES * 1000.0 / duration) / 1e6 << " million tuples per second.\n"; // * 1000 to convert ms to s

    // Cleanup
    for (uint32_t i = 0; i < num_threads; i++) 
    {
            for (uint32_t j = 0; j < thread_data[i].num_partitions; j++) 
                delete[] thread_data[i].output_buffers[j].buffer;  // delete buffer for each partition

            delete[] thread_data[i].output_buffers;  // delete the pointer array
    }

    free(tuples);

    delete[] threads;
    delete[] thread_data;

    return 0;
}
