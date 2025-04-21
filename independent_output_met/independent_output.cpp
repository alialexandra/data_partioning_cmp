#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <random>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <pthread.h>
#include <cmath>

using namespace std;
using namespace chrono;

// Constants
constexpr uint32_t NUM_TUPLES = 1 << 24; // 2^24 tuples (16 MB)
constexpr uint32_t CACHE_LINE_SIZE = 64; // Bytes - verified it on this system it is indeed 64 Bytes by default
// constexpr uint32_t PAGE_SIZE = 256 * 1024 * 1024;  // 256 MB in Bytes

// Structure for tuples
struct Tuple
{
    uint64_t key;
    uint64_t payload;

    Tuple(uint64_t k = 0, uint64_t p = 0) : key(k), payload(p) {}
};

// Structure to hold partitioning metadata and buffers
struct PartitionBuffer
{
    /* alignas(CACHE_LINE_SIZE) */ uint32_t write_index; // Prevents false sharing with anything else in the struct bc `write_index` starts at the beginning of each cache line
    Tuple *buffer;                                       // Points to the actual virtual memory holding the tuples assigned to that partition
};

// Thread Data Structure
struct ThreadData
{
    uint32_t thread_id;
    uint32_t num_partitions;
    uint32_t num_tuples_to_handle;
    uint32_t buffer_size;
    Tuple *tuples; // pointer to the global array of input tuples
    PartitionBuffer **output_buffers;
};

// `inline` tells the compiler to replace the function call with it's actual code (reduces function call overhead)
// The number formed with the last b bits of the key defined the partition index the tuple would be assigned to
inline int hash_function(uint64_t key, int num_partitions)
{                                      // no real parameter "copy" since it is inline
    return key & (num_partitions - 1); // Faster than modulo - key % (2^b) (extracts the lowest b bits of the key)
}

// Allocate memory with 256MB page size
/*
The OS reserves a virtual memory region of `size` bytes.
No physical memory is allocated yet (just a virtual address range).
The OS will allocate physical memory only when you access (write to) the memory !!!!
*/
// Tuple* allocate_memory(size_t num_tuples) {
//     size_t size = num_tuples * sizeof(Tuple);
//     void* ptr = aligned_alloc(PAGE_SIZE, size); // Ensure memory is aligned to 256MB
//     if (!ptr) {
//         cerr << "Memory allocation failed.\n";
//         exit(EXIT_FAILURE);
//     }
//     return static_cast<Tuple*>(ptr);
// }

// Function to preallocate memory and touch all pages before running to avoid page faults during measurements
// Allocation of memory in 256MB Chunks to make sure each thread's buffer respects the page allignment
// Writing to tuples[i] forces the OS to allocate a physical page
// The OS now maps the virtual memory to physical memory (RAM)
// Future accesses won’t trigger page faults, because the memory is already mapped
// void initialize_memory(Tuple* tuples, size_t num_tuples) {
//     for (size_t i = 0; i < num_tuples; i+=64 / sizeof(Tuple)) {
//         tuples[i] = Tuple();  // Touch every cache line
//     }
// }

// Function ran by every thread for the `Independent Output` Partitioning
void *independent_output(void *args)
{
    ThreadData *thread = static_cast<ThreadData *>(args);

    Tuple *tuples = thread->tuples;
    uint32_t num_tuples_to_handle = thread->num_tuples_to_handle;
    uint32_t num_partitions = thread->num_partitions;

    // Buffers for each partition
    PartitionBuffer *thread_buffers = new PartitionBuffer[num_partitions]; // !!! not buffer_size

    // Calculate buffer size with 50% extra space
    uint32_t buffer_size = thread->buffer_size * 10;
    // Initialize buffers
    uint32_t i;
    for (i = 0; i < num_partitions; i++)
    {
        thread_buffers[i].write_index = 0;
        thread_buffers[i].buffer = new Tuple[buffer_size];
    }

    // Partition tuples
    for (i = 0; i < num_tuples_to_handle; i++)
    {
        // uint64_t key = tuples[thread->thread_id * num_tuples_to_handle + i].key; // computes global index in `tuples`
        uint64_t key = tuples[i].key;                             // each thread has access to its assigned region
        int partition_index = hash_function(key, num_partitions); // the hash function decides in which partition will the Tuple be

        // Write tuple to buffer
        uint32_t idx = thread_buffers[partition_index].write_index++;
        if (idx >= buffer_size)
        {
            cerr << "Buffer overflow detected! Partition: " << partition_index
                 << " Thread: " << thread->thread_id
                 << " current partition has no of tuples already assigned: " << idx
                 << " and buffer_size = " << buffer_size << endl;
            exit(EXIT_FAILURE);
        }

        // thread_buffers[partition_index].buffer[idx] = tuples[thread->thread_id * num_tuples_to_handle + i];
        thread_buffers[partition_index].buffer[idx] = tuples[i];
    }

    // Store buffers in thread data
    thread->output_buffers = new PartitionBuffer *[num_partitions]; // a full row in the Partition Matrix
    for (i = 0; i < num_partitions; i++)
    {
        thread->output_buffers[i] = &thread_buffers[i];
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
    uint32_t num_partitions = 1 << hash_bits;                     // Compute 2^b dynamically
    uint32_t num_tuples_to_handle = NUM_TUPLES / num_threads;     // note that num_threads will always be a power of 2 so it is evenly divisible
    uint32_t buffer_size = num_tuples_to_handle / num_partitions; // !!! Hashing functions are fast but not perfectly uniform so what if we need more space than we allocated for an output buffer??

    std::cout << "Running Independent Output Partitioning with " << num_threads
              << " threads and " << num_partitions << " partitions (2^" << hash_bits << ").\n";

    // Allocate memory with 256MB alignment
    // Tuple* tuples = allocate_memory(NUM_TUPLES);
    // initialize_memory(tuples, NUM_TUPLES);
    Tuple *tuples = new Tuple[NUM_TUPLES];

    // Generate random tuples
    random_device rd;     // ensure unique seed for each run (!!! queries the OS every time - slows down execution)
    mt19937_64 gen(rd()); // seeds the generator with a unique value
    // mt19937_64 gen(42); // Fixed seed (!!! faster; if performance is critical and true randomness isn't needed)
    uniform_int_distribution<uint64_t> dis(1, 1000000); // generates random value between 1 and 1 mil (ensures nr are uniformly distrbuted)

    // Initialize each tuple with a random key and a 0 payload
    uint32_t i;
    for (i = 0; i < NUM_TUPLES; i++)
    {
        tuples[i] = Tuple(dis(gen), 0);
    }

    // Thread management
    std::vector<pthread_t> threads(num_threads);
    std::vector<ThreadData> thread_data(num_threads);

    // pthread_t *threads = new pthread_t[num_threads];
    // ThreadData *thread_data = new ThreadData[num_threads];

    auto start_time = high_resolution_clock::now();

    uint32_t base = NUM_TUPLES / num_threads;
    uint32_t remainder = NUM_TUPLES % num_threads;
    uint32_t offset = 0;

    // Global variables may not be cache friendly - thread-local data = faster access
    for (i = 0; i < num_threads; i++)
    {
        uint32_t count = base + (i < remainder ? 1 : 0); // each thread gets + 1 tuple to handle only the last one/s get none

        thread_data[i].thread_id = i;
        thread_data[i].num_tuples_to_handle = count;
        thread_data[i].num_partitions = num_partitions;
        thread_data[i].buffer_size = buffer_size; // !! NO UPDATE FOR NOW cuz we will * 1.5 it anyway
        thread_data[i].tuples = tuples + offset;  // so the current thread starts from it's assigned region
        pthread_create(&threads[i], NULL, independent_output, &thread_data[i]);
        offset += count;
    }

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
    // delete[] tuples;
    // for (int i = 0; i < num_threads; i++) {
    //     delete thread_data[i];
    // }

    // delete[] threads;
    // delete[] thread_data;

    return 0;
}
