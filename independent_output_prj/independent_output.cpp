#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <random>
#include <cstdlib>
#include <cstring> // For memset
#include <chrono>
#include <pthread.h>
#include <cmath>   // For pow()

using namespace std;
using namespace chrono;

// Constants
constexpr size_t NUM_TUPLES = 1 << 24;  // 2^24 tuples (16 MB)
constexpr size_t CACHE_LINE_SIZE = 64;     // Bytes
constexpr size_t PAGE_SIZE = 256 * 1024 * 1024;  // 256 MB in Bytes


// Structure for tuples
struct Tuple {
    uint64_t key;
    uint64_t payload;

    Tuple(uint64_t k = 0, uint64_t p = 0) : key(k), payload(p) {}
};

// Structure to hold partitioning metadata and buffers
struct PartitionBuffer {
    alignas(CACHE_LINE_SIZE) int write_index; // Ensure no false sharing bc `write_index` starts at the beginning of each cahce line
    Tuple* buffer; // pointer to the array of tuples that stores the partitioned data
};

// Thread Data Structure
struct ThreadData {
    int thread_id;
    int num_partitions;
    int num_tuples_to_handle;
    int buffer_size;
    Tuple* tuples;
    PartitionBuffer** output_buffers;
};

constexpr uint64_t HASH_CONSTANT = 11400714819323198485ULL; // Fraction of the golden ratio

// Hash function for partitioning (multiplicative hash function)
// `inline` tells the compiler to replace the function call with it's actual code, instead of making a function call (reduces function call overhead)
inline int hash_function(uint64_t key, int num_partitions) { // no real parameter "copy" since it is inline
    return key & (num_partitions - 1); // Faster than modulo
    //int hash_bits = log2(num_partitions); // Extract b from num_partitions (2^b)
    //return (key * HASH_CONSTANT) >> (64 - hash_bits); // Extract b bits
}

// Allocate memory with 256MB page size
/*
The OS reserves a virtual memory region of `size` bytes.
No physical memory is allocated yet (just a virtual address range).
The OS will allocate physical memory only when you access (write to) the memory !!!!
*/
Tuple* allocate_memory(size_t num_tuples) {
    size_t size = num_tuples * sizeof(Tuple);
    void* ptr = aligned_alloc(PAGE_SIZE, size); // Ensure memory is aligned to 256MB
    if (!ptr) {
        cerr << "Memory allocation failed.\n";
        exit(EXIT_FAILURE);
    }
    return static_cast<Tuple*>(ptr);
}

// Function to preallocate memory and touch all pages before running to avoid page faults during measurements
// Allocation of memory in 256MB Chunks to make sure each thread's buffer respects the page allignment
// Writing to tuples[i] forces the OS to allocate a physical page
// The OS now maps the virtual memory to physical memory (RAM)
// Future accesses won’t trigger page faults, because the memory is already mapped
void initialize_memory(Tuple* tuples, size_t num_tuples) {
    for (size_t i = 0; i < num_tuples; i+=64 / sizeof(Tuple)) {
        tuples[i] = Tuple();  // Touch every cache line
    }
}

// Function ran by every thread for the independent output partitioning
void* independent_output(void* args) {
    ThreadData* thread = (ThreadData*)args; // convert args into a pointer to ThreadData
    Tuple* tuples = thread->tuples;

    size_t num_tuples_to_handle = thread->num_tuples_to_handle;
    int num_partitions = thread->num_partitions;

    // Buffers for each partition
    PartitionBuffer* thread_buffers = new PartitionBuffer[num_partitions];
    // std::vector<PartitionBuffer> thread_buffers(num_partitions);


    // Calculate buffer size with 50% extra space

    int buffer_size = thread->buffer_size * 1.5;
    // Initialize buffers
    for (int i = 0; i < num_partitions; i++) {
        thread_buffers[i].write_index = 0;
        thread_buffers[i].buffer = new Tuple[buffer_size];
    }

    // Partition tuples
    for (size_t i = 0; i < num_tuples_to_handle; i++) {
        uint64_t key = tuples[thread->thread_id * num_tuples_to_handle + i].key; // computes global index in `tuples`
        int partition_index = hash_function(key, num_partitions); // the hash function decides in which partition will the Tuple be
        
        // Write tuple to buffer
        int idx = thread_buffers[partition_index].write_index++;
        if (idx >= buffer_size) {
            cerr << "Buffer overflow detected! Partition: " << partition_index 
                 << " Thread: " << thread->thread_id << " idx=" << idx << " buffer_size=" << buffer_size << endl;
            exit(EXIT_FAILURE);
        }
        
        thread_buffers[partition_index].buffer[idx] = tuples[thread->thread_id * num_tuples_to_handle + i];
    }

    // Store buffers in thread data
    thread->output_buffers = new PartitionBuffer*[num_partitions]; // a full row in the Partition Matrix
    for (int i = 0; i < num_partitions; i++) {
        thread->output_buffers[i] = &thread_buffers[i];
    }

    return nullptr;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " <num_threads> <hash_bits>\n";
        return -1;
    }

    int num_threads = atoi(argv[1]);
    int hash_bits = atoi(argv[2]);
    int num_partitions = 1 << hash_bits;  // Compute 2^b dynamically
    int num_tuples_to_handle = NUM_TUPLES / num_threads;
    int buffer_size = num_tuples_to_handle / num_partitions;

    std::cout << "Running Independent Output Partitioning with " << num_threads 
         << " threads and " << num_partitions << " partitions (2^" << hash_bits << ").\n";

    // Allocate memory with 256MB alignment
    Tuple* tuples = allocate_memory(NUM_TUPLES);
    initialize_memory(tuples, NUM_TUPLES);

    // Generate random tuples
    random_device rd; // ensure unique seed for each run (queries the OS every time - slows down execution)
    mt19937_64 gen(rd()); // seeds the generator with a unique value
    // mt19937_64 gen(42); // Fixed seed (faster; if performance is critical and true randomness isn't needed)
    uniform_int_distribution<uint64_t> dis(1, 1000000); // generates random value between 1 and 1 mil (ensures nr are uniformly distrbuted)

    for (size_t i = 0; i < NUM_TUPLES; i++) {
        tuples[i] = Tuple(dis(gen), 0);
    }

    // Thread management
    pthread_t* threads = new pthread_t[num_threads];
    ThreadData** thread_data = new ThreadData*[num_threads];

    auto start_time = high_resolution_clock::now();

    

    // Create threads
    // Global variables may not be cache friendly - thread-local data = faster access
    for (int i = 0; i < num_threads; i++) {
        thread_data[i] = new ThreadData();
        thread_data[i]->thread_id = i;
        thread_data[i]->num_tuples_to_handle = num_tuples_to_handle;
        thread_data[i]->num_partitions = num_partitions;
        thread_data[i]->buffer_size = buffer_size;
        thread_data[i]->tuples = tuples;
        pthread_create(&threads[i], NULL, independent_output, thread_data[i]);
    }

    // Join threads
    for (int i = 0; i < num_threads; i++) {
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
