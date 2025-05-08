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
constexpr uint32_t PAGE_SIZE = 4096; // 4KB
volatile bool start_flag = false;

struct Tuple {
    uint64_t key;
    uint64_t payload;

    Tuple(uint64_t k = 0, uint64_t p = 0) : key(k), payload(p) {}
};

struct alignas(CACHE_LINE_SIZE) PartitionBuffer {
    atomic<uint32_t> write_index;
    uint32_t capacity;
    Tuple* buffer;
};

struct ThreadData {
    uint32_t thread_id;
    uint32_t num_partitions;
    uint32_t num_tuples_to_handle;
    Tuple* tuples;
    PartitionBuffer* output_buffers;
};

inline int hash_function(uint64_t key, int num_partitions) {
    return key & (num_partitions - 1);
}

Tuple* allocate_memory(uint32_t num_tuples) {
    uint32_t size = num_tuples * sizeof(Tuple);
    void* ptr = aligned_alloc(PAGE_SIZE, size);
    if (!ptr) {
        cerr << "Memory allocation failed.\n";
        exit(EXIT_FAILURE);
    }
    return static_cast<Tuple*>(ptr);
}

void initialize_memory(Tuple* tuples, uint32_t num_tuples) {
    uint32_t step = PAGE_SIZE / sizeof(Tuple);
    for (uint32_t i = 0; i < num_tuples; i += step) {
        tuples[i].key = 0;
    }
}

void* concurrent_output(void* args) {
    ThreadData* thread = static_cast<ThreadData*>(args);

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

    while (!start_flag) _mm_pause();
    _mm_mfence();

    Tuple* tuples = thread->tuples;
    uint32_t num_tuples_to_handle = thread->num_tuples_to_handle;
    uint32_t num_partitions = thread->num_partitions;
    PartitionBuffer* output_buffers = thread->output_buffers;

    for (uint32_t i = 0; i < num_tuples_to_handle; i++) 
    {
        uint64_t key = tuples[i].key;
        uint32_t partition_index = hash_function(key, num_partitions);

        PartitionBuffer& buffer = output_buffers[partition_index];
        // atomic increment to ensure no 2 threads write to the same index in the shared PartitionBuffer
        uint32_t idx = buffer.write_index.fetch_add(1, memory_order_relaxed);
        if (idx >= buffer.capacity) {
            cerr << "Buffer overflow at partition " << partition_index << "\n";
            exit(EXIT_FAILURE);
        }
        buffer.buffer[idx] = tuples[i];
    }

    return nullptr;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " <num_threads> <hash_bits>\n";
        return -1;
    }

    uint32_t num_threads = atoi(argv[1]);
    uint32_t hash_bits = atoi(argv[2]);
    uint32_t num_partitions = 1 << hash_bits;
    // uint32_t num_tuples_to_handle = NUM_TUPLES / num_threads;
    uint32_t buffer_size = NUM_TUPLES / num_partitions;
    buffer_size *= (num_partitions >= (1 << 17)) ? 7 : (num_partitions >= (1 << 14)) ? 4 : 2;

    Tuple* tuples = allocate_memory(NUM_TUPLES);
    initialize_memory(tuples, NUM_TUPLES);

    // Generate the tuples
    mt19937_64 gen(time(0));
    uniform_int_distribution<uint64_t> dis(0, (1 << hash_bits) - 1);
    for (uint32_t i = 0; i < NUM_TUPLES; i++) {
        tuples[i] = Tuple(dis(gen), 0);
    }

    PartitionBuffer* shared_buffers = new PartitionBuffer[num_partitions];
    for (uint32_t i = 0; i < num_partitions; i++) {
        shared_buffers[i].write_index = 0;
        shared_buffers[i].capacity = buffer_size;
        shared_buffers[i].buffer = new Tuple[buffer_size];
        for (uint32_t j = 0; j < buffer_size; j += PAGE_SIZE / sizeof(Tuple)) {
            shared_buffers[i].buffer[j].key = 0;
        }
    }

    // Thread management
    pthread_t* threads = new pthread_t[num_threads];
    ThreadData* thread_data = new ThreadData[num_threads];

    uint32_t base = NUM_TUPLES / num_threads;
    uint32_t remainder = NUM_TUPLES % num_threads;
    uint32_t offset = 0;

    for (uint32_t i = 0; i < num_threads; i++) {
        uint32_t count = base + (i < remainder ? 1 : 0);

        thread_data[i].thread_id = i;
        thread_data[i].num_tuples_to_handle = count;
        thread_data[i].num_partitions = num_partitions;
        thread_data[i].tuples = tuples + offset;
        thread_data[i].output_buffers = shared_buffers;
        pthread_create(&threads[i], NULL, concurrent_output, &thread_data[i]);
        offset += count;
    }

    _mm_mfence();
    start_flag = true; // Release the barrier
    _mm_mfence();

    auto start_time = high_resolution_clock::now();

    // Join threads
    for (uint32_t i = 0; i < num_threads; i++) 
    {
        pthread_join(threads[i], NULL);
    }

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();

    cout << "Partitioning completed in " << duration << " ms.\n";
    cout << "Throughput: " << (NUM_TUPLES * 1000.0 / duration) / 1e6 << " million tuples per second.\n";

    // Cleanup
    for (uint32_t i = 0; i < num_partitions; i++) 
    {
        delete[] shared_buffers[i].buffer;
    }

    delete[] shared_buffers;

    free(tuples);

    delete[] threads;
    delete[] thread_data;

    return 0;
}
