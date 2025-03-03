#include <atomic>
#include <thread>
#include <vector>
#include <cstdint>
#include <iostream>
#include <pthread.h>
#include <random>

constexpr size_t CACHE_LINE_SIZE = 64;

// 16B tuple structure (8B key + 8B payload)
struct alignas(CACHE_LINE_SIZE) Tuple
{
    uint64_t key;
    uint64_t payload;
};

// Per-thread, per-partition buffer
struct alignas(CACHE_LINE_SIZE) PartitionBuffer
{
    uint32_t write_idx;
    uint32_t capacity;
    Tuple *data;
};

struct ThreadBuffers
{
    PartitionBuffer *partitions;
    uint32_t num_partitions;
};

// Generate synthetic data with uniform keys
std::vector<Tuple> generate_data(size_t num_tuples)
{
    std::vector<Tuple> data(num_tuples);
    std::mt19937_64 rng(std::random_device{}());

#pragma omp parallel for
    for (size_t i = 0; i < num_tuples; ++i)
    {
        data[i].key = rng(); // Uniform random keys
        data[i].payload = i; // Sequential payload
    }
    return data;
}

uint32_t partition_hash(uint64_t key, uint32_t b)
{
    return key & ((1 << b) - 1);
}

void init_buffers(ThreadBuffers &tb, uint32_t b, uint32_t expected_tuples)
{
    tb.num_partitions = 1 << b;
    tb.partitions = new PartitionBuffer[tb.num_partitions];

    for (uint32_t p = 0; p < tb.num_partitions; ++p)
    {
        uint32_t capacity = expected_tuples * 1.5;
        tb.partitions[p].capacity = capacity;
        tb.partitions[p].write_idx = 0;
        tb.partitions[p].data = new Tuple[capacity];

        // Pre-touch memory pages
        for (uint32_t i = 0; i < capacity; i += 4096 / sizeof(Tuple))
        {
            tb.partitions[p].data[i].key = 0;
        }
    }
}

void process_chunk(ThreadBuffers &tb, Tuple *input, size_t count, uint32_t b)
{
    for (size_t i = 0; i < count; ++i)
    {
        uint32_t p = partition_hash(input[i].key, b);
        PartitionBuffer &buf = tb.partitions[p];
        buf.data[buf.write_idx++] = input[i];
    }
}

int main()
{
    // Configuration
    const uint32_t b = 10; // 1024 partitions
    const uint32_t num_threads = 16;
    const uint64_t total_tuples = 1 << 24; // 16.7M tuples

    // Generate synthetic data
    std::vector<Tuple> data = generate_data(total_tuples);

    // Calculate chunk size per thread
    const size_t tuples_per_thread = total_tuples / num_threads;

    // Thread resources
    std::vector<std::thread> workers;
    std::vector<ThreadBuffers> all_buffers(num_threads);

    auto start = std::chrono::high_resolution_clock::now();

    // Launch threads
    for (uint32_t t = 0; t < num_threads; ++t)
    {
        workers.emplace_back([&, t]
                             {
            // Set CPU affinity (spread across first 16 cores)
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(t % 16, &cpuset);
            pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

            // Initialize buffers
            init_buffers(all_buffers[t], b, tuples_per_thread/(1 << b));

            // Process chunk
            Tuple* chunk_start = data.data() + t * tuples_per_thread;
            process_chunk(all_buffers[t], chunk_start, tuples_per_thread, b); });
    }

    // Wait for completion
    for (auto &worker : workers)
        worker.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Independent Output completed in " << duration.count() << " ms\n";

    // Cleanup
    for (auto &tb : all_buffers)
    {
        for (uint32_t p = 0; p < tb.num_partitions; ++p)
        {
            delete[] tb.partitions[p].data;
        }
        delete[] tb.partitions;
    }

    return 0;
}