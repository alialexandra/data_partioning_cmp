#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <cmath>
#include <random>
#include <stdexcept>

// Constants from paper's experimental setup
constexpr size_t TUPLES_PER_EXPERIMENT = 1 << 24; // 16.7M tuples (2^24)
constexpr size_t TUPLE_SIZE = 16;                 // 16B per tuple (8B key + 8B payload)
constexpr size_t PAGE_SIZE = 256 * 1024 * 1024;   // 256MB pages

struct Tuple
{
    uint64_t key;
    uint64_t payload;
};

struct PartitionBuffer
{
    alignas(64) std::atomic<uint32_t> write_idx;
    uint32_t capacity;
    Tuple *data;
};

struct SharedBuffers
{
    PartitionBuffer *partitions;
    uint32_t num_partitions;
};

// Generate uniformly distributed random keys
void generate_data(Tuple *data, size_t count)
{
    std::mt19937_64 rng(42); // Seed for reproducibility
    std::uniform_int_distribution<uint64_t> dist;

    for (size_t i = 0; i < count; ++i)
    {
        data[i].key = dist(rng);
        data[i].payload = i;
    }
}

// Multiplicative hash function (from paper)
uint32_t partition_hash(uint64_t key, uint32_t b)
{
    const uint64_t multiplier = 0x5bd1e995;
    return ((key * multiplier) >> (64 - b)) & ((1 << b) - 1);
}

void init_buffers(SharedBuffers &buffers, uint32_t b)
{
    buffers.num_partitions = 1 << b;
    buffers.partitions = new PartitionBuffer[buffers.num_partitions];

    const uint32_t expected_per_partition = TUPLES_PER_EXPERIMENT / buffers.num_partitions;

    // Dynamic over-provisioning
    double overprovision_factor = (b <= 16) ? 1.5 : 2.5; // More space for b > 16
    uint32_t min_capacity = (b <= 16) ? 64 : 256;        // Larger minimum for high b

    for (uint32_t p = 0; p < buffers.num_partitions; ++p)
    {
        uint32_t capacity = static_cast<uint32_t>(expected_per_partition * overprovision_factor);
        capacity = std::max(capacity, min_capacity);

        // Handle edge case when partitions > tuples
        if (capacity == 0)
            capacity = 1024;

        buffers.partitions[p].capacity = capacity;
        buffers.partitions[p].write_idx.store(0);
        buffers.partitions[p].data = new Tuple[capacity];

        // Pre-touch memory pages (256MB alignment)
        for (uint32_t i = 0; i < capacity; i += PAGE_SIZE / sizeof(Tuple))
        {
            buffers.partitions[p].data[i].key = 0;
        }
    }
}

void run_experiment(uint32_t num_threads, uint32_t b)
{
    // Allocate input data
    Tuple *input_data = new Tuple[TUPLES_PER_EXPERIMENT];
    generate_data(input_data, TUPLES_PER_EXPERIMENT);

    // Initialize shared buffers
    SharedBuffers buffers;
    init_buffers(buffers, b);

    auto start = std::chrono::high_resolution_clock::now();

    // Create and run threads
    std::vector<std::thread> threads;
    const size_t chunk_size = TUPLES_PER_EXPERIMENT / num_threads;

    for (uint32_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&, t]
                             {
            Tuple* chunk_start = input_data + t * chunk_size;
            size_t remaining = (t == num_threads - 1)
                ? TUPLES_PER_EXPERIMENT - t * chunk_size
                : chunk_size;

            for (size_t i = 0; i < remaining; ++i) {
                uint32_t p = partition_hash(chunk_start[i].key, b);
                PartitionBuffer& buf = buffers.partitions[p];
                uint32_t idx = buf.write_idx.fetch_add(1, std::memory_order_relaxed);

                // Buffer overflow check
                if (idx >= buf.capacity) {
                    std::cerr << "FATAL: Partition " << p << " overflow! "
                              << "Capacity: " << buf.capacity << "\n";
                    std::abort();
                }

                buf.data[idx] = chunk_start[i];
            } });
    }

    // Wait for completion
    for (auto &t : threads)
        t.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Calculate throughput (million tuples/sec)
    double seconds = duration.count() / 1000.0;
    double throughput = TUPLES_PER_EXPERIMENT / (seconds * 1e6);

    std::cout << "Threads: " << num_threads
              << ", Hash Bits: " << b
              << ", Throughput: " << throughput << " MTuple/s\n";

    // Cleanup
    delete[] input_data;
    for (uint32_t p = 0; p < buffers.num_partitions; ++p)
    {
        delete[] buffers.partitions[p].data;
    }
    delete[] buffers.partitions;
}

int main()
{
    // Test parameters matching paper's Figure 5
    std::vector<uint32_t> thread_counts = {1, 2, 4, 8, 16, 32};
    std::vector<uint32_t> hash_bits = {4, 6, 8, 10, 12, 14, 16, 18};

    for (auto threads : thread_counts)
    {
        for (auto b : hash_bits)
        {
            // Run 3 trials and average for stability
            constexpr int TRIALS = 3;
            double total_throughput = 0;

            for (int t = 0; t < TRIALS; ++t)
            {
                run_experiment(threads, b);
            }
        }
    }

    return 0;
}