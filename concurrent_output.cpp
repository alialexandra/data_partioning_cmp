#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <random>
#include <cstdint>

// Constants from paper
constexpr size_t TUPLES_PER_EXPERIMENT = 1 << 24; // 16M tuples
constexpr size_t TUPLE_SIZE = 16;                 // 16 bytes (8B key + 8B payload)
constexpr size_t PAGE_SIZE = 256 * 1024 * 1024;   // 256MB
constexpr int NUM_REPEATS = 8;

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

// Hash function: simple bitmask (multiplicative hashing not required)
inline uint32_t partition_hash(uint64_t key, uint32_t b)
{
    return key & ((1u << b) - 1);
}

// Generate 16M tuples with random, unique keys
void generate_input(Tuple *data, size_t count)
{
    std::mt19937_64 rng(42); // fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> dist;

    for (size_t i = 0; i < count; ++i)
    {
        data[i].key = dist(rng);
        data[i].payload = 0;
    }
}

// Allocate output buffers, one per partition (shared among threads)
bool init_buffers(SharedBuffers &buffers, uint32_t b)
{
    buffers.num_partitions = 1u << b;
    if (buffers.num_partitions > (1 << 18))
    {
        std::cerr << "Too many partitions (" << buffers.num_partitions << "). Aborting.\n";
        return false;
    }
    buffers.partitions = new PartitionBuffer[buffers.num_partitions];

    uint32_t expected_per_partition = TUPLES_PER_EXPERIMENT / buffers.num_partitions;
    uint32_t capacity = static_cast<uint32_t>(expected_per_partition * 2);

    for (uint32_t i = 0; i < buffers.num_partitions; ++i)
    {
        buffers.partitions[i].write_idx.store(0);
        buffers.partitions[i].capacity = capacity;

        try
        {
            buffers.partitions[i].data = new Tuple[capacity];
        }
        catch (const std::bad_alloc &e)
        {
            std::cerr << "Memory allocation failed for partition " << i << ": " << e.what() << "\n";
            return false;
        }

        // Pre-touch pages to avoid page faults (simulate DB buffer reuse)
        for (uint32_t j = 0; j < capacity; j += PAGE_SIZE / sizeof(Tuple))
        {
            buffers.partitions[i].data[j].key = 0;
        }
    }

    return true;
}

double run_concurrent_partition(uint32_t threads, uint32_t b)
{
    Tuple *input = new Tuple[TUPLES_PER_EXPERIMENT];
    generate_input(input, TUPLES_PER_EXPERIMENT);

    SharedBuffers buffers;
    if (!init_buffers(buffers, b))
    {
        delete[] input;
        return -1.0;
    }

    size_t chunk_size = TUPLES_PER_EXPERIMENT / threads;

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> workers;
    for (uint32_t t = 0; t < threads; ++t)
    {
        workers.emplace_back([&, t]()
                             {
            size_t offset = t * chunk_size;
            size_t count = (t == threads - 1) ? TUPLES_PER_EXPERIMENT - offset : chunk_size;
            Tuple* local = input + offset;

            for (size_t i = 0; i < count; ++i) {
                uint32_t p = partition_hash(local[i].key, b);
                uint32_t idx = buffers.partitions[p].write_idx.fetch_add(1, std::memory_order_relaxed);
                if (idx >= buffers.partitions[p].capacity) {
                    std::cerr << "Buffer overflow at partition " << p << ", idx = " << idx << "\n";
                    std::abort();
                }
                buffers.partitions[p].data[idx] = local[i];
            } });
    }

    for (auto &t : workers)
        t.join();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;

    delete[] input;
    for (uint32_t i = 0; i < buffers.num_partitions; ++i)
    {
        delete[] buffers.partitions[i].data;
    }
    delete[] buffers.partitions;

    return TUPLES_PER_EXPERIMENT / (duration.count() * 1e6); // MTuple/sec
}

int main()
{
    std::vector<uint32_t> thread_counts = {1, 2, 4, 8, 16, 32};
    std::vector<uint32_t> hash_bits = {4, 6, 8, 10, 12, 14, 16, 18};

    for (auto threads : thread_counts)
    {
        for (auto b : hash_bits)
        {
            std::vector<double> results;
            for (int i = 0; i < NUM_REPEATS; ++i)
            {
                double throughput = run_concurrent_partition(threads, b);
                if (throughput < 0.0)
                    break;
                results.push_back(throughput);
            }

            if (results.size() == NUM_REPEATS)
            {
                double avg = std::accumulate(results.begin(), results.end(), 0.0) / results.size();
                std::cout << "Threads: " << threads
                          << ", Hash Bits: " << b
                          << ", Throughput: " << avg << " MTuple/s\n";
            }
        }
    }

    return 0;
}
