#include <atomic>
#include <thread>
#include <vector>
#include <cstring>

// Cache line alignment to prevent false sharing
constexpr size_t CACHE_LINE_SIZE = 64;

// Tuple structure: 8B key + 8B payload
struct Tuple
{
    uint64_t key;
    uint64_t payload;
};

// Shared partition buffer with aligned atomic counter
struct alignas(CACHE_LINE_SIZE) PartitionBuffer
{
    std::atomic<uint32_t> write_idx; // Atomic counter
    uint32_t capacity;               // Max tuples in buffer
    Tuple *data;                     // Shared buffer
};

// Shared buffers across all threads
struct SharedBuffers
{
    PartitionBuffer *partitions; // Array of 2^b buffers
    uint32_t num_partitions;
};

// Hash function (using lower b bits)
uint32_t partition_hash(uint64_t key, uint32_t b)
{
    return key & ((1 << b) - 1);
}

// Initialize shared buffers
void init_buffers(SharedBuffers &sb, uint32_t b, uint32_t expected_tuples)
{
    sb.num_partitions = 1 << b;
    sb.partitions = new PartitionBuffer[sb.num_partitions];

    for (uint32_t p = 0; p < sb.num_partitions; ++p)
    {
        // Overallocate by 50%
        uint32_t capacity = expected_tuples * 1.5;
        sb.partitions[p].capacity = capacity;
        sb.partitions[p].write_idx.store(0, std::memory_order_relaxed);
        sb.partitions[p].data = new Tuple[capacity];

        // Pre-touch memory pages
        for (uint32_t i = 0; i < capacity; i += 4096 / sizeof(Tuple))
        {
            sb.partitions[p].data[i].key = 0;
        }
    }
}

// Process input chunk with shared buffers
void process_chunk(SharedBuffers &sb, Tuple *input, size_t count, uint32_t b)
{
    for (size_t i = 0; i < count; ++i)
    {
        Tuple &tuple = input[i];
        uint32_t p = partition_hash(tuple.key, b);

        PartitionBuffer &buf = sb.partitions[p];
        // Atomic increment with relaxed ordering (partition-specific)
        uint32_t idx = buf.write_idx.fetch_add(1, std::memory_order_relaxed);

        // Assertion: idx < buf.capacity (ensured by overallocation)
        buf.data[idx] = tuple;
    }
}

int main()
{
    // Configuration
    const uint32_t b = 10; // 1024 partitions
    const uint32_t num_threads = 16;
    const uint64_t total_tuples = 1e8;

    // Calculate expected tuples per partition
    uint32_t expected_per_partition = total_tuples / (1 << b);

    // Initialize shared buffers
    SharedBuffers shared_buffers;
    init_buffers(shared_buffers, b, expected_per_partition);

    // Launch threads
    std::vector<std::thread> workers;
    for (uint32_t t = 0; t < num_threads; ++t)
    {
        workers.emplace_back([&, t]
                             {
            // Process assigned input chunk (pseudo-input)
            Tuple* thread_input = ...;  // Get input pointer for this thread
            size_t tuples_per_thread = total_tuples / num_threads;
            process_chunk(shared_buffers, thread_input, tuples_per_thread, b); });
    }

    // Wait for completion
    for (auto &worker : workers)
        worker.join();

    // Cleanup
    for (uint32_t p = 0; p < shared_buffers.num_partitions; ++p)
    {
        delete[] shared_buffers.partitions[p].data;
    }
    delete[] shared_buffers.partitions;

    return 0;
}