#include <atomic>
#include <thread>
#include <vector>
#include <cstring>

// Cache line size (typically 64 bytes)
constexpr size_t CACHE_LINE_SIZE = 64;

// Tuple structure: 8B key + 8B payload
struct Tuple
{
    uint64_t key;
    uint64_t payload;
};

// Per-thread, per-partition buffer with metadata alignment
struct alignas(CACHE_LINE_SIZE) PartitionBuffer
{
    uint32_t write_idx; // 32-bit counter
    uint32_t capacity;  // Max tuples in buffer
    Tuple *data;        // Allocated buffer
};

// Thread-local storage for all partitions
struct ThreadBuffers
{
    PartitionBuffer *partitions; // Array of 2^b buffers
    uint32_t num_partitions;
};

// Hash function (using lower b bits)
uint32_t partition_hash(uint64_t key, uint32_t b)
{
    return key & ((1 << b) - 1);
}

// Initialize thread-local buffers
void init_buffers(ThreadBuffers &tb, uint32_t b, uint32_t expected_tuples)
{
    tb.num_partitions = 1 << b;
    tb.partitions = new PartitionBuffer[tb.num_partitions];

    for (uint32_t p = 0; p < tb.num_partitions; ++p)
    {
        // Overallocate by 50%
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

// Process input chunk with thread-local buffers
void process_chunk(ThreadBuffers &tb, Tuple *input, size_t count, uint32_t b)
{
    for (size_t i = 0; i < count; ++i)
    {
        Tuple &tuple = input[i];
        uint32_t p = partition_hash(tuple.key, b);

        PartitionBuffer &buf = tb.partitions[p];
        // Assertion: buf.write_idx < buf.capacity (guaranteed by overallocation)
        buf.data[buf.write_idx++] = tuple;
    }
}

int main()
{
    // Configuration
    const uint32_t b = 10; // 1024 partitions
    const uint32_t num_threads = 16;
    const uint64_t total_tuples = 1e8;

    // Calculate expected tuples per partition per thread
    uint32_t expected_per_partition = total_tuples / (num_threads * (1 << b));

    // Initialize thread resources
    std::vector<std::thread> workers;
    std::vector<ThreadBuffers> all_buffers(num_threads);

    // Launch threads
    for (uint32_t t = 0; t < num_threads; ++t)
    {
        workers.emplace_back([&, t]
                             {
            // Initialize thread-local buffers
            init_buffers(all_buffers[t], b, expected_per_partition);
            
            // Process assigned input chunk (pseudo-input)
            Tuple* thread_input = ...;  // Get input pointer for this thread
            size_t tuples_per_thread = total_tuples / num_threads;
            process_chunk(all_buffers[t], thread_input, tuples_per_thread, b); });
    }

    // Wait for completion
    for (auto &worker : workers)
        worker.join();

    // Downstream processing would need to gather partitions from:
    // all_buffers[thread].partitions[partition]

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