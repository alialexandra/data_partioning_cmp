4. Running Experiments
Vary Threads: Test with 1, 2, 4, 8, 16 threads.

Vary Partitions: Use b = 4 (16 partitions) to b = 16 (65K partitions).

Measure:

bash
Copy
perf stat -e cache-misses,dTLB-load-misses ./your_program
5. Key Analysis Points
Contention: Compare throughput of Concurrent Output at low vs. high partitions.

Fragmentation: Independent Output requires merging partitions post-processing.

Cache Behavior: Use perf to correlate cache misses with performance drops.

6. Core Affinity
Set thread affinity to reduce cache contention:

cpp
Copy
#include <sched.h>
void set_affinity(std::thread& t, int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
}
7. Expected Results
Concurrent Output: Faster at low partitions (contiguous writes), slower at high partitions (contention).

Independent Output: Scales better with threads but requires post-merging.

Next Steps
Implement data generation.

Integrate timing/measurement code.

Run experiments with varying parameters.

Use perf to collect hardware metrics.

Compare results with the paper’s findings.

Let me know if you need clarification on any part!




for running with cpu affinity 

group999@dionysos:~/workspace/DataPartitionRepo/data_partioning_cmp$ numactl --hardware
available: 2 nodes (0-1)
node 0 cpus: 0 2 4 6 8 10 12 14 16 18 20 22 24 26 28 30
node 0 size: 64131 MB
node 0 free: 52638 MB
node 1 cpus: 1 3 5 7 9 11 13 15 17 19 21 23 25 27 29 31
node 1 size: 64462 MB
node 1 free: 62140 MB
node distances:
node   0   1 
  0:  10  21 
  1:  21  10 

  
