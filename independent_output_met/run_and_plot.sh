#!/bin/bash

EXECUTABLE="./independent_output"

# Output CSV file
OUTPUT_FILE="experiment_results.csv"
PERF_FOLDER="perf_reports"

# Number of repetitions per configuration
REPEAT=10

mkdir -p $PERF_FOLDER

# Remove previous results
rm -f $OUTPUT_FILE

# Create CSV header
echo "Threads,HashBits,Throughput" > $OUTPUT_FILE

echo "Running experiments..."

# Loop over thread counts
for threads in 1 2 4 8 16 32; do
    # Loop over hash bits
    echo "thread $threads"
    for hash_bits in {1..18}; do
        echo "$hash_bits hash bits"
        total_throughput=0
        for ((i=0; i<$REPEAT; i++)); do
            # Run with perf stat and capture output
            PERF_STAT_FILE="$PERF_FOLDER/perf_stat_${threads}_${hash_bits}.txt"

            # Run experiment and extract throughput value
            ex=$(perf stat -e cycles,instructions,cache-references,cache-misses,context-switches,branch-misses,dTLB-loads,dTLB-load-misses,page-faults \
                 -o "$PERF_STAT_FILE" $EXECUTABLE $threads $hash_bits)

            result=$(echo "$ex" | grep "Throughput" | awk '{print $2}')
            total_throughput=$(echo "$total_throughput + $result" | bc)
        done
        # Compute average throughput
        avg_throughput=$(echo "scale=2; $total_throughput / $REPEAT" | bc)
        echo "$threads,$hash_bits,$avg_throughput" >> $OUTPUT_FILE
    done
done

echo "Experiments completed. Results saved to $OUTPUT_FILE"
echo "Perf stats and records saved in $PERF_FOLDER"

# Call the Python script to generate the plot
echo "Generating plots..."
python3 plot_results.py
python3 plot_cycles.py
python3 plot_cache_misses.py
python3 plot_tlb_misses.py
python3 plot_page_faults.py
python3 plot_context_switches.py
echo "Plots saved in plots/ folder"
