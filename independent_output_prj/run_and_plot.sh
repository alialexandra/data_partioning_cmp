#!/bin/bash

# Set the executable name
EXECUTABLE="./independent_output"

# Output CSV file
OUTPUT_FILE="experiment_results.csv"

# Number of repetitions per configuration
REPEAT=1

# Remove previous results
rm -f $OUTPUT_FILE

# Create CSV header
echo "Threads,HashBits,Throughput" > $OUTPUT_FILE

echo "Running experiments..."

# Loop over thread counts
for threads in 16 32; do
    # Loop over hash bits (b values)
    echo "thread $threads"
    for hash_bits in {16..18}; do
        echo "$hash_bits hash bits"
        total_throughput=0
        for ((i=0; i<$REPEAT; i++)); do
            # Run experiment and extract throughput value
            ex=$($EXECUTABLE $threads $hash_bits)
            echo $ex
            result=$($EXECUTABLE $threads $hash_bits | grep "Throughput" | awk '{print $2}')
            # echo $result
            total_throughput=$(echo "$total_throughput + $result" | bc)
        done
        # Compute average throughput
        avg_throughput=$(echo "scale=2; $total_throughput / $REPEAT" | bc)
        echo "$threads,$hash_bits,$avg_throughput" >> $OUTPUT_FILE
    done
done

echo "Experiments completed. Results saved to $OUTPUT_FILE"

# Call the Python script to generate the plot
echo "Generating plot..."
python3 plot_results.py
echo "Plot saved as partitioning_performance.png"
