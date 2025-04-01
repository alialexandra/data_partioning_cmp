#!/bin/bash

# Run the executable and overwrite the results in concurrent_results.csv
rm -rf concurrent_output concurrent_output_affinity
make
echo "Running no affinity example"
mkdir -p ./no_affinity_results

./concurrent_output > ./no_affinity_results/concurrent_results.csv
echo "Done with the affinity example"


set -e

mkdir -p ./affinity_results


PROGRAM=./concurrent_output_affinity

# CPU groupings based on NUMA layout (from numactl output)
NODE0_ADJACENT=(0 2 4 6 8 10 12 14 16 18 20 22 24 26 28 30)
NODE1_ADJACENT=(1 3 5 7 9 11 13 15 17 19 21 23 25 27 29 31)

# Generate CPU sets
get_adjacent_cores() {
  local -n array=$1
  local count=$2
  local result=("${array[@]:0:$count}")
  echo "${result[@]}"
}

get_alternating_cores() {
  local -n array=$1
  local count=$2
  local result=()
  for ((i=0; i<$count; i++)); do
    result+=("${array[$((i*2))]}")
  done
  echo "${result[@]}"
}

get_cross_numa_cores() {
  local -n node0=$1
  local -n node1=$2
  local count=$3
  local result=()
  for ((i=0; i<$count; i++)); do
    if ((i % 2 == 0)); then
      result+=("${node0[$((i/2))]}")
    else
      result+=("${node1[$((i/2))]}")
    fi
  done
  echo "${result[@]}"
}

# Run the program with a label and core list
run_affinity_case() {
  local label="$1"
  shift
  local cores=("$@")
  local core_args="${#cores[@]} ${cores[@]}"
  echo "Running case: $label on cores: ${cores[*]}"
  echo "Running args: $PROGRAM $core_args"

  $PROGRAM $core_args > ./affinity_results/concurrent_${label}.csv
}

# Thread counts to test
THREAD_COUNTS=(1 2 4 8 16 32)

echo "Detected cores (node0): ${NODE0_ADJACENT[*]}"

for threads in "${THREAD_COUNTS[@]}"; do
  echo "\n=== Start running thread count: $threads ==="

  # Same NUMA node, adjacent cores
  run_affinity_case "node0_adjacent_${threads}" $(get_adjacent_cores NODE0_ADJACENT $threads)
  run_affinity_case "node1_adjacent_${threads}" $(get_adjacent_cores NODE1_ADJACENT $threads)

  # Alternating cores on Node 0 and Node 1 (if possible)
  if (( threads * 2 <= ${#NODE0_ADJACENT[@]} )); then
    run_affinity_case "node0_alternating_${threads}" $(get_alternating_cores NODE0_ADJACENT $threads)
  fi
  if (( threads * 2 <= ${#NODE1_ADJACENT[@]} )); then
    run_affinity_case "node1_alternating_${threads}" $(get_alternating_cores NODE1_ADJACENT $threads)
  fi

  # Cross NUMA
  run_affinity_case "crossnuma_${threads}" $(get_cross_numa_cores NODE0_ADJACENT NODE1_ADJACENT $threads)

  # Oversubscription cases on Node 0 and Node 1
  if (( threads >= 4 )); then
    run_affinity_case "oversub_node0_${threads}on2" $(get_adjacent_cores NODE0_ADJACENT 2)
    run_affinity_case "oversub_node1_${threads}on2" $(get_adjacent_cores NODE1_ADJACENT 2)
  fi
  if (( threads >= 8 )); then
    run_affinity_case "oversub_node0_${threads}on4" $(get_adjacent_cores NODE0_ADJACENT 4)
    run_affinity_case "oversub_node1_${threads}on4" $(get_adjacent_cores NODE1_ADJACENT 4)
  fi
  if (( threads >= 16 )); then
    run_affinity_case "oversub_node0_${threads}on8" $(get_adjacent_cores NODE0_ADJACENT 8)
    run_affinity_case "oversub_node1_${threads}on8" $(get_adjacent_cores NODE1_ADJACENT 8)
  fi
  if (( threads >= 32 )); then
    run_affinity_case "oversub_node0_${threads}on16" $(get_adjacent_cores NODE0_ADJACENT 16)
    run_affinity_case "oversub_node1_${threads}on16" $(get_adjacent_cores NODE1_ADJACENT 16)
  fi

done
