
import matplotlib.pyplot as plt
import re
import os

perf_folder = "perf_reports"
metric = "context-switches"

# Initialize data storage
data = {}

# Extract data from perf stat files
for filename in os.listdir(perf_folder):
    if filename.startswith("perf_stat_") and filename.endswith(".txt"):
        parts = filename.strip(".txt").split("_")
        threads = int(parts[2])
        hash_bits = int(parts[3])
        with open(os.path.join(perf_folder, filename), "r") as f:
            for line in f:
                if metric in line:
                    value = int(line.strip().split()[0].replace(",", ""))
                    if threads not in data:
                        data[threads] = {"hash_bits": [], "values": []}
                    data[threads]["hash_bits"].append(hash_bits)
                    data[threads]["values"].append(value)

# Plotting
plt.figure(figsize=(8, 6))
for threads, values in sorted(data.items()):
    sorted_pairs = sorted(zip(values["hash_bits"], values["values"]))
    bits, vals = zip(*sorted_pairs)
    plt.plot(bits, vals, marker="o", linestyle="-", label=f"Threads: {threads}")

plt.xlabel("Hash Bits")
plt.ylabel("Context Switches")
plt.title("Context Switches vs Hash Bits")
plt.legend()
plt.grid()
plt.xticks(range(0, 19, 2))
plt.savefig("plots/context_switches_plot.png")
