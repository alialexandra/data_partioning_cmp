import csv
import matplotlib.pyplot as plt

# Load experiment results from CSV file
data = {}

with open("experiment_results.csv", "r") as file:
    reader = csv.reader(file)
    next(reader)  # Skip header line
    for row in reader:
        threads = int(row[0])
        hash_bits = int(row[1])
        throughput = float(row[2])

        if threads not in data:
            data[threads] = {"hash_bits": [], "throughput": []}
        
        data[threads]["hash_bits"].append(hash_bits)
        data[threads]["throughput"].append(throughput)

# Create plot
plt.figure(figsize=(8, 6))

# Plot throughput for each thread count
for threads, values in sorted(data.items()):
    plt.plot(values["hash_bits"], values["throughput"], marker="o", linestyle="-", label=f"Threads: {threads}")

# Labels and title
plt.xlabel("Hash Bits")
plt.ylabel("Millions of Tuples per Second")
plt.title("Independent Partitioning Performance")
plt.legend()
plt.grid()

# Save and show the plot
plt.savefig("partitioning_performance.png")
plt.show()
