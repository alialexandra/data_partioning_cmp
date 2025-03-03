import re
import pandas as pd
import matplotlib.pyplot as plt
import argparse

# Define the filename containing your results.
#filename = "results.csv"
# Set up argument parser
parser = argparse.ArgumentParser(description='Plot graphs from a CSV file.')
parser.add_argument('filename', type=str, help='The filename containing your results.')
args = parser.parse_args()
# Create an empty list to store parsed data.
data = []

# Define a regex pattern to extract the values.
pattern = r"Threads:\s*(\d+),\s*Hash Bits:\s*(\d+),\s*Throughput:\s*([\d\.]+)"

# Open and parse the file.
with open(args.filename, "r") as filename:
    for line in filename:
        match = re.search(pattern, line)
        if match:
            threads = int(match.group(1))
            hash_bits = int(match.group(2))
            throughput = float(match.group(3))
            data.append({"threads": threads, "hash_bits": hash_bits, "throughput": throughput})

# Create a DataFrame from the parsed data.
df = pd.DataFrame(data)

# Group the data by 'threads' and 'hash_bits' and compute the average throughput.
df_grouped = df.groupby(["threads", "hash_bits"])["throughput"].mean().reset_index()

# Pivot the DataFrame so that 'hash_bits' are the index and each column represents a different thread count.
pivot_df = df_grouped.pivot(index='hash_bits', columns='threads', values='throughput')

# Plot the results.
plt.figure(figsize=(10, 6))
for threads in pivot_df.columns:
    plt.plot(pivot_df.index, pivot_df[threads], marker='o', label=f'Threads: {threads}')

plt.xlabel("Hash Bits")
plt.ylabel("Throughput (MTuple/s)")
plt.title("Concurrent Scaling with Data Partitioning")
plt.grid(True)
plt.legend(title="Threads")
plt.tight_layout()

# Save the figure as a PNG file and display it.
plt.savefig("graph.png")
#plt.show()
