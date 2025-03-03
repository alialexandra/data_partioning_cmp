#!/bin/bash

# Run the executable and overwrite the results in concurrent_results.csv
make
./concurrent_output > concurrent_results.csv