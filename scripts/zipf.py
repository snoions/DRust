#!/usr/bin/python3
import numpy as np

def fast_zipfian(s, N, size):
    ranks = np.arange(1, N + 1)
    weights = 1.0 / ranks**s
    weights /= weights.sum()
    shuffled = np.arange(0, N)
    np.random.shuffle(shuffled)
    return shuffled[np.random.choice(ranks, size=size, p=weights) - 1]

total_kvs = 128*1024*1024

samples = fast_zipfian(0.99, total_kvs, 200000000)

output_file = f"./zipf.csv"
with open(output_file, "wb") as f:
    np.savetxt(output_file, samples, delimiter=',', fmt='%d')

